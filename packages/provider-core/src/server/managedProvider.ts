import {
  DEFAULT_PROVIDER_HEALTH_REFRESH_INTERVAL,
  type ServerProvider,
  ServerSettingsError,
} from "@t3tools/contracts";
import { resolveServerBackgroundActivitySettings } from "@t3tools/shared/backgroundActivitySettings";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Fiber from "effect/Fiber";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Random from "effect/Random";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Semaphore from "effect/Semaphore";

import * as ProviderHost from "./ProviderHost.ts";
import {
  applyUsageLimitsUpdate,
  makeUsageLimits,
  resolveUsageLimitsAfterProbe,
} from "./usageLimits.ts";
import type { ManagedServerProvider } from "./snapshot.ts";

interface ProviderSnapshotState {
  readonly snapshot: ServerProvider;
  readonly enrichmentGeneration: number;
  // null preserves an explicitly cleared plan across probes and enrichment.
  readonly usageAuthLabel: string | null | undefined;
  readonly checked: boolean;
}

function withUsageLimits(
  snapshot: ServerProvider,
  usageLimits: ServerProvider["usageLimits"],
): ServerProvider {
  if (snapshot.usageLimits === usageLimits) {
    return snapshot;
  }
  const { usageLimits: _previous, ...rest } = snapshot;
  return usageLimits ? { ...rest, usageLimits } : rest;
}

const FAILED_CHECK_RECHECK_DELAY_MS = 30_000;
const MIN_SCHEDULED_REFRESH_INTERVAL_MS = 60_000;

/**
 * True inside a `checkProvider` whose failure over a ready provider is discarded for a recheck.
 * A provider that also records check results in its own state must leave that failure out of it.
 */
export const DeferringFailedCheck = Context.Reference<boolean>("t3/provider/DeferringFailedCheck", {
  defaultValue: () => false,
});

export const ClearProviderUsage = Context.Reference<Effect.Effect<void>>(
  "t3/provider/ClearProviderUsage",
  { defaultValue: () => Effect.void },
);

/** True inside a `checkProvider` started by the unattended schedule rather than a client. */
export const UnattendedProviderCheck = Context.Reference<boolean>(
  "t3/provider/UnattendedProviderCheck",
  { defaultValue: () => false },
);

export const makeManagedServerProvider = Effect.fn("makeManagedServerProvider")(function* <
  Settings,
>(input: {
  readonly resolveMaintenance: ManagedServerProvider["resolveMaintenance"];
  readonly getSettings: Effect.Effect<Settings, ServerSettingsError>;
  readonly streamSettings: Stream.Stream<Settings>;
  readonly haveSettingsChanged: (previous: Settings, next: Settings) => boolean;
  readonly initialSnapshot: (settings: Settings) => Effect.Effect<ServerProvider>;
  /** Undefined skips publication when a probe is still shutting down. */
  readonly checkProvider: Effect.Effect<ServerProvider | undefined, ServerSettingsError>;
  readonly checkUsageLimits?: Effect.Effect<ServerProvider["usageLimits"]>;
  readonly invalidateUsageCache?: Effect.Effect<void>;
  readonly enrichSnapshot?: (input: {
    readonly settings: Settings;
    readonly snapshot: ServerProvider;
    readonly getSnapshot: Effect.Effect<ServerProvider>;
    readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  }) => Effect.Effect<void>;
  readonly refreshInterval?: Duration.Input;
  readonly refreshOnInterval?: boolean | ((snapshot: ServerProvider) => boolean);
  /** Waits for a check this instance's previous driver still runs; the first check retries after it. */
  readonly waitForPreviousCheck?: Effect.Effect<void>;
  /**
   * Checks on this plan without a client asking for provider status and while the screen is
   * locked, for snapshots read outside T3 through the status cache. The other host pauses apply.
   * Client polling and confirmation of a failed periodic check wait at least 60 seconds.
   */
  readonly unattendedRefreshInterval?: (
    snapshot: ServerProvider,
    configuredMs: number,
  ) => Effect.Effect<{ readonly intervalMs: number; readonly jitter: boolean } | undefined>;
  /** Publish usage as reported by the probe, without merging previous readings or turn updates. */
  readonly probeOnlyUsage?: boolean;
  /** Check that the probe still owns its usage windows at publication. */
  readonly isProbedUsageCurrent?: () => boolean;
  readonly checkProviderOnSettingsChange?: (previous: Settings, next: Settings) => boolean;
}): Effect.fn.Return<
  ManagedServerProvider & { readonly clearUsage: Effect.Effect<void> },
  ServerSettingsError,
  Scope.Scope | ProviderHost.ProviderHost
> {
  const host = yield* ProviderHost.ProviderHost;
  const refreshSemaphore = yield* Semaphore.make(1);
  const usageRefreshQueued = yield* Ref.make<symbol | null>(null);
  const changesPubSub = yield* Effect.acquireRelease(
    PubSub.unbounded<ServerProvider>(),
    PubSub.shutdown,
  );
  const initialSettings = yield* input.getSettings;
  const initialSnapshot = yield* input.initialSnapshot(initialSettings);
  const unattendedWake = input.unattendedRefreshInterval
    ? yield* Queue.sliding<void>(1)
    : undefined;
  const failedCheckRecheckDelayMs = unattendedWake
    ? MIN_SCHEDULED_REFRESH_INTERVAL_MS
    : FAILED_CHECK_RECHECK_DELAY_MS;
  // The unattended deadline counts from the last published check or unattended attempt.
  const unattendedBaseRef = yield* Ref.make(yield* Clock.currentTimeMillis);
  // Checks published after the first one; the client timer skips a cycle that saw one.
  const laterChecksPublishedRef = yield* Ref.make(0);
  const refreshIntervalChanges = yield* Queue.sliding<void>(1);
  const snapshotStateRef = yield* Ref.make<ProviderSnapshotState>({
    snapshot: initialSnapshot,
    enrichmentGeneration: 0,
    usageAuthLabel: undefined,
    checked: false,
  });
  const settingsRef = yield* Ref.make(initialSettings);
  const recheckAtRef = yield* Ref.make<number | undefined>(undefined);
  const enrichmentFiberRef = yield* Ref.make<Fiber.Fiber<void, unknown> | null>(null);
  const scope = yield* Effect.scope;

  const clearUsage = Ref.modify(snapshotStateRef, (state) => {
    const { usageLimits } = state.snapshot;
    if (usageLimits === undefined || usageLimits.unavailable) return [undefined, state] as const;
    const snapshot = withUsageLimits(state.snapshot, undefined);
    return [snapshot, { ...state, snapshot, usageAuthLabel: undefined }] as const;
  }).pipe(
    Effect.flatMap((snapshot) =>
      snapshot ? PubSub.publish(changesPubSub, snapshot).pipe(Effect.asVoid) : Effect.void,
    ),
  );

  const publishEnrichedSnapshot = Effect.fn("publishEnrichedSnapshot")(function* (
    generation: number,
    nextSnapshot: ServerProvider,
  ) {
    const snapshotToPublish = yield* Ref.modify(snapshotStateRef, (state) => {
      if (state.enrichmentGeneration !== generation) {
        return [null, state] as const;
      }
      // Enrichment must not revert usage updates newer than its input snapshot.
      const merged = withUsageLimits(
        state.usageAuthLabel === undefined
          ? nextSnapshot
          : {
              ...nextSnapshot,
              auth: { ...nextSnapshot.auth, label: state.usageAuthLabel ?? undefined },
            },
        state.snapshot.usageLimits,
      );
      if (Equal.equals(state.snapshot, merged)) {
        return [null, state] as const;
      }
      return [merged, { ...state, snapshot: merged }] as const;
    });
    if (snapshotToPublish === null) {
      return;
    }
    yield* PubSub.publish(changesPubSub, snapshotToPublish);
  });

  const restartSnapshotEnrichment = Effect.fn("restartSnapshotEnrichment")(function* (
    settings: Settings,
    snapshot: ServerProvider,
    generation: number,
  ) {
    const previousFiber = yield* Ref.getAndSet(enrichmentFiberRef, null);
    if (previousFiber) {
      yield* Fiber.interrupt(previousFiber).pipe(Effect.ignore);
    }

    if (!input.enrichSnapshot || !snapshot.enabled) {
      return;
    }

    const fiber = yield* input
      .enrichSnapshot({
        settings,
        snapshot,
        getSnapshot: Ref.get(snapshotStateRef).pipe(Effect.map((state) => state.snapshot)),
        publishSnapshot: (nextSnapshot) => publishEnrichedSnapshot(generation, nextSnapshot),
      })
      .pipe(Effect.ignoreCause({ log: true }), Effect.forkIn(scope));

    yield* Ref.set(enrichmentFiberRef, fiber);
  });

  const applySnapshotBase = Effect.fn("applySnapshot")(function* (
    nextSettings: Settings,
    options?: { readonly forceRefresh?: boolean; readonly deferFailure?: boolean },
  ) {
    const previousSettings = yield* Ref.get(settingsRef);
    const settingsChanged =
      options?.forceRefresh === true || input.haveSettingsChanged(previousSettings, nextSettings);
    // Nothing watches credential files, so any settings apply rechecks a failed sign-in.
    const { snapshot: currentSnapshot } = yield* Ref.get(snapshotStateRef);
    const forceRefresh =
      options?.forceRefresh === true ||
      (currentSnapshot.enabled && currentSnapshot.auth.status === "unauthenticated");
    if (!forceRefresh && !settingsChanged) {
      yield* Ref.set(settingsRef, nextSettings);
      return yield* Ref.get(snapshotStateRef).pipe(Effect.map((state) => state.snapshot));
    }

    if (
      !forceRefresh &&
      input.checkProviderOnSettingsChange?.(previousSettings, nextSettings) === false
    ) {
      const state = yield* Ref.get(snapshotStateRef);
      const nextGeneration = state.enrichmentGeneration + 1;
      yield* Ref.set(snapshotStateRef, {
        ...state,
        enrichmentGeneration: nextGeneration,
      });
      yield* Ref.set(settingsRef, nextSettings);
      yield* restartSnapshotEnrichment(nextSettings, state.snapshot, nextGeneration);
      return state.snapshot;
    }

    // A probe without usage must not erase a report published while its I/O was in flight.
    const usageBeforeProbe = currentSnapshot.usageLimits;
    const deferFailure =
      options?.deferFailure === true &&
      currentSnapshot.enabled &&
      currentSnapshot.status === "ready";
    const probedSnapshot = yield* input.checkProvider.pipe(
      Effect.provideService(DeferringFailedCheck, deferFailure),
      Effect.provideService(ClearProviderUsage, clearUsage),
    );
    if (probedSnapshot === undefined) return (yield* Ref.get(snapshotStateRef)).snapshot;
    // One failed periodic check over a ready provider is often transient; recheck before showing it.
    if (
      deferFailure &&
      probedSnapshot.status === "error" &&
      probedSnapshot.auth.status !== "unauthenticated"
    ) {
      const now = yield* Clock.currentTimeMillis;
      yield* Ref.set(recheckAtRef, now + failedCheckRecheckDelayMs);
      if (unattendedWake) {
        yield* Ref.set(unattendedBaseRef, now);
        yield* Queue.offer(unattendedWake, undefined);
        if (yield* UnattendedProviderCheck) yield* Queue.offer(refreshIntervalChanges, undefined);
      }
      return (yield* Ref.get(snapshotStateRef)).snapshot;
    }
    const {
      snapshot: nextSnapshot,
      generation: nextGeneration,
      wasChecked,
    } = yield* Ref.modify(snapshotStateRef, (state) => {
      const generation = input.enrichSnapshot
        ? state.enrichmentGeneration + 1
        : state.enrichmentGeneration;
      // Failed reads can only retain limits for the same account, workspace and home.
      const previous = state.snapshot.auth;
      const differs = (a: string | undefined, b: string | undefined) =>
        a !== undefined && b !== undefined && a !== b;
      const switchedAccount =
        differs(probedSnapshot.auth.email?.toLowerCase(), previous.email?.toLowerCase()) ||
        (probedSnapshot.auth.workspaceId !== undefined &&
          probedSnapshot.auth.workspaceId !== previous.workspaceId);
      const switchedHome = probedSnapshot.homeFingerprint !== state.snapshot.homeFingerprint;
      const replacesUsage = switchedAccount || switchedHome || input.probeOnlyUsage === true;
      const reportedUsage = probedSnapshot.usageLimits;
      const probedUsage =
        reportedUsage && !reportedUsage.unavailable && input.isProbedUsageCurrent?.() === false
          ? undefined
          : reportedUsage;
      const usageLimits = replacesUsage
        ? probedUsage
        : probedUsage === undefined && state.snapshot.usageLimits !== usageBeforeProbe
          ? state.snapshot.usageLimits
          : resolveUsageLimitsAfterProbe({
              published: state.snapshot.usageLimits,
              probed: probedUsage,
            });
      const workspaceId = usageLimits === probedUsage ? undefined : state.snapshot.auth.workspaceId;
      const usageAuthLabel =
        !replacesUsage && usageLimits?.checkedAt === state.snapshot.usageLimits?.checkedAt
          ? state.usageAuthLabel
          : undefined;
      const labeledProbe =
        usageAuthLabel === undefined
          ? probedSnapshot
          : {
              ...probedSnapshot,
              auth: { ...probedSnapshot.auth, label: usageAuthLabel ?? undefined },
            };
      const snapshot = withUsageLimits(
        workspaceId && !probedSnapshot.auth.workspaceId
          ? { ...labeledProbe, auth: { ...labeledProbe.auth, workspaceId } }
          : labeledProbe,
        usageLimits,
      );
      return [
        { snapshot, generation, wasChecked: state.checked },
        { snapshot, enrichmentGeneration: generation, usageAuthLabel, checked: true },
      ] as const;
    });
    yield* Ref.set(settingsRef, nextSettings);
    yield* Ref.set(recheckAtRef, undefined);
    yield* PubSub.publish(changesPubSub, nextSnapshot);
    if (wasChecked) yield* Ref.update(laterChecksPublishedRef, (count) => count + 1);
    if (unattendedWake) {
      yield* Ref.set(unattendedBaseRef, yield* Clock.currentTimeMillis);
      yield* Queue.offer(unattendedWake, undefined);
    }
    yield* restartSnapshotEnrichment(nextSettings, nextSnapshot, nextGeneration);
    return nextSnapshot;
  });
  const applySnapshot = (nextSettings: Settings, options?: { readonly forceRefresh?: boolean }) =>
    refreshSemaphore.withPermits(1)(applySnapshotBase(nextSettings, options));

  // Providers call this on every token-usage tick, usually without changing the snapshot.
  const applyUsageLimits: ManagedServerProvider["applyUsageLimits"] = Effect.fnUntraced(
    function* (update) {
      if (input.probeOnlyUsage) return;
      const snapshotToPublish = yield* Ref.modify(snapshotStateRef, (state) => {
        const complete = update.complete && update.windows.length > 0;
        const usageLimits = complete
          ? makeUsageLimits(update)
          : applyUsageLimitsUpdate({
              previous: state.snapshot.usageLimits,
              update,
              checkedAt: update.checkedAt,
            });
        const { auth } = state.snapshot;
        const label = update.windows.length > 0 ? update.authLabel : undefined;
        const relabel =
          (label !== undefined || (complete && state.usageAuthLabel !== undefined)) &&
          label !== auth.label;
        if (usageLimits === state.snapshot.usageLimits && !relabel) {
          return [null, state] as const;
        }
        const snapshot = withUsageLimits(
          relabel ? { ...state.snapshot, auth: { ...auth, label } } : state.snapshot,
          usageLimits,
        );
        return [
          snapshot,
          {
            ...state,
            snapshot,
            usageAuthLabel: label ?? (complete ? null : state.usageAuthLabel),
          },
        ] as const;
      });
      if (snapshotToPublish !== null) {
        yield* PubSub.publish(changesPubSub, snapshotToPublish);
      }
    },
  );

  const refreshSnapshot = Effect.fn("refreshSnapshot")(function* () {
    const nextSettings = yield* input.getSettings;
    return yield* applySnapshot(nextSettings, { forceRefresh: true });
  });

  const refreshUsage = Effect.fn("ManagedServerProvider.refreshUsage")(function* () {
    const { snapshot } = yield* Ref.get(snapshotStateRef);
    if (
      !snapshot.enabled ||
      !snapshot.usageLimits ||
      snapshot.usageLimits.unavailable?.reason === "unsupported"
    )
      return;
    // An in-flight refresh may have read cached usage, so one refresh waits behind it.
    const claim = Symbol();
    const release = Ref.update(usageRefreshQueued, (queued) => (queued === claim ? null : queued));
    yield* Effect.uninterruptibleMask((restore) =>
      Ref.modify(usageRefreshQueued, (queued) => [queued !== null, queued ?? claim]).pipe(
        Effect.flatMap((held) =>
          held
            ? Effect.void
            : restore(
                refreshSemaphore.withPermits(1)(
                  release.pipe(
                    Effect.andThen(input.invalidateUsageCache ?? Effect.void),
                    Effect.andThen(input.getSettings),
                    Effect.flatMap((settings) =>
                      applySnapshotBase(settings, { forceRefresh: true }),
                    ),
                  ),
                ),
              ).pipe(Effect.onInterrupt(() => release)),
        ),
      ),
    );
  });

  const needsUsageRecovery = (snapshot: ServerProvider) =>
    input.checkUsageLimits !== undefined &&
    snapshot.installed &&
    snapshot.status === "ready" &&
    snapshot.auth.status === "authenticated" &&
    snapshot.usageLimits?.unavailable?.reason === "probeFailed";

  const needsProviderRefresh = (snapshot: ServerProvider) =>
    input.refreshOnInterval !== false &&
    (typeof input.refreshOnInterval !== "function" || input.refreshOnInterval(snapshot));

  const refreshOnInterval = Effect.fn("ManagedServerProvider.refreshOnInterval")(function* (
    deferFailure: boolean,
  ) {
    const { snapshot } = yield* Ref.get(snapshotStateRef);
    if (!snapshot.enabled) return;
    if (!needsUsageRecovery(snapshot) || !input.checkUsageLimits) {
      if (needsProviderRefresh(snapshot)) {
        yield* applySnapshotBase(yield* input.getSettings, { forceRefresh: true, deferFailure });
      }
      return;
    }
    const probed = yield* input.checkUsageLimits;
    const next = yield* Ref.modify(snapshotStateRef, (state) => {
      const usageLimits = resolveUsageLimitsAfterProbe({
        published: state.snapshot.usageLimits,
        probed,
      });
      const snapshot = withUsageLimits(state.snapshot, usageLimits);
      return [snapshot, { ...state, snapshot }] as const;
    });
    yield* Ref.set(recheckAtRef, undefined);
    yield* PubSub.publish(changesPubSub, next);
  });

  const hasClientDemand = (instanceId: ServerProvider["instanceId"]) =>
    Effect.all([
      host.shouldRunBackgroundWork({ type: "provider-status" }),
      host.shouldRunBackgroundWork({ type: "provider-status", instanceId }),
    ]).pipe(Effect.map(([generic, instance]) => generic || instance));

  const isRefreshEligible = (snapshot: ServerProvider) =>
    snapshot.enabled && (needsUsageRecovery(snapshot) || needsProviderRefresh(snapshot));

  const shouldRefreshForClient = Effect.gen(function* () {
    const { snapshot } = yield* Ref.get(snapshotStateRef);
    return isRefreshEligible(snapshot) && (yield* hasClientDemand(snapshot.instanceId));
  });

  const getRefreshInterval =
    input.refreshInterval !== undefined
      ? Effect.succeed(input.refreshInterval)
      : host.settings.get.pipe(
          Effect.map(
            (settings) =>
              resolveServerBackgroundActivitySettings(settings).providerHealthRefreshInterval,
          ),
          Effect.orElseSucceed(() => DEFAULT_PROVIDER_HEALTH_REFRESH_INTERVAL),
        );

  const getIntervalMs = getRefreshInterval.pipe(
    Effect.map((interval) => Duration.toMillis(Duration.fromInputUnsafe(interval))),
  );

  if (input.refreshInterval === undefined) {
    const serverSettingsChanges = yield* host.settings.subscribe;
    yield* serverSettingsChanges.pipe(
      Stream.map((settings) =>
        Duration.toMillis(
          resolveServerBackgroundActivitySettings(settings).providerHealthRefreshInterval,
        ),
      ),
      Stream.changes,
      Stream.runForEach(() =>
        Queue.offer(refreshIntervalChanges, undefined).pipe(
          Effect.andThen(unattendedWake ? Queue.offer(unattendedWake, undefined) : Effect.void),
          Effect.asVoid,
        ),
      ),
      Effect.forkScoped,
    );
  }

  // A failed check must not end this subscription: the next settings change
  // retries, like the periodic and initial checks below.
  yield* Stream.runForEach(input.streamSettings, (nextSettings) =>
    Effect.asVoid(applySnapshot(nextSettings)).pipe(Effect.ignoreCause({ log: true })),
  ).pipe(Effect.forkScoped);

  yield* Effect.gen(function* () {
    const configuredMs = yield* getIntervalMs;
    const intervalEnabled = configuredMs > 0;
    const clientIntervalMs = unattendedWake
      ? Math.max(MIN_SCHEDULED_REFRESH_INTERVAL_MS, configuredMs)
      : configuredMs;
    const retryAt =
      intervalEnabled || !input.unattendedRefreshInterval
        ? yield* Ref.get(recheckAtRef)
        : undefined;
    const checksBeforeSleep = yield* Ref.get(laterChecksPublishedRef);
    const delayMs =
      retryAt === undefined
        ? intervalEnabled
          ? clientIntervalMs
          : 60_000
        : Math.max(0, retryAt - (yield* Clock.currentTimeMillis));
    const elapsed = yield* Effect.raceFirst(
      Effect.sleep(Duration.millis(delayMs)).pipe(Effect.as(true)),
      Queue.take(refreshIntervalChanges).pipe(Effect.as(false)),
    );
    if (
      !elapsed ||
      (retryAt === undefined && (!intervalEnabled || !(yield* shouldRefreshForClient)))
    )
      return;
    if (retryAt !== undefined && input.waitForPreviousCheck && (yield* shouldRefreshForClient)) {
      yield* input.waitForPreviousCheck;
    }

    yield* refreshSemaphore.withPermits(1)(
      Effect.gen(function* () {
        // A failure or a publication can supersede this tick while it waits for the permit.
        if (
          (yield* Ref.get(recheckAtRef)) !== retryAt ||
          (input.unattendedRefreshInterval &&
            ((yield* getIntervalMs) <= 0 ||
              (yield* Ref.get(laterChecksPublishedRef)) !== checksBeforeSleep))
        )
          return;
        const forClient = yield* shouldRefreshForClient;
        // Without a client the unattended timer owns the next check; a held retry would starve it.
        if (!forClient && retryAt !== undefined && input.unattendedRefreshInterval) {
          yield* Ref.set(recheckAtRef, undefined);
          if (unattendedWake) yield* Queue.offer(unattendedWake, undefined);
          return;
        }
        const refresh = forClient ? refreshOnInterval(retryAt === undefined) : Effect.void;
        yield* refresh.pipe(
          Effect.ensuring(
            Effect.gen(function* () {
              // A skipped retry keeps its place without spinning while the probe is busy.
              if (retryAt !== undefined && (yield* Ref.get(recheckAtRef)) === retryAt) {
                yield* Ref.set(
                  recheckAtRef,
                  (yield* Clock.currentTimeMillis) + failedCheckRecheckDelayMs,
                );
              }
            }),
          ),
        );
      }),
    );
  }).pipe(Effect.ignoreCause({ log: true }), Effect.forever, Effect.forkScoped);

  const unattendedInterval = input.unattendedRefreshInterval;
  if (unattendedWake && unattendedInterval) {
    const getPlan = Effect.gen(function* () {
      const configuredMs = yield* getIntervalMs;
      const { snapshot } = yield* Ref.get(snapshotStateRef);
      const plan =
        configuredMs > 0 && isRefreshEligible(snapshot)
          ? yield* unattendedInterval(snapshot, configuredMs)
          : undefined;
      return { plan, snapshot };
    });
    const hostAllowsUnattendedWork = host.shouldRunUnattendedWork ?? Effect.succeed(true);
    yield* Effect.gen(function* () {
      const { plan, snapshot } = yield* getPlan;
      if (!plan) {
        yield* Queue.take(unattendedWake);
        return;
      }
      const maxJitter = plan.jitter ? Math.floor(Math.min(30_000, plan.intervalMs / 10)) : 0;
      const jitter = maxJitter > 0 ? yield* Random.nextIntBetween(0, maxJitter) : 0;
      const deadline = (intervalMs: number) =>
        Effect.all([Ref.get(unattendedBaseRef), Ref.get(recheckAtRef)]).pipe(
          Effect.map(([base, retryAt]) => Math.max(base + intervalMs + jitter, retryAt ?? 0)),
        );
      const delayMs = (yield* deadline(plan.intervalMs)) - (yield* Clock.currentTimeMillis);
      if (delayMs > 0) {
        const elapsed = yield* Effect.raceFirst(
          Effect.sleep(Duration.millis(delayMs)).pipe(Effect.as(true)),
          Queue.take(unattendedWake).pipe(Effect.as(false)),
        );
        if (!elapsed) return;
      }
      if (!(yield* hostAllowsUnattendedWork)) {
        yield* Ref.set(unattendedBaseRef, yield* Clock.currentTimeMillis);
        return;
      }
      yield* refreshSemaphore.withPermits(1)(
        Effect.gen(function* () {
          const current = yield* getPlan;
          if (
            !current.plan ||
            current.snapshot.checkedAt !== snapshot.checkedAt ||
            (yield* deadline(current.plan.intervalMs)) > (yield* Clock.currentTimeMillis)
          )
            return;
          // A skipped attempt waits a full interval instead of retrying at once.
          yield* Ref.set(unattendedBaseRef, yield* Clock.currentTimeMillis);
          if (!(yield* hostAllowsUnattendedWork)) return;
          // Either timer can confirm a pending failure.
          const deferFailure =
            (yield* Ref.get(recheckAtRef)) === undefined &&
            (yield* hasClientDemand(current.snapshot.instanceId));
          yield* refreshOnInterval(deferFailure).pipe(
            Effect.provideService(UnattendedProviderCheck, true),
          );
        }),
      );
    }).pipe(Effect.ignoreCause({ log: true }), Effect.forever, Effect.forkScoped);
  }

  // The first check must run even with the periodic interval disabled.
  yield* (host.awaitServerReady ?? Effect.void).pipe(
    Effect.andThen(
      Effect.gen(function* () {
        const firstCheck = refreshSemaphore.withPermits(1)(
          Effect.gen(function* () {
            const before = yield* Ref.get(snapshotStateRef);
            if (input.waitForPreviousCheck && before.checked) return true;
            yield* applySnapshotBase(yield* Ref.get(settingsRef), { forceRefresh: true });
            return (yield* Ref.get(snapshotStateRef)).checked;
          }),
        );
        while (!(yield* firstCheck)) {
          if (!input.waitForPreviousCheck) return;
          yield* input.waitForPreviousCheck;
        }
      }),
    ),
    Effect.ignoreCause({ log: true }),
    Effect.forkScoped,
  );

  return {
    resolveMaintenance: input.resolveMaintenance,
    clearUsage,
    getSnapshot: Ref.get(snapshotStateRef).pipe(Effect.map((state) => state.snapshot)),
    isAwaitingFirstCheck: Ref.get(snapshotStateRef).pipe(Effect.map((state) => !state.checked)),
    refresh: input
      .resolveMaintenance({ fresh: true })
      .pipe(Effect.andThen(refreshSnapshot), Effect.tapError(Effect.logError), Effect.orDie),
    applyUsageLimits,
    refreshUsage: refreshUsage(),
    get streamChanges() {
      return Stream.fromPubSub(changesPubSub);
    },
  };
});
