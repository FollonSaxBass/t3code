import type {
  ProviderUsageLimitsUpdate,
  ServerProviderUsageLimits,
  ServerProviderUsageWindow,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";

const WINDOW_KIND_ORDER: Record<ServerProviderUsageWindow["kind"], number> = {
  session: 0,
  weekly: 1,
  monthly: 2,
  other: 3,
};

export function clampPercent(value: number): number {
  return Number.isFinite(value) ? Math.max(0, Math.min(100, value)) : 0;
}

function sortWindows(
  windows: Iterable<ServerProviderUsageWindow>,
): ReadonlyArray<ServerProviderUsageWindow> {
  return [...windows].toSorted(
    (left, right) =>
      WINDOW_KIND_ORDER[left.kind] - WINDOW_KIND_ORDER[right.kind] ||
      left.id.localeCompare(right.id),
  );
}

export function makeUsageLimits(input: {
  readonly checkedAt: string;
  readonly windows: Iterable<ServerProviderUsageWindow>;
}): ServerProviderUsageLimits {
  return { checkedAt: input.checkedAt, windows: sortWindows(input.windows) };
}

export function makeUnavailableUsageLimits(input: {
  readonly checkedAt: string;
  readonly reason: "unsupported" | "probeFailed";
  readonly message?: string;
}): ServerProviderUsageLimits {
  return {
    checkedAt: input.checkedAt,
    windows: [],
    unavailable: {
      reason: input.reason,
      ...(input.message ? { message: input.message } : {}),
    },
  };
}

export function applyUsageLimitsUpdate(input: {
  readonly previous: ServerProviderUsageLimits | undefined;
  readonly update: ProviderUsageLimitsUpdate;
  readonly checkedAt: string;
}): ServerProviderUsageLimits | undefined {
  const { previous, update } = input;
  if (update.windows.length === 0 || previous?.unavailable?.reason === "unsupported") {
    return previous;
  }
  const merged = new Map(previous?.windows.map((window) => [window.id, window] as const));
  // Codex repeats unchanged limits on token ticks; avoid allocating snapshots for no-ops.
  let changed = false;
  for (const window of update.windows) {
    const existing = merged.get(window.id);
    const next: ServerProviderUsageWindow = {
      ...window,
      usedPercent: clampPercent(window.usedPercent),
      ...(window.resetsAt === undefined && existing?.resetsAt !== undefined
        ? { resetsAt: existing.resetsAt }
        : {}),
      ...(window.windowDurationMins === undefined && existing?.windowDurationMins !== undefined
        ? { windowDurationMins: existing.windowDurationMins }
        : {}),
    };
    if (existing === undefined || !usageWindowEquals(existing, next)) {
      merged.set(window.id, next);
      changed = true;
    }
  }
  if (!changed && previous !== undefined) {
    return previous;
  }
  // Session and weekly together are a complete reading, so a failed probe no longer applies.
  const complete =
    update.windows.some((window) => window.kind === "session") &&
    update.windows.some((window) => window.kind === "weekly");
  const unavailable = complete
    ? undefined
    : previous === undefined
      ? { reason: "probeFailed" as const }
      : previous.unavailable;
  return {
    ...makeUsageLimits({ checkedAt: input.checkedAt, windows: merged.values() }),
    ...(previous?.resetCredits !== undefined ? { resetCredits: previous.resetCredits } : {}),
    ...(previous?.zeroSpendCap ? { zeroSpendCap: true } : {}),
    ...(unavailable !== undefined ? { unavailable } : {}),
  };
}

function usageWindowEquals(a: ServerProviderUsageWindow, b: ServerProviderUsageWindow): boolean {
  return (
    a.id === b.id &&
    a.kind === b.kind &&
    a.label === b.label &&
    a.usedPercent === b.usedPercent &&
    (a.modelIds === b.modelIds ||
      (a.modelIds !== undefined &&
        b.modelIds !== undefined &&
        a.modelIds.length === b.modelIds.length &&
        a.modelIds.every((id, index) => id === b.modelIds?.[index]))) &&
    (a.providerIds === b.providerIds ||
      (a.providerIds !== undefined &&
        b.providerIds !== undefined &&
        a.providerIds.length === b.providerIds.length &&
        a.providerIds.every((id, index) => id === b.providerIds?.[index]))) &&
    a.resetsAt === b.resetsAt &&
    a.windowDurationMins === b.windowDurationMins
  );
}

export function resolveUsageLimitsAfterProbe(input: {
  readonly published: ServerProviderUsageLimits | undefined;
  readonly probed: ServerProviderUsageLimits | undefined;
}): ServerProviderUsageLimits | undefined {
  const { published, probed } = input;
  if (probed?.unavailable?.reason === "probeFailed" && published && !published.unavailable) {
    return published;
  }
  if (
    published &&
    probed &&
    !published.unavailable &&
    !probed.unavailable &&
    Date.parse(published.checkedAt) > Date.parse(probed.checkedAt)
  ) {
    return { ...probed, checkedAt: published.checkedAt, windows: published.windows };
  }
  return probed;
}

const USAGE_REQUEST_INTERVAL_MS = Duration.toMillis(Duration.minutes(5));

/**
 * Providers answer usage endpoints with 429s when they are polled often, so every trigger
 * (interval, settings change, manual refresh, reset redemption) shares one request per
 * account every five minutes. A failed attempt spends the window too.
 */
export const makeUsageRequestThrottle = Effect.gen(function* () {
  const last = yield* Ref.make<
    | {
        readonly key: string;
        readonly requestedAt: number;
        readonly reading?: ServerProviderUsageLimits;
      }
    | undefined
  >(undefined);
  /** True claims the window for `key`; false means a request for it ran inside the window. */
  const claim = (key: string) =>
    Effect.flatMap(Clock.currentTimeMillis, (now) =>
      Ref.modify(last, (previous) =>
        previous?.key === key && now - previous.requestedAt < USAGE_REQUEST_INTERVAL_MS
          ? [false, previous]
          : [true, { key, requestedAt: now }],
      ),
    );
  /** Inside the window, returns the last reading, or a failed one when that request failed. */
  const read = <E, R>(key: string, request: Effect.Effect<ServerProviderUsageLimits, E, R>) =>
    Effect.flatMap(claim(key), (claimed) =>
      claimed
        ? Effect.tap(request, (reading) =>
            Ref.update(last, (previous) =>
              previous?.key === key ? { ...previous, reading } : previous,
            ),
          )
        : Effect.flatMap(Ref.get(last), (previous) =>
            previous?.reading
              ? Effect.succeed(previous.reading)
              : Effect.map(DateTime.now, (now) =>
                  makeUnavailableUsageLimits({
                    checkedAt: DateTime.formatIso(now),
                    reason: "probeFailed",
                  }),
                ),
          ),
    );
  return { claim, read, reset: Ref.set(last, undefined) };
});

export type UsageRequestThrottle = Effect.Success<typeof makeUsageRequestThrottle>;
