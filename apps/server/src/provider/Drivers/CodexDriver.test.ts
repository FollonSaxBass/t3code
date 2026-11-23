import * as CodexInstallation from "../CodexInstallation.ts";
import * as CodexManagedTokenBroker from "../CodexManagedTokenBroker.ts";
import * as ServerSecretStore from "../../auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ProviderSessionId, ThreadId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Duration from "effect/Duration";
import * as Exit from "effect/Exit";
import * as Deferred from "effect/Deferred";
import * as Digest from "effect/crypto/Digest";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient, HttpClientResponse } from "effect/http";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import * as ServerConfig from "../../config.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as ResetCreditCoordinator from "../resetCreditCoordinator.ts";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as ModelManifest from "../ModelManifest.ts";
import {
  createProviderVersionAdvisory,
  resolveLatestProviderVersion,
} from "@t3tools/provider-core/server/maintenanceResolver";
import { codexAppServerArgs } from "../codexLaunchArgs.ts";
import { CodexDriver } from "./CodexDriver.ts";
import * as CodexAdapterV2 from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as PortScanner from "../../preview/PortScanner.ts";
import * as ProviderCredentialStore from "../ProviderCredentialStore.ts";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as ProviderRegistry from "../ProviderRegistry.ts";
import * as ProviderInstanceRegistry from "../ProviderInstanceRegistry.ts";
import * as ProviderInstanceRegistryMutator from "../ProviderInstanceRegistryMutator.ts";
import * as ProviderHostLive from "../ProviderHostLive.ts";
import {
  awaitInstanceCodexStatusProbe,
  checkInstanceCodexProviderStatus,
  makeCodexCredentialRefresh,
} from "../CodexProvider.ts";
import { readProviderStatusCache, writeProviderStatusCache } from "../providerStatusCache.ts";

const layerDeps = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-codex-driver-maintenance-",
}).pipe(
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  Layer.provideMerge(IdAllocator.layer),
  Layer.provideMerge(McpProviderSessions.layer),
  Layer.provideMerge(PortScanner.layerTest),
  Layer.provideMerge(
    Layer.mock(CodexAdapterV2.CodexAppServerClientFactory)({
      open: () => Effect.die("Maintenance resolution must not open a Codex session"),
    }),
  ),
  Layer.provideMerge(
    Layer.mock(CodexInstallation.CodexInstallation)({
      managedDirectory: "unused-managed-installation",
    }),
  ),
  Layer.provideMerge(Layer.mock(ServerSecretStore.ServerSecretStore)({})),
  Layer.provideMerge(
    Layer.mock(CodexManagedTokenBroker.CodexManagedTokenBroker)({
      endpoint: Effect.succeed("unused-private-pipe"),
      awaitIdle: Effect.void,
    }),
  ),
  Layer.provideMerge(
    Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
      getEnvironmentId: Effect.succeed(EnvironmentId.make("00000000-0000-4000-8000-000000000001")),
    }),
  ),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(ModelManifest.layerTest),
  Layer.provideMerge(ResetCreditCoordinator.layerTest),
  Layer.provideMerge(
    Layer.mock(BackgroundPolicy.BackgroundPolicy)({
      shouldRunScopeWork: () => Effect.succeed(false),
      shouldRunUnattendedWork: Effect.succeed(true),
    }),
  ),
  Layer.provideMerge(
    Layer.succeed(
      ProviderEventLoggers.ProviderEventLoggers,
      ProviderEventLoggers.NoOpProviderEventLoggers,
    ),
  ),
  Layer.provideMerge(ProviderLatestVersions.layer),
  Layer.provideMerge(
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make(() => Effect.die("Disabled Codex must not make an HTTP request")),
    ),
  ),
);
const layerTest = ProviderHostLive.layer.pipe(Layer.provideMerge(layerDeps));

// The `#!/bin/sh` stub below cannot be resolved as an executable on Windows.
const windowsHost = HostProcess.Platform.defaultValue() === "win32";

const noSpawn = ChildProcessSpawner.make(() =>
  Effect.die("Disabled Codex must not spawn a process"),
);
const encodeCredentials = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

type FakeAnswer =
  | { readonly result: unknown }
  | { readonly error: { code: number; message: string } };

// Process-group checks report no child processes.
const makeFakeCodexAppServer = (input: {
  readonly codexHome: string;
  readonly answers?: Record<string, () => FakeAnswer | undefined>;
  readonly usageGate?: Deferred.Deferred<void>;
  readonly usageRequested?: Deferred.Deferred<void>;
  readonly spawns?: Queue.Queue<void>;
  readonly startupGate?: Effect.Effect<void>;
}) => {
  const requests = new Map<string, number>();
  const encoder = new TextEncoder();
  const results: Record<string, unknown> = {
    initialize: {
      userAgent: "codex_cli_rs/0.167.0",
      codexHome: input.codexHome,
      platformFamily: "unix",
      platformOs: "linux",
    },
    "account/read": {
      account: { type: "chatgpt", email: "test@example.com", planType: "pro" },
      requiresOpenaiAuth: true,
    },
    "config/read": { config: {}, origins: {} },
    "account/rateLimits/read": {
      rateLimits: { limitId: "codex", planType: "pro", primary: { usedPercent: 42 } },
      rateLimitsByLimitId: null,
      rateLimitResetCredits: null,
      accountId: null,
    },
  };
  const exited = (stdout: string) =>
    ChildProcessSpawner.makeHandle({
      pid: ChildProcessSpawner.ProcessId(2),
      exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
      isRunning: Effect.succeed(false),
      kill: () => Effect.void,
      unref: Effect.succeed(Effect.void),
      stdin: Sink.drain,
      stdout: Stream.make(encoder.encode(stdout)),
      stderr: Stream.empty,
      all: Stream.empty,
      getInputFd: () => Sink.drain,
      getOutputFd: () => Stream.empty,
    });
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      if (!ChildProcess.isStandardCommand(command)) return exited("");
      if (command.command === "pgrep") {
        if (input.startupGate) yield* input.startupGate;
        return exited("1\n");
      }
      if (!command.args.includes("app-server")) return exited("codex-cli 0.167.0\n");
      const scope = yield* Effect.scope;
      if (input.spawns) yield* Queue.offer(input.spawns, undefined);
      const output = yield* Queue.unbounded<Uint8Array>();
      const send = (message: unknown) =>
        Queue.offer(output, encoder.encode(`${JSON.stringify(message)}\n`));
      const answer = (line: string) => {
        const { id, method } = JSON.parse(line) as { id?: number; method?: string };
        if (id === undefined || method === undefined) return Effect.void;
        requests.set(method, (requests.get(method) ?? 0) + 1);
        const scripted = input.answers?.[method]?.();
        const reply = send({ id, ...(scripted ?? { result: results[method] ?? { data: [] } }) });
        if (method !== "account/rateLimits/read" || !input.usageGate) return Effect.asVoid(reply);
        return Deferred.await(input.usageGate).pipe(
          Effect.andThen(reply),
          Effect.forkIn(scope),
          Effect.andThen(
            input.usageRequested ? Deferred.succeed(input.usageRequested, undefined) : Effect.void,
          ),
          Effect.asVoid,
        );
      };
      const decoder = new TextDecoder();
      let buffered = "";
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: Effect.never,
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((chunk: Uint8Array) => {
          buffered += decoder.decode(chunk, { stream: true });
          const lines = buffered.split("\n");
          buffered = lines.pop() ?? "";
          return Effect.forEach(lines.filter(Boolean), answer, { discard: true });
        }),
        stdout: Stream.fromQueue(output),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );
  return { spawner, requests };
};

/** A shared and a shadow Codex home, plus a catalog stamp that ends the ChatGPT catalog wait. */
const makeShadowHomes = Effect.fn("CodexDriver.test.makeShadowHomes")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-codex-driver-shadow-" });
  const sharedHome = NodePath.join(tempDir, "shared");
  const shadowHome = NodePath.join(tempDir, "shadow");
  yield* fs.makeDirectory(sharedHome, { recursive: true });
  yield* fs.makeDirectory(shadowHome, { recursive: true });
  yield* fs.writeFileString(
    NodePath.join(shadowHome, "models_cache.json"),
    JSON.stringify({ fetched_at: "2999-01-01T00:00:00.000Z" }),
  );
  yield* fs.writeFileString(NodePath.join(shadowHome, "auth.json"), "{}\n");
  return { fs, sharedHome, shadowHome };
});

const withHealthInterval = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  intervalMs: number,
  options?: { readonly clientDemand?: boolean },
) =>
  ProviderHost.ProviderHost.use((host) =>
    effect.pipe(
      Effect.provideService(ProviderHost.ProviderHost, {
        ...host,
        ...(options?.clientDemand ? { shouldRunBackgroundWork: () => Effect.succeed(true) } : {}),
        settings: {
          ...host.settings,
          get: host.settings.get.pipe(
            Effect.map((settings) => ({
              ...settings,
              backgroundActivity: {
                schemaVersion: 1 as const,
                profile: "custom" as const,
                overrides: { providerHealthRefreshInterval: Duration.millis(intervalMs) },
              },
            })),
          ),
        },
      }),
      Effect.provideService(Random.Random, { nextDoubleUnsafe: () => 0, nextIntUnsafe: () => 0 }),
    ),
  );

it.layer(layerTest)("CodexDriver", (it) => {
  const checkManagedAccount = (replacementTiming: "unchanged" | "during-probe") =>
    Effect.gen(function* () {
      const instanceId = ProviderInstanceId.make("restored-managed-account");
      const credentials = new Map<string, Uint8Array>();
      const catalogTokens: Array<string | undefined> = [];
      const probeEntered = yield* Deferred.make<void>();
      const resumeProbe = yield* Deferred.make<void>();
      const secrets = ServerSecretStore.ServerSecretStore.of({
        get: (key) => Effect.sync(() => Option.fromUndefinedOr(credentials.get(key))),
        set: (key, value) =>
          Effect.sync(() => {
            credentials.set(key, value);
          }),
        remove: (key) =>
          Effect.sync(() => {
            credentials.delete(key);
          }),
        create: () => Effect.die("unused"),
        getOrCreateRandom: () => Effect.die("unused"),
      });
      yield* Effect.gen(function* () {
        const store = yield* ProviderCredentialStore.make("codex-chatgpt", instanceId);
        const json = yield* encodeCredentials({
          clientId: "oaiapp_test",
          accessToken: "dummy-owned-access",
          refreshToken: "dummy-refresh",
          expiresAt: Number.MAX_SAFE_INTEGER,
          earliestRefreshAt: null,
          scopes: ["chatgpt.tokens.use.direct"],
          subject: "test-user",
          email: "account@example.test",
        });
        yield* store.set(new TextEncoder().encode(json));
        const replacement = yield* encodeCredentials({
          clientId: "oaiapp_replacement",
          subject: "replacement-user",
          email: "replacement@example.test",
          accessToken: "replacement-access",
          refreshToken: "replacement-refresh",
          expiresAt: Number.MAX_SAFE_INTEGER,
          earliestRefreshAt: null,
          scopes: ["chatgpt.tokens.use.direct"],
        });
        const replaceAccount = store.set(new TextEncoder().encode(replacement)).pipe(Effect.orDie);
        const executable = {
          executablePath: "/user/bin/codex",
          managedVersionDirectory: null,
          source: "local" as const,
          version: "0.156.1",
        };
        const installation = yield* CodexInstallation.CodexInstallation;
        const serverConfig = yield* ServerConfig.ServerConfig;
        const sharedHome = NodePath.join(serverConfig.stateDir, "shared-codex-home");
        const launches: Array<
          Parameters<CodexAdapterV2.CodexAppServerClientFactoryShape["open"]>[0]
        > = [];

        let interruptedChecks = 0;
        let failAcquire = false;
        const startupChecksInterrupted = yield* Deferred.make<void>();
        const acquire = () =>
          Effect.suspend(() => {
            if (failAcquire)
              return Effect.fail(
                new CodexInstallation.CodexInstallationError({
                  operation: "acquire",
                  detail: "The fixture runtime is unavailable",
                }),
              );
            if (interruptedChecks === 2) return Effect.succeed(executable);
            interruptedChecks += 1;
            return (
              interruptedChecks === 2
                ? Deferred.succeed(startupChecksInterrupted, undefined)
                : Effect.void
            ).pipe(Effect.andThen(Effect.interrupt));
          });
        const instance = yield* CodexDriver.create({
          instanceId,
          displayName: "Restored account",
          enabled: true,
          environment: [{ name: "OPENAI_API_KEY", value: "ambient-key", sensitive: true }],
          config: { ...CodexDriver.defaultConfig(), setupMode: "managed", homePath: sharedHome },
        }).pipe(
          Effect.provideService(
            CodexAdapterV2.CodexAppServerClientFactory,
            CodexAdapterV2.CodexAppServerClientFactory.of({
              open: (launch) =>
                Effect.sync(() => launches.push(launch)).pipe(
                  Effect.andThen(Effect.die("The fixture stops after recording the launch")),
                ),
            }),
          ),
          Effect.provideService(
            CodexInstallation.CodexInstallation,
            CodexInstallation.CodexInstallation.of({
              ...installation,
              managedDirectory: "unused-managed-installation",
              resolve: () => Effect.succeed(executable),
              acquire,
            }),
          ),
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            ChildProcessSpawner.make(() =>
              Effect.gen(function* () {
                if (replacementTiming === "during-probe") {
                  yield* Deferred.succeed(probeEntered, undefined);
                  yield* Deferred.await(resumeProbe);
                }
                return yield* PlatformError.badArgument({
                  module: "ChildProcessSpawner",
                  method: "spawn",
                  description: "The fixture app-server is unavailable",
                });
              }),
            ),
          ),
        );
        const observedAccount = yield* Deferred.make<void>();
        const disconnected = yield* instance.snapshot.streamChanges.pipe(
          Stream.tap((provider) =>
            provider.auth.status === "authenticated"
              ? Deferred.succeed(observedAccount, undefined).pipe(Effect.asVoid)
              : Effect.void,
          ),
          Stream.filter((provider) => provider.auth.status === "unauthenticated"),
          Stream.runHead,
          Effect.forkScoped,
        );
        yield* Deferred.await(startupChecksInterrupted);
        const refresh = yield* instance.snapshot.refresh.pipe(Effect.forkChild);
        if (replacementTiming === "during-probe") {
          yield* Deferred.await(probeEntered);
          yield* replaceAccount;
          yield* Deferred.succeed(resumeProbe, undefined);
        }
        const restored = yield* Fiber.join(refresh);
        if (replacementTiming === "during-probe") {
          expect(catalogTokens).toEqual([]);
          expect(restored.models).toEqual([]);
          expect(restored.auth.email).toBe("replacement@example.test");
          expect(restored.auth.profileId).toBe("oaiapp_replacement");
          return;
        }
        expect(catalogTokens).toEqual(["Bearer dummy-owned-access"]);
        expect(restored.auth.email).toBe("account@example.test");
        expect(restored.runtimePaths?.homePath).toBe(sharedHome);
        expect(restored.runtimePaths?.shadowHomePath).toContain(
          NodePath.join("providers", "codex", instanceId, "shadow"),
        );
        const scan = yield* instance.snapshotForCwd!(serverConfig.stateDir).pipe(Effect.exit);
        expect(Exit.isFailure(scan)).toBe(true);
        failAcquire = true;
        const runtimeScan = yield* instance.snapshotForCwd!(serverConfig.stateDir).pipe(
          Effect.exit,
        );
        failAcquire = false;
        expect(Exit.isFailure(runtimeScan)).toBe(true);
        yield* Deferred.await(observedAccount);

        const threadId = ThreadId.make("managed-account-thread");
        yield* instance.orchestrationAdapter
          .openSession({
            threadId,
            providerSessionId: ProviderSessionId.make("managed-account-session"),
            modelSelection: { instanceId, model: "gpt-5.4" },
            runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
              runtimeMode: "full-access",
              interactionMode: "default",
              cwd: serverConfig.stateDir,
            }),
          })
          .pipe(Effect.scoped, Effect.exit);
        expect(launches).toHaveLength(1);
        const launch = launches[0]!;
        expect(launch.settings.binaryPath).toBe(executable.executablePath);
        expect(launch.settings.launchArgs).toContain("openai_token_sharing");
        expect(launch.environment.ACCESS_TOKEN).toBeUndefined();
        expect(launch.settings.launchArgs).toContain("__managed-chatgpt-token");
        expect(launch.settings.launchArgs).not.toContain("dummy-owned-access");
        expect(launch.environment.OPENAI_API_KEY).toBeUndefined();
        expect(launch.environment.CODEX_HOME).toBe(launch.settings.homePath);
        const before = yield* instance.auth!.subscribe("test-owner").pipe(Stream.runHead);
        expect(Option.getOrThrow(before).phase).toBe("idle");
        yield* instance.auth!.logout(Effect.void);
        const after = Option.getOrThrow(yield* Fiber.join(disconnected));
        expect(after.auth.status).toBe("unauthenticated");
        expect(after.auth.email).toBeUndefined();
        expect(after.installed).toBe(true);
        expect(after.models).toEqual([]);
        expect(Option.isNone(yield* store.get)).toBe(true);
        const signedOutScan = yield* instance.snapshotForCwd!(serverConfig.stateDir);
        expect(signedOutScan.skills).toEqual([]);
      }).pipe(
        Effect.provideService(ServerSecretStore.ServerSecretStore, secrets),
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make((request) =>
            Effect.sync(() => {
              expect([
                "https://auth.openai.com/.well-known/openid-configuration",
                "https://auth.openai.com/revoke",
                "https://api.openai.com/v1/models",
              ]).toContain(request.url);
              if (request.url.endsWith("/models")) {
                catalogTokens.push(request.headers.authorization);
                return HttpClientResponse.fromWeb(request, Response.json({ models: [] }));
              }
              return HttpClientResponse.fromWeb(
                request,
                request.url.endsWith("/revoke")
                  ? new Response(null, { status: 200 })
                  : Response.json({
                      issuer: "https://auth.openai.com",
                      authorization_endpoint: "https://auth.openai.com/api/accounts/authorize",
                      token_endpoint: "https://auth.openai.com/api/accounts/oauth/token",
                      jwks_uri: "https://auth.openai.com/jwks",
                      revocation_endpoint: "https://auth.openai.com/revoke",
                    }),
              );
            }),
          ),
        ),
      );
    }).pipe(Effect.scoped);
  // regression: a sign-in that replaces the account mid-probe must not label the old
  // account's catalog with the new identity.
  it.effect.each(["unchanged", "during-probe"] as const)(
    "managed catalog keeps its account label and token together: %s",
    checkManagedAccount,
  );

  it.effect.skipIf(windowsHost)(
    "runs the standalone updater against the shared home, not the shadow home",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const tempDir = yield* fs
          .makeTempDirectoryScoped({ prefix: "t3-codex-driver-" })
          .pipe(Effect.flatMap((directory) => fs.realPath(directory)));
        const sharedHome = NodePath.join(tempDir, "codex-home");
        const shadowHome = NodePath.join(tempDir, "codex-shadow");
        const binaryPath = NodePath.join(sharedHome, "packages", "standalone", "bin", "codex");
        yield* fs.makeDirectory(NodePath.dirname(binaryPath), { recursive: true });
        yield* fs.writeFileString(binaryPath, "#!/bin/sh\n");
        yield* fs.chmod(binaryPath, 0o755);

        const instance = yield* CodexDriver.create({
          instanceId: ProviderInstanceId.make("codex-shadow"),
          displayName: "Codex test",
          enabled: false,
          environment: [],
          config: {
            ...CodexDriver.defaultConfig(),
            binaryPath,
            homePath: sharedHome,
            shadowHomePath: shadowHome,
          },
        });

        const capabilities = yield* instance.snapshot.resolveMaintenance();
        expect(capabilities.update).toMatchObject({
          executable: binaryPath,
          args: ["update"],
          lockKey: "codex-native",
          env: { CODEX_HOME: sharedHome },
        });
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, noSpawn)),
  );

  it.effect("points a shadow home's bundled marketplace at the shadow CODEX_HOME", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-codex-driver-" });
      const sharedHome = NodePath.join(tempDir, "codex home");
      const shadowHome = NodePath.join(tempDir, "codex shadow");
      const launches: Array<
        Parameters<CodexAdapterV2.CodexAppServerClientFactoryShape["open"]>[0]
      > = [];
      const instance = yield* CodexDriver.create({
        instanceId: ProviderInstanceId.make("codex-shadow"),
        displayName: "Codex test",
        enabled: false,
        environment: [],
        config: {
          ...CodexDriver.defaultConfig(),
          homePath: sharedHome,
          shadowHomePath: shadowHome,
          launchArgs: "-c model_auto_compact_token_limit=250000",
        },
      }).pipe(
        Effect.provideService(
          CodexAdapterV2.CodexAppServerClientFactory,
          CodexAdapterV2.CodexAppServerClientFactory.of({
            open: (launch) =>
              Effect.sync(() => launches.push(launch)).pipe(
                Effect.andThen(Effect.die("The fixture stops after recording the launch")),
              ),
          }),
        ),
      );

      yield* instance.orchestrationAdapter
        .openSession({
          threadId: ThreadId.make("shadow-home-thread"),
          providerSessionId: ProviderSessionId.make("shadow-home-session"),
          modelSelection: { instanceId: instance.instanceId, model: "gpt-5.4" },
          runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd: tempDir,
          }),
        })
        .pipe(Effect.scoped, Effect.exit);

      expect(launches).toHaveLength(1);
      const settings = launches[0]!.settings;
      expect(settings.homePath).toBe(shadowHome);
      const bundledSource = NodePath.join(
        shadowHome,
        ".tmp",
        "bundled-marketplaces",
        "openai-bundled",
      );
      expect(codexAppServerArgs(settings.launchArgs)).toEqual([
        "app-server",
        "-c",
        "thread_unload_delay_secs=0",
        "-c",
        `marketplaces.openai-bundled.source=${JSON.stringify(bundledSource)}`,
        "-c",
        "model_auto_compact_token_limit=250000",
      ]);
    }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, noSpawn), Effect.scoped),
  );

  it.effect.skipIf(windowsHost)(
    "runs the standalone updater against the home that holds a shared install",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const tempDir = yield* fs
          .makeTempDirectoryScoped({ prefix: "t3-codex-driver-" })
          .pipe(Effect.flatMap((directory) => fs.realPath(directory)));
        // One standalone install, linked onto PATH and used by two account homes.
        // `İ` gets longer when lowercased, so the home must not be sliced from a
        // lowercased copy of the path.
        const installHome = NodePath.join(tempDir, "codex-İnstall");
        const realBinaryPath = NodePath.join(
          installHome,
          "packages",
          "standalone",
          "releases",
          "0.162.1-x86_64-unknown-linux-musl",
          "bin",
          "codex",
        );
        const binaryPath = NodePath.join(tempDir, "bin", "codex");
        yield* fs.makeDirectory(NodePath.dirname(realBinaryPath), { recursive: true });
        yield* fs.makeDirectory(NodePath.dirname(binaryPath), { recursive: true });
        yield* fs.writeFileString(realBinaryPath, "#!/bin/sh\n");
        yield* fs.chmod(realBinaryPath, 0o755);
        yield* fs.symlink(realBinaryPath, binaryPath);

        const updates = yield* Effect.forEach(["codex-p", "codex-vm"], (account) =>
          Effect.gen(function* () {
            const homePath = NodePath.join(tempDir, `home-${account}`);
            yield* fs.makeDirectory(homePath, { recursive: true });
            const instance = yield* CodexDriver.create({
              instanceId: ProviderInstanceId.make(account),
              displayName: account,
              enabled: false,
              environment: [],
              config: { ...CodexDriver.defaultConfig(), binaryPath, homePath },
            });
            return (yield* instance.snapshot.resolveMaintenance()).update;
          }),
        );

        for (const update of updates) {
          expect(update).toMatchObject({
            command: `${binaryPath} update`,
            args: ["update"],
            lockKey: "codex-native",
            env: { CODEX_HOME: installHome },
          });
        }
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, noSpawn)),
  );

  it.effect.skipIf(windowsHost)("derives the standalone update home from either command path", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const tempDir = yield* fs
        .makeTempDirectoryScoped({ prefix: "t3-codex-update-home-" })
        .pipe(Effect.flatMap((directory) => fs.realPath(directory)));
      const linkedInstallHome = NodePath.join(tempDir, "install");
      const fixtures = [
        { binary: "codex", realPath: "/packages/standalone/bin/codex", home: "/" },
        { binary: "codex", realPath: "C:\\Packages\\Standalone\\bin\\codex.exe", home: "C:\\" },
        // The standalone tree itself is a symlink, so only the resolved path names its home.
        {
          binary: NodePath.join("install", "packages", "standalone", "bin", "codex"),
          realPath: "/disk/codex/releases/0.163.0/bin/codex",
          home: linkedInstallHome,
        },
      ];

      for (const fixture of fixtures) {
        const binaryPath = NodePath.join(tempDir, fixture.binary);
        yield* fs.makeDirectory(NodePath.dirname(binaryPath), { recursive: true });
        yield* fs.writeFileString(binaryPath, "#!/bin/sh\n");
        yield* fs.chmod(binaryPath, 0o755);
        const instance = yield* CodexDriver.create({
          instanceId: ProviderInstanceId.make("codex-update-home"),
          displayName: "Codex update home",
          enabled: false,
          environment: [],
          config: { ...CodexDriver.defaultConfig(), binaryPath, homePath: tempDir },
        }).pipe(
          // Virtualize only the executable target: these trees do not belong to this test.
          Effect.provideService(FileSystem.FileSystem, {
            ...fs,
            realPath: (path) =>
              path === binaryPath ? Effect.succeed(fixture.realPath) : fs.realPath(path),
          }),
        );
        expect((yield* instance.snapshot.resolveMaintenance()).update?.env).toEqual({
          CODEX_HOME: fixture.home,
        });
      }
    }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, noSpawn)),
  );

  // regression: #23937 `codex update` installs the native channel's release, which can lag npm.
  it.effect.skipIf(windowsHost)(
    "does not advertise npm's latest Codex to a standalone install",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const tempDir = yield* fs
          .makeTempDirectoryScoped({ prefix: "t3-codex-driver-" })
          .pipe(Effect.flatMap((directory) => fs.realPath(directory)));
        const home = NodePath.join(tempDir, "codex-home");
        const binaryPath = NodePath.join(home, "packages", "standalone", "current", "bin", "codex");
        yield* fs.makeDirectory(NodePath.dirname(binaryPath), { recursive: true });
        yield* fs.writeFileString(binaryPath, "#!/bin/sh\n");
        yield* fs.chmod(binaryPath, 0o755);
        // npm's latest tag is ahead of the native channel, as during a staggered release.
        const npmView = ChildProcessSpawner.make((command) => {
          if (!ChildProcess.isStandardCommand(command) || command.args[0] !== "view") {
            return Effect.die("Version lookup must not run a provider or updater");
          }
          return Effect.succeed(
            ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(1),
              exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
              isRunning: Effect.succeed(false),
              kill: () => Effect.void,
              unref: Effect.succeed(Effect.void),
              stdin: Sink.drain,
              stdout: Stream.encodeText(Stream.make('"0.164.0"')),
              stderr: Stream.empty,
              all: Stream.empty,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
            }),
          );
        });
        const instance = yield* CodexDriver.create({
          instanceId: ProviderInstanceId.make("codex-standalone-latest"),
          displayName: "Codex standalone",
          enabled: false,
          environment: [],
          config: { ...CodexDriver.defaultConfig(), binaryPath, homePath: home },
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, npmView),
          Effect.provideService(
            HttpClient.HttpClient,
            HttpClient.make((request) =>
              Effect.succeed(
                HttpClientResponse.fromWeb(
                  request,
                  request.url === "https://releases.openai.com/codex/channels/latest"
                    ? Response.json({ tag_name: "rust-v0.163.0", assets: [] })
                    : new Response(null, { status: 404 }),
                ),
              ),
            ),
          ),
        );
        const capabilities = yield* instance.snapshot.resolveMaintenance();
        expect(capabilities.update?.lockKey).toBe("codex-native");

        const latestVersion = yield* resolveLatestProviderVersion(capabilities).pipe(
          Effect.provideServiceEffect(
            ProviderLatestVersions.ProviderLatestVersions,
            ProviderLatestVersions.make(),
          ),
        );
        expect(latestVersion).toBe("0.163.0");
        expect(
          createProviderVersionAdvisory({
            driver: CodexDriver.driverKind,
            currentVersion: "0.163.0",
            latestVersion,
            maintenanceCapabilities: capabilities,
          }).status,
        ).not.toBe("behind_latest");
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, noSpawn)),
  );

  it.effect("uses each instance's environment CODEX_HOME for continuation", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-codex-driver-" });
      for (const [index, account] of ["work", "personal", "work "].entries()) {
        const codexHome = NodePath.join(tempDir, account);
        const instance = yield* CodexDriver.create({
          instanceId: ProviderInstanceId.make(`account-${index}`),
          displayName: account,
          enabled: false,
          environment: [{ name: "CODEX_HOME", value: codexHome, sensitive: false }],
          config: CodexDriver.defaultConfig(),
        });
        expect(instance.continuationIdentity.continuationKey).toBe(`codex:home:${codexHome}`);
      }
    }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, noSpawn)),
  );

  it.effect.skipIf(windowsHost)(
    "shares state from the environment CODEX_HOME in a shadow home",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const tempDir = yield* fs
          .makeTempDirectoryScoped({ prefix: "t3-codex-driver-" })
          .pipe(Effect.flatMap((directory) => fs.realPath(directory)));
        const codexHome = NodePath.join(tempDir, "custom-codex");
        const shadowHome = NodePath.join(tempDir, "shadow");
        yield* fs.makeDirectory(codexHome, { recursive: true });
        yield* fs.writeFileString(NodePath.join(codexHome, "config.toml"), 'model = "custom"\n');
        const instance = yield* CodexDriver.create({
          instanceId: ProviderInstanceId.make("env-shadow"),
          displayName: "env-shadow",
          enabled: false,
          environment: [{ name: "CODEX_HOME", value: codexHome, sensitive: false }],
          config: { ...CodexDriver.defaultConfig(), shadowHomePath: shadowHome },
        }).pipe(Effect.provideService(HostProcess.HomeDirectory, NodePath.join(tempDir, "user")));
        expect(instance.continuationIdentity.continuationKey).toBe(`codex:home:${codexHome}`);
        expect(yield* fs.readLink(NodePath.join(shadowHome, "config.toml"))).toBe(
          NodePath.join(codexHome, "config.toml"),
        );
        expect(yield* fs.readLink(NodePath.join(shadowHome, "sessions"))).toBe(
          NodePath.join(codexHome, "sessions"),
        );
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, noSpawn)),
  );

  for (const fixture of [
    {
      name: "leaves mise npm-backend installations manual-only",
      installSegments: ["mise", "installs", "npm-openai-codex", "0.110.0"],
      npmOwned: false,
    },
    {
      name: "leaves mise tool aliases backed by npm manual-only",
      installSegments: ["mise", "installs", "codex", "0.110.0"],
      npmOwned: false,
    },
    {
      name: "keeps npm updates for globals in a mise Node installation",
      installSegments: ["mise", "installs", "node", "24.0.0"],
      npmOwned: true,
    },
    {
      name: "keeps npm updates for ordinary global installations",
      installSegments: ["npm-global"],
      npmOwned: true,
    },
  ] as const) {
    it.effect.skipIf(windowsHost)(fixture.name, () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const tempDir = yield* fs
          .makeTempDirectoryScoped({ prefix: "t3-codex-installer-" })
          .pipe(Effect.flatMap((directory) => fs.realPath(directory)));
        const installPath = NodePath.join(tempDir, ...fixture.installSegments);
        const realBinaryPath = NodePath.join(
          installPath,
          "lib",
          "node_modules",
          "@openai",
          "codex",
          "bin",
          "codex.js",
        );
        const binaryPath = NodePath.join(tempDir, "bin", "codex");
        yield* fs.makeDirectory(NodePath.dirname(realBinaryPath), { recursive: true });
        yield* fs.makeDirectory(NodePath.dirname(binaryPath), { recursive: true });
        yield* fs.writeFileString(realBinaryPath, "#!/bin/sh\n");
        yield* fs.chmod(realBinaryPath, 0o755);
        yield* fs.symlink(realBinaryPath, binaryPath);

        const instance = yield* CodexDriver.create({
          instanceId: ProviderInstanceId.make("codex-installer"),
          displayName: "Codex installer test",
          enabled: false,
          environment: [],
          config: {
            ...CodexDriver.defaultConfig(),
            binaryPath,
            homePath: NodePath.join(tempDir, "codex-home"),
          },
        });

        const update = (yield* instance.snapshot.resolveMaintenance()).update;
        if (fixture.npmOwned) {
          expect(update).toMatchObject({
            executable: "npm",
            args: [
              "install",
              "-g",
              "--prefix",
              installPath,
              "--allow-scripts=@openai/codex",
              "@openai/codex@latest",
            ],
          });
        } else {
          expect(update).toBeNull();
        }
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, noSpawn)),
    );
  }

  it.effect.each([
    {
      name: "conventional shim",
      dataRoot: "mise",
      commandName: "codex",
      version: "0.153.4",
      nodeFirst: false,
    },
    {
      name: "custom data directory",
      dataRoot: "custom-tool-data",
      commandName: "codex",
      version: "0.153.4",
      nodeFirst: false,
    },
    {
      name: "renamed configured command",
      dataRoot: "mise",
      commandName: "custom-codex",
      version: "0.153.4",
      nodeFirst: false,
    },
    {
      name: "outdated provider",
      dataRoot: "mise",
      commandName: "codex",
      version: "0.153.3",
      nodeFirst: false,
    },
    {
      name: "npm before shim",
      dataRoot: "mise",
      commandName: "codex",
      version: "0.153.4",
      nodeFirst: true,
    },
  ])(
    "does not mistake Homebrew mise for Codex's installer: $name",
    (fixture) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const tempDir = yield* fs
          .makeTempDirectoryScoped({ prefix: "t3-codex-mise-shim-" })
          .pipe(Effect.flatMap((directory) => fs.realPath(directory)));
        const brewPrefix = NodePath.join(tempDir, "homebrew");
        const brewPath = NodePath.join(brewPrefix, "bin", "brew");
        const misePath = NodePath.join(brewPrefix, "Cellar", "mise", "2026.9.1", "bin", "mise");
        const shimDir = NodePath.join(tempDir, fixture.dataRoot, "shims");
        const npmPrefix = NodePath.join(tempDir, "mise", "installs", "node", "24.13.0");
        const npmBin = NodePath.join(npmPrefix, "bin");
        const npmPath = NodePath.join(npmBin, "npm");
        const npmEntry = NodePath.join(
          npmPrefix,
          "lib",
          "node_modules",
          "@openai",
          "codex",
          "bin",
          "codex.js",
        );
        for (const file of [brewPath, misePath, npmPath, npmEntry]) {
          yield* fs.makeDirectory(NodePath.dirname(file), { recursive: true });
          yield* fs.writeFileString(file, "#!/bin/sh\n");
          yield* fs.chmod(file, 0o755);
        }
        yield* fs.makeDirectory(shimDir, { recursive: true });
        yield* fs.makeDirectory(npmBin, { recursive: true });
        yield* fs.symlink(misePath, NodePath.join(shimDir, fixture.commandName));
        yield* fs.symlink(npmEntry, NodePath.join(npmBin, fixture.commandName));
        const lookupPath = [
          ...(fixture.nodeFirst ? [npmBin, shimDir] : [shimDir, npmBin]),
          NodePath.dirname(brewPath),
        ].join(NodePath.delimiter);
        const probes: Array<ReadonlyArray<string>> = [];
        const metadataSpawner = ChildProcessSpawner.make((command) => {
          const isNpmOutdated =
            ChildProcess.isStandardCommand(command) &&
            command.command === npmPath &&
            command.args[0] === "outdated";
          if (
            !ChildProcess.isStandardCommand(command) ||
            (command.command !== brewPath && !isNpmOutdated)
          ) {
            return Effect.die("Provider resolution must not execute a provider or updater");
          }
          if (!isNpmOutdated) probes.push(command.args.map(String));
          const stdout = isNpmOutdated
            ? JSON.stringify({ "@openai/codex": { latest: "0.153.4" } })
            : command.args[0] === "--prefix"
              ? brewPrefix
              : JSON.stringify({ formulae: [{ versions: { stable: "2026.9.1" } }] });
          return Effect.succeed(
            ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(1),
              exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
              isRunning: Effect.succeed(false),
              kill: () => Effect.void,
              unref: Effect.succeed(Effect.void),
              stdin: Sink.drain,
              stdout: Stream.encodeText(Stream.make(stdout)),
              stderr: Stream.empty,
              all: Stream.empty,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
            }),
          );
        });
        const instance = yield* CodexDriver.create({
          instanceId: ProviderInstanceId.make("codex-mise-shim"),
          displayName: "Codex shim test",
          enabled: false,
          environment: [{ name: "PATH", value: lookupPath, sensitive: false }],
          config: {
            ...CodexDriver.defaultConfig(),
            binaryPath: fixture.commandName,
            homePath: NodePath.join(tempDir, "codex-home"),
          },
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, metadataSpawner));
        const capabilities = yield* instance.snapshot
          .resolveMaintenance()
          .pipe(Effect.provideService(HostProcess.Environment, { PATH: "" }));
        const latestVersion = yield* resolveLatestProviderVersion(capabilities).pipe(
          Effect.provideServiceEffect(
            ProviderLatestVersions.ProviderLatestVersions,
            ProviderLatestVersions.make([["@openai/codex", "0.153.4"]]),
          ),
        );
        expect(probes).toEqual([]);
        expect(latestVersion).toBe("0.153.4");
        expect(
          createProviderVersionAdvisory({
            driver: CodexDriver.driverKind,
            currentVersion: fixture.version,
            latestVersion,
            maintenanceCapabilities: capabilities,
          }),
        ).toMatchObject({
          status: fixture.version === "0.153.4" ? "current" : "behind_latest",
          currentVersion: fixture.version,
          latestVersion: "0.153.4",
          canUpdate: fixture.nodeFirst,
        });
        if (fixture.nodeFirst) {
          expect(capabilities.update).toMatchObject({
            executable: "npm",
            args: expect.arrayContaining([
              "--prefix",
              yield* fs.realPath(npmPrefix),
              "@openai/codex@latest",
            ]),
          });
        } else {
          expect(capabilities.update).toBeNull();
        }
      }),
    { skip: windowsHost },
  );

  const revoked: FakeAnswer = {
    error: {
      code: -32603,
      message:
        'failed to fetch codex rate limits: GET https://chatgpt.com/backend-api/wham/usage failed: 401 Unauthorized; body={"error": {"code": "token_revoked"}}',
    },
  };
  const limitsAnswer = (usedPercent: number): FakeAnswer => ({
    result: {
      rateLimits: { limitId: "codex", planType: "pro", primary: { usedPercent } },
      rateLimitsByLimitId: null,
      rateLimitResetCredits: null,
      accountId: null,
    },
  });

  it.effect.skipIf(windowsHost)(
    "skips a retry while a timed-out probe still has a pending request",
    () =>
      Effect.gen(function* () {
        const { shadowHome } = yield* makeShadowHomes();
        const usageGate = yield* Deferred.make<void>();
        const usageRequested = yield* Deferred.make<void>();
        const spawns = yield* Queue.unbounded<void>();
        const fake = makeFakeCodexAppServer({
          codexHome: shadowHome,
          usageGate,
          usageRequested,
          spawns,
        });
        const instance = yield* CodexDriver.create({
          instanceId: ProviderInstanceId.make("codex-timeout-retry"),
          displayName: undefined,
          enabled: true,
          environment: [],
          config: { ...CodexDriver.defaultConfig(), homePath: shadowHome },
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));

        yield* Queue.take(spawns);
        yield* Deferred.await(usageRequested);
        const timedOut = yield* instance.snapshot.streamChanges.pipe(
          Stream.filter(
            (snapshot) =>
              snapshot.usageLimits?.unavailable?.message ===
              "Codex did not answer the usage request.",
          ),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true }),
        );
        yield* TestClock.adjust("21 seconds");
        const last = Option.getOrThrow(yield* Fiber.join(timedOut));
        const retry = yield* Effect.raceFirst(
          instance.snapshot.refresh,
          Queue.take(spawns).pipe(Effect.as(undefined)),
        ).pipe(Effect.ensuring(Deferred.succeed(usageGate, undefined)));
        expect(retry).toMatchObject(last);
        expect(fake.requests.get("initialize")).toBe(1);
      }).pipe(Effect.scoped),
  );

  it.effect.skipIf(windowsHost)("publishes a refused login when model discovery also fails", () =>
    Effect.gen(function* () {
      const { shadowHome } = yield* makeShadowHomes();
      const fake = makeFakeCodexAppServer({
        codexHome: shadowHome,
        answers: {
          "model/list": () => ({ error: { code: -32603, message: "catalog unavailable" } }),
          "account/rateLimits/read": () => revoked,
        },
      });
      const instance = yield* CodexDriver.create({
        instanceId: ProviderInstanceId.make("codex-refusal-discovery"),
        displayName: undefined,
        enabled: true,
        environment: [],
        config: { ...CodexDriver.defaultConfig(), homePath: shadowHome },
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));

      const refused = yield* instance.snapshot.refresh;
      expect(refused.auth.status).toBe("unauthenticated");
      expect(refused.usageLimits?.unavailable?.message).toBe(
        "The service refused this Codex login.",
      );
    }).pipe(Effect.scoped),
  );

  it.effect.skipIf(windowsHost)(
    "keeps a refused login signed out without its old limits until a read returns limits",
    () =>
      Effect.gen(function* () {
        const { fs, sharedHome, shadowHome } = yield* makeShadowHomes();
        let usage = limitsAnswer(42);
        let config: FakeAnswer = { result: { config: {}, origins: {} } };
        let account: FakeAnswer | undefined;
        const fake = makeFakeCodexAppServer({
          codexHome: shadowHome,
          answers: {
            "account/rateLimits/read": () => usage,
            "config/read": () => config,
            "account/read": () => account,
          },
        });
        const instance = yield* CodexDriver.create({
          instanceId: ProviderInstanceId.make("codex-refused-login"),
          displayName: "Codex refused login",
          enabled: true,
          environment: [],
          config: {
            ...CodexDriver.defaultConfig(),
            homePath: sharedHome,
            shadowHomePath: shadowHome,
          },
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));
        // SHA-256 of the UTF-8 real path of the home the probe ran in.
        const shadowFingerprint = yield* (yield* Digest.Digest).digestHex(
          Digest.sha256,
          new TextEncoder().encode(yield* fs.realPath(shadowHome)),
        );

        const signedIn = yield* instance.snapshot.refresh;
        expect(signedIn.usageLimits?.windows[0]?.usedPercent).toBe(42);
        expect(signedIn.homeFingerprint).toBe(shadowFingerprint);

        usage = revoked;
        const refused = yield* instance.snapshot.refresh;
        expect(refused.status).toBe("error");
        expect(refused.auth.status).toBe("unauthenticated");
        expect(refused.usageLimits?.windows).toEqual([]);
        expect(refused.usageLimits?.unavailable?.message).toBe(
          "The service refused this Codex login.",
        );
        expect(refused.homeFingerprint).toBe(shadowFingerprint);

        // A bare 401 and an unread config are no answer about the login, so it stays refused.
        usage = { error: { code: -32603, message: "401 Unauthorized" } };
        expect((yield* instance.snapshot.refresh).auth.status).toBe("unauthenticated");
        config = { error: { code: -32603, message: "config unavailable" } };
        const unread = yield* instance.snapshot.refresh;
        expect(unread.auth.status).toBe("unauthenticated");
        expect(unread.usageLimits?.windows).toEqual([]);

        // A turn's rate-limit update does not bring the refused login's windows back.
        yield* instance.snapshot.applyUsageLimits({
          checkedAt: "2026-11-22T12:00:00.000Z",
          complete: true,
          windows: [{ id: "primary", kind: "session", label: "Session", usedPercent: 3 }],
        });
        expect((yield* instance.snapshot.getSnapshot).usageLimits?.windows).toEqual([]);

        // Signing out shows a signed-out login, not a refused one.
        config = { result: { config: {}, origins: {} } };
        account = { result: { account: null, requiresOpenaiAuth: true } };
        const signedOut = yield* instance.snapshot.refresh;
        expect(signedOut.auth.status).toBe("unauthenticated");
        expect(signedOut.message).toContain("Codex CLI is not authenticated");
        usage = revoked;
        account = undefined;
        expect((yield* instance.snapshot.refresh).auth.status).toBe("unauthenticated");
        account = { result: { account: { type: "apiKey" }, requiresOpenaiAuth: true } };
        expect((yield* instance.snapshot.refresh).auth.status).toBe("authenticated");
        account = undefined;
        expect((yield* instance.snapshot.refresh).auth.status).toBe("unauthenticated");
        config = { result: { config: { openai_base_url: "https://example.test" }, origins: {} } };
        const customRoute = yield* instance.snapshot.refresh;
        expect(customRoute.auth.status).toBe("authenticated");
        expect(customRoute.usageLimits?.unavailable?.reason).toBe("unsupported");

        config = { result: { config: {}, origins: {} } };
        expect((yield* instance.snapshot.refresh).auth.status).toBe("unauthenticated");
        usage = limitsAnswer(7);
        const recovered = yield* instance.snapshot.refresh;
        expect(recovered.status).toBe("ready");
        expect(recovered.auth.status).toBe("authenticated");
        expect(recovered.usageLimits?.windows[0]?.usedPercent).toBe(7);
      }).pipe(Effect.scoped),
  );

  it.effect.skipIf(windowsHost)(
    "restores a cached refusal for the same login and clears it when the account changes",
    () =>
      Effect.gen(function* () {
        const { fs, shadowHome } = yield* makeShadowHomes();
        const instanceId = ProviderInstanceId.make("codex-cached-refusal");
        const config = yield* ServerConfig.ServerConfig;
        const filePath = NodePath.join(config.providerStatusCacheDir, `${instanceId}.json`);
        let usage = revoked;
        let email: string | null = "test@example.com";
        let workspaceId = "old-workspace";
        let failAccountRead = false;
        const fake = makeFakeCodexAppServer({
          codexHome: shadowHome,
          answers: {
            "account/rateLimits/read": () => usage,
            "account/read": () =>
              failAccountRead
                ? { error: { code: -32603, message: "account read failed" } }
                : {
                    result: {
                      account: { type: "chatgpt", email, planType: "pro" },
                      requiresOpenaiAuth: true,
                      workspaceId,
                    },
                  },
          },
        });
        const check = Effect.scoped(
          CodexDriver.create({
            instanceId,
            displayName: undefined,
            enabled: true,
            environment: [],
            config: { ...CodexDriver.defaultConfig(), homePath: shadowHome },
          }).pipe(
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
            Effect.flatMap((instance) => instance.snapshot.refresh),
          ),
        );

        const refused = yield* check;
        expect(refused.auth.status).toBe("unauthenticated");
        expect(refused.codexLoginRefusal).toMatchObject({
          homeFingerprint: refused.homeFingerprint,
          email: "test@example.com",
          workspaceId: "old-workspace",
          plan: "pro",
          authGeneration: expect.stringMatching(/^[a-f0-9]{64}$/),
        });
        yield* writeProviderStatusCache({ filePath, provider: refused });

        failAccountRead = true;
        const unread = yield* check;
        expect(unread.auth.status).toBe("unknown");
        expect(unread.homeFingerprint).toBeUndefined();
        expect(unread.codexLoginRefusal).toEqual(refused.codexLoginRefusal);
        yield* writeProviderStatusCache({ filePath, provider: unread });

        failAccountRead = false;
        email = "TEST@example.com";
        usage = { error: { code: -32603, message: "401 Unauthorized" } };
        expect((yield* check).auth.status).toBe("unauthenticated");
        email = null;
        expect((yield* check).auth.status).toBe("unknown");
        email = "test@example.com";
        workspaceId = "other-workspace";
        const switched = yield* check;
        expect(switched.auth.status).toBe("authenticated");
        expect(switched.codexLoginRefusal).toBeUndefined();

        workspaceId = "old-workspace";
        yield* fs.writeFileString(NodePath.join(shadowHome, "auth.json"), "{ }\n");
        const relogin = yield* check;
        expect(relogin.auth.status).toBe("authenticated");
        expect(relogin.codexLoginRefusal).toBeUndefined();
      }).pipe(Effect.scoped),
  );

  it.effect.skipIf(windowsHost)(
    "drops previous windows when the reported Codex home resolves to a different directory",
    () =>
      Effect.gen(function* () {
        const { fs, sharedHome, shadowHome } = yield* makeShadowHomes();
        const reportedHome = NodePath.join(NodePath.dirname(shadowHome), "reported-home");
        yield* fs.symlink(shadowHome, reportedHome);
        yield* fs.writeFileString(NodePath.join(sharedHome, "auth.json"), "{}\n");
        yield* fs.copyFile(
          NodePath.join(shadowHome, "models_cache.json"),
          NodePath.join(sharedHome, "models_cache.json"),
        );
        let usage = limitsAnswer(42);
        const fake = makeFakeCodexAppServer({
          codexHome: reportedHome,
          answers: { "account/rateLimits/read": () => usage },
        });
        const instance = yield* CodexDriver.create({
          instanceId: ProviderInstanceId.make("codex-retarget"),
          displayName: undefined,
          enabled: true,
          environment: [],
          config: { ...CodexDriver.defaultConfig(), homePath: "~/shadow" },
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
          Effect.provideService(HostProcess.HomeDirectory, NodePath.dirname(shadowHome)),
        );

        const digest = yield* Digest.Digest;
        const initial = yield* instance.snapshot.refresh;
        expect(initial.homeFingerprint).toBe(
          yield* digest.digestHex(
            Digest.sha256,
            new TextEncoder().encode(yield* fs.realPath(shadowHome)),
          ),
        );
        expect(initial.usageLimits?.windows[0]?.usedPercent).toBe(42);
        yield* instance.snapshot.applyUsageLimits({
          checkedAt: "2999-01-01T00:00:00.000Z",
          complete: true,
          windows: [{ id: "primary", kind: "session", label: "Session", usedPercent: 3 }],
        });
        expect((yield* instance.snapshot.getSnapshot).usageLimits).toEqual(initial.usageLimits);

        usage = { error: { code: -32603, message: "429 Too Many Requests" } };
        expect((yield* instance.snapshot.refresh).usageLimits?.unavailable?.reason).toBe(
          "probeFailed",
        );

        yield* fs.remove(reportedHome);
        yield* fs.symlink(sharedHome, reportedHome);
        usage = limitsAnswer(87);
        const retargeted = yield* instance.snapshot.refresh;
        expect(retargeted.usageLimits).toBeUndefined();
        expect(retargeted.homeFingerprint).toBeUndefined();

        yield* fs.remove(reportedHome);
        yield* fs.symlink(shadowHome, reportedHome);
        const afterRetarget = yield* instance.snapshot.refresh;
        expect(afterRetarget.homeFingerprint).toBe(initial.homeFingerprint);
        expect(afterRetarget.usageLimits?.windows[0]?.usedPercent).toBe(87);
      }).pipe(Effect.scoped),
  );

  it.effect.skipIf(windowsHost)("withholds usage when auth.json is missing or unreadable", () =>
    Effect.gen(function* () {
      const { fs, shadowHome } = yield* makeShadowHomes();
      const authPath = NodePath.join(yield* fs.realPath(shadowHome), "auth.json");
      const fake = makeFakeCodexAppServer({ codexHome: shadowHome });
      const check = (fileSystem: FileSystem.FileSystem) =>
        CodexDriver.create({
          instanceId: ProviderInstanceId.make("codex-unverified-auth"),
          displayName: undefined,
          enabled: true,
          environment: [],
          config: { ...CodexDriver.defaultConfig(), homePath: shadowHome },
        }).pipe(
          Effect.flatMap((instance) => instance.snapshot.refresh),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.scoped,
        );

      const unreadable = yield* check({
        ...fs,
        access: (path, options) =>
          path === authPath && options?.readable
            ? Effect.fail(
                PlatformError.systemError({
                  _tag: "PermissionDenied",
                  module: "FileSystem",
                  method: "access",
                }),
              )
            : fs.access(path, options),
      });
      yield* fs.remove(authPath);
      const missing = yield* check(fs);

      for (const snapshot of [missing, unreadable]) {
        expect(snapshot.usageLimits).toBeUndefined();
        expect(snapshot.homeFingerprint).toBeUndefined();
      }
    }).pipe(Effect.scoped),
  );

  it.effect.skipIf(windowsHost)(
    "refreshes on the provider health interval without a client, also after a config failure",
    () =>
      withHealthInterval(
        Effect.gen(function* () {
          const { sharedHome, shadowHome } = yield* makeShadowHomes();
          const instanceId = ProviderInstanceId.make("codex-unattended-refresh");
          let config: FakeAnswer = { result: { config: {}, origins: {} } };
          const fake = makeFakeCodexAppServer({
            codexHome: shadowHome,
            answers: { "config/read": () => config },
          });
          const reads = () => fake.requests.get("account/rateLimits/read") ?? 0;
          const instance = yield* CodexDriver.create({
            instanceId,
            displayName: "Codex unattended refresh",
            enabled: true,
            environment: [],
            config: {
              ...CodexDriver.defaultConfig(),
              homePath: sharedHome,
              shadowHomePath: shadowHome,
            },
          }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));
          yield* instance.snapshot.refresh;

          // No client asks for provider status in this test.
          const nextCheck = Effect.gen(function* () {
            yield* awaitInstanceCodexStatusProbe(instanceId);
            const before = (yield* instance.snapshot.getSnapshot).checkedAt;
            const scheduled = yield* instance.snapshot.streamChanges.pipe(
              Stream.filter((provider) => provider.checkedAt !== before),
              Stream.runHead,
              Effect.forkChild({ startImmediately: true }),
            );
            yield* TestClock.adjust("5 minutes");
            return Option.getOrThrow(yield* Fiber.join(scheduled));
          });
          config = { error: { code: -32603, message: "config unavailable" } };
          const beforeFailure = reads();
          const configFailed = yield* nextCheck;
          expect(reads()).toBe(beforeFailure);
          expect(configFailed.usageLimits?.unavailable?.reason).toBe("probeFailed");
          config = { result: { config: {}, origins: {} } };
          const readsBefore = reads();
          yield* nextCheck;
          expect(reads()).toBe(readsBefore + 1);

          config = { result: { config: { openai_base_url: "https://example.test" }, origins: {} } };
          yield* awaitInstanceCodexStatusProbe(instanceId);
          const custom = yield* instance.snapshot.refresh;
          expect(custom.usageLimits?.unavailable?.reason).toBe("unsupported");
          const spawns = fake.requests.get("initialize");
          yield* awaitInstanceCodexStatusProbe(instanceId);
          yield* TestClock.adjust("6 minutes");
          expect(fake.requests.get("initialize")).toBe(spawns);
          config = { result: { config: {}, origins: {} } };
          const recovered = yield* instance.snapshot.streamChanges.pipe(
            Stream.filter((snapshot) => snapshot.usageLimits?.windows[0]?.usedPercent === 42),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          yield* TestClock.adjust("24 minutes");
          expect(Option.getOrThrow(yield* Fiber.join(recovered)).auth.status).toBe("authenticated");
        }).pipe(Effect.scoped),
        5 * 60_000,
      ),
  );

  it.effect.skipIf(windowsHost)(
    "preserves the cached refusal across a rebuild during teardown and a restart",
    () =>
      withHealthInterval(
        Effect.gen(function* () {
          const { shadowHome } = yield* makeShadowHomes();
          const instanceId = ProviderInstanceId.make("codex-rebuild-during-teardown");
          const startupGate = yield* Deferred.make<void>();
          yield* Effect.addFinalizer(() => Deferred.succeed(startupGate, undefined));
          let usage = revoked;
          let failAccountRead = false;
          const fake = makeFakeCodexAppServer({
            codexHome: shadowHome,
            startupGate: Deferred.await(startupGate),
            answers: {
              "account/rateLimits/read": () => usage,
              "account/read": () =>
                failAccountRead
                  ? { error: { code: -32603, message: "account read failed" } }
                  : undefined,
            },
          });
          const make = CodexDriver.create({
            instanceId,
            displayName: undefined,
            enabled: true,
            environment: [],
            config: { ...CodexDriver.defaultConfig(), homePath: shadowHome },
          }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));

          const rootScope = yield* Effect.scope;
          const retiredScope = yield* Scope.fork(rootScope);
          const original = yield* make.pipe(Scope.provide(retiredScope));
          const refused = yield* original.snapshot.refresh;
          expect(refused.auth.status).toBe("unauthenticated");
          expect(refused.codexLoginRefusal).toBeDefined();

          const active = yield* Ref.make(original);
          const changes = yield* PubSub.unbounded<void>();
          const registryContext = yield* Layer.build(
            ProviderRegistry.layer.pipe(
              Layer.provide(
                Layer.succeed(ProviderInstanceRegistry.ProviderInstanceRegistry, {
                  getInstance: () => Ref.get(active),
                  listInstances: Ref.get(active).pipe(Effect.map((instance) => [instance])),
                  listUnavailable: Effect.succeed([]),
                  streamChanges: Stream.fromPubSub(changes),
                  subscribeChanges: PubSub.subscribe(changes),
                }),
              ),
              Layer.provide(
                Layer.mock(ProviderInstanceRegistryMutator.ProviderInstanceRegistryMutator)({
                  rebuild: () => Effect.void,
                }),
              ),
            ),
          ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));
          const registry = yield* ProviderRegistry.ProviderRegistry.pipe(
            Effect.provide(registryContext),
          );
          const config = yield* ServerConfig.ServerConfig;
          const cachePath = NodePath.join(config.providerStatusCacheDir, `${instanceId}.json`);
          expect((yield* readProviderStatusCache(cachePath))?.codexLoginRefusal).toEqual(
            refused.codexLoginRefusal,
          );

          yield* Scope.close(retiredScope, Exit.void);
          usage = { error: { code: -32603, message: "401 Unauthorized" } };
          const replacementScope = yield* Scope.fork(rootScope);
          const replacement = yield* make.pipe(Scope.provide(replacementScope));
          yield* Ref.set(active, replacement);
          yield* PubSub.publish(changes, undefined);
          expect((yield* registry.refreshInstance(instanceId))[0]?.codexLoginRefusal).toEqual(
            refused.codexLoginRefusal,
          );
          expect((yield* readProviderStatusCache(cachePath))?.codexLoginRefusal).toEqual(
            refused.codexLoginRefusal,
          );
          expect(fake.requests.get("initialize")).toBe(1);

          const retried = yield* registry.streamChanges.pipe(
            Stream.filter((providers) => providers[0]?.checkedAt !== refused.checkedAt),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          yield* TestClock.adjust("1 second");
          yield* Deferred.succeed(startupGate, undefined);
          expect(Option.getOrThrow(yield* Fiber.join(retried))[0]?.auth.status).toBe(
            "unauthenticated",
          );
          expect(fake.requests.get("initialize")).toBe(2);
          expect((yield* readProviderStatusCache(cachePath))?.codexLoginRefusal).toEqual(
            refused.codexLoginRefusal,
          );

          yield* Scope.close(replacementScope, Exit.void);
          failAccountRead = true;
          const restarted = yield* make;
          const firstAfterRestart = yield* restarted.snapshot.streamChanges.pipe(
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          const inconclusive = Option.getOrThrow(yield* Fiber.join(firstAfterRestart));
          expect(inconclusive.auth.status).toBe("unknown");
          expect(inconclusive.codexLoginRefusal).toEqual(refused.codexLoginRefusal);
        }).pipe(Effect.scoped),
        0,
      ),
  );

  it.effect.skipIf(windowsHost)(
    "withholds a reading when the reported home retargets during a probe",
    () =>
      Effect.gen(function* () {
        const { fs, sharedHome, shadowHome } = yield* makeShadowHomes();
        const reportedHome = NodePath.join(NodePath.dirname(shadowHome), "reported-home");
        yield* fs.symlink(shadowHome, reportedHome);
        yield* fs.copyFile(
          NodePath.join(shadowHome, "models_cache.json"),
          NodePath.join(sharedHome, "models_cache.json"),
        );
        const usageGate = yield* Deferred.make<void>();
        const usageRequested = yield* Deferred.make<void>();
        const fake = makeFakeCodexAppServer({ codexHome: reportedHome, usageGate, usageRequested });
        const instance = yield* CodexDriver.create({
          instanceId: ProviderInstanceId.make("codex-retarget-during-probe"),
          displayName: undefined,
          enabled: true,
          environment: [],
          config: { ...CodexDriver.defaultConfig(), homePath: shadowHome },
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));
        const check = yield* instance.snapshot.streamChanges.pipe(
          Stream.runHead,
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.await(usageRequested);
        yield* fs.remove(reportedHome);
        yield* fs.symlink(sharedHome, reportedHome);
        yield* Deferred.succeed(usageGate, undefined);
        const result = Option.getOrThrow(yield* Fiber.join(check));
        expect(result.auth.status).toBe("unknown");
        expect(result.homeFingerprint).toBeUndefined();
        expect(result.usageLimits).toBeUndefined();
      }).pipe(Effect.scoped),
  );

  it.effect.skipIf(windowsHost)("defers unattended checks while a skills app-server is live", () =>
    withHealthInterval(
      Effect.gen(function* () {
        const { shadowHome } = yield* makeShadowHomes();
        const fake = makeFakeCodexAppServer({ codexHome: shadowHome });
        const instance = yield* CodexDriver.create({
          instanceId: ProviderInstanceId.make("codex-live-skills"),
          displayName: undefined,
          enabled: true,
          environment: [],
          config: { ...CodexDriver.defaultConfig(), homePath: shadowHome },
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));

        const checked = yield* instance.snapshot.refresh;
        const before = fake.requests.get("initialize") ?? 0;
        yield* TestClock.adjust("55 seconds");
        expect(fake.requests.get("initialize")).toBe(before);
        // RcMap lookups run with the caller's services, so the scan needs the fake spawner too.
        yield* instance.snapshotForCwd!(shadowHome).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
        );
        expect(fake.requests.get("initialize")).toBe(before + 1);
        yield* TestClock.adjust("5 seconds");
        expect(fake.requests.get("initialize")).toBe(before + 1);
        expect((yield* instance.snapshot.getSnapshot).checkedAt).toBe(checked.checkedAt);
        yield* instance.snapshot.refresh;
        expect(fake.requests.get("initialize")).toBe(before + 2);
      }).pipe(Effect.scoped),
      1_000,
    ),
  );

  for (const { name, clientDemand } of [
    { name: "without a client", clientDemand: false },
    { name: "with a client holding a lease", clientDemand: true },
  ]) {
    it.effect.skipIf(windowsHost)(
      `checks at the unsupported-route deadline even with a longer interval, ${name}`,
      () =>
        withHealthInterval(
          Effect.gen(function* () {
            const { shadowHome } = yield* makeShadowHomes();
            const instanceId = ProviderInstanceId.make("codex-unsupported-deadline");
            let config = { openai_base_url: "https://example.test" };
            const fake = makeFakeCodexAppServer({
              codexHome: shadowHome,
              answers: { "config/read": () => ({ result: { config, origins: {} } }) },
            });
            const instance = yield* CodexDriver.create({
              instanceId,
              displayName: undefined,
              enabled: true,
              environment: [],
              config: { ...CodexDriver.defaultConfig(), homePath: shadowHome },
            }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));
            expect((yield* instance.snapshot.refresh).usageLimits?.unavailable?.reason).toBe(
              "unsupported",
            );
            yield* awaitInstanceCodexStatusProbe(instanceId);
            const before = fake.requests.get("initialize");
            config = { openai_base_url: "https://api.openai.com" };
            const recovered = yield* instance.snapshot.streamChanges.pipe(
              Stream.filter((provider) => provider.usageLimits?.windows[0]?.usedPercent === 42),
              Stream.runHead,
              Effect.forkChild({ startImmediately: true }),
            );
            yield* TestClock.adjust("1799 seconds");
            expect(fake.requests.get("initialize")).toBe(before);
            yield* TestClock.adjust("1 second");
            expect(Option.getOrThrow(yield* Fiber.join(recovered)).auth.status).toBe(
              "authenticated",
            );
          }).pipe(Effect.scoped),
          60 * 60_000,
          { clientDemand },
        ),
    );
  }

  for (const change of ["replacement", "sign-out", "replacement during startup"] as const) {
    it.effect.skipIf(windowsHost)(`withholds usage for a ${change} in an active probe`, () =>
      withHealthInterval(
        Effect.gen(function* () {
          const { fs, shadowHome } = yield* makeShadowHomes();
          const authPath = NodePath.join(shadowHome, "auth.json");
          const instanceId = ProviderInstanceId.make("codex-auth-change-mid-check");
          let replaceDuringProbe = false;
          let replaced = false;
          let replacedDuringStartup = false;
          const replace = () => {
            if (!replaceDuringProbe || replaced) return false;
            replaced = true;
            if (change === "sign-out") NodeFS.rmSync(authPath);
            else NodeFS.writeFileSync(authPath, '{"replacement":true}\n');
            return true;
          };
          const fake = makeFakeCodexAppServer({
            codexHome: shadowHome,
            answers: {
              initialize: () => {
                replacedDuringStartup = change === "replacement during startup" && replace();
                return undefined;
              },
              "account/read": () => ({
                result: {
                  account:
                    replaced && change === "sign-out"
                      ? null
                      : {
                          type: "chatgpt",
                          email:
                            replaced && !replacedDuringStartup
                              ? "after@example.test"
                              : "before@example.test",
                          planType: "pro",
                        },
                  requiresOpenaiAuth: true,
                },
              }),
              "account/rateLimits/read": () => {
                if (replacedDuringStartup || replace()) return limitsAnswer(99);
                return limitsAnswer(replaced ? 77 : 42);
              },
            },
          });
          const instance = yield* CodexDriver.create({
            instanceId,
            displayName: undefined,
            enabled: true,
            environment: [],
            config: { ...CodexDriver.defaultConfig(), homePath: shadowHome },
          }).pipe(
            Effect.provideService(FileSystem.FileSystem, { ...fs, watch: () => Stream.empty }),
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
          );
          expect((yield* instance.snapshot.refresh).usageLimits?.windows[0]?.usedPercent).toBe(42);
          yield* awaitInstanceCodexStatusProbe(instanceId);

          const accountReadsBeforeReplacement = fake.requests.get("account/read") ?? 0;
          const verified = yield* instance.snapshot.streamChanges.pipe(
            Stream.filter((snapshot) =>
              change === "sign-out"
                ? snapshot.auth.status === "unauthenticated"
                : snapshot.auth.email === "after@example.test",
            ),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          replaceDuringProbe = true;
          const unverified = yield* instance.snapshot.refresh;
          expect(unverified.auth.status).toBe("unknown");
          expect(unverified.usageLimits).toBeUndefined();
          const recovered = Option.getOrThrow(yield* Fiber.join(verified));
          expect(recovered.usageLimits?.windows.map(({ usedPercent }) => usedPercent)).toEqual(
            change === "sign-out" ? [] : [77],
          );
          expect(fake.requests.get("account/read")).toBe(accountReadsBeforeReplacement + 2);
        }).pipe(Effect.scoped),
        0,
      ),
    );
  }

  for (const nextLogin of ["apiKey", "workspace", "no account identifiers"] as const) {
    it.effect.skipIf(windowsHost)(
      `drops native windows after a credential replacement with ${nextLogin}`,
      () =>
        withHealthInterval(
          Effect.gen(function* () {
            const { fs, shadowHome } = yield* makeShadowHomes();
            const instanceId = ProviderInstanceId.make("codex-credential-replacement");
            const authPath = NodePath.join(shadowHome, "auth.json");
            let changed = false;
            const fake = makeFakeCodexAppServer({
              codexHome: shadowHome,
              answers: {
                "account/read": () => ({
                  result: {
                    account:
                      changed && nextLogin === "apiKey"
                        ? { type: "apiKey" }
                        : {
                            type: "chatgpt",
                            email: "same@example.test",
                            planType: "pro",
                            ...(nextLogin === "no account identifiers"
                              ? {}
                              : { workspaceId: changed ? "workspace-b" : "workspace-a" }),
                          },
                    requiresOpenaiAuth: true,
                  },
                }),
                "account/rateLimits/read": () =>
                  changed
                    ? { error: { code: -32603, message: "Temporary failure." } }
                    : {
                        result: {
                          rateLimits: { limitId: "codex", primary: { usedPercent: 42 } },
                          accountId: nextLogin === "no account identifiers" ? null : "workspace-a",
                        },
                      },
              },
            });
            const instance = yield* CodexDriver.create({
              instanceId,
              displayName: undefined,
              enabled: true,
              environment: [],
              config: { ...CodexDriver.defaultConfig(), homePath: shadowHome },
            }).pipe(
              Effect.provideService(FileSystem.FileSystem, { ...fs, watch: () => Stream.empty }),
              Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
            );
            const first = yield* instance.snapshot.refresh;
            expect(first.usageLimits?.windows[0]?.usedPercent).toBe(42);
            expect(first.auth.workspaceId).toBe(
              nextLogin === "no account identifiers" ? undefined : "workspace-a",
            );
            yield* awaitInstanceCodexStatusProbe(instanceId);

            changed = true;
            yield* fs.writeFileString(authPath, '{"replacement":true}\n');
            const next = yield* instance.snapshot.refresh;
            expect(next.usageLimits?.windows).toEqual([]);
            expect(next.auth.type).toBe(nextLogin === "apiKey" ? "apiKey" : "chatgpt");
            expect(next.auth.workspaceId).toBe(
              nextLogin === "workspace" ? "workspace-b" : undefined,
            );
            expect(next.usageLimits?.unavailable?.reason).toBe(
              nextLogin === "apiKey" ? "unsupported" : "probeFailed",
            );
          }).pipe(Effect.scoped),
          0,
        ),
    );
  }

  it.effect.skipIf(windowsHost)(
    "clears usage for a credential replacement even when a periodic failure is deferred",
    () =>
      withHealthInterval(
        Effect.gen(function* () {
          const { fs, shadowHome } = yield* makeShadowHomes();
          const instanceId = ProviderInstanceId.make("codex-credential-change-periodic-failure");
          const failedProbeClosing = yield* Deferred.make<void>();
          const releaseProbe = yield* Deferred.make<void>();
          yield* Effect.addFinalizer(() => Deferred.succeed(releaseProbe, undefined));
          let accountReadFails = false;
          const fake = makeFakeCodexAppServer({
            codexHome: shadowHome,
            startupGate: Effect.suspend(() =>
              accountReadFails
                ? Deferred.succeed(failedProbeClosing, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseProbe)),
                  )
                : Effect.void,
            ),
            answers: {
              "account/read": () =>
                accountReadFails
                  ? { error: { code: -32603, message: "Temporary failure." } }
                  : undefined,
            },
          });
          const instance = yield* CodexDriver.create({
            instanceId,
            displayName: undefined,
            enabled: true,
            environment: [],
            config: { ...CodexDriver.defaultConfig(), homePath: shadowHome },
          }).pipe(
            Effect.provideService(FileSystem.FileSystem, { ...fs, watch: () => Stream.empty }),
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
          );
          const firstCheck = yield* instance.snapshot.streamChanges.pipe(
            Stream.filter((snapshot) => snapshot.usageLimits?.windows[0]?.usedPercent === 42),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          if (!(yield* instance.snapshot.getSnapshot).usageLimits?.windows.length)
            yield* Fiber.join(firstCheck);
          yield* awaitInstanceCodexStatusProbe(instanceId);

          accountReadFails = true;
          yield* fs.writeFileString(NodePath.join(shadowHome, "auth.json"), '{"login":2}\n');
          yield* TestClock.adjust("60 seconds");
          yield* Deferred.await(failedProbeClosing);
          const snapshot = yield* instance.snapshot.getSnapshot;
          expect(snapshot.status).toBe("ready");
          expect(snapshot.usageLimits).toBeUndefined();
        }).pipe(Effect.scoped),
        30_000,
        { clientDemand: true },
      ),
  );

  it.effect.skipIf(windowsHost)(
    "bounds retries when every probe rewrites auth.json at interval zero",
    () =>
      withHealthInterval(
        Effect.gen(function* () {
          const { fs, shadowHome } = yield* makeShadowHomes();
          const authPath = NodePath.join(shadowHome, "auth.json");
          const instanceId = ProviderInstanceId.make("codex-rewriting-probe");
          const spawns = yield* Queue.unbounded<void>();
          let rewrite = true;
          let reads = 0;
          const fake = makeFakeCodexAppServer({
            codexHome: shadowHome,
            spawns,
            answers: {
              "account/rateLimits/read": () => {
                reads++;
                if (rewrite) NodeFS.writeFileSync(authPath, " ".repeat(reads + 3) + "{}");
                return limitsAnswer(42);
              },
            },
          });
          const instance = yield* CodexDriver.create({
            instanceId,
            displayName: undefined,
            enabled: true,
            environment: [],
            config: { ...CodexDriver.defaultConfig(), homePath: shadowHome },
          }).pipe(
            Effect.provideService(FileSystem.FileSystem, { ...fs, watch: () => Stream.empty }),
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
          );
          yield* Queue.take(spawns);
          yield* Queue.take(spawns);
          yield* awaitInstanceCodexStatusProbe(instanceId);
          yield* TestClock.adjust("10 minutes");
          expect(reads).toBe(2);
          expect((yield* instance.snapshot.getSnapshot).usageLimits).toBeUndefined();

          rewrite = false;
          expect((yield* instance.snapshot.refresh).usageLimits?.windows[0]?.usedPercent).toBe(42);
          expect(reads).toBe(3);
        }).pipe(Effect.scoped),
        0,
      ),
  );

  it.effect.skipIf(windowsHost)("publishes a disable while the previous probe is closing", () =>
    Effect.gen(function* () {
      const { shadowHome } = yield* makeShadowHomes();
      const startupGate = yield* Deferred.make<void>();
      yield* Effect.addFinalizer(() => Deferred.succeed(startupGate, undefined));
      const fake = makeFakeCodexAppServer({
        codexHome: shadowHome,
        startupGate: Deferred.await(startupGate),
      });
      const make = (enabled: boolean) =>
        CodexDriver.create({
          instanceId: ProviderInstanceId.make("codex-disable-during-teardown"),
          displayName: undefined,
          enabled,
          environment: [],
          config: { ...CodexDriver.defaultConfig(), homePath: shadowHome },
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));
      const retiredScope = yield* Scope.fork(yield* Effect.scope);
      const original = yield* make(true).pipe(Scope.provide(retiredScope));
      expect((yield* original.snapshot.refresh).auth.status).toBe("authenticated");
      yield* Scope.close(retiredScope, Exit.void);

      const disabled = yield* make(false);
      const published = yield* disabled.snapshot.refresh;
      expect(published.enabled).toBe(false);
      expect(published.message).toBe("Codex is disabled in T3 Code settings.");
      expect(yield* disabled.snapshot.isAwaitingFirstCheck!).toBe(false);
      expect(fake.requests.get("initialize")).toBe(1);
    }).pipe(Effect.scoped),
  );

  it.effect.skipIf(windowsHost)(
    "runs one credential refresh after the instance's closing probe stops",
    () =>
      Effect.gen(function* () {
        const { shadowHome } = yield* makeShadowHomes();
        const startupGate = yield* Deferred.make<void>();
        yield* Effect.addFinalizer(() => Deferred.succeed(startupGate, undefined));
        const fake = makeFakeCodexAppServer({
          codexHome: shadowHome,
          startupGate: Deferred.await(startupGate),
        });
        const instanceId = ProviderInstanceId.make("codex-credential-coalesce");
        yield* checkInstanceCodexProviderStatus(
          instanceId,
          { ...CodexDriver.defaultConfig(), enabled: true, homePath: shadowHome },
          {},
          {},
        ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));

        // Each run records whether the earlier probe had already closed.
        const runs = yield* Queue.unbounded<boolean>();
        const request = yield* makeCodexCredentialRefresh(
          instanceId,
          Deferred.isDone(startupGate).pipe(Effect.flatMap((closed) => Queue.offer(runs, closed))),
        );
        yield* request;
        yield* request;
        yield* request;
        // Lets the refresh fiber act on the requests before the probe can close.
        yield* TestClock.adjust("0 millis");
        yield* Deferred.succeed(startupGate, undefined);
        expect(yield* Queue.take(runs)).toBe(true);
      }).pipe(Effect.scoped),
  );

  it.effect.skipIf(windowsHost)(
    "clears usage before teardown and only follows up once for credential changes",
    () =>
      Effect.gen(function* () {
        const { fs, shadowHome } = yield* makeShadowHomes();
        const startupGate = yield* Deferred.make<void>();
        const followUpGate = yield* Deferred.make<void>();
        yield* Effect.addFinalizer(() => Deferred.succeed(startupGate, undefined));
        yield* Effect.addFinalizer(() => Deferred.succeed(followUpGate, undefined));
        const homeEvents = yield* Queue.unbounded<FileSystem.WatchEvent>();
        const spawns = yield* Queue.unbounded<void>();
        let email = "before@example.com";
        let closingProbes = 0;
        const instanceId = ProviderInstanceId.make("codex-credential-refresh");
        const fake = makeFakeCodexAppServer({
          codexHome: shadowHome,
          startupGate: Effect.suspend(() =>
            Deferred.await(++closingProbes === 1 ? startupGate : followUpGate),
          ),
          spawns,
          answers: {
            "account/read": () => ({
              result: {
                account: { type: "chatgpt", email, planType: "pro" },
                requiresOpenaiAuth: true,
              },
            }),
          },
        });
        const instance = yield* CodexDriver.create({
          instanceId,
          displayName: undefined,
          enabled: true,
          environment: [],
          config: { ...CodexDriver.defaultConfig(), homePath: shadowHome },
        }).pipe(
          Effect.provideService(FileSystem.FileSystem, {
            ...fs,
            watch: () => Stream.fromQueue(homeEvents),
          }),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
        );
        const initial = yield* instance.snapshot.refresh;
        expect(initial.auth.email).toBe("before@example.com");
        expect(initial.usageLimits?.windows[0]?.usedPercent).toBe(42);
        yield* Queue.take(spawns);

        email = "after@example.com";
        const authPath = NodePath.join(shadowHome, "auth.json");
        yield* fs.writeFileString(authPath, '{"login":2}\n');
        const cleared = yield* instance.snapshot.streamChanges.pipe(
          Stream.filter((snapshot) => snapshot.usageLimits === undefined),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Queue.offer(homeEvents, { _tag: "Update", path: authPath });
        yield* Fiber.join(cleared);
        expect((yield* instance.snapshot.getSnapshot).usageLimits).toBeUndefined();
        expect(fake.requests.get("initialize")).toBe(1);

        const reprobed = yield* instance.snapshot.streamChanges.pipe(
          Stream.filter((snapshot) => snapshot.auth.email === "after@example.com"),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.succeed(startupGate, undefined);
        yield* Queue.take(spawns);
        expect(
          Option.getOrThrow(yield* Fiber.join(reprobed)).usageLimits?.windows[0]?.usedPercent,
        ).toBe(42);
        expect(fake.requests.get("initialize")).toBe(2);

        // This replacement lands after the follow-up's account read but before its teardown.
        email = "third@example.com";
        yield* fs.writeFileString(authPath, '{"login":"third"}\n');
        yield* Deferred.succeed(followUpGate, undefined);
        yield* awaitInstanceCodexStatusProbe(instanceId);
        expect((yield* instance.snapshot.getSnapshot).usageLimits).toBeUndefined();

        yield* Queue.offer(homeEvents, { _tag: "Update", path: authPath });
        yield* TestClock.adjust("1 second");
        expect(fake.requests.get("initialize")).toBe(2);
        expect((yield* instance.snapshot.refresh).auth.email).toBe("third@example.com");
      }).pipe(Effect.scoped),
  );

  it.effect("probes again when auth.json in the Codex home changes", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const codexHome = yield* fs.makeTempDirectoryScoped({ prefix: "t3-codex-auth-watch-" });
      const watchedPath = yield* Deferred.make<string>();
      const homeEvents = yield* Queue.unbounded<FileSystem.WatchEvent>();
      const probes = yield* Queue.unbounded<void>();
      yield* CodexDriver.create({
        instanceId: ProviderInstanceId.make("codex-auth-watch"),
        displayName: "Codex auth watch",
        enabled: true,
        environment: [],
        config: { ...CodexDriver.defaultConfig(), homePath: codexHome },
      }).pipe(
        // OS watchers can miss events written right after they start, so the test supplies them.
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          watch: (path) =>
            Stream.unwrap(
              Deferred.succeed(watchedPath, path).pipe(Effect.as(Stream.fromQueue(homeEvents))),
            ),
        }),
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make((command) =>
            (ChildProcess.isStandardCommand(command) && command.args.includes("app-server")
              ? Queue.offer(probes, undefined)
              : Effect.void
            ).pipe(
              Effect.andThen(
                PlatformError.badArgument({
                  module: "ChildProcessSpawner",
                  method: "spawn",
                  description: "The fixture has no Codex executable",
                }),
              ),
            ),
          ),
        ),
      );
      expect(yield* Deferred.await(watchedPath)).toBe(codexHome);
      yield* Queue.take(probes);

      yield* fs.writeFileString(NodePath.join(codexHome, "auth.json"), "{}\n");
      yield* Queue.offer(homeEvents, { _tag: "Create", path: "auth.json" });
      yield* TestClock.adjust("250 millis");

      yield* Queue.take(probes);
    }),
  );
});
