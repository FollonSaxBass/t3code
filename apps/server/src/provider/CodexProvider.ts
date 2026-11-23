// @effect-diagnostics nodeBuiltinImport:off - Fingerprints hash a resolved local filesystem path.
import * as NodeCrypto from "node:crypto";

import { resolveProviderProbeTimeoutMs } from "@t3tools/provider-core/server/snapshotProbe";
import { pipe } from "effect/Function";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as RcMap from "effect/RcMap";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Types from "effect/Types";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexSchema from "effect-codex-app-server/schema";
import * as CodexErrors from "effect-codex-app-server/errors";

import type {
  CodexSettings,
  CustomModelSetting,
  ServerProvider,
  ProviderInstanceId,
  ServerProviderState,
  ModelCapabilities,
  ProviderOptionDescriptor,
  ServerProviderModel,
  ServerProviderSkill,
} from "@t3tools/contracts";
import { PREFERRED_DEFAULT_CODEX_MODELS, ServerSettingsError } from "@t3tools/contracts";

import {
  codexModelFamily,
  createModelCapabilities,
  formatCodexModelName,
} from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { codexAppServerArgs, resolveCodexLaunchArgs } from "./codexLaunchArgs.ts";
import {
  buildServerProvider,
  COMPACT_SLASH_COMMAND,
  DEFAULT_TIMEOUT_MS,
  providerModelsFromSettings,
  providerProbeEnvironment,
  spawnAndCollect,
  type ServerProviderDraft,
} from "@t3tools/provider-core/server/snapshotProbe";
import {
  ClearProviderUsage,
  UnattendedProviderCheck,
} from "@t3tools/provider-core/server/managedProvider";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import { makeUnavailableUsageLimits } from "@t3tools/provider-core/server/usageLimits";
import {
  CODEX_LOGIN_REFUSED_USAGE_MESSAGE,
  codexRateLimitsFailureMessage,
  codexRateLimitsToLimits,
  isCodexLoginRefusal,
  type CodexRateLimitSnapshot,
  type CodexResetCreditsSummary,
} from "./codexUsageLimits.ts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import packageJson from "../../package.json" with { type: "json" };
const isCodexAppServerSpawnError = Schema.is(CodexErrors.CodexAppServerSpawnError);

type CodexRateLimitsProbe =
  | {
      readonly snapshot: CodexRateLimitSnapshot;
      readonly rateLimitsByLimitId?:
        | Readonly<Record<string, CodexRateLimitSnapshot>>
        | null
        | undefined;
      readonly resetCredits: CodexResetCreditsSummary | null | undefined;
      readonly accountId?: string | null | undefined;
    }
  | { readonly failure: string; readonly loginRefused?: true };

const CodexRoutingConfig = Schema.Struct({
  model_provider: Schema.optionalKey(Schema.NullOr(Schema.String)),
  openai_base_url: Schema.optionalKey(Schema.NullOr(Schema.String)),
  model_providers: Schema.optionalKey(
    Schema.Record(
      Schema.String,
      Schema.Struct({ base_url: Schema.optionalKey(Schema.NullOr(Schema.String)) }),
    ),
  ),
});
const decodeCodexRoutingConfig = Schema.decodeUnknownOption(CodexRoutingConfig);

const CodexCyberAccessConfig = Schema.Struct({
  daybreak: Schema.optionalKey(Schema.NullOr(Schema.Boolean)),
  features: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        api_key_cyber_access_programs: Schema.optionalKey(Schema.NullOr(Schema.Boolean)),
      }),
    ),
  ),
});
const decodeCodexCyberAccessConfig = Schema.decodeUnknownOption(CodexCyberAccessConfig);

// Null is Codex's auth-dependent OpenAI default; undefined is a route that cannot be read.
function codexRouteBaseUrl(config: unknown): string | null | undefined {
  const decoded = decodeCodexRoutingConfig(config);
  if (Option.isNone(decoded)) return undefined;
  const routing = decoded.value;
  const providerId = routing.model_provider ?? "openai";
  if (providerId === "openai") return routing.openai_base_url || null;
  const provider = routing.model_providers?.[providerId];
  return provider ? (provider.base_url ?? null) : undefined;
}

function routesToNativeOpenai(config: unknown): boolean {
  const baseUrl = codexRouteBaseUrl(config);
  if (baseUrl === undefined) return false;
  if (baseUrl === null) return true;
  if (!URL.canParse(baseUrl)) return false;
  const url = new URL(baseUrl);
  return (
    url.protocol === "https:" &&
    (url.hostname === "api.openai.com" || url.hostname === "chatgpt.com")
  );
}

/** The origin of the custom endpoint Codex sends model requests to, if it uses one. */
export function codexCustomEndpoint(config: unknown): string | undefined {
  const baseUrl = codexRouteBaseUrl(config);
  const url = baseUrl ? URL.parse(baseUrl) : null;
  return url && /^https?:$/.test(url.protocol) && !routesToNativeOpenai(config)
    ? url.origin
    : undefined;
}

// A stored login does not mean this provider's traffic spends that login's quota.
export const codexRoutesSpendNativeQuota = (
  account: CodexSchema.V2GetAccountResponse,
  config: unknown,
): boolean =>
  account.requiresOpenaiAuth && account.account?.type !== "apiKey" && routesToNativeOpenai(config);

const CODEX_APP_SERVER_PROBE_FORCE_KILL_AFTER = "2 seconds" as const;
const CODEX_CATALOG_REFRESH_TIMEOUT_MS = 3_000;
const CODEX_CONFIG_PROBE_TIMEOUT_MS = 3_000;
const CODEX_CATALOG_REFRESH_POLL = "100 millis" as const;
const CODEX_CLIENT_NAME = "T3 Code";
// Handshake plus account/models/skills requests can exceed the shared 10s probe budget.
const CODEX_AUTH_PROBE_TIMEOUT_MS = 20_000;
const CODEX_DISCOVERY_MARGIN_MS = 250;
// `codex-cli 0.159.0`, `codex-cli 0.160.0-alpha.1` or the npm CLI's bare `0.1.4161006`.
// Keep a prerelease suffix so compatibility stays unknown for it, as it does for `initialize`.
const CODEX_CLI_VERSION_PATTERN = /(?:^|\s)v?(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)(?=\s|$)/u;

const CODEX_PRESENTATION = {
  displayName: "Codex",
  showInteractionModeToggle: true,
  reportsContextWindow: true,
} as const;

export interface CodexAppServerProviderSnapshot extends CodexObservedAccount {
  readonly endpoint?: string | undefined;
  readonly nativeUsage?: boolean;
  /** `config/read` failed, so whether this login's quota is spent here is unknown. */
  readonly configUnavailable?: boolean;
  /** The CODEX_HOME the app-server reported; never published as a path. */
  readonly codexHome?: string;
  readonly rateLimits?: CodexRateLimitsProbe;
  readonly version: string | undefined;
  readonly models: ReadonlyArray<ServerProviderModel> | undefined;
  readonly serviceTier?: string | undefined;
  /** Undefined when `config/read` failed, so T3 cannot know Codex's default program. */
  readonly daybreakEnabled?: boolean | undefined;
  readonly cyberAccessProgramsUnavailable?: boolean | undefined;
  readonly skills: ReadonlyArray<ServerProviderSkill>;
}

const REASONING_EFFORT_LABELS: Readonly<Record<string, string>> = {
  none: "None",
  minimal: "Minimal",
  low: "Low",
  medium: "Medium",
  high: "High",
  xhigh: "Extra High",
  max: "Max",
  ultra: "Ultra",
};

const DEFAULT_SERVICE_TIER_ID = "default";

const SERVICE_TIER_DESCRIPTIONS: Readonly<Record<string, string>> = {
  ultrafast: "Even faster, more expensive",
};

const CYBER_ACCESS_PROGRAM_LABELS: Readonly<Record<string, string>> = {
  standard: "Standard",
  daybreakBlue: "Daybreak Blue",
  daybreakRed: "Daybreak Red",
};

function reasoningEffortLabel(reasoningEffort: string): string {
  return REASONING_EFFORT_LABELS[reasoningEffort] ?? reasoningEffort;
}

function codexAccountAuthLabel(
  account: CodexSchema.V2GetAccountResponse["account"],
  usagePlanType: string | null | undefined,
) {
  if (!account) return undefined;
  if (account.type === "apiKey") return "OpenAI API Key";
  if (account.type === "amazonBedrock") return "Amazon Bedrock";
  if (account.type !== "chatgpt") return undefined;
  // account/read takes the plan from the cached login token; the usage read asks the backend.
  const usageLabel = usagePlanType === "unknown" ? undefined : codexPlanLabel(usagePlanType);
  return usageLabel ?? codexPlanLabel(account.planType);
}

export function codexPlanLabel(planType: string | null | undefined): string | undefined {
  switch (planType) {
    case "free":
      return "ChatGPT Free Subscription";
    case "go":
      return "ChatGPT Go Subscription";
    case "plus":
      return "ChatGPT Plus Subscription";
    case "pro":
      return "ChatGPT Pro 200 Subscription";
    case "prolite":
      return "ChatGPT Pro 100 Subscription";
    case "promax":
      return "ChatGPT Pro 500 Subscription";
    case "self_serve_business_prolite":
      return "ChatGPT Business Premium Subscription";
    case "team":
    case "self_serve_business_usage_based":
    case "business":
      return "ChatGPT Business Subscription";
    case "ent26":
    case "enterprise_cbp_automation":
    case "enterprise_cbp_usage_based":
    case "enterprise":
      return "ChatGPT Enterprise Subscription";
    case "edu":
    case "edu_plus":
    case "edu_pro":
      return "ChatGPT Edu Subscription";
    case "unknown":
      return "ChatGPT Subscription";
    default:
      return undefined;
  }
}

function codexAccountEmail(account: CodexSchema.V2GetAccountResponse["account"]) {
  if (!account || account.type !== "chatgpt") return undefined;
  return account.email;
}

export function codexDefaultReasoningEffortOverride(model: string): "medium" | undefined {
  return codexModelFamily(model) === "gpt-6-astra" ? "medium" : undefined;
}

export function mapCodexModelCapabilities(
  model: CodexSchema.V2ModelListResponse__Model,
): ModelCapabilities {
  const reasoningOptions = model.supportedReasoningEfforts.map(({ reasoningEffort }) =>
    reasoningEffort ===
    (codexDefaultReasoningEffortOverride(model.model) ?? model.defaultReasoningEffort)
      ? {
          id: reasoningEffort,
          label: reasoningEffortLabel(reasoningEffort),
          isDefault: true,
        }
      : {
          id: reasoningEffort,
          label: reasoningEffortLabel(reasoningEffort),
        },
  );
  const defaultReasoning = reasoningOptions.find((option) => option.isDefault)?.id;
  const serviceTiers =
    model.serviceTiers && model.serviceTiers.length > 0
      ? model.serviceTiers
      : (model.additionalSpeedTiers ?? []).map((id) => ({
          id,
          name: id === "fast" ? "Fast" : id,
          description: "",
        }));
  const catalogDefaultServiceTier = serviceTiers.some(
    (tier) => tier.id === model.defaultServiceTier,
  )
    ? model.defaultServiceTier
    : null;
  const defaultServiceTier = catalogDefaultServiceTier ?? DEFAULT_SERVICE_TIER_ID;
  const optionDescriptors: ProviderOptionDescriptor[] = [];

  if (reasoningOptions.length > 0) {
    optionDescriptors.push({
      id: "reasoningEffort",
      label: "Reasoning",
      type: "select",
      options: reasoningOptions,
      ...(defaultReasoning ? { currentValue: defaultReasoning } : {}),
    });
  }
  if (serviceTiers.length > 0) {
    optionDescriptors.push({
      id: "serviceTier",
      label: "Service Tier",
      type: "select",
      options: [
        {
          id: DEFAULT_SERVICE_TIER_ID,
          label: "Standard",
          ...(defaultServiceTier === DEFAULT_SERVICE_TIER_ID ? { isDefault: true } : {}),
        },
        ...serviceTiers.map((tier) => {
          const description = SERVICE_TIER_DESCRIPTIONS[tier.id] ?? tier.description;
          return {
            id: tier.id,
            label: tier.name,
            ...(description ? { description } : {}),
            ...(defaultServiceTier === tier.id ? { isDefault: true } : {}),
          };
        }),
      ],
      currentValue: defaultServiceTier,
    });
  }
  const cyberAccessPrograms = model.availableAccessPrograms?.cyber ?? [];
  if (cyberAccessPrograms.length > 0) {
    const hasStandard = cyberAccessPrograms.includes("standard");
    const defaultProgram = hasStandard ? "standard" : cyberAccessPrograms[0];
    optionDescriptors.push({
      id: "cyberAccessProgram",
      label: "Cyber Access",
      type: "select",
      // Standard stays selectable as an opt-out; Codex rejects it visibly when the login cannot use it.
      options: (hasStandard ? cyberAccessPrograms : ["standard", ...cyberAccessPrograms]).map(
        (program) => ({
          id: program,
          label: CYBER_ACCESS_PROGRAM_LABELS[program] ?? program,
          ...(program === defaultProgram ? { isDefault: true } : {}),
        }),
      ),
      ...(defaultProgram === undefined ? {} : { currentValue: defaultProgram }),
    });
  }

  return createModelCapabilities({
    optionDescriptors,
  });
}

function parseCodexModelListResponse(
  response: CodexSchema.V2ModelListResponse,
): ReadonlyArray<ServerProviderModel> {
  return response.data.map((model) => ({
    slug: model.model,
    name: formatCodexModelName(model.displayName),
    isCustom: false,
    ...(model.isDefault ? { isDefault: true } : {}),
    capabilities: mapCodexModelCapabilities(model),
  }));
}

export function applyPreferredCodexDefaultModel(
  models: ReadonlyArray<ServerProviderModel>,
): ReadonlyArray<ServerProviderModel> {
  const preferredSlug = PREFERRED_DEFAULT_CODEX_MODELS.flatMap((slug) =>
    models.filter((model) => !model.isCustom && codexModelFamily(model.slug) === slug),
  )[0]?.slug;
  if (!preferredSlug) {
    return models;
  }
  return models.map((model) => {
    if (model.slug === preferredSlug) {
      return model.isDefault ? model : { ...model, isDefault: true };
    }
    if (!model.isDefault) {
      return model;
    }
    const { isDefault: _isDefault, ...rest } = model;
    return rest;
  });
}

function mapCodexOptionDescriptors(
  models: ReadonlyArray<ServerProviderModel>,
  update: (
    descriptors: ReadonlyArray<ProviderOptionDescriptor>,
    model: ServerProviderModel,
  ) => ReadonlyArray<ProviderOptionDescriptor>,
): ReadonlyArray<ServerProviderModel> {
  return models.map((model) => {
    if (!model.capabilities?.optionDescriptors) return model;
    return {
      ...model,
      capabilities: {
        ...model.capabilities,
        optionDescriptors: update(model.capabilities.optionDescriptors, model),
      },
    };
  });
}

function withConfiguredSelectDefault(
  models: ReadonlyArray<ServerProviderModel>,
  descriptorId: string,
  pick: (options: ReadonlyArray<{ readonly id: string }>) => string | undefined,
): ReadonlyArray<ServerProviderModel> {
  return mapCodexOptionDescriptors(models, (descriptors) =>
    descriptors.map((descriptor) => {
      if (descriptor.id !== descriptorId || descriptor.type !== "select") return descriptor;
      const value = pick(descriptor.options);
      if (!value) return descriptor;
      return {
        ...descriptor,
        currentValue: value,
        options: descriptor.options.map(({ isDefault: _isDefault, ...option }) =>
          option.id === value ? { ...option, isDefault: true } : option,
        ),
      };
    }),
  );
}

function applyConfiguredCodexServiceTier(
  models: ReadonlyArray<ServerProviderModel>,
  serviceTier: string | undefined,
): ReadonlyArray<ServerProviderModel> {
  if (!serviceTier) return models;
  return withConfiguredSelectDefault(
    models,
    "serviceTier",
    (options) =>
      options.find(
        (option) =>
          option.id === serviceTier || (serviceTier === "fast" && option.id === "priority"),
      )?.id,
  );
}

/** Mirrors Codex: `daybreak = true` prefers a Daybreak program, Blue before Red. */
function applyConfiguredCodexCyberAccess(
  models: ReadonlyArray<ServerProviderModel>,
  snapshot: Pick<
    CodexAppServerProviderSnapshot,
    "daybreakEnabled" | "cyberAccessProgramsUnavailable"
  >,
): ReadonlyArray<ServerProviderModel> {
  // Without a descriptor no turn sends a program; without a default only an explicit choice goes.
  const offered = mapCodexOptionDescriptors(models, (descriptors, model) =>
    descriptors.flatMap((descriptor): ReadonlyArray<ProviderOptionDescriptor> => {
      if (descriptor.id !== "cyberAccessProgram" || descriptor.type !== "select") {
        return [descriptor];
      }
      if (model.isCustom || snapshot.cyberAccessProgramsUnavailable === true) return [];
      if (snapshot.daybreakEnabled !== undefined) return [descriptor];
      const { currentValue: _currentValue, ...withoutDefault } = descriptor;
      return [
        {
          ...withoutDefault,
          options: descriptor.options.map(({ isDefault: _isDefault, ...option }) => option),
        },
      ];
    }),
  );
  if (snapshot.daybreakEnabled !== true) return offered;
  return withConfiguredSelectDefault(offered, "cyberAccessProgram", (options) =>
    ["daybreakBlue", "daybreakRed"].find((program) =>
      options.some((option) => option.id === program),
    ),
  );
}

/**
 * Codex has no static default capability set, so a bare custom slug borrows
 * the first built-in's descriptors; an entry with its own capabilities keeps
 * them.
 */
function appendCustomCodexModels(
  models: ReadonlyArray<ServerProviderModel>,
  customModels: ReadonlyArray<CustomModelSetting>,
): ReadonlyArray<ServerProviderModel> {
  const fallbackCapabilities = models.find((model) => model.capabilities)?.capabilities ?? null;
  return providerModelsFromSettings(models, customModels, fallbackCapabilities);
}

function parseCodexSkillsListResponse(
  response: CodexSchema.V2SkillsListResponse,
  cwd: string,
): ReadonlyArray<ServerProviderSkill> {
  const matchingEntry = response.data.find((entry) => entry.cwd === cwd);
  const skills = matchingEntry
    ? matchingEntry.skills
    : response.data.flatMap((entry) => entry.skills);

  return skills.map((skill) => {
    const shortDescription =
      skill.shortDescription ?? skill.interface?.shortDescription ?? undefined;

    const parsedSkill: Types.Mutable<ServerProviderSkill> = {
      name: skill.name,
      path: skill.path,
      enabled: skill.enabled,
    };

    if (skill.description) {
      parsedSkill.description = skill.description;
    }
    if (skill.scope) {
      parsedSkill.scope = skill.scope;
    }
    if (skill.interface?.displayName) {
      parsedSkill.displayName = skill.interface.displayName;
    }
    if (shortDescription) {
      parsedSkill.shortDescription = shortDescription;
    }

    return parsedSkill;
  });
}

const requestAllCodexModels = Effect.fn("requestAllCodexModels")(function* (
  client: CodexClient.CodexAppServerClient["Service"],
) {
  const models: ServerProviderModel[] = [];
  let cursor: string | null | undefined = undefined;

  do {
    const response: CodexSchema.V2ModelListResponse = yield* client.request(
      "model/list",
      cursor ? { cursor } : {},
    );
    models.push(...parseCodexModelListResponse(response));
    cursor = response.nextCursor;
  } while (cursor);

  return models;
});

const decodeCodexModelsCacheStamp = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.Struct({ fetched_at: Schema.String })),
);

const readCodexModelsCacheFetchedAt = Effect.fn("readCodexModelsCacheFetchedAt")(function* (
  codexHome: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const text = yield* fileSystem
    .readFileString(path.join(codexHome, "models_cache.json"))
    .pipe(Effect.option);
  return Option.flatMap(text, decodeCodexModelsCacheStamp).pipe(
    Option.flatMap((stamp) => DateTime.make(stamp.fetched_at)),
    Option.match({ onNone: () => Number.NaN, onSome: DateTime.toEpochMillis }),
  );
});

const withCodexDiscoveryDeadline = Effect.fnUntraced(function* <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  deadlineMs: number,
  timeoutMs = Infinity,
) {
  const remainingMs = Math.min(timeoutMs, deadlineMs - DateTime.toEpochMillis(yield* DateTime.now));
  if (remainingMs <= 0) return undefined;
  return yield* effect.pipe(
    Effect.timeoutOption(Duration.millis(remainingMs)),
    Effect.map(Option.getOrUndefined),
  );
});

/**
 * Every Codex client on the machine shares that file, and the backend tailors
 * the catalog to each client's originator: the plain CLI does not get Sol
 * Ultrafast. App-server refreshes the catalog online at startup, so wait until
 * a write newer than this process's spawn lands, then list again.
 */
const requestCodexChatGptModels = Effect.fn("requestCodexChatGptModels")(function* (input: {
  readonly client: CodexClient.CodexAppServerClient["Service"];
  readonly codexHome: string;
  readonly spawnedAtMs: number;
  readonly deadlineMs: number;
}) {
  const models = yield* withCodexDiscoveryDeadline(
    requestAllCodexModels(input.client),
    input.deadlineMs,
  );
  if (models === undefined) return undefined;
  const refreshed = yield* withCodexDiscoveryDeadline(
    readCodexModelsCacheFetchedAt(input.codexHome).pipe(
      Effect.repeat({
        schedule: Schedule.spaced(CODEX_CATALOG_REFRESH_POLL),
        until: (fetchedAtMs) => fetchedAtMs >= input.spawnedAtMs,
      }),
    ),
    input.deadlineMs,
    CODEX_CATALOG_REFRESH_TIMEOUT_MS,
  );
  if (refreshed === undefined) {
    yield* Effect.logWarning(
      "Codex did not refresh its model catalog in time; using the cached catalog.",
    );
    return models;
  }
  return (
    (yield* withCodexDiscoveryDeadline(requestAllCodexModels(input.client), input.deadlineMs)) ??
    models
  );
});

/** Set originator at spawn; the pre-initialize refresh otherwise overwrites shared cache with plain-CLI models. */
export const codexAppServerEnvironment = (
  environment: NodeJS.ProcessEnv | undefined,
): NodeJS.ProcessEnv => ({
  CODEX_INTERNAL_ORIGINATOR_OVERRIDE: CODEX_CLIENT_NAME,
  ...environment,
});

export function buildCodexInitializeParams(): CodexSchema.V1InitializeParams {
  return {
    clientInfo: {
      name: CODEX_CLIENT_NAME,
      title: CODEX_CLIENT_NAME,
      version: packageJson.version,
    },
    capabilities: {
      experimentalApi: true,
    },
  };
}

const codexProcessEnvironment = Effect.fn("codexProcessEnvironment")(function* (input: {
  readonly homePath?: string | undefined;
  readonly environment?: NodeJS.ProcessEnv | undefined;
}) {
  // Expand CODEX_HOME explicitly: spawn passes a literal ~ that Codex rejects.
  const resolvedHomePath = input.homePath
    ? expandHomePath(input.homePath, yield* HostProcess.HomeDirectory)
    : undefined;
  return {
    ...codexAppServerEnvironment(input.environment),
    ...(resolvedHomePath ? { CODEX_HOME: resolvedHomePath } : {}),
  };
});

export const withCodexAppServerClient = Effect.fn("withCodexAppServerClient")(function* (input: {
  readonly binaryPath: string;
  readonly homePath?: string | undefined;
  readonly launchArgs?: string | undefined;
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv | undefined;
  readonly probe?: {
    readonly recordPid: (pid: number) => void;
    readonly track: ReturnType<typeof makeCodexRequestTracker>;
  };
}) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const environment = yield* codexProcessEnvironment(input);
  const spawnCommand = yield* resolveSpawnCommand(
    input.binaryPath,
    codexAppServerArgs(input.launchArgs),
    { env: environment, extendEnv: true },
  );
  const spawn = spawner
    .spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        cwd: input.cwd,
        env: environment,
        extendEnv: true,
        forceKillAfter: CODEX_APP_SERVER_PROBE_FORCE_KILL_AFTER,
        shell: spawnCommand.shell,
      }),
    )
    .pipe(
      Effect.mapError(
        (cause) =>
          new CodexErrors.CodexAppServerSpawnError({
            command: `${input.binaryPath} app-server`,
            cause,
          }),
      ),
    );
  const probe = input.probe;
  const child = yield* probe
    ? Effect.uninterruptible(
        spawn.pipe(Effect.tap((child) => Effect.sync(() => probe.recordPid(child.pid)))),
      )
    : spawn;
  const clientContext = yield* Layer.build(CodexClient.layerChildProcess(child));
  const rawClient = yield* Effect.service(CodexClient.CodexAppServerClient).pipe(
    Effect.provide(clientContext),
  );
  const client: CodexClient.CodexAppServerClient["Service"] = probe
    ? {
        ...rawClient,
        request: (method, payload) => probe.track(rawClient.request(method, payload)),
        raw: {
          ...rawClient.raw,
          request: (method, payload) => probe.track(rawClient.raw.request(method, payload)),
        },
      }
    : rawClient;
  const initialize = yield* client.request("initialize", buildCodexInitializeParams());
  yield* client.notify("initialized", undefined);
  return { client, initialize, pid: child.pid };
});

// Codex runs git (ssh for ssh URLs) for plugin marketplaces at launch and exits without waiting
// for it, so stopping a short probe mid-fetch strands ssh in an SSH-agent request.
const CODEX_STARTUP_WORK_GRACE = Duration.minutes(1);

const codexProcessGroupIsBusy = Effect.fn("codexProcessGroupIsBusy")(
  function* (pid: number) {
    const { stdout } = yield* spawnAndCollect(
      "pgrep",
      ChildProcess.make("pgrep", ["-g", `${pid}`]),
    );
    return stdout.split("\n").some((line) => line.trim() !== "" && Number(line) !== pid);
  },
  Effect.orElseSucceed(() => false),
);

// Codex may be refreshing the login and still need to write the rotated token to auth.json.
const CODEX_IN_FLIGHT_REQUEST_GRACE = Duration.seconds(60);

// The request outlives its waiter. Fork and registration must be atomic.
const makeCodexRequestTracker =
  (processScope: Scope.Scope, inFlight: Array<Fiber.Fiber<unknown, unknown>>) =>
  <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
    Effect.uninterruptibleMask((restore) =>
      restore(effect).pipe(
        Effect.forkIn(processScope),
        Effect.tap((fiber) => Effect.sync(() => inFlight.push(fiber))),
        Effect.flatMap((fiber) => restore(Fiber.join(fiber))),
      ),
    );

const stopCodexAfterStartupWork = (pid: number, processScope: Scope.Closeable) =>
  codexProcessGroupIsBusy(pid).pipe(
    Effect.repeat({ schedule: Schedule.spaced("500 millis"), while: (busy) => busy }),
    Effect.timeoutOption(CODEX_STARTUP_WORK_GRACE),
    Effect.ensuring(Scope.close(processScope, Exit.void)),
  );

// Only answers within the probe deadline are classified, even though Codex can finish afterward.
const readCodexRateLimits = (
  client: CodexClient.CodexAppServerClient["Service"],
  deadlineMs: number,
) =>
  withCodexDiscoveryDeadline(client.request("account/rateLimits/read", null), deadlineMs).pipe(
    Effect.map((response): CodexRateLimitsProbe =>
      response
        ? {
            snapshot: response.rateLimits,
            rateLimitsByLimitId: response.rateLimitsByLimitId,
            resetCredits: response.rateLimitResetCredits,
            accountId: response.accountId,
          }
        : { failure: "Codex did not answer the usage request." },
    ),
    Effect.catch((error) => {
      const loginRefused = isCodexLoginRefusal(error);
      // The provider's text can name the account, so neither the log nor the snapshot keeps it.
      return Effect.logDebug("Codex rate-limit read failed.", {
        error: error._tag,
        ...(error._tag === "CodexAppServerRequestError" ? { code: error.code } : {}),
        loginRefused,
      }).pipe(
        Effect.as<CodexRateLimitsProbe>(
          loginRefused
            ? { failure: CODEX_LOGIN_REFUSED_USAGE_MESSAGE, loginRefused: true }
            : { failure: codexRateLimitsFailureMessage(error) },
        ),
      );
    }),
  );

type CodexLoginObservation = Pick<
  CodexAppServerProviderSnapshot,
  "account" | "codexHome" | "nativeUsage" | "configUnavailable" | "rateLimits"
> &
  Pick<NonNullable<ServerProvider["codexLoginRefusal"]>, "accountId" | "workspaceId">;

const CodexAccountIdentifiers = Schema.Struct({
  accountId: Schema.optionalKey(Schema.NullOr(Schema.String)),
  workspaceId: Schema.optionalKey(Schema.NullOr(Schema.String)),
  chatgptAccountId: Schema.optionalKey(Schema.NullOr(Schema.String)),
});
const decodeCodexAccountIdentifiers = Schema.decodeUnknownOption(
  Schema.Struct({
    ...CodexAccountIdentifiers.fields,
    account: Schema.optionalKey(Schema.NullOr(CodexAccountIdentifiers)),
  }),
);
const decodeCodexAccount = Schema.decodeUnknownEffect(CodexSchema.V2GetAccountResponse);

interface CodexObservedAccount {
  readonly account: CodexSchema.V2GetAccountResponse;
  readonly accountId?: string;
  readonly workspaceId?: string;
}

const readCodexProviderAccount = Effect.fn("readCodexProviderAccount")(function* (
  client: CodexClient.CodexAppServerClient["Service"],
) {
  const raw = yield* client.raw.request("account/read", {});
  const account = yield* decodeCodexAccount(raw).pipe(
    Effect.mapError((cause) =>
      CodexErrors.CodexAppServerProtocolParseError.fromSchemaError(
        "decode-response-payload",
        cause,
        {
          method: "account/read",
        },
      ),
    ),
  );
  const ids = Option.getOrUndefined(decodeCodexAccountIdentifiers(raw));
  const accountId = (
    ids?.accountId ??
    ids?.chatgptAccountId ??
    ids?.account?.accountId ??
    ids?.account?.chatgptAccountId
  )?.trim();
  const workspaceId = (ids?.workspaceId ?? ids?.account?.workspaceId)?.trim();
  return { account, ...(accountId ? { accountId } : {}), ...(workspaceId ? { workspaceId } : {}) };
});

interface CodexProbeHome {
  readonly reportedPath: string;
  readonly realPath?: string;
  readonly authGeneration?: string;
  readonly authMetadataUnavailable?: true;
}

const resolveCodexProbeHomePath = Effect.fn("resolveCodexProbeHomePath")(function* (
  homePath: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const expandedPath = expandHomePath(homePath, yield* HostProcess.HomeDirectory);
  return yield* fs.realPath(expandedPath).pipe(Effect.option, Effect.map(Option.getOrUndefined));
});

const readCodexProbeHome = Effect.fn("readCodexProbeHome")(function* (
  reportedPath: string,
): Effect.fn.Return<CodexProbeHome, never, FileSystem.FileSystem | Path.Path> {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const realPath = yield* resolveCodexProbeHomePath(reportedPath);
  if (realPath === undefined) return { reportedPath };
  const authPath = path.join(realPath, "auth.json");
  const auth = yield* fs.stat(authPath).pipe(
    Effect.catchReason("PlatformError", "NotFound", () => Effect.undefined),
    Effect.option,
  );
  const home = { reportedPath, realPath };
  if (Option.isNone(auth)) return { ...home, authMetadataUnavailable: true };
  const metadata = auth.value;
  if (!metadata) return home;
  const ino = Option.getOrUndefined(metadata.ino);
  const mtime = Option.getOrUndefined(metadata.mtime);
  if (
    metadata.type !== "File" ||
    ino === undefined ||
    mtime === undefined ||
    Option.isNone(yield* fs.access(authPath, { readable: true }).pipe(Effect.option))
  )
    return { ...home, authMetadataUnavailable: true };
  const authGeneration = NodeCrypto.createHash("sha256")
    .update(`${metadata.dev}:${ino}:${metadata.size}:${mtime.getTime()}`, "utf8")
    .digest("hex");
  return { ...home, authGeneration };
});

const sameCredentialGeneration = (a: CodexProbeHome, b: CodexProbeHome) =>
  a.realPath === b.realPath &&
  (a.realPath !== undefined || a.reportedPath === b.reportedPath) &&
  a.authGeneration === b.authGeneration &&
  a.authMetadataUnavailable === b.authMetadataUnavailable;

interface CodexProviderProbeInput {
  readonly probeTimeoutMs: number;
  readonly binaryPath: string;
  readonly homePath?: string;
  readonly launchArgs?: string;
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly skipNativeUsage?: boolean;
  readonly sampleHome?: (
    reportedPath: string,
  ) => Effect.Effect<CodexProbeHome, never, FileSystem.FileSystem | Path.Path>;
  readonly observeLogin?: (observation: CodexLoginObservation) => void;
}

const probeCodexAppServerProvider = Effect.fn("probeCodexAppServerProvider")(function* (
  input: CodexProviderProbeInput,
  ownProcessScope?: (scope: Scope.Scope) => Effect.Effect<void>,
) {
  const spawnedAtMs = DateTime.toEpochMillis(yield* DateTime.now);
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const inFlight: Array<Fiber.Fiber<unknown, unknown>> = [];
  let pid: number | undefined;
  const processScope = yield* Effect.uninterruptible(
    Effect.gen(function* () {
      const childScope = yield* Scope.make();
      // Teardown can outlive the refresh semaphore, but the instance stays busy until it closes.
      yield* Effect.addFinalizer(() =>
        pid === undefined
          ? Scope.close(childScope, Exit.void)
          : Fiber.awaitAll(inFlight).pipe(
              Effect.timeoutOption(CODEX_IN_FLIGHT_REQUEST_GRACE),
              Effect.andThen(stopCodexAfterStartupWork(pid, childScope)),
              Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
              Effect.forkDetach({ startImmediately: true }),
              Effect.asVoid,
            ),
      );
      if (ownProcessScope) yield* ownProcessScope(childScope);
      return childScope;
    }),
  );
  const { client, initialize } = yield* withCodexAppServerClient({
    ...input,
    probe: {
      recordPid: (spawned) => {
        pid = spawned;
      },
      track: makeCodexRequestTracker(processScope, inFlight),
    },
  }).pipe(Scope.provide(processScope));

  const versionMatch = initialize.userAgent.match(/\/([^\s]+)/);
  const version = versionMatch ? versionMatch[1] : undefined;

  if (input.sampleHome) yield* input.sampleHome(initialize.codexHome);
  const observedAccount = yield* readCodexProviderAccount(client);
  const { account: accountResponse } = observedAccount;
  const login = { ...observedAccount, codexHome: initialize.codexHome };
  input.observeLogin?.(login);
  if (!accountResponse.account && accountResponse.requiresOpenaiAuth) {
    return {
      ...login,
      version,
      models: [],
      skills: [],
    } satisfies CodexAppServerProviderSnapshot;
  }

  // Optional discovery must finish before the outer timeout can discard the account result.
  const deadlineMs = spawnedAtMs + input.probeTimeoutMs - CODEX_DISCOVERY_MARGIN_MS;
  const [skillsResult, modelsResult, route] = yield* Effect.all(
    [
      // Discovery failure must let the deadline-bound usage read finish.
      withCodexDiscoveryDeadline(
        client.request("skills/list", { cwds: [input.cwd] }),
        deadlineMs,
      ).pipe(Effect.result),
      (accountResponse.account?.type === "chatgpt"
        ? requestCodexChatGptModels({
            client,
            codexHome: initialize.codexHome,
            spawnedAtMs,
            deadlineMs,
          })
        : withCodexDiscoveryDeadline(requestAllCodexModels(client), deadlineMs)
      ).pipe(Effect.result),
      // The shared catalog must exclude config from the server's launch project.
      withCodexDiscoveryDeadline(
        client.request("config/read", { includeLayers: false }),
        deadlineMs,
        CODEX_CONFIG_PROBE_TIMEOUT_MS,
      ).pipe(
        Effect.catch(() => Effect.logDebug("Codex config read failed.").pipe(Effect.as(undefined))),
        Effect.flatMap((config) => {
          // Only a confirmed native route may probe or publish the login's quota.
          const nativeUsage =
            !input.skipNativeUsage &&
            config !== undefined &&
            codexRoutesSpendNativeQuota(accountResponse, config.config);
          const configUnavailable =
            config === undefined &&
            !input.skipNativeUsage &&
            accountResponse.requiresOpenaiAuth &&
            accountResponse.account?.type === "chatgpt";
          const readUsage: Effect.Effect<CodexRateLimitsProbe | undefined> = nativeUsage
            ? readCodexRateLimits(client, deadlineMs)
            : Effect.undefined;
          return readUsage.pipe(
            Effect.map((rateLimits) => {
              input.observeLogin?.({
                ...login,
                nativeUsage,
                configUnavailable,
                ...(rateLimits ? { rateLimits } : {}),
              });
              return { config: config?.config, nativeUsage, configUnavailable, rateLimits };
            }),
          );
        }),
      ),
    ],
    { concurrency: "unbounded" },
  );

  if (Result.isFailure(skillsResult)) return yield* skillsResult.failure;
  if (Result.isFailure(modelsResult)) return yield* modelsResult.failure;
  const skillsResponse = skillsResult.success;
  const { config, nativeUsage, configUnavailable, rateLimits } = route;
  const cyberAccess = Option.getOrUndefined(decodeCodexCyberAccessConfig(config));
  return {
    ...login,
    endpoint: config ? codexCustomEndpoint(config) : undefined,
    nativeUsage,
    configUnavailable,
    ...(rateLimits ? { rateLimits } : {}),
    version,
    models: modelsResult.success,
    serviceTier: config?.service_tier ?? undefined,
    daybreakEnabled: config === undefined ? undefined : cyberAccess?.daybreak === true,
    // An unread config cannot rule out an API key with the programs turned off.
    cyberAccessProgramsUnavailable:
      accountResponse.account?.type === "apiKey" &&
      (config === undefined || cyberAccess?.features?.api_key_cyber_access_programs === false),
    skills: skillsResponse ? parseCodexSkillsListResponse(skillsResponse, input.cwd) : [],
  } satisfies CodexAppServerProviderSnapshot;
});

// Old CLIs can exit before initialize reports the version needed for compatibility advice.
const probeCodexCliVersion = Effect.fn("probeCodexCliVersion")(
  function* (input: {
    readonly binaryPath: string;
    readonly homePath?: string | undefined;
    readonly cwd: string;
    readonly environment?: NodeJS.ProcessEnv | undefined;
  }) {
    const environment = yield* codexProcessEnvironment(input);
    const spawnCommand = yield* resolveSpawnCommand(input.binaryPath, ["--version"], {
      env: environment,
      extendEnv: true,
    });
    const result = yield* spawnAndCollect(
      input.binaryPath,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        cwd: input.cwd,
        env: environment,
        extendEnv: true,
        forceKillAfter: CODEX_APP_SERVER_PROBE_FORCE_KILL_AFTER,
        shell: spawnCommand.shell,
      }),
    );
    if (result.code !== 0) return null;
    return CODEX_CLI_VERSION_PATTERN.exec(result.stdout)?.[1] ?? null;
  },
  (effect, input) =>
    effect.pipe(
      Effect.timeoutOption(
        resolveProviderProbeTimeoutMs("codex", DEFAULT_TIMEOUT_MS, input.environment),
      ),
    ),
  Effect.map(Option.getOrNull),
  Effect.catch((cause) =>
    Effect.logDebug("Codex --version fallback failed.", { cause }).pipe(Effect.as(null)),
  ),
);

export const probeCodexSkillsForCwd = Effect.fn("probeCodexSkillsForCwd")(function* (input: {
  readonly binaryPath: string;
  readonly homePath?: string;
  readonly launchArgs?: string;
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv;
}) {
  const { client } = yield* withCodexAppServerClient({
    ...input,
    environment: providerProbeEnvironment(input.environment ?? process.env),
  });
  const skillsResponse = yield* client.request("skills/list", { cwds: [input.cwd] });
  return parseCodexSkillsListResponse(skillsResponse, input.cwd);
});

// Long enough for a burst of workspace switches; short because the helper holds the user's
// binary open, and Windows refuses to overwrite or delete a running executable.
const CODEX_SKILLS_HELPER_IDLE_TTL = "30 seconds";

/** Reuses one app-server per launch identity; each read passes its cwd and rescans from disk. */
export const makeCodexSkillsProbe = Effect.fn("makeCodexSkillsProbe")(function* (
  retainAppServer: Effect.Effect<void, never, Scope.Scope> = Effect.void,
) {
  const helpers = yield* RcMap.make({
    lookup: (launch: {
      readonly binaryPath: string;
      readonly homePath?: string;
      readonly launchArgs?: string;
      readonly environment: NodeJS.ProcessEnv;
    }) =>
      retainAppServer.pipe(
        Effect.andThen(withCodexAppServerClient({ ...launch, cwd: process.cwd() })),
        Effect.map(({ client }) => client),
      ),
    idleTimeToLive: CODEX_SKILLS_HELPER_IDLE_TTL,
  });
  return Effect.fn("CodexSkillsProbe.probe")(function* (input: {
    readonly binaryPath: string;
    readonly homePath?: string;
    readonly launchArgs?: string;
    readonly cwd: string;
    readonly environment?: NodeJS.ProcessEnv;
  }) {
    const { cwd, ...rest } = input;
    const launch = {
      ...rest,
      environment: providerProbeEnvironment(rest.environment ?? process.env),
    };
    // Retire helpers for a previous binary, home, credentials, or launch options.
    const stale = Array.from(yield* RcMap.keys(helpers)).filter(
      (key) => !Equal.equals(key, launch),
    );
    yield* Effect.forEach(stale, (key) => RcMap.invalidate(helpers, key), { discard: true });
    const skillsResponse = yield* RcMap.get(helpers, launch).pipe(
      Effect.flatMap((client) => client.request("skills/list", { cwds: [cwd], forceReload: true })),
      Effect.scoped,
      // RcMap keeps a failed spawn, and a dead helper keeps failing; drop it so the retry respawns.
      Effect.onError(() => RcMap.invalidate(helpers, launch)),
      Effect.retry({ times: 1 }),
    );
    return parseCodexSkillsListResponse(skillsResponse, cwd);
  });
});

const emptyCodexModelsFromSettings = (codexSettings: CodexSettings): ServerProvider["models"] =>
  appendCustomCodexModels([], codexSettings.customModels);

const makePendingCodexProvider = Effect.fn("CodexProvider.makePendingCodexProvider")(function* (
  codexSettings: CodexSettings,
): Effect.fn.Return<ServerProviderDraft> {
  const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
  const models = emptyCodexModelsFromSettings(codexSettings);

  if (!codexSettings.enabled) {
    return buildServerProvider({
      presentation: CODEX_PRESENTATION,
      enabled: false,
      checkedAt,
      models,
      skills: [],
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Codex is disabled in T3 Code settings.",
      },
    });
  }

  return buildServerProvider({
    presentation: CODEX_PRESENTATION,
    enabled: true,
    checkedAt,
    models,
    skills: [],
    probe: {
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      message: "Codex provider status has not been checked in this session yet.",
    },
  });
});

const quotePath = Schema.encodeSync(Schema.fromJsonString(Schema.String));

function accountProbeStatus(
  account: CodexSchema.V2GetAccountResponse,
  codexHome: string | undefined,
  loginRefused: boolean,
  workspaceId: string | undefined,
  rateLimits: CodexRateLimitsProbe | undefined,
): {
  readonly status: Exclude<ServerProviderState, "disabled">;
  readonly auth: ServerProvider["auth"];
  readonly message?: string;
} {
  const authLabel = codexAccountAuthLabel(
    account.account,
    rateLimits && "snapshot" in rateLimits ? rateLimits.snapshot.planType : undefined,
  );
  const authEmail = codexAccountEmail(account.account);
  const auth = {
    status: account.account ? ("authenticated" as const) : ("unknown" as const),
    ...(account.account?.type ? { type: account.account?.type } : {}),
    ...(authLabel ? { label: authLabel } : {}),
    ...(authEmail ? { email: authEmail } : {}),
    ...(workspaceId ? { workspaceId } : {}),
  } satisfies ServerProvider["auth"];

  if (loginRefused) {
    return {
      status: "error",
      auth: { ...auth, status: "unauthenticated" },
      message: codexLoginRefusedMessage(codexHome),
    };
  }

  if (account.account) {
    return { status: "ready", auth };
  }

  if (account.requiresOpenaiAuth) {
    return {
      status: "error",
      auth: { status: "unauthenticated" },
      message: codexHome
        ? `Codex CLI is not authenticated. Run \`codex login\` with CODEX_HOME set to ${quotePath(codexHome)} and try again.`
        : "Codex CLI is not authenticated. Run `codex login` and try again.",
    };
  }

  return { status: "ready", auth };
}

const codexLoginRefusedMessage = (codexHome: string | undefined) =>
  codexHome
    ? `The service refused this Codex login. Run \`codex login\` with CODEX_HOME set to ${quotePath(codexHome)} and try again.`
    : "The service refused this Codex login. Run `codex login` and try again.";

const CODEX_CONFIG_UNAVAILABLE_USAGE_MESSAGE =
  "Codex did not return its configuration in time, so usage was not read.";

type CodexLoginRefusal = ServerProvider["codexLoginRefusal"];

const loginRefusedAfter = (observation: CodexLoginObservation): boolean | undefined => {
  const { account, nativeUsage, configUnavailable, rateLimits } = observation;
  if (account.account?.type !== "chatgpt" || (nativeUsage === false && !configUnavailable))
    return false;
  if (!rateLimits) return undefined;
  return "snapshot" in rateLimits ? false : rateLimits.loginRefused;
};

const fingerprintCodexHome = (path: string | undefined) =>
  path && NodeCrypto.createHash("sha256").update(path, "utf8").digest("hex");

const loginFields = [
  "homeFingerprint",
  "email",
  "plan",
  "accountId",
  "workspaceId",
  "authGeneration",
] as const;

export const checkCodexProviderStatus = Effect.fn("checkCodexProviderStatus")(function* (
  codexSettings: CodexSettings,
  probe: (
    input: CodexProviderProbeInput,
  ) => Effect.Effect<
    CodexAppServerProviderSnapshot,
    CodexErrors.CodexAppServerError,
    ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path | Scope.Scope
  > = probeCodexAppServerProvider,
  environment?: NodeJS.ProcessEnv,
  managedAuth?: ServerProvider["auth"],
  loginRefusedState?: Ref.Ref<CodexLoginRefusal>,
  credentials?: CodexCredentialMonitor,
): Effect.fn.Return<
  ServerProviderDraft,
  ServerSettingsError,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> {
  const resolvedEnvironment = providerProbeEnvironment(environment ?? process.env);
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const emptyModels = emptyCodexModelsFromSettings(codexSettings);

  if (!codexSettings.enabled) {
    return buildServerProvider({
      presentation: CODEX_PRESENTATION,
      enabled: false,
      checkedAt,
      models: emptyModels,
      skills: [],
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Codex is disabled in T3 Code settings.",
      },
    });
  }

  const probeTimeoutMs = resolveProviderProbeTimeoutMs(
    "codex",
    CODEX_AUTH_PROBE_TIMEOUT_MS,
    resolvedEnvironment,
  );
  let observedLogin: CodexLoginObservation | undefined;
  let initializedHome: CodexProbeHome | undefined;
  const clearUsage = yield* ClearProviderUsage;
  const launchHome = credentials ? yield* credentials.sample(clearUsage) : undefined;
  const sampleHome = Effect.fn("sampleCodexProbeHome")(function* (
    reportedPath: string,
  ): Effect.fn.Return<CodexProbeHome, never, FileSystem.FileSystem | Path.Path> {
    if (!credentials) return yield* readCodexProbeHome(reportedPath);
    const configured = yield* credentials.sample(clearUsage);
    const reportedRealPath = yield* resolveCodexProbeHomePath(reportedPath);
    if (reportedRealPath !== undefined && reportedRealPath === configured.realPath) {
      return { ...configured, reportedPath };
    }
    yield* clearUsage;
    return { reportedPath, authMetadataUnavailable: true };
  });
  const probeResult = yield* probe({
    probeTimeoutMs,
    binaryPath: codexSettings.binaryPath,
    homePath: codexSettings.homePath,
    launchArgs: resolveCodexLaunchArgs(codexSettings.launchArgs, resolvedEnvironment),
    cwd: process.cwd(),
    environment: resolvedEnvironment,
    sampleHome: (reportedPath) =>
      sampleHome(reportedPath).pipe(
        Effect.tap((home) =>
          Effect.sync(() => {
            initializedHome = home;
          }),
        ),
      ),
    ...(managedAuth
      ? { skipNativeUsage: true }
      : {
          observeLogin: (observation) => {
            observedLogin = observation;
          },
        }),
  }).pipe(
    // Process teardown can outlast the work budget without discarding a completed snapshot.
    Effect.timeoutOption(Duration.millis(probeTimeoutMs)),
    Effect.scoped,
    Effect.result,
  );
  const probeFailure = Result.isFailure(probeResult) ? probeResult.failure : undefined;
  const installed = probeFailure === undefined || !isCodexAppServerSpawnError(probeFailure);
  const fallbackVersion =
    probeFailure !== undefined && installed
      ? yield* probeCodexCliVersion({
          binaryPath: codexSettings.binaryPath,
          homePath: codexSettings.homePath,
          cwd: process.cwd(),
          environment: resolvedEnvironment,
        })
      : null;
  const completed = Result.isSuccess(probeResult)
    ? Option.getOrUndefined(probeResult.success)
    : undefined;
  const observation = observedLogin ?? completed;
  const reportedHome = initializedHome?.reportedPath ?? observation?.codexHome;
  const currentHome = reportedHome ? yield* sampleHome(reportedHome) : undefined;
  if (!reportedHome && credentials) yield* credentials.sample(clearUsage);
  // App-server can load credentials before it answers initialize.
  const changedDuringLaunch =
    launchHome !== undefined &&
    initializedHome !== undefined &&
    !sameCredentialGeneration(launchHome, initializedHome);
  const generationChanged =
    changedDuringLaunch ||
    (initializedHome !== undefined &&
      (currentHome === undefined ||
        currentHome.authMetadataUnavailable === true ||
        !sameCredentialGeneration(initializedHome, currentHome)));
  const homeFingerprint = fingerprintCodexHome(currentHome?.realPath);
  const verifiedHome =
    !generationChanged && initializedHome?.authGeneration !== undefined
      ? initializedHome
      : undefined;
  const refusal = observation && !generationChanged ? loginRefusedAfter(observation) : undefined;
  const account = generationChanged ? undefined : observation?.account.account;
  const email = codexAccountEmail(account)?.trim().toLowerCase();
  const plan = account?.type === "chatgpt" ? account.planType.trim() : undefined;
  const accountId = account?.type === "chatgpt" ? observedLogin?.accountId : undefined;
  const workspaceId = account?.type === "chatgpt" ? observedLogin?.workspaceId : undefined;
  const authGeneration = currentHome?.authGeneration;
  const identity = { homeFingerprint, email, plan, accountId, workspaceId, authGeneration };
  const verifiedLogin =
    !generationChanged &&
    homeFingerprint &&
    plan &&
    (email || accountId || workspaceId) &&
    authGeneration &&
    authGeneration === initializedHome?.authGeneration;
  const stickyRefusal = loginRefusedState
    ? yield* Ref.updateAndGet(loginRefusedState, (previous) => {
        if (refusal === true && verifiedLogin) {
          return {
            homeFingerprint,
            plan,
            authGeneration,
            ...(email ? { email } : {}),
            ...(accountId ? { accountId } : {}),
            ...(workspaceId ? { workspaceId } : {}),
          };
        }
        if (
          refusal === false ||
          (currentHome?.realPath && previous && previous.homeFingerprint !== homeFingerprint) ||
          (currentHome?.realPath &&
            !currentHome.authMetadataUnavailable &&
            previous?.authGeneration &&
            previous.authGeneration !== currentHome.authGeneration) ||
          loginFields.some(
            (field) =>
              previous?.[field] !== undefined &&
              identity[field] !== undefined &&
              previous[field] !== identity[field],
          )
        )
          return undefined;
        return previous;
      })
    : undefined;
  const activeRefusal =
    stickyRefusal !== undefined &&
    Boolean(verifiedLogin) &&
    stickyRefusal.plan !== undefined &&
    stickyRefusal.authGeneration !== undefined &&
    loginFields.every(
      (field) => stickyRefusal[field] === undefined || stickyRefusal[field] === identity[field],
    );
  const loginRefused = refusal === true || activeRefusal;
  const loginUnverified =
    generationChanged || (stickyRefusal !== undefined && !activeRefusal && refusal !== true);
  const refusalField = stickyRefusal ? { codexLoginRefusal: stickyRefusal } : {};
  const refusalUsage = loginRefused
    ? {
        usageLimits: makeUnavailableUsageLimits({
          checkedAt,
          reason: "probeFailed",
          message: CODEX_LOGIN_REFUSED_USAGE_MESSAGE,
        }),
      }
    : {};
  const configuredHome = codexSettings.homePath
    ? completed
      ? expandHomePath(codexSettings.homePath, yield* HostProcess.HomeDirectory)
      : (observation?.codexHome ?? codexSettings.homePath)
    : undefined;

  if (Result.isFailure(probeResult)) {
    const error = probeResult.failure;
    return {
      ...buildServerProvider({
        presentation: CODEX_PRESENTATION,
        enabled: codexSettings.enabled,
        checkedAt,
        models: emptyModels,
        skills: [],
        probe: {
          installed,
          version: fallbackVersion,
          status: "error",
          auth: { status: loginRefused ? "unauthenticated" : "unknown" },
          ...refusalUsage,
          message: installed
            ? loginRefused
              ? codexLoginRefusedMessage(configuredHome)
              : `Codex app-server provider probe failed: ${error.message}.`
            : `Could not start Codex CLI (\`${codexSettings.binaryPath}\`). Check Settings → Providers → Codex → Binary path on the server.` +
              (codexSettings.binaryPath === "codex"
                ? " Installing ChatGPT or Codex desktop may not add codex to PATH."
                : " Make sure the configured executable exists and can be run."),
        },
      }),
      ...refusalField,
    };
  }

  if (Option.isNone(probeResult.success)) {
    return {
      ...buildServerProvider({
        presentation: CODEX_PRESENTATION,
        enabled: codexSettings.enabled,
        checkedAt,
        models: emptyModels,
        skills: [],
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: loginRefused ? "unauthenticated" : "unknown" },
          ...refusalUsage,
          message: loginRefused
            ? codexLoginRefusedMessage(configuredHome)
            : "Timed out while checking Codex app-server provider status.",
        },
      }),
      ...refusalField,
    };
  }

  const snapshot = probeResult.success.value;
  if (verifiedHome) credentials?.verify(verifiedHome);
  const rateLimits = generationChanged ? undefined : snapshot.rateLimits;
  const usageWorkspaceId =
    rateLimits && "accountId" in rateLimits ? rateLimits.accountId?.trim() || undefined : undefined;
  const publishedWorkspaceId = snapshot.workspaceId ?? usageWorkspaceId;
  const accountStatus = managedAuth
    ? { status: "ready" as const, auth: managedAuth, message: undefined }
    : accountProbeStatus(
        snapshot.account,
        configuredHome,
        loginRefused,
        publishedWorkspaceId,
        rateLimits,
      );
  const conflictingWorkspace =
    usageWorkspaceId !== undefined &&
    snapshot.workspaceId !== undefined &&
    usageWorkspaceId !== snapshot.workspaceId;
  const unavailableMessage = loginRefused
    ? CODEX_LOGIN_REFUSED_USAGE_MESSAGE
    : conflictingWorkspace
      ? "Codex reported different workspaces for the login and its usage."
      : (accountStatus.message ??
        (snapshot.configUnavailable ? CODEX_CONFIG_UNAVAILABLE_USAGE_MESSAGE : undefined) ??
        (rateLimits && "failure" in rateLimits ? rateLimits.failure : undefined));
  const usageLimits =
    managedAuth || loginUnverified || (!verifiedHome && rateLimits && "snapshot" in rateLimits)
      ? undefined
      : !snapshot.nativeUsage && !snapshot.configUnavailable && !accountStatus.message
        ? makeUnavailableUsageLimits({ checkedAt, reason: "unsupported" })
        : rateLimits === undefined || "failure" in rateLimits || conflictingWorkspace
          ? makeUnavailableUsageLimits({
              checkedAt,
              reason: "probeFailed",
              ...(unavailableMessage ? { message: unavailableMessage } : {}),
            })
          : codexRateLimitsToLimits({
              snapshot: rateLimits.snapshot,
              rateLimitsByLimitId: rateLimits.rateLimitsByLimitId,
              resetCredits: rateLimits.resetCredits,
              checkedAt,
            });

  const provider = buildServerProvider({
    presentation: CODEX_PRESENTATION,
    enabled: codexSettings.enabled,
    checkedAt,
    models: pipe(
      appendCustomCodexModels(snapshot.models ?? [], codexSettings.customModels),
      applyPreferredCodexDefaultModel,
      (models) => applyConfiguredCodexServiceTier(models, snapshot.serviceTier),
      (models) => applyConfiguredCodexCyberAccess(models, snapshot),
    ),
    skills: snapshot.skills,
    slashCommands: [
      COMPACT_SLASH_COMMAND,
      {
        name: "goal",
        description: "Set a goal Codex keeps working toward until it is done",
        input: { hint: "Objective, or pause, resume, clear" },
      },
      {
        name: "feedback",
        description: "Send this thread and Codex logs to OpenAI",
        input: { hint: "Describe the issue (optional)" },
      },
    ],
    probe: {
      installed: true,
      version: snapshot.version ?? null,
      status:
        accountStatus.auth.status === "unauthenticated" || snapshot.models !== undefined
          ? accountStatus.status
          : "warning",
      auth: loginUnverified
        ? { status: "unknown" }
        : snapshot.endpoint
          ? { ...accountStatus.auth, endpoint: snapshot.endpoint }
          : accountStatus.auth,
      ...(accountStatus.message
        ? { message: accountStatus.message }
        : snapshot.models === undefined
          ? { message: "Codex did not return its model catalog in time." }
          : {}),
      ...(usageLimits ? { usageLimits } : {}),
    },
  });
  return {
    ...provider,
    ...refusalField,
    ...(verifiedHome && homeFingerprint ? { homeFingerprint } : {}),
  };
});

// A replacement driver can start before the retired driver's detached process stops.
const activeCodexStatusProbes = new WeakMap<
  ProviderHost.ProviderHost["Service"],
  Map<ProviderInstanceId, Deferred.Deferred<void>>
>();
const liveCodexAppServers = new WeakMap<
  ProviderHost.ProviderHost["Service"],
  Map<ProviderInstanceId, number>
>();

export const retainInstanceCodexAppServer = Effect.fn("retainInstanceCodexAppServer")(function* (
  instanceId: ProviderInstanceId,
) {
  const host = yield* ProviderHost.ProviderHost;
  const live = liveCodexAppServers.get(host) ?? new Map<ProviderInstanceId, number>();
  liveCodexAppServers.set(host, live);
  yield* Effect.acquireRelease(
    Effect.sync(() => live.set(instanceId, (live.get(instanceId) ?? 0) + 1)),
    () =>
      Effect.sync(() => {
        const remaining = (live.get(instanceId) ?? 1) - 1;
        if (remaining === 0) live.delete(instanceId);
        else live.set(instanceId, remaining);
      }),
  );
});

export const awaitInstanceCodexStatusProbe = Effect.fn("awaitInstanceCodexStatusProbe")(function* (
  instanceId: ProviderInstanceId,
) {
  const host = yield* ProviderHost.ProviderHost;
  const active = activeCodexStatusProbes.get(host)?.get(instanceId);
  if (active) yield* Deferred.await(active);
});

export const makeCodexCredentialRefresh = Effect.fn("makeCodexCredentialRefresh")(function* (
  instanceId: ProviderInstanceId,
  refresh: Effect.Effect<unknown>,
) {
  const requests = yield* Queue.sliding<void>(1);
  yield* Effect.gen(function* () {
    yield* Queue.take(requests);
    yield* awaitInstanceCodexStatusProbe(instanceId);
    yield* Queue.clear(requests);
    yield* refresh;
  }).pipe(Effect.ignoreCause({ log: true }), Effect.forever, Effect.forkScoped);
  return Queue.offer(requests, undefined).pipe(Effect.asVoid);
});

type CodexCredentialMonitor = Effect.Success<ReturnType<typeof makeCodexCredentialMonitor>>;

export const makeCodexCredentialMonitor = Effect.fn("makeCodexCredentialMonitor")(function* (
  homePath: string,
) {
  const services = yield* Effect.context<FileSystem.FileSystem | Path.Path>();
  let observed = yield* readCodexProbeHome(homePath);
  let verified: CodexProbeHome | undefined;
  let followingUp = false;
  const requests = yield* Queue.sliding<void>(1);
  const samplePermit = yield* Semaphore.make(1);
  const sample = Effect.fn("CodexCredentialMonitor.sample")(
    function* (clearUsage: Effect.Effect<void>) {
      const home = yield* readCodexProbeHome(homePath);
      const changed = !sameCredentialGeneration(observed, home);
      observed = home;
      if (changed) {
        verified = undefined;
        yield* clearUsage;
        if (!followingUp) yield* Queue.offer(requests, undefined);
      }
      return home;
    },
    Effect.provide(services),
    samplePermit.withPermits(1),
  );
  const isCurrent = () => verified !== undefined && sameCredentialGeneration(verified, observed);
  const run = Effect.fn("CodexCredentialMonitor.run")(function* (
    instanceId: ProviderInstanceId,
    refresh: Effect.Effect<unknown>,
  ) {
    const wait = awaitInstanceCodexStatusProbe(instanceId);
    yield* Effect.gen(function* () {
      yield* Queue.take(requests);
      yield* wait;
      yield* Queue.clear(requests);
      if (isCurrent()) return;

      // The probe and its teardown still invalidate usage, but cannot queue another follow-up.
      followingUp = true;
      yield* refresh.pipe(
        Effect.ignoreCause({ log: true }),
        Effect.andThen(wait),
        Effect.ensuring(
          Effect.sync(() => {
            followingUp = false;
          }),
        ),
      );
    }).pipe(Effect.ignoreCause({ log: true }), Effect.forever, Effect.forkScoped);
  });
  return {
    sample,
    run,
    isCurrent,
    verify: (home: CodexProbeHome) => {
      verified = home;
    },
  };
});

export const checkInstanceCodexProviderStatus = Effect.fn("checkInstanceCodexProviderStatus")(
  function* (
    instanceId: ProviderInstanceId,
    settings: CodexSettings,
    environment: NodeJS.ProcessEnv,
    options: {
      readonly loginRefusedState?: Ref.Ref<CodexLoginRefusal>;
      readonly managedAuth?: ServerProvider["auth"];
      readonly credentials?: CodexCredentialMonitor;
    },
  ) {
    // A disabled check spawns nothing, so it need not wait for an earlier probe to close.
    if (!settings.enabled) return yield* checkCodexProviderStatus(settings);
    const host = yield* ProviderHost.ProviderHost;
    const unattended = yield* UnattendedProviderCheck;
    const clearUsage = yield* ClearProviderUsage;
    const sample = options.credentials?.sample(clearUsage) ?? Effect.void;
    const active =
      activeCodexStatusProbes.get(host) ?? new Map<ProviderInstanceId, Deferred.Deferred<void>>();
    activeCodexStatusProbes.set(host, active);

    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (
          active.has(instanceId) ||
          (unattended && liveCodexAppServers.get(host)?.has(instanceId))
        ) {
          yield* restore(sample);
          return undefined;
        }
        const stopped = Deferred.makeUnsafe<void>();
        active.set(instanceId, stopped);
        const release = Effect.sync(() => active.delete(instanceId)).pipe(
          Effect.andThen(Deferred.succeed(stopped, undefined)),
        );
        let processOwnsRelease = false;
        return yield* restore(
          checkCodexProviderStatus(
            settings,
            (input) =>
              probeCodexAppServerProvider(input, (scope) =>
                Scope.addFinalizer(scope, sample.pipe(Effect.ensuring(release))).pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      processOwnsRelease = true;
                    }),
                  ),
                ),
              ),
            environment,
            options.managedAuth,
            options.loginRefusedState,
            options.credentials,
          ),
        ).pipe(Effect.ensuring(Effect.suspend(() => (processOwnsRelease ? Effect.void : release))));
      }),
    );
  },
);

export { makePendingCodexProvider };
