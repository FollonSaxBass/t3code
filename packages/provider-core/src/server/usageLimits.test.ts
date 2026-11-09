import { it as effectIt } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";
import { describe, expect, it } from "vite-plus/test";

import {
  applyUsageLimitsUpdate,
  makeUsageRequestThrottle,
  resolveUsageLimitsAfterProbe,
} from "./usageLimits.ts";

const checkedAt = "2026-09-03T12:00:00.000Z";
const session = {
  id: "five_hour",
  kind: "session",
  label: "Session",
  usedPercent: 40,
  windowDurationMins: 300,
  resetsAt: "2026-09-03T14:00:00.000Z",
} as const;
const weekly = {
  id: "seven_day",
  kind: "weekly",
  label: "Weekly",
  usedPercent: 20,
  windowDurationMins: 10_080,
} as const;
const published = { checkedAt, windows: [session, weekly] };

describe("applyUsageLimitsUpdate", () => {
  it("returns the published object itself when no window moved", () => {
    const next = applyUsageLimitsUpdate({
      previous: published,
      checkedAt: "2026-09-03T12:00:05.000Z",
      update: {
        windows: [
          { ...weekly },
          { id: "five_hour", kind: "session", label: "Session", usedPercent: 40 },
        ],
      },
    });
    expect(next).toBe(published);
    const scoped = { checkedAt, windows: [{ ...weekly, modelIds: ["opencode/a"] }] };
    const providerScoped = { checkedAt, windows: [{ ...weekly, providerIds: ["opencode"] }] };
    expect(
      applyUsageLimitsUpdate({
        previous: providerScoped,
        checkedAt,
        update: { windows: [{ ...weekly, providerIds: ["console-custom"] }] },
      })?.windows[0]?.providerIds,
    ).toEqual(["console-custom"]);
    expect(
      applyUsageLimitsUpdate({
        previous: scoped,
        checkedAt,
        update: { windows: [{ ...weekly, modelIds: ["opencode/a"] }] },
      }),
    ).toBe(scoped);
    expect(
      applyUsageLimitsUpdate({
        previous: scoped,
        checkedAt,
        update: { windows: [{ ...weekly, modelIds: ["opencode/b"] }] },
      })?.windows[0]?.modelIds,
    ).toEqual(["opencode/b"]);
  });

  it("upserts by id and keeps the reset a percent-only update omits", () => {
    const next = applyUsageLimitsUpdate({
      previous: published,
      checkedAt: "2026-09-03T12:00:05.000Z",
      update: {
        windows: [{ id: "five_hour", kind: "session", label: "Session", usedPercent: 55 }],
      },
    });
    expect(next).not.toBe(published);
    expect(next).toEqual({
      checkedAt: "2026-09-03T12:00:05.000Z",
      windows: [{ ...session, usedPercent: 55 }, weekly],
    });
  });

  it("leaves an unsupported account and an empty update alone", () => {
    const unsupported = { checkedAt, windows: [], unavailable: { reason: "unsupported" as const } };
    expect(
      applyUsageLimitsUpdate({ previous: unsupported, checkedAt, update: { windows: [session] } }),
    ).toBe(unsupported);
    expect(
      applyUsageLimitsUpdate({ previous: published, checkedAt, update: { windows: [] } }),
    ).toBe(published);
  });

  it("marks a first sparse update as unavailable", () => {
    expect(
      applyUsageLimitsUpdate({ previous: undefined, checkedAt, update: { windows: [weekly] } }),
    ).toEqual({ checkedAt, windows: [weekly], unavailable: { reason: "probeFailed" } });
  });

  it("clears a failed probe once a streamed update carries session and weekly windows", () => {
    const failed = { checkedAt, windows: [], unavailable: { reason: "probeFailed" as const } };
    expect(
      applyUsageLimitsUpdate({
        previous: failed,
        checkedAt,
        update: { windows: [session, weekly] },
      }),
    ).toEqual({ checkedAt, windows: [session, weekly] });
    expect(
      applyUsageLimitsUpdate({ previous: failed, checkedAt, update: { windows: [weekly] } }),
    ).toEqual({ checkedAt, windows: [weekly], unavailable: { reason: "probeFailed" } });
  });

  it("preserves reset credits and a $0 cap when a streamed window update changes usage", () => {
    const resetCredits = { availableCount: 2, nextExpiresAt: "2026-10-01T00:00:00.000Z" };
    const next = applyUsageLimitsUpdate({
      previous: { ...published, resetCredits, zeroSpendCap: true },
      checkedAt: "2026-09-03T12:00:05.000Z",
      update: { windows: [{ ...session, usedPercent: 55 }] },
    });

    expect(next).toEqual({
      checkedAt: "2026-09-03T12:00:05.000Z",
      windows: [{ ...session, usedPercent: 55 }, weekly],
      resetCredits,
      zeroSpendCap: true,
    });
  });
});

describe("resolveUsageLimitsAfterProbe", () => {
  it("uses read order for windows while taking metadata from the successful probe", () => {
    const live = {
      checkedAt: "2026-09-03T12:00:05.000Z",
      windows: [
        { ...session, usedPercent: 95 },
        { ...weekly, usedPercent: 70 },
      ],
      resetCredits: { availableCount: 2 },
      externalUsage: { label: "Old dashboard", url: "https://example.com/old" },
    };
    const probed = {
      ...published,
      windows: [{ ...session, usedPercent: 0 }],
      resetCredits: { availableCount: 1 },
      externalUsage: { label: "Usage dashboard", url: "https://example.com/usage" },
    };

    expect(resolveUsageLimitsAfterProbe({ published: live, probed })).toEqual({
      ...probed,
      checkedAt: live.checkedAt,
      windows: live.windows,
    });

    const sameTime = { ...probed, checkedAt: live.checkedAt };
    const newer = { ...probed, checkedAt: "2026-09-03T12:00:06.000Z" };
    expect(resolveUsageLimitsAfterProbe({ published: live, probed: sameTime })).toEqual(sameTime);
    expect(resolveUsageLimitsAfterProbe({ published: live, probed: newer })).toEqual(newer);
  });

  it("keeps the last good windows through a failed probe but not an unsupported one", () => {
    const failed = { checkedAt, windows: [], unavailable: { reason: "probeFailed" as const } };
    const unsupported = { checkedAt, windows: [], unavailable: { reason: "unsupported" as const } };
    expect(resolveUsageLimitsAfterProbe({ published, probed: failed })).toBe(published);
    expect(resolveUsageLimitsAfterProbe({ published, probed: unsupported })).toBe(unsupported);
    expect(resolveUsageLimitsAfterProbe({ published: undefined, probed: failed })).toBe(failed);
  });
});

describe("makeUsageRequestThrottle", () => {
  effectIt.effect(
    "requests one account's usage at most every five minutes, failures included",
    () =>
      Effect.gen(function* () {
        const throttle = yield* makeUsageRequestThrottle;
        let requests = 0;
        const request = (outcome: "ok" | "429") =>
          Effect.suspend(() => {
            requests++;
            return outcome === "ok" ? Effect.succeed(published) : Effect.fail("429" as const);
          });

        expect(yield* throttle.read("account-a", request("ok"))).toBe(published);
        yield* TestClock.adjust("4 minutes");
        expect(yield* throttle.read("account-a", request("ok"))).toBe(published);
        expect(requests).toBe(1);
        // Another account has its own budget.
        yield* throttle.read("account-b", request("ok"));
        expect(requests).toBe(2);

        yield* TestClock.adjust("5 minutes");
        expect(yield* Effect.flip(throttle.read("account-b", request("429")))).toBe("429");
        const reused = yield* throttle.read("account-b", request("ok"));
        expect(reused.unavailable?.reason).toBe("probeFailed");
        expect(yield* throttle.claim("account-b")).toBe(false);
        expect(requests).toBe(3);

        yield* TestClock.adjust("5 minutes");
        expect(yield* throttle.claim("account-b")).toBe(true);
        yield* throttle.reset;
        expect(yield* throttle.read("account-b", request("ok"))).toBe(published);
        expect(requests).toBe(4);
      }),
  );
});
