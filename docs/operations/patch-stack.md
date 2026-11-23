# Fork patch stack

## ACP session lineage

Patch trailer: `Patch-Stack: acp-session-lineage`.

This patch adds ACP session lineage metadata for agents that opt in. Enable
`sessionLineage` on an ACP provider instance to share its thread ID, root and
parent thread IDs, relationship, title, and the active parent's native session
reference under `_meta["t3code/lineage"]`. The setting defaults to `false` and
sends no lineage metadata when disabled. New, load, resume, and fork requests use
the same version 1 payload. Forks from earlier turns merge lineage with the
agent-specific fork metadata, whose keys take precedence. Empty titles and the
default "New thread" placeholder are sent as `null`.

Only native Claude Code and Codex parents identify their native session kind.
Registry and other parents carry a null kind until their session ID semantics
are verified. Keep this restriction when rebasing the patch onto upstream.

## Codex usage refresh

Patch trailer `Patch-Stack: codex-usage-refresh`.

Tools can read the last published Codex snapshot at
`~/.t3/caches/<instanceId>.json` for the default T3 home. A custom T3 home uses
its own `caches` directory. The server still uses the base T3 home for this cache
in dev mode.

- Missing `usageLimits` means the snapshot has no usage reading. A successful
  probe publishes native Codex windows only when the app-server reports the
  configured Codex home and the generation of its readable `auth.json` matches
  before and after the account and usage reads. T3 derives the generation from
  file metadata alone. A detected change, including file removal after sign-out,
  clears old windows immediately. Cache writes for each instance run in sequence
  and read the latest snapshot before writing, so a delayed write cannot restore
  cleared windows. A canceled refresh still writes any invalidation it committed
  in memory. Top-level `checkedAt` records the provider check.
  `usageLimits.checkedAt` records the usage reading. A confirmed non-native
  route, including an API-key login, reports `unsupported`.
- A verified probe sets `homeFingerprint` to the lowercase hexadecimal SHA-256
  of the UTF-8 bytes of the resolved path of the configured Codex home. For a
  shadow instance this is its shadow home. T3 expands `~` and resolves symlinks
  before comparing homes. The probe withholds the fingerprint and windows if the
  paths differ or `auth.json` is missing or unreadable.
- A refusal observed within the usage deadline sets `auth.status` to
  `unauthenticated` and provider `status` to `error`, even if model or skill
  discovery fails. Earlier usage windows are dropped. T3 records the refusal with
  the home fingerprint, available account identifiers and plan, and a generation
  derived from `auth.json` metadata. It never reads that file for this check.
  A saved refusal stays dormant until the current home and account match. An
  inconclusive check reports `auth.status: "unknown"` and retains the record.
  A different home, account or auth generation, a successful usage read, a
  sign-out, or a confirmed non-native route clears the refusal. Without an
  `auth.json` file, for example with credentials in the system keyring, there is
  no generation and a refusal is shown for that check only, never saved.

Native Codex instances check at the provider health interval even when no client
is connected or the screen is locked. The Balanced profile uses five minutes.
The client and unattended timers, including failure retries, schedule at most
about one usage read per minute for each provider instance on each T3 server.
Instances and environments signed into the same account check independently.
A manual refresh, opening Limits, a turn failing because of a usage limit, or
the follow-up after an `auth.json` change can trigger an immediate read.
Zero disables periodic checks. The unattended timer counts its interval from
the last published check or skipped attempt. Host suspension and serious or
critical thermal pressure pause unattended checks. Low power mode and battery
follow the background activity profile.
Native-route timers add up to a tenth of the interval in jitter, capped at
30 seconds. Unsupported routes get an unattended recheck after six effective
intervals or 30 minutes, whichever comes first, when host policy permits. The
timer waits for that deadline even when the configured interval is longer.

The unattended timer defers its probe and keeps the last snapshot while the
instance has a live Codex app-server, then tries again one interval later. A
session counts as live for as long as it is open, including its idle period of
up to 30 minutes. A session with pending background work or a delegated child
awaiting delivery stays pinned for up to 4 hours, counted from its first
deferred idle release, so the worst case is about 4.5 hours. Skills scans and
reset-credit redemption count while they run. Client demand and manual
refreshes still run in those cases. The timer does not give way to a connected
client unless a check was published since it was scheduled or is still
running. With a client connected, a single failed unattended check is held
back like a failed client check. Either timer can confirm a held failure at
least 60 seconds after it occurred. Both check the retry deadline after
acquiring the probe permit. If the client stops asking for status, the unattended
timer takes over and schedules the next check one effective interval (at least
60 seconds) after the failure, plus any jitter. A newer published check cancels
the retry. The client timer skips a cycle when any check other than the first
was published while it waited.

A check skips when another status probe for that instance is still closing.
After an `auth.json` change or a managed sign-in, the refresh waits for that
probe's teardown, up to about two minutes. Native Codex coalesces watcher events
by file metadata and can immediately check the new login once per burst.
If credentials change again during that follow-up or its teardown, usage stays
unknown until a scheduled or manual check verifies the generation. With the
interval at zero, a manual refresh can recover the reading. A new credential change after teardown
can start another burst. A disabled instance publishes at once. A
replacement driver keeps the last checked row while it is being built and
retries its first check after teardown, even when the periodic interval is zero.

A check starts one `codex app-server`. Some failures also run a bounded
`codex --version` fallback. Teardown waits up to 60 seconds for T3's pending
requests, then gives the process group its startup-work grace period.

Accepted limitations:

- Codex can refresh or rewrite credentials in-process during startup, before T3
  sends any request, and neither wait covers that work. T3 does not coordinate
  OAuth refresh across Codex processes, so a probe can race a session or another
  tool that refreshes the same login. A running app-server can still report its
  earlier login after another process replaces the file. T3 checks again in a
  fresh app-server when it detects a generation change.
- Only unattended checks avoid a second app-server for the instance. A session,
  skills scan or reset-credit redemption can start during an unattended check or
  its teardown.
- The fence is per instance id. Two instances that share a `CODEX_HOME` can
  probe and run sessions on the same login at the same time.
- Each unattended check runs Codex's startup work, including the plugin
  marketplace fetch, which can ask an SSH agent for a key. The only opt-out is a
  provider health interval of zero.
- Without a connected client, a single transient failure of an unattended
  check clears the usage reading until the next interval.
