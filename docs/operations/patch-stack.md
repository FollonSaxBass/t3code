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
