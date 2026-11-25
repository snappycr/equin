# Pi runtime (fork design)

Status: draft for `feat/pi-runtime`. Upstream: equinai/equin (Apache-2.0).
Pi: earendil-works/pi-mono (MIT), package `@earendil-works/pi-coding-agent`.

## Goal

Run equin conversations on **Pi** instead of Claude Code, with Pi's providers
(DeepSeek first), while keeping equin's agent manager, tasks, worktrees,
transcripts, status and UI intact.

## Non-goals (v1)

- Replacing the Claude path (it stays the default).
- Feature parity with Claude Code (sub-agents, plan mode, hooks, permissions).
- Upstreaming before the fork proves the seam.

## The seam: equin already has a process boundary

A conversation is one detached agent process (`bin/agent.ts`) that:

- reads `launch.json` (`AgentLaunchOptions`, written by the backend),
- takes newline-delimited JSON commands on a Unix socket
  (`core/protocol.ts`: `hello`, `turn`, `interrupt`, `answer`, `dismiss`,
  `setModel`, `rewindFiles`, `shutdown`, `subscribe`, `reconnectMcp`),
- appends numbered events to `events.jsonl` (the durable contract; the backend
  replays from its DB cursor),
- writes a pid file and a usage file, logs to `agent.log`.

Everything downstream (status reconciler, turn ledger, question relay,
transcript, usage, UI) reads that contract, not Claude directly. **So Pi
support is a second agent host, not a rewrite.**

    backend (machine/) ──socket──> agent-pi.ts ──RPC──> pi --mode rpc
            ^                          │
            └──── events.jsonl ────────┘

## Two ways to embed Pi

| | A. In-process SDK | B. RPC subprocess (recommended) |
|---|---|---|
| API | `createAgentSession()` from `@earendil-works/pi-coding-agent` | `pi --mode rpc` JSONL on stdin/stdout |
| Coupling | Pi in equin's Bun build + lockfile | version-independent, process-isolated |
| Events | typed TS events | JSON records, same content |
| Upgrade | dependency bump, rebuild | bump the `pi` binary |
| Extra process | none | bridge + pi (or bridge is the host) |

Start with **B**. The host (`agent-pi.ts`) owns equin's socket and events file
and spawns `pi --mode rpc` as a child; the translation layer stays small and
Pi can be upgraded without touching equin's build.

## Changes, file by file

### Phase 1 — MVP: one Pi conversation

1. `plugins/conversations/plugins/runtime-sdk/bin/agent-host-pi.ts` (new)
   - mirrors `agent-host.ts` boot/lease, then execs `agent-pi.ts`.
2. `plugins/conversations/plugins/runtime-sdk/bin/agent-pi.ts` (new)
   - read `launch.json`; create socket; write pid file;
   - spawn `pi --mode rpc --no-session --cwd <cwd> --provider <p> --model <m>`;
   - bridge: `turn`→`prompt`, `interrupt`→`abort`, `setModel`→`set_model`,
     `subscribe`→event stream, `shutdown`→close stdin + SIGTERM;
   - append translated events to `events.jsonl` with monotonic `seq`;
   - `hello` reports `version`, `pid`, `seq`, `ready`, `cliVersion`
     (`pi --version`), `openRequests`.
3. `core/protocol.ts`
   - additive only: `harness: "claude" | "pi"` in the launch options and
     `hello`; no `PROTOCOL_VERSION` bump (older peers ignore it).
4. `machine/internal/launch-options.ts`
   - branch on `harness`; Pi intent carries `provider`, `model`,
     `thinkingLevel`, `tools`, and the provider credential name — not
     Claude flags.
5. `machine/internal/paths.ts` / `agent-process.ts`
   - choose `agent-host-pi.ts` for `harness: "pi"`; skip the Claude-account
     argv and the `claude` disclaim-exec copy.
6. `agent-launch/server/internal/agent-session-env.ts`
   - keep `env -i` and the allowlist; deliver **only** the selected provider's
     key (e.g. `DEEPSEEK_API_KEY`) from the secret store. Never pass the
     user's whole environment.
7. `model-provider` (minimal)
   - accept `pi:<provider>/<model>` as a launch-time model string without the
     Claude id grammar; store it in a Pi-specific extension row until Phase 3.

**Acceptance:** `harness=pi`, no `claude` binary required, one turn against
DeepSeek streams into the UI, message persists, status goes
`working → waiting/gone`, Stop works.

### Phase 2 — protocol parity

- `turnId`: Pi has none — host keeps a map and emits `turn_started` when the
  prompt is accepted; the ledger confirms by id (no legacy text path needed).
- `subscribe`: translate `message_update/assistantMessageEvent/text_delta`
  into `PartialFrame`.
- Questions/approvals: Pi has no permission system. Register a Pi **custom
  tool** `ask_user` whose handler emits equin's `permission_request` and awaits
  `answer`/`dismiss` on the socket. For tool policy, wrap tools in a Pi
  extension that consults equin's allow/deny lists — or explicitly run
  unguarded and mark it in the UI (security decision, not a default).
- `context_usage`: map Pi's context stats per turn; emit
  `context_usage_failed` when unavailable.
- `rewindFiles`: Pi can branch sessions but has no file checkpointing. Return
  a typed `unsupported` and hide Stop-rewind in the UI for Pi conversations.
- `reconnectMcp`: Pi's MCP extension, if RPC exposes a reconnect; else
  unsupported with a clear error.
- Stop/error semantics: map Pi `agent_settled` / provider errors onto the
  existing `result` + `stoppedAt`/`stopError` shape.

### Phase 3 — models, providers, usage

- `model-provider`: add a provider dimension to `ModelChoice` and the catalog;
  discover Pi models from `pi --list-models` (or pi-ai's catalog) and show
  "Pi · DeepSeek" in the pickers. Keep Claude ids untouched.
- Credentials: a per-provider secret entry (DeepSeek/OpenRouter/…), not the
  `sk-ant-` schema; the health row shows the Pi provider's sign-in state.
- Usage/cost: Pi reports token usage and cost per message; write a Pi usage
  source beside the Claude ledger (don't force Anthropic pricing onto it).
  Subscription limits (`claude-usage`, auto-resume) are Claude-only; Pi
  conversations must not be held by them.

### Phase 4 — harness parity / hardening

- Tools and MCP: decide whether Pi uses equin's MCP servers and guards
  (`.claude` deny rules are Claude-specific; port as Pi extensions).
- Context: `CLAUDE.md`, skills, slash commands, compaction. Pi has its own
  skills/AGENTS.md discovery and compaction; adapt the system prompt and
  accept different compaction markers, or the transcript projection will
  drift.
- Installer/doctor: add `pi` (pinned) via mise; skip Claude sign-in when no
  Claude conversations exist; keep the Claude doctor row.
- Upstream: keep changes additive (new host + optional fields) so merges stay
  tractable; pin upstream by tag in the fork's README.

## Risks

1. **No permissions in Pi** — equin's approval UX and deny rules are not
   enforceable without a Pi extension. Treat as a product decision.
2. **Claude-shaped assumptions** — AskUserQuestion, sub-agents, hooks, plan
   mode, `--resume`, transcript JSONL, compaction. Pi deliberately omits
   several; v1 must degrade visibly, not silently.
3. **Model identity** — the id grammar (`<family>-<major>[-minor]`) and DB
   rows, filters, pricing all assume Claude. Provider dimension is invasive.
4. **Usage ledger** — Claude transcripts + Anthropic pricing are the source of
   truth today; Pi needs a parallel source.
5. **Upstream churn** — equin is a fast monorepo with generated surfaces;
   expect conflicts. Keep the diff behind one host file + additive fields.
6. **Licensing** — equin Apache-2.0, Pi MIT: a fork is fine. Keep NOTICE
   attribution.

## Spike (before Phase 1)

`spike/pi-bridge/probe.ts`: spawn `pi --mode rpc --no-session`, send one
prompt, print event types until `agent_settled`. Run on the VM with
`DEEPSEEK_API_KEY` set. Proves: headless auth, event names, model selection,
and whether Pi's tool events carry enough to build equin's event lines.

## Status (first cut, on `feat/pi-runtime`)

Implemented:

- `bin/agent-pi.ts` — the Pi host: socket, `events.jsonl`, pid file, launch
  prompt, `turn` / `interrupt` / `setModel` / `shutdown`.
- `harness: "claude" | "pi"` in the launch options; `AGENT_SCRIPT_PI`;
  `agent-process.ts` skips Claude's bundled binary, launcher, account and
  usage file for Pi; PATH carries `pi`.
- `EQUIN_HARNESS=pi` switches every new launch in the backend (Phase 1),
  with `EQUIN_PI_PROVIDER` / `EQUIN_PI_MODEL` (default deepseek /
  deepseek-chat).

Proven with Pi 0.85.1 + DeepSeek, no equin backend:

    REPLY hello  -> version 2, harness pi
    EVENT started, system/init, turn_started, ready
    EVENT first_token, first_message
    EVENT sdk/assistant  [{"type":"text","text":"ok"}]
    EVENT sdk/result     success

Known gaps for the next pass:

1. **Transcript view.** `transcript-source` reads Claude Code JSONL session
   files; the Pi host writes equin events, not that file. The agent manager and
   status work, the conversation transcript will be empty until the host also
   writes Claude-shaped JSONL (or the app's memory transcript provider is
   wired).
2. **Model picker** is still Claude-only; the Pi model comes from env.
3. **Questions/approvals**: Pi has no permission layer; `answer` / `dismiss`
   are refused. The user's subagent extension covers agents, not approvals.
4. **Usage/cost**: Pi usage is not in the ledger yet; `result` carries no cost.
5. `rewindFiles`, `reconnectMcp`, partial `subscribe` are refused.

## Trying it

On the VM, after the normal equin install and `pi /login` (DeepSeek):

    # gateway (systemd user unit) and builds must see the switch
    EQUIN_HARNESS=pi \
    EQUIN_PI_PROVIDER=deepseek \
    EQUIN_PI_MODEL=deepseek-flash \
      ./equin start --force
    ./equin build --allow-main --skip-checks

Then start a conversation. `agent.log` under
`~/.equin/state/sdk-agents/<ns>/<conv>/agent.log` names Pi's own errors; the
`events.jsonl` beside it is the contract.
