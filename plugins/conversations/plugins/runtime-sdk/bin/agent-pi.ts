#!/usr/bin/env bun
// One Pi agent process: equin's conversation runtime, backed by `pi --mode rpc`.
//
// It speaks exactly the wire `bin/agent.ts` speaks (core/protocol.ts) and keeps
// the same files, so the backend's consumer, reconciler, ledger and transcript
// need no Pi-specific knowledge:
//
//   options   -> <dir>/launch.json
//   commands  <- --socket            (newline-delimited JSON)
//   events    -> <dir>/events.jsonl  (numbered; the durable contract)
//   pid       -> --pid-file          (pid, then the harness name)
//   stdout    -> agent.log
//
// The translation is deliberately small: Pi session events become the Claude
// Agent SDK message shapes the projection already reads (assistant, user with
// tool_result, result) plus equin's lifecycle lines. Anything Pi cannot do
// (question relay, rewindFiles, MCP reconnect, partial subscribe) is refused
// loudly instead of being faked.
import { spawn, type ChildProcess } from "node:child_process";
import {
  appendFileSync,
  existsSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { createServer, type Socket } from "node:net";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { parseArgs } from "node:util";

const PROTOCOL_VERSION = 2;
const EVENTS_FILE = "events.jsonl";

interface LaunchOptions {
  cwd: string;
  prompt: { turnId: string; text: string } | null;
  seqFloor?: number;
  sessionId?: string;
  piProvider?: string | null;
  piModel?: string | null;
}

interface AgentEvent {
  seq: number;
  t: number;
  kind: "sdk" | "agent" | "permission_request" | "permission_resolved";
  payload: unknown;
  sleep?: null;
}

const { values: args } = parseArgs({
  options: {
    dir: { type: "string" },
    socket: { type: "string" },
    "pid-file": { type: "string" },
    account: { type: "string" },
    "usage-file": { type: "string" },
  },
  allowPositionals: true,
});

if (!args.dir || !args.socket || !args["pid-file"]) {
  console.error("agent-pi: --dir, --socket and --pid-file are required");
  process.exit(2);
}

const dir = args.dir;
const socketPath = args.socket;
const pidFile = args["pid-file"];

const options: LaunchOptions = JSON.parse(
  readFileSync(join(dir, "launch.json"), "utf8"),
) as LaunchOptions;

// -- events ------------------------------------------------------------------

let seq = options.seqFloor ?? 0;
const eventsPath = join(dir, EVENTS_FILE);
try {
  if (existsSync(eventsPath)) {
    const lines = readFileSync(eventsPath, "utf8").trim().split("\n").slice(-1);
    const last = lines[0] ? (JSON.parse(lines[0]) as AgentEvent) : null;
    if (last && Number.isInteger(last.seq)) seq = Math.max(seq, last.seq);
  }
} catch {
  // A torn last line is the backend's problem to report; number above what we
  // can read.
}

function emit(event: AgentEvent["payload"], kind: AgentEvent["kind"] = "agent"): void {
  seq += 1;
  const line: AgentEvent = { seq, t: Date.now(), kind, payload: event };
  if (kind === "agent") line.sleep = null;
  appendFileSync(eventsPath, JSON.stringify(line) + "\n");
}

// -- Pi child ----------------------------------------------------------------

let pi: ChildProcess | null = null;
let piVersion = "unknown";
let sessionId = options.sessionId ?? randomUUID();
let ready = false;
let sawFirstToken = false;
let sawFirstMessage = false;
let lastTurnId: string | null = null;
let disposed = false;

const pending = new Map<
  string,
  { resolve: (data: unknown) => void; reject: (err: Error) => void }
>();

function piCommand(
  command: Record<string, unknown>,
  timeoutMs = 30_000,
): Promise<unknown> {
  if (!pi || !pi.stdin || pi.stdin.destroyed) {
    return Promise.reject(new Error("pi is not running"));
  }
  const id = randomUUID();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`pi command ${String(command.type)} timed out`));
    }, timeoutMs);
    pending.set(id, {
      resolve: (data) => {
        clearTimeout(timer);
        resolve(data);
      },
      reject: (err) => {
        clearTimeout(timer);
        reject(err);
      },
    });
    pi!.stdin!.write(JSON.stringify({ id, ...command }) + "\n");
  });
}

function contentOf(message: {
  content?: unknown;
}): unknown[] {
  const content = message.content;
  if (typeof content === "string") return [{ type: "text", text: content }];
  if (!Array.isArray(content)) return [];
  return content.flatMap((block: unknown): Record<string, unknown>[] => {
    if (typeof block !== "object" || block === null) return [];
    const b = block as Record<string, unknown>;
    switch (b.type) {
      case "text":
        return [{ type: "text", text: String(b.text ?? "") }];
      case "thinking":
        return [{ type: "thinking", thinking: String(b.thinking ?? "") }];
      case "toolCall":
        return [
          {
            type: "tool_use",
            id: String(b.id ?? ""),
            name: String(b.name ?? ""),
            input: b.arguments ?? {},
          },
        ];
      case "image":
        return [];
      default:
        return [];
    }
  });
}

function usageOf(message: { usage?: Record<string, unknown> }): Record<string, number> {
  const u = message.usage ?? {};
  const num = (n: unknown): number => (typeof n === "number" ? n : 0);
  return {
    input_tokens: num(u.input),
    output_tokens: num(u.output),
    cache_read_input_tokens: num(u.cacheRead),
    cache_creation_input_tokens: num(u.cacheWrite),
  };
}

function toolResultText(result: unknown): string {
  if (typeof result === "string") return result;
  if (typeof result === "object" && result !== null) {
    const content = (result as { content?: unknown }).content;
    if (typeof content === "string") return content;
    if (Array.isArray(content)) {
      return content
        .map((block) =>
          typeof block === "object" &&
          block !== null &&
          (block as { type?: unknown }).type === "text"
            ? String((block as { text?: unknown }).text ?? "")
            : "",
        )
        .join("");
    }
  }
  try {
    return JSON.stringify(result);
  } catch {
    return "";
  }
}

function handlePiEvent(record: Record<string, unknown>): void {
  switch (record.type) {
    case "response": {
      const id = typeof record.id === "string" ? record.id : null;
      if (!id) return;
      const waiter = pending.get(id);
      if (!waiter) return;
      pending.delete(id);
      if (record.success === true) waiter.resolve(record.data ?? null);
      else waiter.reject(new Error(String(record.error ?? "pi command failed")));
      return;
    }
    case "agent_start":
      return;
    case "message_start": {
      const message = record.message as { role?: unknown } | undefined;
      if (message?.role === "assistant" && lastTurnId && !readyTurnEmitted) {
        // A turn Pi started itself (a background/queued run): open it.
        emit(
          {
            event: "turn_started",
            turnId: null,
            promptUuid: randomUUID(),
            text: "",
          },
          "agent",
        );
        readyTurnEmitted = true;
      }
      return;
    }
    case "message_update": {
      const ev = record.assistantMessageEvent as
        | { type?: unknown; delta?: unknown }
        | undefined;
      if (typeof ev?.delta === "string" && ev.delta.length > 0 && !sawFirstToken) {
        sawFirstToken = true;
        emit({ event: "first_token" }, "agent");
      }
      return;
    }
    case "message_end": {
      const message = record.message as
        | {
            role?: unknown;
            content?: unknown;
            usage?: Record<string, unknown>;
            stopReason?: unknown;
            errorMessage?: unknown;
            model?: unknown;
            provider?: unknown;
          }
        | undefined;
      if (!message) return;
      if (message.role === "assistant") {
        if (!sawFirstMessage) {
          sawFirstMessage = true;
          emit({ event: "first_message" }, "agent");
        }
        const failed =
          message.stopReason === "error" || message.stopReason === "aborted";
        emit(
          {
            type: "assistant",
            uuid: randomUUID(),
            parent_tool_use_id: null,
            session_id: sessionId,
            ...(failed && typeof message.errorMessage === "string"
              ? { error: message.errorMessage }
              : {}),
            message: {
              id: `msg_${randomUUID()}`,
              type: "message",
              role: "assistant",
              model: typeof message.model === "string" ? message.model : undefined,
              content: contentOf(message),
              stop_reason:
                message.stopReason === "toolUse" ? "tool_use" : "end_turn",
              usage: usageOf(message),
            },
          },
          "sdk",
        );
        return;
      }
      if (message.role === "toolResult") {
        const m = record.message as {
          toolCallId?: unknown;
          toolName?: unknown;
          content?: unknown;
          isError?: unknown;
        };
        emit(
          {
            type: "user",
            uuid: randomUUID(),
            parent_tool_use_id: null,
            session_id: sessionId,
            message: {
              role: "user",
              content: [
                {
                  type: "tool_result",
                  tool_use_id: String(m.toolCallId ?? ""),
                  content: toolResultText({ content: m.content }),
                  is_error: m.isError === true,
                },
              ],
            },
          },
          "sdk",
        );
      }
      return;
    }
    case "tool_execution_end": {
      emit(
        {
          type: "user",
          uuid: randomUUID(),
          parent_tool_use_id: null,
          session_id: sessionId,
          message: {
            role: "user",
            content: [
              {
                type: "tool_result",
                tool_use_id: String(record.toolCallId ?? ""),
                content: toolResultText(record.result),
                is_error: record.isError === true,
              },
            ],
          },
        },
        "sdk",
      );
      return;
    }
    case "agent_settled": {
      emit(
        {
          type: "result",
          subtype: record.aborted === true ? "error_during_execution" : "success",
          is_error: record.aborted === true,
          duration_ms: 0,
          num_turns: 0,
          session_id: sessionId,
        },
        "sdk",
      );
      return;
    }
    case "agent_end":
    case "compaction_start":
    case "compaction_end":
    case "queue_update":
    case "turn_start":
    case "turn_end":
      return;
    case "extension_ui_request": {
      // No extension UI is wired in this harness; cancel so Pi never waits on
      // a dialog nobody can answer.
      const id = record.id;
      if (typeof id === "string" && pi?.stdin && !pi.stdin.destroyed) {
        pi.stdin.write(
          JSON.stringify({ type: "extension_ui_response", id, cancelled: true }) + "\n",
        );
      }
      return;
    }
    default:
      console.error(`agent-pi: unhandled pi event ${String(record.type)}`);
  }
}

let readyTurnEmitted = false;

let buf = "";
function onPiData(chunk: Buffer): void {
  buf += chunk.toString("utf8");
  let nl: number;
  while ((nl = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, nl).replace(/\r$/, "");
    buf = buf.slice(nl + 1);
    if (!line.trim()) continue;
    try {
      handlePiEvent(JSON.parse(line) as Record<string, unknown>);
    } catch {
      console.error(`agent-pi: unparseable pi line: ${line.slice(0, 300)}`);
    }
  }
}

async function spawnPi(): Promise<void> {
  const argv = ["--mode", "rpc", "--no-session"];
  if (options.piProvider && options.piModel) {
    argv.push("--provider", options.piProvider, "--model", options.piModel);
  }
  pi = spawn("pi", argv, {
    cwd: options.cwd,
    stdio: ["pipe", "pipe", "pipe"],
    env: process.env,
  });
  pi.stdout?.on("data", onPiData);
  pi.stderr?.on("data", (d: Buffer) => process.stderr.write(d));
  pi.on("error", (err) => {
    console.error(`agent-pi: could not start pi: ${err.message}`);
    emit({ event: "exited", reason: `pi failed to start: ${err.message}` }, "agent");
    void shutdown(1);
  });
  pi.on("exit", (code, signal) => {
    if (disposed) return;
    console.error(`agent-pi: pi exited (${String(code)}/${String(signal)})`);
    emit({ event: "exited", reason: `pi exited ${String(code ?? signal)}` }, "agent");
    void shutdown(1);
  });
  const v = spawn("pi", ["--version"], { stdio: ["ignore", "pipe", "ignore"] });
  let out = "";
  v.stdout?.on("data", (d: Buffer) => (out += d.toString("utf8")));
  await new Promise<void>((resolve) => v.on("exit", () => resolve()));
  if (out.trim()) piVersion = out.trim();
}

async function startTurn(
  turnId: string | null,
  text: string,
  promptUuid: string,
): Promise<void> {
  if (!sawTurnInit) {
    sawTurnInit = true;
    emit({ type: "system", subtype: "init", session_id: sessionId }, "sdk");
  }
  lastTurnId = turnId;
  readyTurnEmitted = true;
  emit(
    { event: "turn_started", turnId, promptUuid, text },
    "agent",
  );
  await piCommand({ type: "prompt", message: text }, 60_000);
}

let sawTurnInit = false;

// -- socket ------------------------------------------------------------------

const turns = new Map<string, { promptUuid: string }>();

function reply(socket: Socket, value: unknown): void {
  if (socket.destroyed) return;
  socket.write(JSON.stringify(value) + "\n");
}

async function handleCommand(
  socket: Socket,
  command: Record<string, unknown>,
): Promise<void> {
  try {
    switch (command.type) {
      case "hello": {
        reply(socket, {
          ok: true,
          result: {
            version: PROTOCOL_VERSION,
            pid: process.pid,
            seq,
            ready,
            cliVersion: `pi ${piVersion}`,
            openRequests: [],
            turnIds: true,
            harness: "pi",
          } satisfies Record<string, unknown>,
        });
        return;
      }
      case "turn": {
        const turnId =
          typeof command.turnId === "string" ? command.turnId : null;
        const text = String(command.text ?? "");
        const existing = turnId ? turns.get(turnId) : undefined;
        if (existing) {
          reply(socket, { ok: true, result: { promptUuid: existing.promptUuid } });
          return;
        }
        const promptUuid = randomUUID();
        if (turnId) turns.set(turnId, { promptUuid });
        await startTurn(turnId, text, promptUuid);
        reply(socket, { ok: true, result: { promptUuid } });
        return;
      }
      case "interrupt": {
        await piCommand({ type: "abort" }, 15_000).catch(() => undefined);
        reply(socket, { ok: true });
        return;
      }
      case "setModel": {
        const model = typeof command.model === "string" ? command.model : "";
        const slash = model.indexOf("/");
        const provider = slash > 0 ? model.slice(0, slash) : options.piProvider;
        const modelId = slash > 0 ? model.slice(slash + 1) : model;
        if (!provider || !modelId) {
          reply(socket, { ok: false, error: "setModel needs provider/modelId" });
          return;
        }
        await piCommand({ type: "set_model", provider, modelId }, 30_000);
        reply(socket, { ok: true });
        return;
      }
      case "shutdown": {
        await shutdown(0);
        reply(socket, { ok: true });
        return;
      }
      case "subscribe":
        reply(socket, {
          ok: false,
          error: "this harness does not stream partial messages",
        });
        return;
      case "rewindFiles":
        reply(socket, {
          ok: false,
          error: "rewindFiles is not supported by the pi harness",
        });
        return;
      case "reconnectMcp":
        reply(socket, {
          ok: false,
          error: "reconnectMcp is not supported by the pi harness",
        });
        return;
      case "answer":
      case "dismiss":
        reply(socket, {
          ok: false,
          error: "question relay is not wired for the pi harness",
        });
        return;
      default:
        reply(socket, {
          ok: false,
          error: `unknown command ${String(command.type)}`,
        });
    }
  } catch (err) {
    reply(socket, {
      ok: false,
      error: err instanceof Error ? err.message : String(err),
    });
  }
}

const server = createServer((socket) => {
  let data = "";
  socket.on("data", (chunk) => {
    data += chunk.toString("utf8");
    let nl: number;
    while ((nl = data.indexOf("\n")) >= 0) {
      const line = data.slice(0, nl);
      data = data.slice(nl + 1);
      if (!line.trim()) continue;
      void handleCommand(socket, JSON.parse(line) as Record<string, unknown>);
    }
  });
  socket.on("error", () => undefined);
});

async function shutdown(code: number): Promise<void> {
  if (disposed) return;
  disposed = true;
  emit({ event: "exited", reason: code === 0 ? "shutdown" : "pi exited" }, "agent");
  server.close();
  try {
    rmSync(socketPath, { force: true });
  } catch {
    // best effort
  }
  try {
    rmSync(pidFile, { force: true });
  } catch {
    // best effort
  }
  if (pi) {
    try {
      pi.stdin?.end();
    } catch {
      // best effort
    }
    const child = pi;
    setTimeout(() => {
      try {
        child.kill("SIGTERM");
      } catch {
        // best effort
      }
    }, 1_000).unref();
  }
  setTimeout(() => process.exit(code), 1_500).unref();
}

process.on("SIGTERM", () => void shutdown(0));
process.on("SIGINT", () => void shutdown(0));

// -- boot --------------------------------------------------------------------

async function main(): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, () => resolve());
  });
  writeFileSync(pidFile, `${String(process.pid)}\npi\n`);
  emit(
    {
      event: "started",
      pid: process.pid,
      resume: null,
      resumeAt: null,
      fork: false,
      account: "pi",
      turnIds: true,
    },
    "agent",
  );

  await spawnPi();
  try {
    const state = (await piCommand({ type: "get_state" }, 20_000)) as {
      sessionId?: unknown;
    };
    if (typeof state?.sessionId === "string") sessionId = state.sessionId;
  } catch (err) {
    console.error(`agent-pi: get_state failed: ${String(err)}`);
  }
  ready = true;
  emit({ event: "ready", cliVersion: `pi ${piVersion}` }, "agent");

  if (options.prompt && options.prompt.text.length > 0) {
    await startTurn(options.prompt.turnId, options.prompt.text, randomUUID());
  }
}

main().catch((err) => {
  console.error(`agent-pi: boot failed: ${String(err)}`);
  emit({ event: "exited", reason: `boot failed: ${String(err)}` }, "agent");
  void shutdown(1);
});
