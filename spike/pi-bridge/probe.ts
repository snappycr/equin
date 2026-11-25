#!/usr/bin/env bun
// Spike: prove Pi RPC works headless and see its event stream.
//   DEEPSEEK_API_KEY=... bun spike/pi-bridge/probe.ts "list the files here"
// Not part of equin's build. Pi must be on PATH (pi --version).
import { spawn } from "node:child_process";

const prompt = process.argv.slice(2).join(" ") || "Say hello in five words.";
const cwd = process.env.PI_CWD || process.cwd();

const child = spawn("pi", ["--mode", "rpc", "--no-session"], {
  cwd,
  stdio: ["pipe", "pipe", "pipe"],
});
child.stderr.on("data", (d) => process.stderr.write(d));

let buf = "";
child.stdout.on("data", (chunk) => {
  buf += chunk.toString("utf8");
  let nl;
  while ((nl = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, nl).replace(/\r$/, "");
    buf = buf.slice(nl + 1);
    if (!line.trim()) continue;
    let rec;
    try { rec = JSON.parse(line); } catch { console.log("RAW", line); continue; }
    if (rec.type === "message_update") {
      const ev = rec.assistantMessageEvent;
      if (ev?.type === "text_delta") process.stdout.write(ev.delta);
    }
    console.log("\nEVENT", rec.type, JSON.stringify(rec).slice(0, 400));
    if (rec.type === "agent_settled") {
      child.stdin.end();
      setTimeout(() => process.exit(0), 200);
    }
  }
});

child.on("exit", (code) => {
  console.log("\nPI EXIT", code);
  process.exit(code ?? 0);
});

child.stdin.write(JSON.stringify({ id: "1", type: "prompt", message: prompt }) + "\n");
setTimeout(() => { console.error("timeout"); child.kill("SIGTERM"); }, 120000);
