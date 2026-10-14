// Stream keepalive self-check.
// Run: node open-sse/utils/streamKeepaliveSelfCheck.mjs
// No framework, no deps. Uses assert. Mirrors claudeToolTypeSelfCheck.mjs style.
//
// Covers the two ways silent prefill broke a client: no bytes at all before the
// first token, and the keepalive frame itself must not leak into the payload.
import { withStreamKeepalive, withEarlySseHeaders, clientWantsStream } from "./streamHandler.js";

const results = [];
function run(name, fn) {
  return Promise.resolve()
    .then(fn)
    .then(() => results.push({ name, ok: true }))
    .catch((err) => results.push({ name, ok: false, err: err.message }));
}
const assert = {
  equal(a, b, msg) { if (a !== b) throw new Error(`${msg || ""} expected ${b}, got ${a}`); },
  ok(v, msg) { if (!v) throw new Error(msg || "expected truthy"); },
};

const decoder = new TextDecoder();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function streamOf(chunks, gapMs = 0) {
  return new ReadableStream({
    async start(controller) {
      for (const c of chunks) {
        if (gapMs) await sleep(gapMs);
        controller.enqueue(new TextEncoder().encode(c));
      }
      controller.close();
    }
  });
}

async function readAll(readable) {
  const reader = readable.getReader();
  let out = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) return out;
    out += decoder.decode(value);
  }
}

// 1. A silent upstream produces keepalive frames instead of nothing.
await run("silent upstream emits keepalive frames", async () => {
  const src = new ReadableStream({
    async start(controller) {
      await sleep(90);
      controller.enqueue(new TextEncoder().encode("data: real\n\n"));
      controller.close();
    }
  });
  const out = await readAll(withStreamKeepalive(src, false, 20));
  assert.ok(out.includes(": keepalive"), "keepalive comment emitted");
  assert.ok(out.includes("data: real"), "real chunk still delivered");
});

// 2. A Claude client gets the ping event the real API also sends.
await run("claude client gets ping event, not a comment", async () => {
  const silent = new ReadableStream({
    async start(controller) {
      await sleep(60);
      controller.close();
    }
  });
  const out = await readAll(withStreamKeepalive(silent, true, 20));
  assert.ok(out.includes("event: ping"), "ping event emitted");
  assert.ok(!out.includes(": keepalive"), "no comment frame for claude client");
});

// 3. Keepalives never split or reorder the payload.
await run("payload is passed through byte-for-byte", async () => {
  const chunks = ["event: message_start\ndata: {}\n\n", "event: message_stop\ndata: {}\n\n"];
  const out = await readAll(withStreamKeepalive(streamOf(chunks, 0), true, 1000));
  assert.equal(out, chunks.join(""), "payload unchanged");
  assert.ok(!out.includes("ping"), "no keepalive when data flows");
});

// 4. Interval 0 disables the wrapper entirely.
await run("interval 0 disables keepalive", async () => {
  const src = streamOf(["a", "b"], 0);
  assert.equal(withStreamKeepalive(src, false, 0), src, "same stream returned");
});

// 5. A slow reply hands its body over: early pings, then the real payload, no loss.
await run("early SSE forwards the real body after its pings", async () => {
  const res = await withEarlySseHeaders(async () => {
    await sleep(60);
    // The header is load-bearing: a reply that is not labelled as a stream is
    // treated as a protocol mismatch and turned into a terminal error frame.
    return new Response(streamOf(["data: real\n\n"], 0), { headers: { "content-type": "text/event-stream" } });
  }, true, 20);
  assert.equal(res.headers.get("content-type"), "text/event-stream", "SSE headers");
  const out = await readAll(res.body);
  assert.ok(out.includes("event: ping"), "pinged while waiting");
  assert.ok(out.includes("data: real"), "real body forwarded");
});

// 6. A fast failure keeps its own status — it must not be reported as a 200 stream.
await run("fast failure keeps its real status", async () => {
  const res = await withEarlySseHeaders(async () => new Response("nope", { status: 400 }), true, 200);
  assert.equal(res.status, 400, "status preserved");
  assert.equal(await res.text(), "nope", "body preserved");
});

// 7. Grace 0 disables the early commit entirely.
await run("grace 0 awaits the real response", async () => {
  const res = await withEarlySseHeaders(async () => new Response("ok"), true, 0);
  assert.equal(res.status, 200, "status preserved");
  assert.equal(await res.text(), "ok", "not converted to SSE");
});

// 8. A non-streaming client is never answered with a stream, however slow.
await run("non-streaming client gets JSON, not SSE", async () => {
  const req = new Request("http://x/v1/messages", {
    method: "POST",
    headers: { "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify({ stream: false, messages: [] }),
  });
  assert.equal(await clientWantsStream(req), false, "stream:false client wants JSON");
  const res = await withEarlySseHeaders(async () => {
    await sleep(30);
    return new Response(JSON.stringify({ id: "m1" }), { headers: { "content-type": "application/json" } });
  }, true, { graceMs: 1, wantsStream: await clientWantsStream(req) });
  assert.equal(res.status, 200, "status preserved");
  assert.equal(res.headers.get("content-type"), "application/json", "not committed to SSE");
  assert.equal(JSON.parse(await res.text()).id, "m1", "real body preserved");
});

// 9. An omitted stream flag is the non-streaming default, a true one is not.
await run("clientWantsStream reads accept, then the body", async () => {
  const mk = (headers, body) => new Request("http://x/v1/messages", { method: "POST", headers, body: JSON.stringify(body) });
  assert.equal(await clientWantsStream(mk({ accept: "text/event-stream", "content-type": "application/json" }, { stream: true })), true, "SSE accept");
  assert.equal(await clientWantsStream(mk({ accept: "*/*", "content-type": "application/json" }, { stream: true })), true, "stream:true in body");
  assert.equal(await clientWantsStream(mk({ accept: "*/*", "content-type": "application/json" }, { messages: [] })), false, "no stream flag is non-streaming");
  assert.equal(await clientWantsStream(mk({ accept: "application/json", "content-type": "application/json" }, { stream: true })), true, "body wins over a JSON accept");
});

// 10. A streaming client whose reply is JSON gets a terminal error frame, not the blob.
await run("JSON reply becomes a terminal error frame", async () => {
  const res = await withEarlySseHeaders(async () => {
    await sleep(30);
    return new Response(JSON.stringify({ error: { message: "rate limited upstream" } }), { status: 429, headers: { "content-type": "application/json" } });
  }, true, { graceMs: 1 });
  const out = await readAll(res.body);
  assert.ok(out.includes("event: error"), "terminal error event");
  assert.ok(out.includes("rate limited upstream"), "upstream reason preserved");
  assert.ok(!out.startsWith("{"), "no raw JSON blob");
});

for (const r of results) {
  console.log(`${r.ok ? "PASS" : "FAIL"}  ${r.name}${r.ok ? "" : `\n      ${r.err}`}`);
}
const failed = results.filter((r) => !r.ok).length;
console.log(`\n${results.length - failed}/${results.length} passed`);
process.exit(failed === 0 ? 0 : 1);
