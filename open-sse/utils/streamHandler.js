// Stream handler with disconnect detection - shared for all providers
import { STREAM_STALL_TIMEOUT_MS, STREAM_KEEPALIVE_MS, STREAM_KEEPALIVE_GRACE_MS, HTTP_STATUS } from "../config/runtimeConfig.js";
import { dbg, isDebugEnabled } from "./debugLog.js";
import { buildStreamErrorBytes } from "./streamHelpers.js";
import { FORMATS } from "../translator/formats.js";

// Get HH:MM:SS timestamp
function getTimeString() {
  return new Date().toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

/**
 * Create stream controller with abort and disconnect detection
 * @param {object} options
 * @param {function} options.onDisconnect - Callback when client disconnects
 * @param {object} options.log - Logger instance
 * @param {string} options.provider - Provider name
 * @param {string} options.model - Model name
 */
export function createStreamController({ onDisconnect, onError, log, provider, model, reqTag = "" } = {}) {
  const abortController = new AbortController();
  const startTime = Date.now();
  let disconnected = false;
  let abortTimeout = null;

  // Only abnormal terminations are logged; normal completion is covered by "📊 done".
  // isError uses errorLine (always shown, ignores LOG_LEVEL) so failures survive quiet levels.
  const logStream = (symbol, status, isError = false) => {
    const duration = Date.now() - startTime;
    const emit = isError ? log?.errorLine : log?.line;
    if (emit) emit(reqTag, symbol, `${status} · ${provider}/${model} · ${duration}ms`);
    else console.log(`[${getTimeString()}] ${symbol} ${provider}/${model} · ${status} · ${duration}ms`);
  };

  return {
    signal: abortController.signal,
    startTime,

    isConnected: () => !disconnected,

    // Call when client disconnects
    handleDisconnect: (reason = "client_closed") => {
      if (disconnected) return;
      disconnected = true;

      // Debug-only: Responses API has no [DONE] sentinel, so codex/droid close the
      // socket on every completed request. "📊 done" is the authoritative outcome line.
      dbg("CTRL", `${provider}/${model} | disconnect=${reason} | dur=${Date.now() - startTime}ms`);

      // Delay abort to allow cleanup
      abortTimeout = setTimeout(() => {
        abortController.abort();
      }, 500);

      onDisconnect?.({ reason, duration: Date.now() - startTime });
    },

    // Call when stream completes normally (no line here — "📊 done" is authoritative)
    handleComplete: () => {
      if (disconnected) return;
      disconnected = true;

      if (abortTimeout) {
        clearTimeout(abortTimeout);
        abortTimeout = null;
      }
    },

    // Call on error
    handleError: (error) => {
      if (disconnected) return;
      disconnected = true;

      if (abortTimeout) {
        clearTimeout(abortTimeout);
        abortTimeout = null;
      }

      if (error.name === "AbortError") {
        logStream("⚡", "ABORTED");
        return;
      }

      logStream("✗", `ERROR: ${error.message}${error.stack ? `\n    ${error.stack}` : ""}`, true);
      onError?.(error);
    },

    abort: () => abortController.abort()
  };
}

/**
 * Create transform stream with disconnect detection
 * Wraps existing transform stream and adds abort capability.
 *
 * Stall detection lives in pipeWithDisconnect (tied to upstream byte
 * activity), not here — output of the transform stream may be silent
 * for long periods while raw bytes still flow (e.g. Kiro EventStream
 * binary frames buffering, Claude reasoning streams).
 *
 * @param {function} [onAbortTerminal] - Receives a human-readable abort
 * message and returns terminal SSE bytes to emit downstream.
 */
export function createDisconnectAwareStream(transformStream, streamController, onAbortTerminal = null) {
  const reader = transformStream.readable.getReader();
  const writer = transformStream.writable.getWriter();
  let terminalEmitted = false;

  // Emit a synthesized terminal payload (e.g. Responses response.failed + [DONE]) once
  const emitTerminal = (controller) => {
    if (terminalEmitted || !onAbortTerminal) return;
    terminalEmitted = true;
    try {
      const bytes = onAbortTerminal();
      if (bytes) controller.enqueue(bytes);
    } catch { /* best-effort terminal */ }
  };

  return new ReadableStream({
    async pull(controller) {
      if (!streamController.isConnected()) {
        emitTerminal(controller);
        controller.close();
        return;
      }

      try {
        const { done, value } = await reader.read();

        if (done) {
          streamController.handleComplete();
          controller.close();
          return;
        }
        controller.enqueue(value);
      } catch (error) {
        const wasConnected = streamController.isConnected();
        // Controller already closed = downstream ended; not an upstream error, skip noisy log.
        const msg0 = error?.message || "";
        const isControllerClosed = msg0.includes("already closed") || msg0.includes("Invalid state");
        if (!isControllerClosed) streamController.handleError(error);
        reader.cancel().catch(() => {});
        writer.abort().catch(() => {});

        // Treat network resets / socket hang up / abort as graceful close
        const msg = error?.message || "";
        const code = error?.code || error?.cause?.code || "";
        const isNetworkClose =
          error.name === "AbortError" ||
          msg.includes("aborted") ||
          msg.includes("socket hang up") ||
          msg.includes("ECONNRESET") ||
          msg.includes("ETIMEDOUT") ||
          msg.includes("EPIPE") ||
          code === "ECONNRESET" ||
          code === "ETIMEDOUT" ||
          code === "EPIPE" ||
          code === "UND_ERR_SOCKET";

        // Graceful close on network/abort, or when a structured terminal is available
        // (Responses passthrough prefers response.failed + [DONE] over a raw transport error)
        try {
          if (!wasConnected || isNetworkClose || onAbortTerminal) {
            emitTerminal(controller);
            controller.close();
          } else {
            controller.error(error);
          }
        } catch (e) { /* already closed or cancelled */ }
      }
    },

    cancel(reason) {
      streamController.handleDisconnect(reason || "cancelled");
      reader.cancel();
      writer.abort();
    }
  });
}

/**
 * Pipe provider response through transform with disconnect detection.
 *
 * Stall watchdog tracks raw upstream byte activity, not transform output.
 * Reasoning models (Claude thinking via Kiro, etc.) can produce zero SSE
 * output for long stretches while partial EventStream frames keep arriving.
 * Measuring stall on the transform output caused false stalls and the
 * "failed to pipe response" error in Next.
 *
 * Any upstream chunk resets the timer. If no bytes arrive for
 * STREAM_STALL_TIMEOUT_MS, abort the underlying fetch via the controller.
 *
 * @param {Response} providerResponse - Response from provider
 * @param {TransformStream} transformStream - Transform stream for SSE
 * @param {object} streamController - Stream controller from createStreamController
 */
export function pipeWithDisconnect(providerResponse, transformStream, streamController, onAbortTerminal = null, stallTimeoutMs = STREAM_STALL_TIMEOUT_MS) {
  let stallTimer = null;
  let chunkCount = 0;
  let totalBytes = 0;
  let lastChunkAt = Date.now();
  let abortMessage = "upstream connection lost";
  const t0 = Date.now();
  const tag = "STREAM";
  const clearStall = () => {
    if (stallTimer) { clearTimeout(stallTimer); stallTimer = null; }
  };
  const armStall = () => {
    clearStall();
    stallTimer = setTimeout(() => {
      stallTimer = null;
      abortMessage = "stream stall timeout";
      dbg(tag, `STALL TIMEOUT ${stallTimeoutMs}ms | chunks=${chunkCount} | bytes=${totalBytes} | sinceLast=${Date.now() - lastChunkAt}ms`);
      streamController.handleError?.(new Error("stream stall timeout"));
      streamController.abort?.();
    }, stallTimeoutMs);
  };

  // Wrap controller so every termination path clears the stall timer.
  // Without this, abort/cancel/downstream-error paths leave the timer armed
  // and a stale abort could fire after the request has already ended.
  const wrappedController = {
    signal: streamController.signal,
    startTime: streamController.startTime,
    isConnected: () => streamController.isConnected(),
    handleComplete: () => { dbg(tag, `complete | chunks=${chunkCount} | bytes=${totalBytes} | dur=${Date.now() - t0}ms`); clearStall(); streamController.handleComplete(); },
    handleError: (e) => { dbg(tag, `error: ${e?.message} | chunks=${chunkCount} | bytes=${totalBytes} | dur=${Date.now() - t0}ms`); clearStall(); streamController.handleError(e); },
    handleDisconnect: (r) => { dbg(tag, `disconnect: ${r} | chunks=${chunkCount} | bytes=${totalBytes} | dur=${Date.now() - t0}ms`); clearStall(); streamController.handleDisconnect(r); },
    abort: () => { clearStall(); streamController.abort(); }
  };

  armStall();
  dbg(tag, `pipe start | stallTimeout=${stallTimeoutMs}ms`);

  const upstreamTap = new TransformStream({
    transform(chunk, controller) {
      chunkCount++;
      const sz = chunk?.byteLength || chunk?.length || 0;
      totalBytes += sz;
      const now = Date.now();
      const gap = now - lastChunkAt;
      lastChunkAt = now;
      if (isDebugEnabled && (chunkCount <= 5 || chunkCount % 20 === 0 || gap > 5000)) {
        dbg(tag, `chunk #${chunkCount} | size=${sz}B | gap=${gap}ms | total=${totalBytes}B`);
      }
      armStall();
      controller.enqueue(chunk);
    },
    flush() { dbg(tag, `upstream EOF | chunks=${chunkCount} | bytes=${totalBytes} | dur=${Date.now() - t0}ms`); clearStall(); }
  });

  const transformedBody = providerResponse.body
    .pipeThrough(upstreamTap)
    .pipeThrough(transformStream);

  return createDisconnectAwareStream(
    { readable: transformedBody, writable: { getWriter: () => ({ abort: () => Promise.resolve() }) } },
    wrappedController,
    onAbortTerminal ? () => onAbortTerminal(abortMessage) : null
  );
}

/**
 * Keep the client's own idle timer alive while it waits for output.
 *
 * Prefill on the free upstreams this router fans out to can take a minute or
 * more before the first token, and nothing is written to the client in the
 * meantime, so the client gives up first — observed as a single stream event
 * followed by silence and a "no events" failure, after which the client retries
 * the request non-streaming. A keepalive resets that timer without adding
 * content: an Anthropic client gets the `ping` event the real API also sends,
 * every other client a comment frame, which the SSE spec tells parsers to
 * ignore.
 *
 * @param {ReadableStream} readable client-facing SSE body
 * @param {boolean} isClaudeClient whether the client speaks the Claude format
 */
export function withStreamKeepalive(readable, isClaudeClient, intervalMs = STREAM_KEEPALIVE_MS) {
  if (!(intervalMs > 0)) return readable;
  const reader = readable.getReader();
  const encoder = new TextEncoder();
  const frame = isClaudeClient ? 'event: ping\ndata: {"type":"ping"}\n\n' : ": keepalive\n\n";
  // One read is kept outstanding across pulls: starting a fresh read on every
  // keepalive would queue reads behind each other and reorder the chunks.
  let pending = reader.read();
  return new ReadableStream({
    async pull(controller) {
      let timer = null;
      const keepaliveWon = await Promise.race([
        pending.then(() => false).catch(() => false),
        new Promise((resolve) => { timer = setTimeout(() => resolve(true), intervalMs); })
      ]);
      clearTimeout(timer);
      if (keepaliveWon) { controller.enqueue(encoder.encode(frame)); return; }
      let result;
      try {
        result = await pending;
      } catch {
        controller.close();
        return;
      }
      if (result.done) { controller.close(); return; }
      controller.enqueue(result.value);
      pending = reader.read();
    },
    cancel(reason) {
      pending.catch(() => {});
      return reader.cancel(reason);
    }
  });
}


/**
 * Whether the client expects an event stream on this connection.
 *
 * The wire default is non-streaming on both the OpenAI and the Anthropic
 * format: a request that does not say `stream: true` is answered with a single
 * JSON body. Accept is checked first because it is free; the body is parsed
 * only when the header says nothing useful.
 *
 * @param {Request} request
 * @returns {Promise<boolean>}
 */
export async function clientWantsStream(request) {
  const accept = (request?.headers?.get?.("accept") || "").toLowerCase();
  if (accept.includes("text/event-stream")) return true;
  if (typeof request?.clone !== "function") return true;
  try {
    const body = await request.clone().json();
    return body?.stream === true;
  } catch {
    // Unreadable body: this request is about to fail on its own terms, so keep
    // the streaming default instead of gambling on the client's contract.
    return true;
  }
}

/**
 * Commit a streaming request to SSE while the upstream handshake is still in
 * flight, then hand the real reply's body over to the same stream.
 *
 * `handleChatCore` answers only after the upstream returns headers — it walks
 * the account fallbacks and waits out their backoff, so a struggling upstream
 * leaves the client with no response bytes at all (measured: 21s from request
 * to first byte, every one of those seconds spent here). A client on an idle
 * timer gives up first, and its retry takes a different code path. Emitting the
 * keepalive early resets that timer; the body is forwarded rather than closed
 * so nothing about the reply itself changes.
 *
 * A failure that resolves inside the grace period is returned untouched, with
 * its real status code — a bad key or unknown model still answers as itself.
 *
 * @param {() => Promise<Response>} run work resolving to the real response
 * @param {boolean} isClaudeClient whether the client speaks the Claude format
 * @param {{ graceMs?: number, wantsStream?: boolean }|number} opts graceMs to
 *   wait for the real reply, wantsStream false when the client asked for a
 *   single JSON body and must not be committed to SSE (a bare number is read as
 *   graceMs)
 */
export async function withEarlySseHeaders(run, isClaudeClient, opts = {}) {
  const { graceMs = STREAM_KEEPALIVE_GRACE_MS, wantsStream = true } =
    typeof opts === "number" ? { graceMs: opts } : opts;
  const settled = Promise.resolve().then(run);
  // Claim a rejection handler: the losing branch of the race stays a live
  // promise, so a later failure must not reach the process as unhandled.
  settled.catch(() => {});
  // A client that asked for one JSON body cannot be handed an event stream: the
  // frames are not JSON, so it parses nothing out of a 200 and reports an empty
  // response — observed as "the non-streaming request was answered with a
  // stream" on the Anthropic-format client's non-stream retry. Such a request
  // waits for the real answer instead of being committed here.
  if (!wantsStream || !(graceMs > 0)) return settled;

  let early = false;
  await Promise.race([
    settled.then(() => {}, () => {}),
    new Promise((resolve) => setTimeout(() => { early = true; resolve(); }, graceMs))
  ]);
  if (!early) return settled;

  const encoder = new TextEncoder();
  const frame = isClaudeClient ? 'event: ping\ndata: {"type":"ping"}\n\n' : ": keepalive\n\n";
  let heartbeat = null;
  const body = new ReadableStream({
    async start(controller) {
      controller.enqueue(encoder.encode(frame));
      heartbeat = setInterval(() => {
        try { controller.enqueue(encoder.encode(frame)); } catch { clearInterval(heartbeat); }
      }, STREAM_KEEPALIVE_MS);
      try {
        const res = await settled;
        clearInterval(heartbeat);
        // Stream already ended without a body: surface it as a terminal error
        // frame rather than a silent stall.
        if (!res?.body) {
          controller.enqueue(buildStreamErrorBytes(HTTP_STATUS.BAD_GATEWAY, "Upstream returned no response body", isClaudeClient ? FORMATS.CLAUDE : FORMATS.OPENAI));
          controller.close();
          return;
        }
        // The reply has to be a stream itself. A JSON error (rate limit, bad
        // key, model not found) pasted into the event stream leaves the client
        // with zero events and no reason — it reports an empty response rather
        // than the upstream's message, so surface that as a terminal error
        // frame in the client's own format.
        const contentType = res.headers.get("content-type") || "";
        if (!contentType.includes("text/event-stream")) {
          const text = await res.text().catch(() => "");
          let message = `Upstream answered ${res.status} without a stream`;
          try {
            const parsed = JSON.parse(text);
            const detail = parsed?.error?.message || parsed?.error || parsed?.message;
            if (typeof detail === "string" && detail) message = detail;
          } catch { /* not JSON: keep the status-based message */ }
          controller.enqueue(buildStreamErrorBytes(res.ok ? HTTP_STATUS.BAD_GATEWAY : res.status, message, isClaudeClient ? FORMATS.CLAUDE : FORMATS.OPENAI));
          controller.close();
          return;
        }
        const reader = res.body.getReader();
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          controller.enqueue(value);
        }
        controller.close();
      } catch (err) {
        clearInterval(heartbeat);
        try {
          controller.enqueue(buildStreamErrorBytes(HTTP_STATUS.BAD_GATEWAY, "Upstream stream failed", isClaudeClient ? FORMATS.CLAUDE : FORMATS.OPENAI));
          controller.close();
        } catch { /* client already gone */ }
        dbg("KEEPALIVE", `early SSE stream failed: ${err?.message || err}`);
      }
    },
    cancel() {
      clearInterval(heartbeat);
      // Only the client can observe this stream, so its cancellation is the
      // disconnect; the upstream aborts through the shared stream controller.
      settled.then((res) => res?.body?.cancel?.().catch(() => {}), () => {});
    }
  });
  return new Response(body, { headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", "Connection": "keep-alive", "Access-Control-Allow-Origin": "*" } });
}
