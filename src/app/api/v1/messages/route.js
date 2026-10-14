import { handleChat } from "@/sse/handlers/chat.js";
import { initTranslators } from "open-sse/translator/index.js";
import { withEarlySseHeaders, clientWantsStream } from "open-sse/utils/streamHandler.js";

let initialized = false;

/**
 * Initialize translators once
 */
async function ensureInitialized() {
  if (!initialized) {
    await initTranslators();
    initialized = true;
  }
}

/**
 * Handle CORS preflight
 */
export async function OPTIONS() {
  return new Response(null, {
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "*"
    }
  });
}

/**
 * POST /v1/messages - Claude format (auto convert via handleChat)
 *
 * handleChat answers only after the upstream handshake resolves, which is tens
 * of seconds when the account fallbacks are being tried; a streaming client
 * would time out on the silence and retry down a different code path, so its
 * request is committed to SSE up front. A request that asked for one JSON body
 * (stream:false) is not — committing it to SSE breaks the client's parse. See
 * withEarlySseHeaders and clientWantsStream.
 */
export async function POST(request) {
  await ensureInitialized();
  return withEarlySseHeaders(() => handleChat(request), true, { wantsStream: await clientWantsStream(request) });
}

