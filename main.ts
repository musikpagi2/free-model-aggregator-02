// main.ts — free-model-aggregator-02
//
// One server that aggregates OpenRouter's fourteen free models behind a
// single endpoint. It serves the OpenAI Chat Completions wire and the
// Anthropic Messages wire, dials every free model on the gateway's Chat
// Completions route, and picks the highest-rated free model that meets
// each prompt's requirements (JSON mode needs a json-capable model, a
// tools prompt needs a tools-capable model). On a provider 429 or 5xx it
// fails over to the next best model that meets the prompt.
//
// Node 22, standard library only. No dependency is installed at runtime.
// Run: `node main.ts` (listens, registers with the env, serves, exits
// when the env's GET /work answers done true). `node main.ts --list`
// prints the catalog document and exits 0.

import { createServer } from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";
import { readFileSync } from "node:fs";

// ---------------------------------------------------------------------------
// The fourteen free models (models.json, vendored with this entry).
// ---------------------------------------------------------------------------

interface FreeModel {
  id: string;
  rating_milli: number;
  context_length: number;
  tools: boolean;
  vision: boolean;
  json: boolean;
  free: boolean;
}

interface ModelFile {
  schema: string;
  models: FreeModel[];
}

const MODELS: FreeModel[] = (
  JSON.parse(readFileSync(new URL("./models.json", import.meta.url), "utf8")) as ModelFile
).models;

const MODEL_IDS: string[] = MODELS.map((m) => m.id);

// The catalog document. --list prints exactly this, and registration sends
// exactly this, so the two are byte-identical by construction.
function buildCatalog(): Record<string, unknown> {
  return {
    providers: [
      {
        id: "openrouter",
        base_url: "https://openrouter.ai/api/v1",
        key_env: "OPENROUTER_API_KEY",
        wire: "openai",
        models: MODELS.map((m) => ({
          id: m.id,
          free: true,
          context_length: m.context_length,
        })),
      },
    ],
  };
}

const CATALOG: Record<string, unknown> = buildCatalog();
const CATALOG_TEXT: string = JSON.stringify(CATALOG);

if (process.argv.includes("--list")) {
  process.stdout.write(`${CATALOG_TEXT}\n`);
  process.exit(0);
}

// ---------------------------------------------------------------------------
// Configuration: the env and the gateway come from the environment.
// ---------------------------------------------------------------------------

const ENV_URL: string = (process.env.MAKEFUN_ENV_URL ?? "").replace(/\/$/, "");
const GATEWAY_BASE: string = (process.env.OPENAI_BASE_URL ?? "").replace(/\/$/, "");
// The trial's key is already in the env; the gateway swaps it for the
// entrant's real OpenRouter key. This server never sees the real key.
const GATEWAY_KEY: string =
  process.env.OPENAI_API_KEY ?? process.env.ANTHROPIC_API_KEY ?? "";

// Determinism is by construction: this server uses no wall clock and no
// randomness anywhere (MAKEFUN_TRIAL_SEED is therefore not needed), so
// every route is a function of the requests and the gateway's answers
// seen so far, and a replay leaves the same state export.

// A trial may make at most 60 model calls; a prompt tries at most this many
// candidates before it gives up.
const MAX_ATTEMPTS_PER_PROMPT = 8;
const MAX_FAILURE_ROWS = 8;

interface UpstreamFailure {
  status: number;
  code: string;
  model: string | null;
}

// ---------------------------------------------------------------------------
// Small deterministic helpers (no wall clock, no Math.random anywhere).
// ---------------------------------------------------------------------------

function isRecord(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

// Rough token estimate (about four characters a token) for the fallback
// usage count when a provider answer carries no usage at all.
function estimateTokens(text: string): number {
  return Math.max(0, Math.ceil(text.length / 4));
}

// ---------------------------------------------------------------------------
// Model choice: the highest-rated model that meets the prompt, ties broken
// by catalog order (Array.sort is stable, so equal ratings keep it).
// ---------------------------------------------------------------------------

function rankCandidates(requiresJson: boolean, requiresTools: boolean): FreeModel[] {
  return MODELS.filter(
    (m) => (!requiresJson || m.json) && (!requiresTools || m.tools),
  ).sort((a, b) => b.rating_milli - a.rating_milli);
}

// ---------------------------------------------------------------------------
// Usage: the wire checks want a whole usage count (input >= 0, output >= 1).
// Pass the provider's usage through when it is whole; otherwise fill the
// gap from the text that was actually served.
// ---------------------------------------------------------------------------

interface Usage {
  input: number;
  output: number;
}

function wholeUsage(
  provider: unknown,
  fallbackInputText: string,
  fallbackOutputText: string,
): Usage {
  let input = 0;
  let output = 0;
  if (isRecord(provider)) {
    const p = provider as Record<string, unknown>;
    const promptTokens = p.prompt_tokens ?? p.input_tokens;
    const completionTokens = p.completion_tokens ?? p.output_tokens;
    if (typeof promptTokens === "number" && Number.isFinite(promptTokens)) {
      input = Math.max(0, Math.floor(promptTokens));
    } else {
      input = estimateTokens(fallbackInputText);
    }
    if (typeof completionTokens === "number" && Number.isFinite(completionTokens)) {
      output = Math.max(0, Math.floor(completionTokens));
    } else {
      output = estimateTokens(fallbackOutputText);
    }
  } else {
    input = estimateTokens(fallbackInputText);
    output = estimateTokens(fallbackOutputText);
  }
  // A served answer always spent at least one output token.
  if (fallbackOutputText.length > 0 && output < 1) output = 1;
  return { input, output };
}

// ---------------------------------------------------------------------------
// Server-sent events: parse a gateway stream into its frames.
// ---------------------------------------------------------------------------

interface SseFrame {
  event: string | null;
  data: string;
}

function parseSse(text: string): SseFrame[] {
  const frames: SseFrame[] = [];
  const normalized = text.replace(/\r\n/g, "\n");
  for (const chunk of normalized.split("\n\n")) {
    if (chunk.trim() === "") continue;
    let event: string | null = null;
    const data: string[] = [];
    for (const line of chunk.split("\n")) {
      if (line.startsWith("event:")) event = line.slice("event:".length).trim();
      else if (line.startsWith("data:")) data.push(line.slice("data:".length).replace(/^ /, ""));
      // ": comment" lines are ignored.
    }
    if (data.length > 0) frames.push({ event, data: data.join("\n") });
  }
  return frames;
}

function sseEvent(event: string | null, payload: string): string {
  const head = event === null ? "" : `event: ${event}\n`;
  return `${head}data: ${payload}\n\n`;
}

// ---------------------------------------------------------------------------
// Failure rows for the x-makefun-upstream-failures header.
// ---------------------------------------------------------------------------

function errorCodeOf(bodyText: string, status: number): string {
  try {
    const value: unknown = JSON.parse(bodyText);
    if (isRecord(value) && isRecord(value.error)) {
      const err = value.error as Record<string, unknown>;
      if (typeof err.code === "string" && err.code !== "") return err.code.slice(0, 64);
      if (typeof err.type === "string" && err.type !== "") return err.type.slice(0, 64);
    }
  } catch {
    // Not JSON: fall through to the status default.
  }
  if (status === 429) return "rate_limit_exceeded";
  if (status === 401) return "unauthorized";
  if (status === 400) return "bad_request";
  if (status >= 500) return "server_error";
  return "upstream_error";
}

// ---------------------------------------------------------------------------
// Wire translation: an Anthropic Messages request becomes a Chat
// Completions body before it is dialled.
// ---------------------------------------------------------------------------

function anthropicContentToText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  const parts: string[] = [];
  for (const block of content) {
    if (isRecord(block) && block.type === "text" && typeof block.text === "string") {
      parts.push(block.text);
    }
  }
  return parts.join("");
}

function anthropicToOpenAI(body: Record<string, unknown>): Record<string, unknown> {
  const messages: Record<string, unknown>[] = [];

  // system: a string, or an array of text blocks.
  if (typeof body.system === "string" && body.system !== "") {
    messages.push({ role: "system", content: body.system });
  } else if (Array.isArray(body.system)) {
    const text = anthropicContentToText(body.system);
    if (text !== "") messages.push({ role: "system", content: text });
  }

  const inputMessages = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of inputMessages) {
    if (!isRecord(raw)) continue;
    const role = raw.role === "assistant" ? "assistant" : "user";
    if (typeof raw.content === "string") {
      messages.push({ role, content: raw.content });
      continue;
    }
    if (!Array.isArray(raw.content)) continue;
    const textParts: string[] = [];
    const toolCalls: Record<string, unknown>[] = [];
    const toolResults: Record<string, unknown>[] = [];
    for (const block of raw.content) {
      if (!isRecord(block)) continue;
      if (block.type === "text" && typeof block.text === "string") {
        textParts.push(block.text);
      } else if (block.type === "tool_use") {
        toolCalls.push({
          id: typeof block.id === "string" ? block.id : "call_unknown",
          type: "function",
          function: {
            name: typeof block.name === "string" ? block.name : "",
            arguments: JSON.stringify(isRecord(block.input) ? block.input : {}),
          },
        });
      } else if (block.type === "tool_result") {
        toolResults.push({
          role: "tool",
          tool_call_id:
            typeof block.tool_use_id === "string" ? block.tool_use_id : "call_unknown",
          content:
            typeof block.content === "string"
              ? block.content
              : JSON.stringify(block.content ?? ""),
        });
      }
    }
    if (role === "user" && toolResults.length > 0) {
      if (textParts.length > 0) {
        messages.push({ role: "user", content: textParts.join("") });
      }
      for (const result of toolResults) messages.push(result);
    } else if (toolCalls.length > 0) {
      messages.push({
        role: "assistant",
        content: textParts.join("") === "" ? null : textParts.join(""),
        tool_calls: toolCalls,
      });
    } else {
      messages.push({ role, content: textParts.join("") });
    }
  }

  const out: Record<string, unknown> = {
    model: "auto",
    messages,
    max_tokens: body.max_tokens,
    temperature: body.temperature,
  };
  if (Array.isArray(body.tools) && body.tools.length > 0) {
    out.tools = body.tools
      .filter((t): t is Record<string, unknown> => isRecord(t))
      .map((t) => ({
        type: "function",
        function: {
          name: typeof t.name === "string" ? t.name : "",
          description: typeof t.description === "string" ? t.description : "",
          parameters: isRecord(t.input_schema)
            ? t.input_schema
            : { type: "object", properties: {} },
        },
      }));
  }
  if (body.stream === true) {
    out.stream = true;
    // Ask the gateway for a usage chunk so the Anthropic message_delta
    // can carry a whole output count.
    out.stream_options = { include_usage: true };
  }
  if (Array.isArray(body.stop_sequences) && body.stop_sequences.length > 0) {
    out.stop = body.stop_sequences;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Wire translation: a gateway Chat Completions answer becomes the wire the
// client used. The provider names the model without the openrouter/
// prefix; the id dialled goes back on every answer and every chunk.
// ---------------------------------------------------------------------------

function openAIFinishToAnthropic(finish: string): string {
  if (finish === "tool_calls" || finish === "function_call") return "tool_use";
  if (finish === "length") return "max_tokens";
  if (finish === "content_filter") return "refusal";
  return "end_turn";
}

// Non-streaming: Chat Completions JSON -> Anthropic Messages JSON.
function openAIToAnthropic(
  gateway: Record<string, unknown>,
  dialledModel: string,
  anthropicId: string,
  requestText: string,
): Record<string, unknown> {
  const choices = Array.isArray(gateway.choices) ? gateway.choices : [];
  const first = choices.length > 0 && isRecord(choices[0]) ? choices[0] : {};
  const message = isRecord(first.message) ? first.message : {};
  const content: Record<string, unknown>[] = [];
  const text = anthropicContentToText(message.content);
  if (text !== "") content.push({ type: "text", text });
  if (Array.isArray(message.tool_calls)) {
    for (const call of message.tool_calls) {
      if (!isRecord(call) || !isRecord(call.function)) continue;
      let input: Record<string, unknown> = {};
      try {
        const parsed: unknown = JSON.parse(
          typeof call.function.arguments === "string" ? call.function.arguments : "{}",
        );
        if (isRecord(parsed)) input = parsed;
      } catch {
        input = {};
      }
      content.push({
        type: "tool_use",
        id: typeof call.id === "string" ? call.id : "call_unknown",
        name: typeof call.function.name === "string" ? call.function.name : "",
        input,
      });
    }
  }
  if (content.length === 0) content.push({ type: "text", text: "" });
  const finish =
    typeof first.finish_reason === "string" ? first.finish_reason : "stop";
  const usage = wholeUsage(gateway.usage, requestText, text);
  return {
    id: anthropicId,
    type: "message",
    role: "assistant",
    content,
    model: dialledModel,
    stop_reason: openAIFinishToAnthropic(finish),
    stop_sequence: null,
    usage: { input_tokens: usage.input, output_tokens: usage.output },
  };
}

// Streaming: buffered gateway Chat Completions chunks -> Anthropic SSE
// events, in canonical order: message_start, content_block_start, deltas,
// content_block_stop, message_delta, message_stop.
function openAIStreamToAnthropic(
  chunks: Record<string, unknown>[],
  dialledModel: string,
  anthropicId: string,
  requestText: string,
): string {
  let text = "";
  let finish = "stop";
  let sawFinish = false;
  let usage: Usage | null = null;
  // Tool calls arrive as fragments keyed by index; reassemble them.
  const toolFragments = new Map<
    number,
    { id: string; name: string; args: string }
  >();
  for (const chunk of chunks) {
    const choices = Array.isArray(chunk.choices) ? chunk.choices : [];
    // A usage chunk carries choices: [] and a usage object; take the
    // provider's own numbers whenever any chunk carries them.
    if (isRecord(chunk.usage)) {
      usage = wholeUsage(chunk.usage, requestText, text);
    }
    if (choices.length === 0) continue;
    const choice = isRecord(choices[0]) ? choices[0] : {};
    const delta = isRecord(choice.delta) ? choice.delta : {};
    if (typeof delta.content === "string") text += delta.content;
    if (Array.isArray(delta.tool_calls)) {
      for (const call of delta.tool_calls) {
        if (!isRecord(call)) continue;
        const index =
          typeof call.index === "number" ? call.index : toolFragments.size;
        const prev = toolFragments.get(index) ?? { id: "", name: "", args: "" };
        if (typeof call.id === "string" && call.id !== "") prev.id = call.id;
        if (isRecord(call.function)) {
          if (typeof call.function.name === "string" && call.function.name !== "") {
            prev.name = call.function.name;
          }
          if (typeof call.function.arguments === "string") {
            prev.args += call.function.arguments;
          }
        }
        toolFragments.set(index, prev);
      }
    }
    if (typeof choice.finish_reason === "string") {
      finish = choice.finish_reason;
      sawFinish = true;
    }
  }
  if (!sawFinish) finish = "stop";
  const finalUsage =
    usage ?? wholeUsage(null, requestText, text);
  const events: string[] = [];
  events.push(
    sseEvent(
      "message_start",
      JSON.stringify({
        type: "message_start",
        message: {
          id: anthropicId,
          type: "message",
          role: "assistant",
          content: [],
          model: dialledModel,
          stop_reason: null,
          stop_sequence: null,
          usage: { input_tokens: finalUsage.input, output_tokens: 0 },
        },
      }),
    ),
  );
  // Text block (index 0) when the answer carried text.
  if (text !== "") {
    events.push(
      sseEvent(
        "content_block_start",
        JSON.stringify({
          type: "content_block_start",
          index: 0,
          content_block: { type: "text", text: "" },
        }),
      ),
    );
    // One delta per gateway chunk that carried text, preserving order.
    for (const chunk of chunks) {
      const choices = Array.isArray(chunk.choices) ? chunk.choices : [];
      if (choices.length === 0 || !isRecord(choices[0])) continue;
      const delta = isRecord(choices[0].delta) ? choices[0].delta : {};
      if (typeof delta.content === "string" && delta.content !== "") {
        events.push(
          sseEvent(
            "content_block_delta",
            JSON.stringify({
              type: "content_block_delta",
              index: 0,
              delta: { type: "text_delta", text: delta.content },
            }),
          ),
        );
      }
    }
    events.push(
      sseEvent("content_block_stop", JSON.stringify({ type: "content_block_stop", index: 0 })),
    );
  }
  // Tool-use blocks follow the text block, one per reassembled call.
  let blockIndex = text !== "" ? 1 : 0;
  for (const frag of toolFragments.values()) {
    let input: Record<string, unknown> = {};
    try {
      const parsed: unknown = JSON.parse(frag.args === "" ? "{}" : frag.args);
      if (isRecord(parsed)) input = parsed;
    } catch {
      input = {};
    }
    events.push(
      sseEvent(
        "content_block_start",
        JSON.stringify({
          type: "content_block_start",
          index: blockIndex,
          content_block: { type: "tool_use", id: frag.id || "call_unknown", name: frag.name, input: {} },
        }),
      ),
    );
    events.push(
      sseEvent(
        "content_block_delta",
        JSON.stringify({
          type: "content_block_delta",
          index: blockIndex,
          delta: { type: "input_json_delta", partial_json: frag.args === "" ? "{}" : frag.args },
        }),
      ),
    );
    events.push(
      sseEvent(
        "content_block_stop",
        JSON.stringify({ type: "content_block_stop", index: blockIndex }),
      ),
    );
    blockIndex += 1;
  }
  events.push(
    sseEvent(
      "message_delta",
      JSON.stringify({
        type: "message_delta",
        delta: {
          stop_reason: openAIFinishToAnthropic(finish),
          stop_sequence: null,
        },
        usage: { output_tokens: finalUsage.output },
      }),
    ),
  );
  events.push(sseEvent("message_stop", JSON.stringify({ type: "message_stop" })));
  return events.join("");
}

// Streaming on the OpenAI wire: rewrite the gateway's chunks so the model
// field is the id dialled, keep the finish chunk exactly once, and make
// sure a usage chunk (when the client asked for one) and [DONE] close it.
function rewriteOpenAIStream(
  chunks: Record<string, unknown>[],
  dialledModel: string,
  requestText: string,
  wantUsage: boolean,
): string {
  const out: string[] = [];
  let text = "";
  let sawFinish = false;
  let sawUsage = false;
  let created = 0;
  let chunkId = "";
  for (const chunk of chunks) {
    if (typeof chunk.created === "number") created = chunk.created;
    if (typeof chunk.id === "string" && chunk.id !== "") chunkId = chunk.id;
    const choices = Array.isArray(chunk.choices) ? chunk.choices : [];
    if (choices.length > 0 && isRecord(choices[0])) {
      const choice = choices[0];
      const delta = isRecord(choice.delta) ? choice.delta : {};
      if (typeof delta.content === "string") text += delta.content;
      if (typeof choice.finish_reason === "string") sawFinish = true;
    }
    if (isRecord(chunk.usage)) sawUsage = true;
    const rewritten: Record<string, unknown> = { ...chunk, model: dialledModel };
    out.push(`data: ${JSON.stringify(rewritten)}\n\n`);
  }
  // The usage chunk is normally last, but take the last chunk that
  // carries a usage object wherever it sits.
  let providerUsage: unknown = null;
  for (const chunk of chunks) {
    if (isRecord(chunk.usage)) providerUsage = chunk.usage;
  }
  const usage = wholeUsage(providerUsage, requestText, text);
  if (!sawFinish) {
    out.push(
      `data: ${JSON.stringify({
        id: chunkId === "" ? "chatcmpl-aggregator" : chunkId,
        object: "chat.completion.chunk",
        created,
        model: dialledModel,
        choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
      })}\n\n`,
    );
  }
  if (wantUsage && !sawUsage) {
    out.push(
      `data: ${JSON.stringify({
        id: chunkId === "" ? "chatcmpl-aggregator" : chunkId,
        object: "chat.completion.chunk",
        created,
        model: dialledModel,
        choices: [],
        usage: {
          prompt_tokens: usage.input,
          completion_tokens: usage.output,
          total_tokens: usage.input + usage.output,
        },
      })}\n\n`,
    );
  }
  out.push("data: [DONE]\n\n");
  return out.join("");
}

// ---------------------------------------------------------------------------
// The gateway: every model call goes through it, never to OpenRouter
// directly. The key in the env is the trial's own; the gateway swaps it
// for the entrant's real key.
// ---------------------------------------------------------------------------

const GATEWAY_TIMEOUT_MS = 25000;

async function dialGateway(
  requestBody: Record<string, unknown>,
): Promise<{ status: number; bodyText: string }> {
  if (GATEWAY_BASE === "" || GATEWAY_KEY === "") {
    return { status: 502, bodyText: "" };
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), GATEWAY_TIMEOUT_MS);
  try {
    const response = await fetch(`${GATEWAY_BASE}/chat/completions`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${GATEWAY_KEY}`,
        accept: requestBody.stream === true ? "text/event-stream" : "application/json",
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal,
    });
    return { status: response.status, bodyText: await response.text() };
  } catch {
    return { status: 502, bodyText: "" };
  } finally {
    clearTimeout(timer);
  }
}

// ---------------------------------------------------------------------------
// Serving one prompt: pick the best model that meets it, dial, fail over
// on a refusal, and answer in the wire the client used.
// ---------------------------------------------------------------------------

let anthropicCounter = 0;

interface ServedAnswer {
  status: number;
  contentType: string;
  headers: Record<string, string>;
  body: string;
}

function failureHeaders(failures: UpstreamFailure[]): Record<string, string> {
  if (failures.length === 0) return {};
  return {
    "x-makefun-upstream-failures": JSON.stringify(
      failures.slice(0, MAX_FAILURE_ROWS),
    ),
  };
}

function requestTextOf(body: Record<string, unknown>): string {
  if (Array.isArray(body.messages)) return JSON.stringify(body.messages);
  return JSON.stringify(body);
}

async function serveCompletion(
  wire: "openai" | "anthropic",
  clientBody: Record<string, unknown>,
): Promise<ServedAnswer> {
  const requiresJson =
    wire === "openai" &&
    isRecord(clientBody.response_format) &&
    clientBody.response_format.type === "json_object";
  const requiresTools =
    Array.isArray(clientBody.tools) && clientBody.tools.length > 0;
  const candidates = rankCandidates(requiresJson, requiresTools);
  const requestText = requestTextOf(clientBody);
  const failures: UpstreamFailure[] = [];

  // The gateway body: the client's own, or the Anthropic one translated.
  const baseGatewayBody: Record<string, unknown> =
    wire === "openai" ? { ...clientBody } : anthropicToOpenAI(clientBody);
  // Every model call must name an output cap.
  if (
    baseGatewayBody.max_tokens === undefined &&
    baseGatewayBody.max_completion_tokens === undefined
  ) {
    baseGatewayBody.max_tokens = 512;
  }
  const isStream = clientBody.stream === true;

  for (let attempt = 0; attempt < candidates.length && attempt < MAX_ATTEMPTS_PER_PROMPT; attempt++) {
    const model = candidates[attempt];
    const gatewayBody: Record<string, unknown> = {
      ...baseGatewayBody,
      model: model.id,
    };
    let dialled: { status: number; bodyText: string };
    try {
      dialled = await dialGateway(gatewayBody);
    } catch {
      dialled = { status: 502, bodyText: "" };
    }

    if (dialled.status >= 200 && dialled.status < 300) {
      if (isStream) {
        const frames = parseSse(dialled.bodyText);
        const chunks: Record<string, unknown>[] = [];
        let sawDone = false;
        for (const frame of frames) {
          if (frame.data.trim() === "[DONE]") {
            sawDone = true;
            continue;
          }
          try {
            const parsed: unknown = JSON.parse(frame.data);
            if (isRecord(parsed)) chunks.push(parsed);
          } catch {
            // A non-JSON frame in a stream is a broken upstream answer.
          }
        }
        if (chunks.length === 0 && !sawDone) {
          failures.push({ status: 502, code: "upstream_error", model: model.id });
          continue;
        }
        const dialledModel = model.id;
        if (wire === "openai") {
          const wantUsage =
            isRecord(clientBody.stream_options) &&
            clientBody.stream_options.include_usage === true;
          return {
            status: 200,
            contentType: "text/event-stream",
            headers: failureHeaders(failures),
            body: rewriteOpenAIStream(chunks, dialledModel, requestText, wantUsage),
          };
        }
        anthropicCounter += 1;
        const anthropicId = `msg-agg-${anthropicCounter}`;
        return {
          status: 200,
          contentType: "text/event-stream",
          headers: failureHeaders(failures),
          body: openAIStreamToAnthropic(chunks, dialledModel, anthropicId, requestText),
        };
      }
      // Non-streaming: the gateway answer is one JSON object.
      let gateway: Record<string, unknown> | null = null;
      try {
        const parsed: unknown = JSON.parse(dialled.bodyText);
        if (isRecord(parsed)) gateway = parsed;
      } catch {
        gateway = null;
      }
      const choices = gateway !== null && Array.isArray(gateway.choices) ? gateway.choices : [];
      if (gateway === null || choices.length === 0) {
        failures.push({ status: 502, code: "upstream_error", model: model.id });
        continue;
      }
      const dialledModel = model.id;
      if (wire === "openai") {
        const first = isRecord(choices[0]) ? choices[0] : {};
        const message = isRecord(first.message) ? first.message : {};
        const text = anthropicContentToText(
          typeof message.content === "string" ? message.content : message.content,
        );
        const usage = wholeUsage(gateway.usage, requestText, text);
        const answer: Record<string, unknown> = {
          id: typeof gateway.id === "string" ? gateway.id : "chatcmpl-aggregator",
          object: "chat.completion",
          created: typeof gateway.created === "number" ? gateway.created : 0,
          model: dialledModel,
          choices: gateway.choices,
          usage: {
            prompt_tokens: usage.input,
            completion_tokens: usage.output,
            total_tokens: usage.input + usage.output,
          },
        };
        return {
          status: 200,
          contentType: "application/json",
          headers: failureHeaders(failures),
          body: `${JSON.stringify(answer)}\n`,
        };
      }
      anthropicCounter += 1;
      const anthropicId = `msg-agg-${anthropicCounter}`;
      return {
        status: 200,
        contentType: "application/json",
        headers: failureHeaders(failures),
        body: `${JSON.stringify(
          openAIToAnthropic(gateway, dialledModel, anthropicId, requestText),
        )}\n`,
      };
    }

    // A refusal: record it and fail over to the next candidate. A 401 is
    // an auth failure, not a model refusal, so it ends the attempts.
    failures.push({
      status: dialled.status,
      code: errorCodeOf(dialled.bodyText, dialled.status),
      model: model.id,
    });
    if (dialled.status === 401) break;
  }

  // Every candidate refused (or there was no gateway to dial): the prompt
  // scores nothing, so answer with a well-formed error, not a fabrication.
  return {
    status: 502,
    contentType: "application/json",
    headers: failureHeaders(failures),
    body: `${JSON.stringify({
      error: {
        code: "upstream_unavailable",
        message: "every free model refused the prompt",
      },
    })}\n`,
  };
}

// ---------------------------------------------------------------------------
// GET /v1/models: the OpenAI shape for a Bearer header, the Anthropic
// shape for an x-api-key header. Both list exactly the fourteen ids.
// ---------------------------------------------------------------------------

function modelListBody(req: IncomingMessage): string {
  const auth = req.headers.authorization ?? "";
  const apiKey = req.headers["x-api-key"];
  const anthropic = apiKey !== undefined && apiKey !== "" && auth === "";
  if (anthropic) {
    return `${JSON.stringify({
      data: MODEL_IDS.map((id) => ({ id, type: "model" })),
    })}\n`;
  }
  return `${JSON.stringify({
    object: "list",
    data: MODEL_IDS.map((id) => ({ id, object: "model" })),
  })}\n`;
}

// ---------------------------------------------------------------------------
// HTTP plumbing.
// ---------------------------------------------------------------------------

const MAX_BODY_BYTES = 1024 * 1024;

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    req.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size <= MAX_BODY_BYTES) chunks.push(chunk);
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

function sendJson(res: ServerResponse, status: number, value: unknown): void {
  const body = `${JSON.stringify(value)}\n`;
  res.writeHead(status, {
    "content-type": "application/json",
    "content-length": String(Buffer.byteLength(body)),
  });
  res.end(body);
}

let inFlight = 0;

async function handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? "/", "http://server");
  const path = url.pathname;
  try {
    if (req.method === "GET" && path === "/v1/models") {
      const body = modelListBody(req);
      res.writeHead(200, {
        "content-type": "application/json",
        "content-length": String(Buffer.byteLength(body)),
      });
      res.end(body);
      return;
    }
    if (req.method === "POST" && (path === "/v1/chat/completions" || path === "/v1/messages")) {
      const raw = await readBody(req);
      let clientBody: unknown = null;
      try {
        clientBody = JSON.parse(raw);
      } catch {
        sendJson(res, 400, { error: { code: "bad_request", message: "the body is not JSON" } });
        return;
      }
      if (!isRecord(clientBody)) {
        sendJson(res, 400, { error: { code: "bad_request", message: "the body is not one JSON object" } });
        return;
      }
      inFlight += 1;
      try {
        const answer = await serveCompletion(
          path === "/v1/chat/completions" ? "openai" : "anthropic",
          clientBody,
        );
        res.writeHead(answer.status, {
          "content-type": answer.contentType,
          ...answer.headers,
        });
        res.end(answer.body);
      } finally {
        inFlight -= 1;
      }
      return;
    }
    sendJson(res, 404, { error: { code: "not_found", message: `no route ${req.method} ${path}` } });
  } catch (err) {
    const message = err instanceof Error ? err.message : "internal error";
    try {
      sendJson(res, 500, { error: { code: "internal", message } });
    } catch {
      // The response is already gone; nothing left to do.
    }
  }
}

// ---------------------------------------------------------------------------
// Registration with the env, then poll GET /work until it says done.
// ---------------------------------------------------------------------------

async function register(port: number): Promise<boolean> {
  if (ENV_URL === "") return false;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const response = await fetch(`${ENV_URL}/register`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ protocol_version: 1, port, catalog: CATALOG }),
      });
      if (response.status >= 200 && response.status < 300) {
        const value: unknown = await response.json();
        if (isRecord(value) && value.ok === true) return true;
      }
    } catch {
      // The env may not be listening yet; retry below.
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  return false;
}

function pollWork(): void {
  if (ENV_URL === "") return;
  const timer = setInterval(() => {
    void (async () => {
      try {
        const response = await fetch(`${ENV_URL}/work`);
        if (response.status < 200 || response.status >= 300) return;
        const value: unknown = await response.json();
        if (isRecord(value) && value.done === true && inFlight === 0) {
          clearInterval(timer);
          process.exit(0);
        }
      } catch {
        // The env is briefly unreachable; keep polling.
      }
    })();
  }, 500);
  // A 500 ms interval must not keep the process alive on its own.
  timer.unref();
}

process.on("SIGTERM", () => process.exit(0));

const server = createServer((req, res) => {
  void handleRequest(req, res);
});

server.listen(0, "0.0.0.0", () => {
  const address = server.address();
  const port =
    address !== null && typeof address === "object" ? address.port : 0;
  process.stdout.write(`free-model-aggregator-02: listening on ${port}\n`);
  void (async () => {
    if (ENV_URL !== "") {
      const ok = await register(port);
      if (!ok) {
        process.stderr.write(
          "free-model-aggregator-02: registration with the env failed\n",
        );
      }
      pollWork();
    }
  })();
});
