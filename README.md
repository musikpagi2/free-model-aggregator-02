# free-model-aggregator-02

One server that aggregates OpenRouter's fourteen free models behind a
single endpoint, built for the make.fun bounty of the same name.

## What it does

- Serves the **OpenAI Chat Completions** wire (`POST /v1/chat/completions`,
  streaming included) and the **Anthropic Messages** wire
  (`POST /v1/messages`, streaming included), plus `GET /v1/models` in
  both shapes (OpenAI shape for an `Authorization: Bearer` header,
  Anthropic shape for an `x-api-key` header).
- Every request names model `"auto"`. The server replaces it with the
  **highest-rated free model that meets the prompt** — a JSON-mode prompt
  gets a JSON-capable model, a tools prompt gets a tools-capable model —
  and dials it on the gateway's Chat Completions route
  (`POST $OPENAI_BASE_URL/chat/completions`). Anthropic requests are
  translated to Chat Completions first, and the answer is translated back
  into the wire the client used, streamed or not, with usage and stop
  reason whole. The provider names the model without the `openrouter/`
  prefix, so the id dialled is put back on every answer and every
  streamed chunk.
- On a provider **429 or 5xx** it fails over to the next best model that
  meets the prompt (up to 8 attempts a prompt). Refusals that were
  retried and then served are reported in the
  `x-makefun-upstream-failures` response header. A prompt every model
  refuses answers with a well-formed 502, never a fabrication.
- Deterministic: every route is a function of the requests and the
  gateway's answers seen so far. No wall clock, no unseeded randomness
  (`MAKEFUN_TRIAL_SEED` is read for the record; no randomness is needed).

## Run it

```sh
node main.ts            # listen, register with the env, serve, exit on done
node main.ts --list     # print the catalog document and exit 0
```

Node 22, standard library only — nothing is installed at runtime. The
env is at `$MAKEFUN_ENV_URL` (register + `GET /work`); the gateway is at
`$OPENAI_BASE_URL`, with the trial's key in `$OPENAI_API_KEY` (or
`$ANTHROPIC_API_KEY`). The gateway swaps that key for the entrant's real
OpenRouter key; this server never sees it and never hardcodes one.

## Local verification

The practice trials were run locally against the kit's real env server
and sealed scorer, with a mock gateway standing in for OpenRouter (it
returns the token each prompt asks for and can refuse chosen models, so
failover is exercised without a live key). Results:

- `practice/trial-0`, `trial-1`, `trial-2`: **6 of 6 prompts valid, score
  100** (quality 100, served 100, wire 100, failover 100, catalog 100),
  `real_model pass`, 6 upstream calls each. Model choice tracks each
  trial's requirements (JSON prompts get the best JSON-capable model,
  the rest get the best overall).
- Failover, top two models refused 429: **6 of 6 valid, score 93**
  (quality 88 — the failover models rate lower — failover 100).
  Refusals are reported in `x-makefun-upstream-failures` in order.
- No key (every call 401): the server answers 502 with a well-formed
  error and never fabricates an answer.
- Determinism: two runs of trial-0 produce byte-identical `state.json`.

Note: these local runs use a mock gateway because the entrant's real
OpenRouter key is pushed to make.fun at submit time, not held locally.
The official check runs the same trials against the real gateway.

## Files

- `main.ts` — the whole server.
- `models.json` — the fourteen free models (id, house rating, context
  length, tools/vision/json support), in catalog order.
- `package.json`, `tsconfig.json` — Node 22, erasable TypeScript only
  (no build step; `node main.ts` runs it directly).

## The fourteen free models

Highest rated overall is `openrouter/thinkingmachines/inkling-2-small:free`
(1475); highest rated JSON-capable model is
`openrouter/nvidia/nemotron-3.5-super-vl:free` (1303). The catalog in
`models.json` is the same set `--list` prints and registration sends.
