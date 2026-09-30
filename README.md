# Cohort Insights API

A FastAPI service that accepts text documents, runs them through a simulated two-stage
pipeline (summary → tags), and serves the results to the submitting user and to an external
partner system that knows documents by its own `client_doc_ref`.

Stack: Python 3.11+, FastAPI, MongoDB (Motor), Redis, Docker Compose.

## Running

```bash
cp .env.example .env
docker compose up --build
```

If you ran an earlier version of this project, reset the old data first with
`docker compose down -v`. Documents created before the staleness schema existed have no
`stages` field.

- API: http://localhost:8000
- Interactive docs: http://localhost:8000/docs
- Health: `GET /health` (checks MongoDB and Redis)

Tests are integration tests (pytest + `httpx.AsyncClient`) against a real MongoDB on
`localhost:27017` and Redis on `localhost:6379`. They use database `cohort_test` and Redis
db 15, and speed up the pipeline with `STAGE_TIME_SCALE=0.05`.

```bash
pip install -r requirements-dev.txt
pytest
```

They cover the pipeline, ownership 404s, PATCH staleness, dropping an in-flight result for an
old version, enriching failure that keeps the summary, racing PATCHes, the rate limit, the
content cache, crosswalk repeats (including an `explain()` check that by-ref uses the
index), and list scoping and filtering.

## Configuration

All settings are environment variables. See `.env.example`.

| Variable | Default | Meaning |
|---|---|---|
| `MONGO_URL` / `MONGO_DB` | `mongodb://localhost:27017` / `cohort` | MongoDB connection |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `MAX_ACTIVE_PER_USER` | `3` | Active pipeline documents allowed per user |
| `ACTIVE_KEY_TTL` | `3600` | TTL (s) of the rate-limit counter |
| `CACHE_TTL` | `86400` | TTL (s) of content-cache entries |
| `WORKER_COUNT` | `2` | Pipeline workers (processing capacity) |
| `STAGE_TIME_SCALE` | `1` | Multiplier on stage durations and backoff (tests use `0.05`) |
| `FAILURE_RATE` | `0.1` | Simulated failure chance per stage attempt |
| `MAX_ATTEMPTS` | `3` | Attempts per stage before it's marked `failed` |
| `LOG_LEVEL` | `INFO` | Log level; application logs are JSON lines |

## Endpoints

| Method | Path | Success | Errors |
|---|---|---|---|
| POST | `/documents` | 201 `{document_id, status}` | 409 ref conflict, 422 validation, 429 rate limit |
| PATCH | `/documents/{document_id}` | 200 | 404, 409 version conflict, 422, 429 |
| GET | `/documents/{document_id}` | 200 | 404 |
| GET | `/users/{user_id}/documents?page=&page_size=&status=` | 200 | 404 if `user_id` ≠ caller, 422 |
| GET | `/documents/by-ref/{client_doc_ref}` | 200 | 404 |
| GET | `/health` | 200 / 503 | |

A `POST` whose content is already in the cache returns 201 with `status: "completed"`.

The caller's identity comes from the `X-User-Id` header (see Assumptions).

## Schema & Staleness Design

### The document

Every document lives in a single MongoDB document. All derived data is embedded next to the
content it was derived from:

```js
{
  _id: ObjectId,
  user_id: "u1",
  title: "Q3 notes",
  client_doc_ref: "cms-123",          // absent when not supplied
  content: "...",
  content_hash: "sha256:…",           // hash of the current content
  content_version: 3,                 // +1 on every PATCH, starts at 1
  status: "enriching",                // queued | processing | enriching | completed | failed
  stages: {
    processing: {
      state: "completed",             // pending | running | completed | failed
      content_version: 3,             // version of the content this summary came from
      summary: "…",
      attempts: 1,
      error: null
    },
    enriching: {
      state: "running",
      content_version: 3,             // version of the content these tags came from
      tags: null,
      attempts: 1,
      error: null
    }
  },
  created_at, updated_at
}
```

`status` is the top-level position in the pipeline. `stages.<name>.state` and
`stages.<name>.error` say exactly which stage failed and why. There's no ad-hoc
`"failed_at_enriching"` string. A caller reads `status: "failed"` plus the one stage whose
`state` is `failed`.

### The mechanism: a version counter on every derived field

`content_version` is an integer that belongs to the content. Each derived field (the summary
and the tags) carries the `content_version` it was computed from. A reader never has to
guess: it compares numbers.

- The summary is current if `stages.processing.content_version == content_version`.
- The tags are current if `stages.enriching.content_version == content_version`.

### Why a mismatch is impossible, not just unlikely

Three rules, each enforced by MongoDB's single-document atomicity:

1. **PATCH is one atomic update.** In a single `update_one`, PATCH sets the new `content`,
   `content_hash`, increments `content_version`, resets `status` to `queued`, and resets both
   stages to `pending` with `summary` and `tags` cleared. MongoDB applies all the fields of a
   single-document update together, so no reader can ever see the new content next to the
   old summary. They either see the whole old document or the whole new one.

2. **Workers write conditionally on the version they read.** A worker that started on
   version 3 writes its result with the filter
   `{_id: id, content_version: 3, "stages.processing.state": "running"}`. If a PATCH bumped
   the document to version 4 in the meantime, the filter matches nothing, and the old result
   is dropped. A late write from an old run cannot land on newer content.

3. **Enriching only reads a summary of the current version.** The enriching stage reads
   `summary` together with `content_version` in one read, and writes tags with the same
   version guard. Its tags are therefore always derived from a summary of the same
   version.

On top of that, `GET /documents/{id}` only returns `summary` and `tags` when their stored
version equals the current `content_version`. It also always returns both version numbers, so
a caller can check this itself:

```json
{
  "document_id": "…",
  "status": "processing",
  "content_version": 4,
  "stages": {
    "processing": {"state": "running", "content_version": null, "summary": null},
    "enriching":  {"state": "pending", "content_version": null, "tags": null}
  }
}
```

A reader mid-reprocessing sees `summary: null` instead of the old summary, so it's never told
the old summary is current.

### PATCH vs PATCH (optimistic concurrency)

PATCH accepts an optional `expected_version`. The update filter includes
`content_version: expected_version`. If two PATCHes race from the same version, exactly one
matches. The loser gets **409 Conflict** with the current version, and can re-read and
retry. Without `expected_version`, the last write wins, which is still safe because each
PATCH is atomic and bumps the version.

## Two-Stage Pipeline

```
queued → processing → enriching → completed
             ↓             ↓
           failed        failed
```

- **processing** (10–20 s): writes a mock summary. About 10% random failure.
- **enriching** (5–15 s): reads the summary and writes mock tags. About 10% random failure.
- **Workers** are `WORKER_COUNT` asyncio tasks started with the app. That's the limited
  processing capacity. Each loop claims an enriching job first (to finish work already in
  progress), then a queued one, and otherwise sleeps 0.5 s.
- **Claiming a job** is an atomic `find_one_and_update`. For processing, the filter is
  `{status: "queued", "stages.processing.state": "pending"}`, and the update sets
  `status: "processing"` and the state to `running`. When processing succeeds, it sets
  `status: "enriching"` with the enriching stage still `pending`. Enriching is then claimed the
  same way. Two workers can't claim the same stage, because only one update can match the
  `pending` filter.
- **A failure at enriching keeps the summary.** The processing stage stays `completed` with
  its summary. Retries only re-run enriching.
- **Retry with backoff:** each stage makes up to `MAX_ATTEMPTS` (3) attempts, waiting 1 s
  then 2 s between them. Only after the last attempt fails are the stage and the document
  marked `failed`. `attempts` and `error` are recorded on the stage.
- **Restart recovery:** on startup, jobs left `running` by a previous process are put back
  to `pending`. A document stuck in processing goes back to `queued`. A document stuck in
  enriching restarts enriching only, and keeps its summary. This assumes a single API
  process (see "More time").

## Crosswalk: `client_doc_ref`

- Optional at submission. When present, it's unique across all documents.
- Index: `{client_doc_ref: 1}`, **unique, partial** (`partialFilterExpression:
  {client_doc_ref: {$type: "string"}}`). Documents without a ref don't collide with each
  other.
- The query is `{client_doc_ref: {$eq: ref, $type: "string"}}`. The `$type` matters: MongoDB
  only uses a partial index when the query provably falls inside the partial filter, and a
  bare `{client_doc_ref: ref}` equality does not, so it falls back to a collection scan. With
  `$type`, `GET /documents/by-ref/{ref}` is an index point lookup on `client_doc_ref_unique`.
  A test checks this with `explain()`.
- By-ref is scoped by owner like every other read, so another user's ref returns 404.

**Decision for a repeat submission with the same ref:**

| Repeat POST with an existing ref | Result |
|---|---|
| Same user, same content (same `content_hash`) | **200**, returns the existing document. It's a retry, so it's idempotent. |
| Same user, different content | **409 Conflict**, with the existing `document_id` and `content_version`. To change content, the partner must `PATCH`. |
| Different user | **409 Conflict** (without revealing the other user's document) |

Why reject instead of "treat as a new version": the partner may send out of order, and a
bare POST carries no ordering information. If a late, older POST were treated as a new
version, it would silently overwrite newer content. Rejecting forces the partner to state
intent with PATCH plus `expected_version`, which has defined ordering. Retries stay safe
because identical resubmissions are idempotent.

The unique index also covers the race where two POSTs with the same new ref arrive at once.
One insert wins, and the other gets a `DuplicateKeyError`, which maps to the same rules above.

## Per-User Rate Limiting (Redis)

- Key `active:{user_id}`: the number of the user's documents in `queued`, `processing` or
  `enriching`.
- On submit, `INCR` and `EXPIRE` run in one `MULTI` transaction. If the new value is over 3,
  the API `DECR`s it back and returns **429**. `INCR` is atomic, so two concurrent submits
  can never both see a count of 3.
- A PATCH takes a slot only when the document was not already active. If it was active, the
  in-flight run's final write is dropped by the version guard, so that run never releases
  its slot, and the new run releases it exactly once.
- When a document reaches `completed` or `failed`, the worker decrements the key. It does this
  only if its guarded write actually matched.
- The key has a TTL (1 h), refreshed on each change, so a crashed worker can't leave a user
  blocked forever.
- **If Redis is down**, the limit falls back to counting in MongoDB with
  `{user_id, status: {$in: [...]}}` (served by the `user_id + status` index).

## Content Cache (Redis)

- Key `cache:{sha256(content)}` → `{summary, tags}`, TTL 24 h.
- It's written only when a document reaches `completed`.
- On POST or PATCH, if the hash hits, the document is stored as `completed` right away with
  the cached summary and tags, stamped with the document's **current** `content_version`.
- The key is the content hash, never the `document_id`. After a PATCH, the new content has a
  new hash, so the old content's cache entry can't be returned for the new content.
- **If Redis is down**, the lookup falls back to MongoDB:
  `{content_hash, status: "completed"}` on the `content_hash` index. That's safe because a
  `completed` document's hash and both stage results were written at the same version.

## Ownership

Every read checks `user_id` from the `X-User-Id` header against the document's `user_id`,
inside the query itself (`{_id: id, user_id: caller}`). A document owned by someone else, an
ID that doesn't exist, and an invalid ID all return the same **404 "Document not found"**, so
a non-owner can't confirm that a document exists.

## Indexes

| Index | Used by |
|---|---|
| `{user_id: 1, created_at: -1}` | List a user's documents, newest first |
| `{user_id: 1, status: 1, created_at: -1}` | List with a status filter; rate-limit fallback count |
| `{client_doc_ref: 1}` unique, partial | `GET /documents/by-ref/{ref}`, uniqueness of refs |
| `{content_hash: 1}` | Content-cache fallback when Redis is down |
| `{status: 1, updated_at: 1}` | Workers finding jobs to claim, and stuck-job recovery |

## At 100×

**A single user with 500K documents.** The `{user_id, created_at}` index still finds that
user's documents quickly. The problem is everything after that. A listing with a status filter
walks a huge key range when the filter is selective, which is why the index includes `status`
before `created_at`. The bigger problem is skip/limit: page 20,000 must walk and discard
200,000 index entries before returning 10. The rate-limit fallback count is fine, because it's
bounded by the active documents, at most 3.

**Shard key.** `{user_id: "hashed", _id: 1}` is my choice. A plain `user_id` range key
concentrates new writes on whichever chunk holds the busiest users, and a single 500K-doc user
becomes a "jumbo" chunk that can't be split. That's the hot partition. A monotonically
increasing key like `_id` or `created_at` alone sends every insert to the last chunk. Hashing
`user_id` spreads users across shards. The second field lets one big user's documents split
across chunks. Most of our queries include `user_id`, so the listing stays a targeted query on
one shard instead of a scatter-gather. The one query without `user_id`, by-ref, is served by a
separate lookup collection keyed by `client_doc_ref`, and so is also targeted.

**Redis rate limiter at 100× QPS.** An atomic check-and-increment script is correct at any
QPS, but all traffic for one user hits one key on one Redis node. That's fine, since the
limit is 3. The real risks are counter drift and a single Redis instance. Drift comes from
crashes between increment and decrement, and grows with volume. I would replace the counter
with a sorted set per user, holding the active `document_id`s scored by lease expiry. Expired
entries are removed on every check, so drift heals itself. Then I'd move to Redis Cluster,
which distributes keys by hash slot.

**skip/limit pagination.** It doesn't hold up. Its cost grows with the page number, and
results shift when new documents arrive between pages, so users see duplicates or miss items.
I'd use cursor (keyset) pagination: return an opaque cursor `(created_at, _id)` of the last
item, and query `{user_id, (created_at, _id) < cursor}` sorted descending with a limit. Every
page is then a bounded index seek, whatever its depth.

## Assumptions

- **Identity:** there's no real auth. The caller's identity is the `X-User-Id` header. In
  production, this would come from a verified token.
- **`POST /documents` body `user_id`** must match `X-User-Id`, otherwise the API returns 403.
- **PATCH** changes only `content`. The title and the ref are immutable.
- **Cache scope:** identical content from different users shares the cached result, since
  the mock output depends only on the content.
- **Timestamps** are UTC.

## What I'd do differently with more time

- A real job queue (Redis Streams or arq) instead of in-process worker tasks, with separate
  worker containers, and lease-based stuck-job recovery (`running` with an expiry) instead of
  resetting on startup, which is only safe with one process.
- A retry endpoint for documents that end `failed`, resuming at the failed stage.
- A transactional outbox, so the "document saved" and "job enqueued" steps can't diverge.
- Cursor pagination in the API itself, not just in the design.
- Real authentication (JWT) instead of a header.
- Metrics (queue depth, stage latency, failure rate) and tracing.
- Load tests for the rate limiter and the pipeline.
