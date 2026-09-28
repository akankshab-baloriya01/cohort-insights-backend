# Cohort Insights API

A FastAPI service that ingests documents, runs them through a two-stage simulated pipeline
(`processing` → summary, `enriching` → tags), and serves results to the submitting user and
to a partner system that addresses documents by its own `client_doc_ref`.

Stack: Python 3.12 · FastAPI · MongoDB 7 (PyMongo async) · Redis 7 · Docker Compose.

---

## Quick start

```bash
docker-compose up --build          # or: docker compose up --build
# API on http://localhost:8000, OpenAPI docs at http://localhost:8000/docs
```

This starts four containers: `api`, `worker` (the pipeline), `mongo`, `redis`.
Scale the pipeline with `docker-compose up --scale worker=3`. For a faster demo:
`PROCESSING_MIN_SECONDS=1 PROCESSING_MAX_SECONDS=2 ENRICHING_MIN_SECONDS=1 ENRICHING_MAX_SECONDS=2 docker-compose up`.

**Tests** (integration tests against real MongoDB and Redis, using `pytest` + `httpx.AsyncClient`):

```bash
docker-compose run --rm api pytest            # uses the compose mongo/redis (separate test DBs, Redis DB 15)

# or locally, against any MongoDB/Redis:
python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
TEST_MONGO_URI=mongodb://localhost:27017 TEST_REDIS_URL=redis://localhost:6379/15 .venv/bin/pytest
```

All configuration is environment-based; see [.env.example](.env.example) for every variable, with notes.

### Example session

```bash
curl -s -XPOST localhost:8000/documents -H 'content-type: application/json' \
  -d '{"user_id":"alice","title":"Q3 notes","content":"Revenue grew ...","client_doc_ref":"cms-42"}'
# 201 {"document_id":"66f...","status":"queued","content_version":1,"outcome":"created","served_from_cache":false}

curl -s localhost:8000/documents/66f... -H 'X-User-Id: alice'
curl -s localhost:8000/documents/by-ref/cms-42 -H 'X-User-Id: alice'
curl -s 'localhost:8000/users/alice/documents?page=1&page_size=20&status=completed' -H 'X-User-Id: alice'
curl -s -XPATCH localhost:8000/documents/66f... -H 'X-User-Id: alice' -H 'content-type: application/json' \
  -d '{"content":"Revised text","expected_version":1}'
curl -s -XPOST localhost:8000/documents/66f.../retry -H 'X-User-Id: alice'   # resume a failed document
curl -s localhost:8000/health
```

---

## API

| Method & path | Success | Errors |
|---|---|---|
| `POST /documents` | **201** created · **200** repeat of a known `client_doc_ref` | 409 stale/conflicting `ref_version`, 422, 429 |
| `PATCH /documents/{id}` | 200 (new `content_version`, pipeline restarted) | 401, 404, 409 `expected_version` mismatch, 422, 429 |
| `GET /documents/{id}` | 200 | 401, 404 |
| `GET /users/{user_id}/documents?page&page_size&status` | 200, newest first | 401, 404 (not the caller), 422 |
| `GET /documents/by-ref/{client_doc_ref}` | 200 | 401, 404 |
| `POST /documents/{id}/retry` | 202, resumes from the failed stage | 404, 409 not failed, 429 |
| `GET /health` | 200 `ok` / 200 `degraded` (Redis down) | 503 (MongoDB down) |

Errors share one shape: `{"error": {"code": "version_conflict", "message": "..."}}`.

**Identity.** There is no auth system in scope, so reads and writes identify the caller with an
`X-User-Id` header (401 if missing or malformed). In production that value would come from a verified token
set by a gateway. `POST /documents` takes `user_id` in the body, as the spec asks.

### What a poll returns

```jsonc
{
  "document_id": "66f...", "content_version": 2, "content_hash": "9c1e...",
  "status": "processing",                 // queued | processing | enriching | completed | failed
  "failed_stage": null,                   // "processing" | "enriching" when status == failed
  "stages": {
    "processing": {"state": "running", "attempts": 1, "started_at": "...", "finished_at": null, "error": null},
    "enriching":  {"state": "pending", "attempts": 0, ...}
  },
  "result": {                             // last published result, or null
    "content_version": 1,                 // the version BOTH summary and tags came from
    "content_hash": "41ab...",
    "is_current": false,                  // result.content_version == content_version
    "summary": "...", "tags": ["..."], "source": "pipeline", "completed_at": "..."
  }
}
```

Stage states are `pending | running | retry_scheduled | succeeded | failed | skipped` (skipped = served from
the content cache). The top-level `status` is the coarse position. `stages` is the structured detail, so
"which stage failed, after how many attempts, with what error" can be read from it without extra string fields.

---

## Schema & Staleness Design

### The document

```
_id              ObjectId
user_id          str
title, content   str
content_hash     sha256(content)
content_version  int      1 at insert; +1 on every content change. The fencing token.
client_doc_ref   str      optional; field omitted (not null) when absent
ref_version      int?     partner-supplied ordering for the ref
status           str      queued | processing | enriching | completed | failed
stages           { processing: StageInfo, enriching: StageInfo }   always describe content_version
draft            { content_version, content_hash, summary } | null  stage-1 output of the current run
result           { content_version, content_hash, summary, tags, completed_at, source } | null
lease            { owner, token, expires_at } | null                 worker claim
next_run_at      datetime | null                                     retry backoff / claimability
created_at, updated_at
```

### The mechanism

Derived data is stamped with the version it came from, and every write that could produce derived data is a
compare-and-set on that version. The rules:

1. **`summary` and `tags` are never stored as separate fields.** They exist only inside `result`, and
   `result` is only ever written as a whole sub-document in one `$set` (`build_result()` is the only
   constructor). MongoDB writes to a single document are atomic, and a read returns one snapshot of the
   document. So a reader sees the whole old `result` or the whole new one, never half of each.
2. **`result` carries its own `content_version` and `content_hash`,** next to the top-level
   `content_version`/`content_hash` of the same snapshot. The reader always has two values to compare, not a
   bare flag: `is_current` is literally `result.content_version == content_version`, and a client can also check
   `result.content_hash` against a hash of the content it holds.
3. **Content and version only change together.** `PATCH` (and a repeat submission with new content) does one
   `find_one_and_update` filtered on the `content_version` it read. That single update sets the new
   `content` + `content_hash`, runs `$inc content_version`, clears `draft`, resets `stages`, clears `lease`, and
   sets status `queued`. The old `result` stays, still labelled with the old version, so it shows as
   `is_current: false`.
4. **Every worker write is fenced.** Workers write with `filter = {_id, content_version: v, lease.token: t}`.
   The stage-2 publish also requires `draft.content_version: v`. Tags are computed from `draft.summary`, and the
   draft and the tags go into `result` in the publishing write.

**Why a mix cannot be observed:**

- Take any write that sets `result` from a run of version `v`. It only succeeds if the document's
  `content_version` is still `v` at that moment. Rule 3 means the content is then still the version-`v` content.
  So the published `result` is the output for exactly the content its label names.
- If a PATCH commits first, the version is `v+1` and the fenced write matches zero documents. The worker
  logs "discarded" and nothing is written. This covers a stage finishing after a PATCH
  (`test_inflight_stage1_for_old_content_is_discarded`). It also covers a v1 summary in `draft` meeting v2 tags:
  the PATCH cleared the draft and bumped the version, so the v1 enrichment cannot publish
  (`test_old_summary_never_combined_with_new_tags`).
- If the publish commits first, it is a correct v1 result. The PATCH then relabels it as not current by bumping
  the version beside it.
- Content-cache hits follow the same rule. The cached result is looked up by the new content's hash and is
  written in the same CAS that bumps the version.

None of this depends on timing. The only primitive used is single-document atomicity. Each invariant holds in
every snapshot: `result.content_version <= content_version`, and `result` equals the pipeline's output for
`content@result.content_version`. `test_polling_during_repeated_patches_never_sees_mixed_versions` checks both
after every step of an interleaved run. On a replica set, reading from a lagging secondary can return an *older*
snapshot. That snapshot is still internally consistent, because the same atomicity applies on replicas.

### Two-stage pipeline and partial progress

`queued → processing → enriching → completed | failed`. When stage 1 succeeds, it writes `draft` and sets
`status=enriching`, and it **releases the lease**. Stage 2 is a separate claim that any worker can pick up.
If enrichment fails, the draft is kept. Automatic retries and the manual `POST /documents/{id}/retry` both
resume at `enriching` and never re-run `processing` (`test_enrichment_failure_preserves_stage1_and_retry_resumes_there`).

Each stage retries on its own with exponential backoff and jitter (`STAGE_MAX_ATTEMPTS`,
`RETRY_BACKOFF_*`). Between attempts the stage shows `retry_scheduled` with the last error, and `next_run_at`
hides the job from workers until the backoff ends. Once attempts run out, the document becomes `failed`, and
`failed_stage` plus `stages.<stage>.error` say exactly where.

### Concurrency

| Race | Outcome |
|---|---|
| Two workers claim the same job/stage | The claim is one atomic `find_one_and_update` that sets `lease`; only one wins (`test_two_workers_cannot_claim_the_same_job`). |
| A worker stalls past its lease | Another worker reclaims it with a new token. The stale worker's writes fail the token fence (`test_expired_lease_is_reclaimed_and_old_worker_is_fenced`). A crash-looping stage is failed once attempts run out. |
| PATCH races an in-flight stage | The version fence drops the stale write (see above). |
| Two PATCHes with the same `expected_version` | Exactly one gets 200, the other gets **409 `version_conflict`** (`test_racing_patches_with_expected_version_exactly_one_wins`). |
| Two PATCHes without `expected_version` | Last writer wins, but each is applied as its own version (2 then 3). The loser re-reads and re-applies, so no write is silently lost. |
| Concurrent POSTs with the same `client_doc_ref` | One document is created. The others become repeat submissions (see below). |

---

## Crosswalk (`client_doc_ref`)

**Index:** `user_client_doc_ref_unique` = `{user_id: 1, client_doc_ref: 1}`, `unique: true`,
`partialFilterExpression: {client_doc_ref: {$exists: true}}`. `GET /documents/by-ref/{ref}` queries
`{user_id, client_doc_ref}` and resolves via IXSCAN on this index. `test_by_ref_lookup_uses_the_index` asserts
this with `explain()`. Documents without a ref leave the field out, so they are not indexed and do not collide.

**Assumption: refs are unique per owner, not globally.** A globally unique ref would let one tenant probe for,
or squat on, another tenant's refs: submitting a ref that someone else owns would have to fail in a way that
reveals it exists. Scoping by owner also keeps the unique index shardable (see At 100×). If the partner is one
system that must see all its refs, the fix is to authenticate the partner as the owner of its documents, not to
weaken tenant isolation.

**Repeat submissions of a known ref** (same owner). The rule is: *a ref names one logical document, and
re-sending it is an upsert of that document's content.*

| Incoming | Result |
|---|---|
| Same content (same hash) | **200 `outcome: "unchanged"`**. Idempotent: same `document_id`, no reprocessing, no rate-limit slot. Safe for blind partner retries. |
| Different content | **200 `outcome: "new_version"`**. Same `document_id`, content replaced exactly like a PATCH (`content_version+1`, both stages re-run, old result marked not current). |
| `ref_version` older than stored | **409 `stale_ref_version`**. An out-of-order delivery cannot overwrite newer content. |
| `ref_version` equal to stored, different content | **409 `ref_version_conflict`**. The partner broke its own versioning contract. |

`ref_version` is optional. Without it, the latest arrival wins (arrival order). With it, updates are ordered by
the partner's own revision, and the check is part of the CAS filter, so two racing redeliveries still apply in
revision order. Concurrent first submissions of one ref are serialized by a 5-second Redis lock per
`(user, ref)`, so only one of them takes a rate-limit slot. The others wait and then take the repeat path. If
Redis is down, the unique index still guarantees a single document. All of this is tested in
[tests/test_crosswalk.py](tests/test_crosswalk.py).

---

## Ownership

Every read is filtered by `{_id, user_id}` in the database query itself, not checked after loading. Another
user's document, a nonexistent id, and a malformed id all return the same **404** body
(`test_foreign_document_indistinguishable_from_missing`). A 403 would confirm that the id exists. Listing
another user's documents also returns 404. By-ref lookup is scoped the same way.

## Per-user rate limiting (Redis)

At most `MAX_ACTIVE_DOCS_PER_USER` (3) documents per user across `queued`, `processing`, and `enriching`. Going
over returns **429**. Each user has a Redis **sorted set** `rl:active:{user}` whose members are pipeline runs
(`{doc_id}:{content_version}`), with an expiry timestamp as the score. One Lua script purges expired members,
checks the count, and adds the member atomically. Compared with an `INCR/DECR` counter:

- **Idempotent.** Adding or removing the same run twice cannot drift the count.
- **Self-healing.** A release lost to a crash or a Redis blip expires after `ACTIVE_SLOT_TTL_SECONDS`. Workers
  refresh the score at every stage claim.
- **Safe against the PATCH/complete race.** A PATCH on an active document moves the slot from `id:v` to
  `id:v+1` in the same script, so the old run's late release touches nothing. A PATCH on an idle
  (completed/failed) document needs a new slot. A cache hit never takes one.

## Content cache (Redis, keyed by content hash)

The key is `cache:result:{sha256(content)}` → `{content_hash, summary, tags}`, with a TTL
(`CONTENT_CACHE_TTL_SECONDS`). The key never involves `document_id`: after a PATCH the lookup uses a different
key, so an old version's entry has nothing to leak into (`test_cache_is_keyed_by_content_across_patch`). The
value repeats its hash and is checked against the key on read. MongoDB is the second tier: on a Redis miss, the
indexed lookup `result.content_hash` finds any published result for that content and refills Redis. A hit
completes the document immediately (`status: completed`, `source: "cache"`, stages `skipped`) without touching
the rate limit.

The cache is **shared across users**. Summary and tags are a pure function of the content, so a hit tells a user
nothing beyond content they already submitted. With a real LLM (non-deterministic output, per-tenant prompts),
I would scope the key per tenant.

## Redis unavailability (graceful degradation)

| Concern | When Redis is down |
|---|---|
| Rate limit | Falls back to `count_documents({user_id, status ∈ active})` on the `user_status_created` index. Still correct, just racy under concurrency. Slots are neither failed open nor failed closed. |
| Content cache | Skips to the MongoDB tier. Writes are logged and skipped. |
| Ref creation lock | Proceeds without it. The unique index still arbitrates. |
| `/health` | `200 {"status": "degraded"}`. MongoDB down → `503`. |

Tested by `test_redis_outage_degrades_gracefully`. Redis socket timeouts are short (0.5s) so an outage degrades
quickly instead of hanging requests. Every Redis call catches `RedisError` specifically, and every catch logs.

## Indexes

| Name | Keys | Serves |
|---|---|---|
| `user_created` | `user_id, created_at↓, _id↓` | list, newest first |
| `user_status_created` | `user_id, status, created_at↓, _id↓` | list with `status` filter; rate-limit fallback count |
| `user_client_doc_ref_unique` | `user_id, client_doc_ref` (unique, partial) | by-ref lookup, ref uniqueness |
| `result_content_hash` | `result.content_hash` (partial) | content cache second tier |
| `claim` | `status, next_run_at` | worker claim query |

`GET /documents/{id}` uses `_id`. Indexes are created at startup (idempotently) by both the API and the worker.

## Project layout

```
app/
  main.py            app factory, lifespan, request-id + access-log middleware
  config.py          pydantic-settings (env-based)
  domain.py          statuses, stage states, hashing, result constructor
  models.py          Pydantic request/response models + validation
  repository.py      all MongoDB access, indexes, fenced CAS writes
  resources.py       client construction shared by api and worker
  dependencies.py    DI: service, caller identity
  routers/           documents, users, health
  services/          documents (use-cases), rate_limiter, content_cache, locks, stages (mock pipeline)
  worker.py          claim/lease loop, retries with backoff
tests/               integration (API + worker in-process, real Mongo/Redis) and unit tests
```

## Assumptions

- `content` and `title` are whitespace-stripped. The hash is over the stripped UTF-8 content, with no further
  normalisation, so a one-character change counts as new content. Content is capped at 100k characters.
  Control characters are rejected.
- `PATCH` changes content only, per the spec. `user_id` and unknown fields are rejected (`extra="forbid"`).
- A PATCH with identical content is a no-op (200, same version). It does not reprocess.
- Refs are restricted to `[A-Za-z0-9._:-]{1,128}` so they are always a single URL path segment.
- `page_size` is capped at `MAX_PAGE_SIZE`. `page` is capped at 10,000 as a guard against deep skips (see At 100×).
- MongoDB is the job queue: workers poll with an indexed claim query. This keeps jobs durable and makes
  claiming atomic without a second source of truth. Redis stays purely for the limiter and cache, as the brief
  asks, and a Redis outage never loses a job.

## What I'd do differently with more time

- **Push-based dispatch.** Replace polling with Redis Streams or a change stream to wake workers, keeping Mongo
  as the source of truth. Add lease heartbeats so stage duration isn't bounded by `LEASE_SECONDS`.
- **Keyset pagination** and an opaque cursor (see below). Drop the exact `total`, which is a count on every call.
- **Real authentication** (JWT or partner API keys) in place of `X-User-Id`, with the partner as a first-class
  principal. Per-tenant cache scoping.
- **Observability.** Prometheus metrics (queue depth, stage latency, failure/retry rates, discarded-by-fence
  counts) and OpenTelemetry tracing across API → worker.
- **A periodic reconciler** that rebuilds each user's Redis slot set from MongoDB, instead of relying only on slot
  expiry after a Redis failover. Also a dead-letter view for documents that exhausted retries.
- Replica-set MongoDB in compose, so `majority` write/read concerns and transactions are available and tested.

---

## At 100×

**500K documents for one user.** The `user_id`-prefixed compound indexes still find the *first* page in
O(log n): the B-tree seeks to `user_id` and reads 20 keys in `created_at` order. What breaks is everything that
touches the user's whole range. `count_documents` for `total` walks 500K index keys on every list call. A deep
`skip` walks every skipped key. A rarely-true `status` filter on the non-status index would scan the range, which
is why `user_status_created` exists. The bigger problem is sharding: with the key `user_id` alone, all 500K
documents share one shard-key value. That makes an unsplittable **jumbo chunk**, pinning a whale user's whole
write and read load to one shard.

**Shard key: `{user_id: 1, _id: 1}`.** Every hot query includes `user_id` (get-by-id is `{_id, user_id}`, list,
by-ref, rate-limit fallback), so they are all *targeted* to one shard, or a few for a whale. The `_id` suffix
lets a whale's range split into many chunks. Why not `user_id` alone: jumbo chunks, as above. Why not hashed
`_id`: listing a user's documents would become scatter-gather across every shard. The usual objection to a
monotonic suffix is a hot last chunk, and that is bounded here. The rate limiter caps each user at 3 in-flight
documents, so one user cannot flood a chunk with inserts. It also keeps `(user_id, client_doc_ref)` enforceable,
because MongoDB only enforces unique indexes that are prefixed by the shard key. A *global* ref uniqueness could
not be kept after sharding. What does not target is the worker's `claim` query (`status, next_run_at`): it would
scatter. At this scale I would move dispatch to a partitioned queue (Kafka or Redis Streams keyed by `user_id`)
and keep Mongo as the source of truth.

**Redis limiter at 100× submit QPS.** The design scales: one key per user, a Lua script over a set of at most 3
members (O(log 3)), and single-key scripts, so it runs unchanged on **Redis Cluster** with users spread across
slots. What breaks is a single Redis primary's CPU. Every submit runs a script, and every rejected 429 still costs
one. I would shard it with Redis Cluster, add `Retry-After` to cheapen retry storms, and reject obvious abusers
earlier with a cheap token bucket at the edge. Failover is the real correctness gap. Async replication can drop
recent `ZADD`s, briefly under-counting, so I'd add a reconciler that rebuilds sets from Mongo. The Mongo fallback
would stampede at 100×, so it needs a circuit breaker with a per-process local limit.

**Pagination.** `skip/limit` does not survive. `skip(N)` costs O(N) index keys, so page 20,000 of a 500K-document
user reads 400K keys. Pages also shift as new documents arrive at the head, which causes duplicates and gaps.
I'd switch to **keyset (cursor) pagination**: return an opaque cursor for the last item's `(created_at, _id)`,
query `{user_id, (created_at, _id) < cursor}` sorted descending with `limit+1` for `has_next`, and drop the exact
`total` (or show an approximate, cached count). Every page then costs O(page_size) on the existing indexes,
however deep it is.
