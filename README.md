# Syncer

> A **stateful sync agent written in Go** that keeps a **WooCommerce** store in step with a **Holoo POS** database — prices, stock and barcodes — automatically, around the clock, while the accounting system itself is left completely **read-only**.

**English** · [فارسی](README.fa.md)

> The published copy of this agent lives in its own repository: **[HolooDB_Sync](https://github.com/DeathGunSR/HolooDB_Sync)**.

![Go 1.25](https://img.shields.io/badge/Go-1.25.x-00ADD8?logo=go&logoColor=white)
![Platform](https://img.shields.io/badge/platform-Windows%207%2B-0078D6?logo=windows&logoColor=white)
![Source](https://img.shields.io/badge/source-Holoo%20POS%20%C2%B7%20SQL%20Server-CC2927?logo=microsoftsqlserver&logoColor=white)
![Target](https://img.shields.io/badge/target-WooCommerce%20REST%20v3-96588A?logo=woocommerce&logoColor=white)
![Catalog](https://img.shields.io/badge/catalog-%E2%89%8812%2C500%20SKUs-333333)

| | |
|---|---|
| **Role** | one-way sync agent: **Holoo → WooCommerce** |
| **Synced fields** | price, stock, barcodes (`SKU` is the join key) |
| **Source** | Holoo views on SQL Server — `SELECT` only, never a write |
| **Target** | WooCommerce REST API v3 — `/products` (paged reads), `/products/batch` (writes) |
| **Sync model** | poll → fingerprint → diff → batch push, with state persisted on disk |
| **Footprint** | one static binary + one `config.json` — no database, no queue, no framework |

---

## Why this exists

**Holoo** is a widely used Iranian retail accounting / POS system running on SQL Server. Shops that also sell online typically run **WooCommerce**, and from that moment on they have two catalogs: the one at the till and the one on the web.

Keeping them aligned by hand means stale prices, oversold stock and embarrassing discount mismatches. Replacing either system is not an option, and the accounting database must never be modified by a web integration.

This project is the bridge between them:

- **towards Holoo: read-only.** The agent only ever issues `SELECT` statements against two documented views. A bug in the sync can never damage the books.
- **towards the store: the single writer.** Prices, stock and barcodes are pushed through the official REST API, in batches, with retries.
- **cheap enough to run every 30 seconds** on an ordinary Windows PC — thanks to change fingerprinting and on-disk state (see [How it works](#how-it-works--one-cycle)).

---

## Scope — what is synced, and what deliberately is not

**Synced to WooCommerce**

- **Prices** — cover price vs. sale price become `regular_price` / `sale_price` (the source stores a scaled figure; the agent normalizes it by dividing by 10). If there is no real discount, no sale price is published.
- **Stock** — the `Exist` quantity becomes `manage_stock` + `stock_quantity`; negative stock is clamped to `0`.
- **Barcodes** — main + extra barcodes are stored as a comma-separated list in the `_holoo_barcodes` meta field, and the cover price is mirrored in `_cover_price`.
- **New products** — created with name + SKU, but with status **`draft`**, so a human always reviews an item before it can be sold online.

**Deliberately not synced**

- Names / descriptions of *existing* products — the storefront keeps editorial control; updates never touch `name` or `status`.
- Categories, images, attributes and variations — only `simple` products are handled.
- Deletions — an item removed from Holoo stays in the store until a human removes it (a safer default for retail).
- **Anything at all inside Holoo** — the agent has no write path to the accounting database.

---

## How it works — one cycle

```
          ┌──────────────┐                     ┌─────────────┐                     ┌──────────────┐
          │ Holoo POS    │◀──── read-only ────▶│  Sync agent │◀─── REST API v3 ───▶│ WooCommerce  │
          │ (SQL Server) │     SELECT only     │  (main.go)  │   /products/batch  │  store       │
          └──────────────┘                     └──────┬──────┘                     └──────────────┘
```

Every `sync_interval_seconds` (30 s in the sample config) the agent runs:

1. **Refresh the store state.** A full paged scan (100 items/page, `_fields` field projection, 1 s between pages) happens **only when the TTL expires** — `wc_cache_ttl_hours`, default 6 h — or on the very first run. Otherwise the persisted snapshot in `wc_cache.json` is reused, including right after a restart.
2. **Read Holoo.** One read-only query over `W_ShowArticleForSearch_Big` plus a barcode sweep over `W_ShowArticleForSearchCodes`, merged per SKU.
3. **Fingerprint.** An MD5 digest is computed over exactly the five fields that get synced (sale price, cover price, stock, main barcode, extra barcodes). Only SKUs whose digest differs from `holoo_hash.json` continue — everything else is already in sync.
4. **Diff.** Each surviving SKU is compared field-by-field against the stored store snapshot (normalizing `0` ⇄ `""` in prices) and split into `update` and `create` batches.
5. **Push.** `POST /products/batch` in chunks of `batch_size`, `batch_delay_seconds` apart — updates first, then creates — each chunk retried under a policy that distinguishes network failures, 5xx and 4xx.
6. **Persist.** The fingerprint map and the snapshot are written back, so the next cycle — and the next process start — resumes exactly where this one stopped.

A quiet cycle therefore pushes **nothing at all**; a cycle in which 40 products changed pushes 40 products, not the whole catalog.

---

## Engineering highlights

- **Two-level change detection.** A cheap, disk-persisted MD5 fingerprint decides *whether* a SKU is even looked at; the field-level diff decides *what* worth of payload is actually sent. Outbound API load scales with the number of real changes, not with catalog size.
- **Cost moved off the hot path.** The expensive full re-scan of the store happens at most once per TTL window, and a restart does **not** trigger it — state is loaded from disk on startup.
- **Rate-limit friendly by design.** Field projection on reads, 100-item paging with a delay, chunked batch writes with pauses between them, and a fixed per-request timeout of 120 s.
- **Retry policy that distinguishes failure kinds.** Network error → wait and retry; HTTP 5xx → 20 s cooldown and retry; HTTP 4xx → log and stop that chunk, because re-sending a rejected payload will not fix it. Full-scan pages get up to 5 attempts each.
- **Idempotent by construction.** `SKU` is the identity: existing products are updated, never duplicated; and a SKU is only pushed when its computed value differs from what the store reports.
- **Safe publishing.** New items land in `draft`; updates carry only price/stock/sku/meta, so titles and statuses chosen in the store survive.
- **Concurrency-safe state.** Both state maps are guarded by `sync.RWMutex`, so the caches stay consistent even as the code grows.
- **Operational friendliness.** UTF-8 console output on Windows (`SetConsoleOutputCP`), `http://` endpoints upgraded to `https://`, and fatal configuration/DB errors end with `Press Enter to exit` instead of a window that vanishes before the message can be read.

---

## Technology stack

| Component | Role in this project |
|---|---|
| **Go 1.25** | Single ~800-line source file, single static binary — nothing to install on the target machine. |
| **`database/sql` + `go-mssqldb`** | Read-only access to the Holoo SQL Server; Windows Authentication by default (empty user), or SQL login, with optional custom port. |
| **WooCommerce REST API v3** | Paged catalog reads and `batch` writes, authenticated with `consumer_key` / `consumer_secret`. |
| **stdlib `crypto/md5`** | Per-SKU change fingerprint — used purely as a fast, non-cryptographic digest. |
| **JSON state files** | Crash-safe warm start; the agent itself needs no database, cache service or queue. |
| **`sync.RWMutex`** | Thread-safe access to the in-memory caches. |
| **Win32 (`SetConsoleOutputCP`)** | Correct UTF-8 rendering of product names on the Windows console. |

---

## Configuration

`config.json` (all values live in one file next to the binary):

| Key | Meaning | Sample |
|---|---|---|
| `sql_server.server` | Host of the Holoo SQL Server, `host` or `host:port` | `127.0.0.1` |
| `sql_server.database` | Database name | `Holoo1` |
| `sql_server.user` / `password` | SQL login; **leave `user` empty** to use Windows Authentication (`trusted_connection`) | |
| `woocommerce.url` | Store base URL — `http://` values are automatically upgraded to `https://` | `https://shop.example.com` |
| `woocommerce.consumer_key` / `consumer_secret` | WooCommerce REST keys with **read/write** permission | `ck_…` / `cs_…` |
| `sync_interval_seconds` | Seconds between cycles (bounds how stale the store can get) | `30` |
| `batch_size` | Products per `/products/batch` call (falls back to `20` if ≤ 0) | `20` |
| `batch_delay_seconds` | Pause between batch calls (falls back to `2` if ≤ 0) | `2` |
| `wc_cache_ttl_hours` | Hours before the full store re-scan runs again (falls back to `6` if ≤ 0) | `6` |
| `holoo_hash_cache_enabled` | Turn the MD5 change filter on/off; `false` re-compares **all** SKUs every cycle | `true` |

```json
{
    "sql_server":      { "server": "127.0.0.1", "database": "Holoo1",
                         "user": "Sync Agent", "password": "••••••••" },
    "woocommerce":      { "url": "https://shop.example.com",
                         "consumer_key": "ck_••••", "consumer_secret": "cs_••••" },
    "sync_interval_seconds": 30,
    "batch_size": 20,
    "batch_delay_seconds": 2,
    "wc_cache_ttl_hours": 6,
    "holoo_hash_cache_enabled": true
}
```

> 🔐 **Secrets stay out of git.** Treat `config.json` values, the two state files and any pre-built binary as deployment artifacts: fill them on the target machine and keep them out of version control.

---

## Getting started

**Prerequisites**

- Windows 7 SP1 or later (the agent uses a Win32 console API).
- Go **1.25+** to build — or simply use the pre-built `sync-agent.exe`.
- A reachable SQL Server hosting the Holoo database.
- A WooCommerce store with REST API keys (WooCommerce → Settings → Advanced → REST API → **Read/Write**).

**Build from source** (run inside this directory):

```bash
go build -o sync-agent.exe .     # Go ≥ 1.25; run `go mod tidy` once if dependencies are missing
```

**Run**

```cmd
:: 1. edit config.json
:: 2. start the agent
sync-agent.exe
```

The **first** run performs a full scan of the store and builds the fingerprint map (for a ~12.5k-SKU catalog that is a one-off, minutes-long operation). Every cycle after that is incremental. Stop with `Ctrl+C`; fatal start-up errors wait for `Enter` so the message stays readable.

**What a healthy cycle looks like** *(log shapes are real; the counts are illustrative)*:

```
[SYNC] Phase 1: Fetching WooCommerce state...
[WC] Using cached state (age: 2.4 hours).
[SYNC] Phase 2: Syncing products...
[READ] Reading products from Holoo DB...
[DB] Products Found: 12513
[HASH] Products changed since last run: 40 out of 12513
[SUM] Create=3, Update=37, Up-to-date=12473
[OK] Batch Success.
... Cycle finished. Sleeping for 30 seconds...
```

---

## Repository layout

```
with state/
├─ main.go            the agent: config, caches, fingerprinting, diff, batch push, retries
├─ go.mod / go.sum    module definition + pinned dependency (go-mssqldb)
├─ config.json        runtime configuration — masked placeholders
├─ sync-agent.exe     pre-built Windows binary
├─ holoo_hash.json    runtime state — MD5 fingerprint per SKU (regenerated by the agent)
├─ wc_cache.json      runtime state — snapshot of the store (regenerated by the agent)
└─ HolooDB_Sync/      published copy of this agent → github.com/DeathGunSR/HolooDB_Sync
```

The two `*.json` state files and the binary are **runtime artifacts**: they are created automatically, and keeping them out of version control keeps the repository clean and prevents real store data from being published.

---

## Tuning & operational characteristics

| Knob | Sample value | What it trades off |
|---|---|---|
| `sync_interval_seconds` | 30 | How stale prices/stock can get vs. how busy the store API is. |
| `wc_cache_ttl_hours` | 6 | Freshness of the store snapshot vs. one full paged re-scan per window. |
| `holoo_hash_cache_enabled` | `true` | `false` = maximum safety (full comparison every cycle) at full API cost. |
| `batch_size` / `batch_delay_seconds` | 20 / 2 | Throughput of bulk updates vs. politeness to the store's rate limits. |
| Page delay / page size | 1 s / 100 (fixed) | Full-scan duration: ≈125 pages for a 12.5k catalog. |
| Batch retries | 3 (network → 10 s, 5xx → 20 s, 4xx → stop) | Resilience vs. not hammering a sick store. |
| Full-scan page retries | 5 | Tolerance of flaky networks during the TTL re-scan. |

---

## Design decisions & challenges

1. **State on disk instead of re-reading everything.** Rebuilding the store's state costs a full paged sweep — ≈125 round trips for a 12.5k catalog — so it is gated behind a TTL. Warm starts make restarts and crashes cheap.
2. **Fingerprint before diffing.** Hashing the five synced fields per SKU (persisted in `holoo_hash.json`) means the common case — "nothing changed at the till today" — results in **zero** outbound requests, while still being correct: any change to price, stock or barcodes invalidates that SKU's digest.
3. **Two independent sources of truth, reconciled deliberately.** Holoo is authoritative for price/stock/barcodes; the store is authoritative for names, descriptions and presentation. The payload construction encodes that split (create carries the name, update never does).
4. **`draft` as a human gate.** Auto-publishing an unseen product can break a storefront (missing images, untranslated text). New SKUs are therefore created as drafts for review.
5. **Batch endpoint with pacing and typed retries.** `/products/batch` plus chunking and deliberate delays turns thousands of individual writes into a rate-limit-friendly stream; retrying only what can succeed (network / 5xx) and stopping on 4xx keeps a broken configuration visible instead of buried in endless loops.
6. **Read-only contract with the accounting system.** The source side is two `SELECT` queries over documented views; there is no write path to Holoo, so the worst-case failure mode of this agent is "the web store is behind by one cycle" — never "the books are damaged".
7. **Forgiving on real-world store servers.** TLS certificate verification is currently skipped so stores with self-signed certificates still work, timeouts are generous (120 s), and non-`https` URLs are upgraded automatically.

---

## Known limitations

Honest inventory of what is **not** there yet:

- **One-way only.** Deletions and renames made in Holoo are not propagated; a product removed at the till stays online until someone removes it there too.
- **Simple products only.** No variations, categories, images, attributes or long descriptions.
- **Fingerprints are written after a batch is queued**, even if the API reported errors — in that unlucky case a changed product may not be retried until its data changes again or the TTL re-scan re-anchors state. Mitigation today: set `holoo_hash_cache_enabled` to `false` (or delete `holoo_hash.json`) to force a full comparison every cycle. Proper fix (advance fingerprints only for acknowledged items) is on the roadmap.
- **No automated test suite yet** — the price rules, diff and fingerprinting are the obvious first targets for table-driven Go tests.
- **Console process, not a Windows service** — schedule it with Task Scheduler / NSSM for autostart and restarts.
- **Configuration and state are plain JSON on disk**, with credentials in cleartext by design; deployment must restrict file permissions and keep them out of git.
- **TLS verification is off** (see decision 7) — a configuration flag to turn it back on is planned.

## Roadmap (not implemented — proposals)

> Nothing listed here exists yet; it is a plan, not a feature.

- Ack-aware fingerprint updates so failed batches are retried automatically.
- A `verify_tls` configuration switch (on by default for new deployments).
- Table-driven tests for price conversion, diff and fingerprinting; a CI workflow.
- Category / image / attribute sync and safe deletion propagation rules.
- Optional webhook/event trigger instead of pure polling.
- Packaged Windows service build and structured, rotating logs.

---

*Maintained as part of a portfolio of retail & POS integration tooling.*
