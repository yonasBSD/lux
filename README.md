<p align="center">
  <img src="logo.png" alt="Lux" width="120" height="120" />
</p>

<h1 align="center">Lux</h1>

<p align="center">
  <strong>A Redis-compatible key-value store. 3-5x faster.</strong><br/>
  Multi-threaded. Written in Rust. MIT licensed forever.
</p>

<p align="center">
  <a href="https://github.com/lux-db/lux/actions/workflows/test.yml"><img src="https://github.com/lux-db/lux/actions/workflows/test.yml/badge.svg" alt="Tests" /></a>
  <a href="https://github.com/lux-db/lux/releases/latest"><img src="https://img.shields.io/github/v/release/lux-db/lux" alt="Release" /></a>
  <a href="https://github.com/lux-db/lux/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License" /></a>
</p>

<p align="center">
  <a href="https://luxdb.dev">Lux Cloud</a> &middot;
  <a href="https://luxdb.dev/vs/redis">Benchmarks</a> &middot;
  <a href="https://luxdb.dev/architecture">Architecture</a>
</p>

---

## Why Lux?

Redis is single-threaded by design. Antirez made that choice in 2009 because it eliminates all locking, race conditions, and concurrency bugs. For most workloads, the bottleneck is network I/O, not CPU, so a single-threaded event loop is fast enough. It was a brilliant simplification.

But it has a ceiling. Once you saturate one core, that's it. Redis can't use the other 15 cores on your machine. The official answer is to run multiple Redis instances and shard at the client level (Redis Cluster), which adds significant operational complexity.

Lux takes the opposite approach: a **sharded concurrent architecture** that safely uses all your cores in a single process. Each key maps to one of N shards, each protected by a `parking_lot` RwLock. Reads never block reads. Writes only block the single shard they touch. Tokio's async runtime handles thousands of connections across all cores. The result: single-digit microsecond latency at low concurrency (matching Redis), and linear throughput scaling as you add cores and pipeline depth (where Redis flatlines).

"Doesn't multi-threading introduce the bugs Redis avoided?" No. Lux's concurrency is at the shard level, not the command level. Each command acquires a single shard lock, does its work, and releases. There are no cross-shard locks, no lock ordering issues, no deadlocks. The only shared mutable state is inside shards, and the RwLock makes that safe. MULTI/EXEC transactions use WATCH-based optimistic concurrency (shard versioning) rather than global locks, matching what Redis clients actually rely on.

Point your existing Redis client at Lux. Most workloads just work.

**Works with every Redis client** -- ioredis, redis-py, go-redis, Jedis, redis-rb. Zero code changes.

### Benchmarks

`redis-benchmark`, 50 clients, 1M SET requests per pipeline depth. Sequential runs (one server at a time) on a 32-core Intel i9-14900K, 128GB RAM, Ubuntu 24.04.

| Pipeline | Lux | Redis 8.6.1 | Lux/Redis |
|----------|-----|-------------|-----------|
| 1 | 287K | 296K | 0.97x |
| 16 | 3.89M | 2.47M | **1.58x** |
| 64 | **10.87M** | 3.30M | **3.29x** |
| 128 | **15.39M** | 3.52M | **4.37x** |
| 256 | **20.00M** | 3.58M | **5.58x** |
| 512 | **20.01M** | 3.50M | **5.72x** |

At pipeline depth 256, Lux hits **20 million SET ops/sec** -- 5.6x faster than Redis 8. At low concurrency they're equal. The gap grows with pipeline depth and core count because Lux's shard batching scales across all cores while Redis is bound to one.

Reproduce with `./bench.sh` (requires `redis-server` and `redis-benchmark` in PATH).

## Lux Cloud

Don't want to manage infrastructure? **[Lux Cloud](https://luxdb.dev)** is managed Lux hosting.

- **$5/mo** per instance, 1GB memory
- 4x more memory than Redis Cloud at the same price
- Deploy in seconds, connect with any Redis client
- Persistence, monitoring, and web console included

## Features

- **140+ Redis commands** -- strings, lists, hashes, sets, sorted sets, pub/sub, transactions
- **RESP2 protocol** -- compatible with every Redis client
- **Multi-threaded** -- auto-tuned shards, parking_lot RwLocks, tokio async runtime
- **Zero-copy parser** -- RESP arguments are byte slices into the read buffer
- **Pipeline batching** -- consecutive same-shard commands batched, preserving per-client ordering
- **Persistence** -- automatic snapshots, configurable interval
- **Auth** -- password authentication via `LUX_PASSWORD`
- **Pub/Sub** -- SUBSCRIBE, UNSUBSCRIBE, PUBLISH
- **TTL support** -- EX, PX, EXPIRE, PEXPIRE, PERSIST, TTL, PTTL
- **856KB Docker image** -- the entire database fits in under 1MB. Redis is 30MB. Dragonfly is 180MB.
- **MIT licensed** -- no license rug-pulls, unlike Redis (RSALv2/SSPL)

## Quick Start

```bash
cargo build --release
./target/release/lux
```

Lux starts on `0.0.0.0:6379` by default. Connect with any Redis client:

```bash
redis-cli
> SET hello world
OK
> GET hello
"world"
```

### Docker

```bash
docker run -d -p 6379:6379 ghcr.io/lux-db/lux:latest
```

### Docker Compose

```bash
docker compose up -d        # start
docker compose up -d --build  # rebuild & start
docker compose down         # stop
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LUX_PORT` | `6379` | TCP port |
| `LUX_PASSWORD` | (none) | Enable AUTH |
| `LUX_DATA_DIR` | `.` | Snapshot directory |
| `LUX_SAVE_INTERVAL` | `60` | Snapshot interval in seconds (0 to disable) |
| `LUX_SHARDS` | auto | Shard count (default: num_cpus * 16) |
| `LUX_RESTRICTED` | (none) | Set to `1` to disable KEYS, FLUSHALL, FLUSHDB |

### Node.js (ioredis)

```bash
npm install ioredis
```

```typescript
import Redis from "ioredis"

const redis = new Redis("redis://localhost:6379")
await redis.set("hello", "world")
console.log(await redis.get("hello")) // "world"
```

### Python (redis-py)

```bash
pip install redis
```

```python
import redis

r = redis.Redis(host="localhost", port=6379)
r.set("hello", "world")
print(r.get("hello"))  # b"world"
```

### Go (go-redis)

```go
import "github.com/redis/go-redis/v9"

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
rdb.Set(ctx, "hello", "world", 0)
```

## Testing

Lux has 220 tests across unit and integration suites.

```bash
cargo test -- --test-threads=1
```

| Suite | Tests | What it covers |
|-------|------:|----------------|
| **Unit: cmd** | 62 | Every command handler, arg validation, error paths |
| **Unit: store** | 66 | All data structures, TTL, shard versioning, expiry |
| **Unit: resp** | 19 | RESP parser, serializers, edge cases |
| **Unit: snapshot** | 7 | Roundtrip all data types, TTL preservation |
| **Unit: pubsub** | 5 | Broker subscribe/publish/isolation |
| **Integration: transactions** | 29 | MULTI/EXEC, WATCH/UNWATCH, EXECABORT, DISCARD |
| **Integration: auth** | 6 | Password gating, per-connection state, error paths |
| **Integration: pubsub** | 10 | Cross-connection message delivery, unsubscribe, sub mode |
| **Integration: persistence** | 3 | Snapshot save/restart/restore, FLUSHDB+SAVE |
| **Integration: pipelines** | 3 | Ordering under contention, fast-path batching |
| **Valkey compat** | 10+ | Valkey multi.tcl test suite run against Lux |

Run the benchmark against Redis:

```bash
./bench.sh
```

### CI

Every push and pull request runs:

- `cargo fmt -- --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test --all-targets`
- Integration tests against the Valkey test harness

Release and Docker builds only proceed after tests pass.

## Supported Commands

**Strings:** `SET` `GET` `SETNX` `SETEX` `PSETEX` `GETSET` `GETDEL` `GETEX` `GETRANGE` `SETRANGE` `MGET` `MSET` `MSETNX` `STRLEN` `APPEND` `INCR` `DECR` `INCRBY` `DECRBY` `INCRBYFLOAT`

**Keys:** `DEL` `UNLINK` `EXISTS` `KEYS` `SCAN` `TYPE` `RENAME` `RENAMENX` `RANDOMKEY` `COPY` `TTL` `PTTL` `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `EXPIRETIME` `PEXPIRETIME` `PERSIST` `DBSIZE` `FLUSHDB` `FLUSHALL`

**Lists:** `LPUSH` `RPUSH` `LPUSHX` `RPUSHX` `LPOP` `RPOP` `LLEN` `LRANGE` `LINDEX` `LSET` `LINSERT` `LREM` `LTRIM` `LPOS` `LMOVE` `RPOPLPUSH`

**Hashes:** `HSET` `HSETNX` `HMSET` `HGET` `HMGET` `HDEL` `HGETALL` `HKEYS` `HVALS` `HLEN` `HEXISTS` `HINCRBY` `HINCRBYFLOAT` `HSTRLEN` `HRANDFIELD` `HSCAN`

**Sets:** `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SMISMEMBER` `SCARD` `SPOP` `SRANDMEMBER` `SMOVE` `SUNION` `SINTER` `SDIFF` `SUNIONSTORE` `SINTERSTORE` `SDIFFSTORE` `SINTERCARD` `SSCAN`

**Sorted Sets:** `ZADD` `ZSCORE` `ZMSCORE` `ZRANK` `ZREVRANK` `ZREM` `ZCARD` `ZCOUNT` `ZLEXCOUNT` `ZINCRBY` `ZRANGE` `ZREVRANGE` `ZRANGEBYSCORE` `ZREVRANGEBYSCORE` `ZRANGEBYLEX` `ZREVRANGEBYLEX` `ZPOPMIN` `ZPOPMAX` `ZUNIONSTORE` `ZINTERSTORE` `ZDIFFSTORE` `ZREMRANGEBYRANK` `ZREMRANGEBYSCORE` `ZREMRANGEBYLEX` `ZSCAN`

**Pub/Sub:** `PUBLISH` `SUBSCRIBE` `UNSUBSCRIBE`

**Transactions:** `MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH`

**Server:** `PING` `ECHO` `HELLO` `INFO` `TIME` `SAVE` `BGSAVE` `LASTSAVE` `AUTH` `CONFIG` `CLIENT` `SELECT` `COMMAND` `OBJECT` `MEMORY`

## Known Differences from Redis

Lux is Redis-compatible but not identical. Key differences:

- **No Lua scripting** -- `EVAL`, `EVALSHA`, and `SCRIPT` are not supported
- **No blocking commands** -- `BLPOP`, `BRPOP`, `BLMOVE` etc. are not supported
- **No AOF persistence** -- snapshots only (configurable interval)
- **No RESP3 protocol** -- RESP2 only
- **No cluster mode** -- single-node only (use Lux Cloud for managed hosting)
- **MULTI/EXEC** -- supported with WATCH-based optimistic locking. Commands in a transaction execute sequentially, each acquiring its own shard lock, so another client could observe intermediate state mid-EXEC. Redis avoids this via single-threading. Standard client libraries (Redlock, BullMQ, Sidekiq) rely on WATCH for correctness, not EXEC isolation. Full shard-locking isolation may be added in a future release if there's demand
- **Pipeline ordering** -- per-client command order is preserved. Consecutive same-shard commands are batched for performance

## Architecture

```
Client connections (tokio tasks)
        |
   Zero-Copy RESP Parser (byte slices, no allocations)
        |
   Pipeline Batching (consecutive same-shard commands batched)
        |
   Command Dispatch (byte-level matching, no string conversion)
        |
   Sharded Store (auto-tuned RwLock shards, hashbrown raw_entry)
        |
   FNV Hash -> Shard Selection (pre-computed, reused for HashMap lookup)
```

Read the full deep dive at [luxdb.dev/architecture](https://luxdb.dev/architecture).

## License

MIT
