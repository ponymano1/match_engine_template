# Matching Engine (Rust)

English | [简体中文](./README.zh-CN.md)

A deterministic, MQ-driven central limit order book (CLOB) matching engine
template. Designed to be reusable across projects: drop it in, tweak lightly,
and run. Everything that is *not* matching (accounts, risk, clearing, quoting)
is intentionally pushed out of the core and integrated as upstream/downstream
systems over a message queue.

## Why this exists

Matching engines across different projects are largely the same. This repo
distills that commonality into a clean template so the next project can start
from a working core instead of from scratch. The engine is deliberately
"pure": it only matches, and it only talks to the outside world through MQ.

## Scope

- **In scope**: order book maintenance, price-time priority matching, order
  types (Limit / Market / IOC / FOK / Post-Only), self-trade prevention,
  market protection, deterministic event output.
- **Out of scope** (integrate as separate services): accounts & balance
  freezing, pre-trade risk, clearing/settlement, quoting/market data
  enrichment.

## Matching rules: Price-Time Priority

1. **Price priority** — better prices match first (highest bid / lowest ask).
2. **Time priority** — within the same price level, earliest order matches
   first (FIFO).

## System boundary

```

  Input MQ  ──►  Matching Engine  ──►  Output MQ

```

- **Input**: MQ (Redis now; Kafka / SQS planned)
- **Output**: MQ
- The engine never knows where an order came from. User orders, Risk Service
  re-injections — all look identical to the engine.
- **Note**: Redis is for testing only. In production, prefer another MQ, or
  operate Redis with full high-availability management.

## Upstream / Downstream

```

Order Service ─┬─ Account Service (freeze balance)
               └─ Risk Service   (pre-trade checks)
                      │
              all passed → Order Service publishes to MQ
                      │
                      ▼
              ★ Matching Engine ★
                      │
                      ▼
            Clearing / Settlement  +  Market Data

````

## Data structures

- **Price index**: `BTreeMap<Price, PriceLevel>` — ordered by price.
  - Bids: `max key` = best bid.
  - Asks: `min key` = best ask.
- **Time queue**: each `PriceLevel` holds a `VecDeque<RestingOrder>` (FIFO).
- **Cancel index**: `HashMap<OrderId, (Side, Price)>` for O(1) cancel locate.
- **Level total cache**: each level caches its summed quantity for cheap FOK
  upper-bound pre-checks.

### Complexity

| Operation        | Complexity     | Notes                                   |
|------------------|----------------|-----------------------------------------|
| Read best bid/ask| O(1)           | `BTreeMap` first/last key               |
| Insert (rest)    | O(log N)       | locate level, push to FIFO tail         |
| Cancel           | O(log N + k)   | index lookup + scan within one level    |
| Match one fill   | O(1) amortized | pop from FIFO front                     |
| FOK pre-check    | O(levels)*     | *degrades to O(orders) when STP applies |

## Order types

| Type      | Behavior                                                            |
|-----------|--------------------------------------------------------------------|
| `Limit`   | Match what it can; rest the remainder on the book.                 |
| `Market`  | Take liquidity up to protection price; cancel the remainder.       |
| `Ioc`     | Match immediately; cancel any remainder (never rests).             |
| `Fok`     | Fill fully or cancel entirely (book untouched on kill).            |
| `PostOnly`| Maker-only; reject entirely if it would cross and take.            |

## Self-trade prevention (STP): Cancel Resting

When a taker reaches its own resting order, that maker order is **fully
cancelled** (a `Canceled` event is emitted) and the taker continues to consume
subsequent orders. The taker never trades against itself.

This is the "Cancel Oldest / Cancel Resting" strategy. Note that FOK's
`can_fill_fully` pre-check **excludes the taker's own resting liquidity**, so a
FOK that could only be filled by self-trading is killed before mutating the book.

## Defensive guard: qty == 0

Risk Service should reject zero-quantity orders upstream. As a defensive
backstop, the engine still:

1. Emits `Accepted` first (every `NewOrder` is acknowledged), then
2. Emits `Rejected`.

This is a single integer comparison at the entry point — not in the hot loop —
so it has no measurable throughput impact.

## Performance notes

- **qty == 0 check**: free. One comparison at entry, outside the hot path.
- **STP per-order `user_id` comparison**: effectively free. `RestingOrder` is
  popped from `front()` anyway, and `user_id` shares a cache line with
  `remaining` — no extra memory access.
- **`can_fill_fully` is the only real cost**: excluding self-orders degrades it
  from O(levels) (via the `total` cache) to O(orders crossed). Deep books with
  many small orders make FOK pre-checks slower. This is the one regression worth
  taking seriously.
- **Asymmetry that saves the fast path**: excluding self-orders can only *lower*
  available liquidity, never raise it. So we first do a cheap upper-bound check
  using `level.total` (which includes self). If even the inclusive total is
  below `qty`, the real answer is certainly "no" — return `false` immediately,
  no per-order scan. Only when the upper bound passes do we fall back to the
  per-order pass. Since FOK kills due to insufficient liquidity are the common
  case, most calls still run in O(levels).

## High availability

The engine is deterministic: **same input + same order ⇒ same output**. HA is
achieved by running multiple identical engines in parallel; only the Primary
emits trades.

```

MQ Topic
   ├─► Match Engine A (Primary)  ─► Trade output
   ├─► Match Engine B (Standby)  ─► (no output)
   └─► Match Engine C (Standby)  ─► (no output)

```

If the Primary dies, promote a Standby. **Trade output must be idempotent** so
downstream can dedupe on failover.

## Runtime architecture (threads & channels)

```

                         ┌──────────────────────────────────────────────┐
                         │                  main thread                   │
                         │            (Receiver / Sequencer)              │
  Redis Stream ─poll()──►│  RedisInbound.poll()                           │
                         │     → deserialize Command                      │
                         │     → route by symbol                          │
                         │     → assign (seq, shard_seq, ts)              │
                         └───────────────┬───────────────┬───────────────┘
                                         │               │
                          Sender<Sequenced> per symbol (bounded ring)
                                         │               │
                          ┌──────────────▼───┐   ┌───────▼──────────┐
                          │ match-BTC/USD     │   │ match-ETH/USD    │   ...
                          │ Engine::handle()  │   │ Engine::handle() │
                          └──────────────┬───┘   └───────┬──────────┘
                                         │               │
                          Sender<Event> (shared output ring, bounded)
                                         │               │
                                         └───────┬───────┘
                                                 ▼
                                     ┌───────────────────────┐
                                     │   publisher thread     │
                                     │   RedisOutbound        │
                                     │   .publish(topic, ..)  │
                                     └───────────┬───────────┘
                                                 ▼
                                       Redis Streams (trades / book events)

````

- One **dedicated thread per symbol** → independent order book, no locks.
- **Bounded** channels give natural backpressure when downstream lags.
- Matching threads do **no IO**: read input ring, write output ring, nothing else.

## Call flow: a NewOrder, end to end

```

main loop
└─ in_mq.poll()                         // mq.rs   RedisInbound::poll
└─ serde_json::from_slice::<Command>
└─ routes.get(&symbol)                  // drop if unknown symbol
└─ assign seq / shard_seq / ts
└─ tx.send(Sequenced)             ──────► (per-symbol channel)

match-<symbol> thread
└─ Engine::handle(&Sequenced)           // engine.rs
└─ on_new_order(o, seq, ts)
├─ push Event::Accepted
├─ if qty == 0 → push Rejected, return
├─ PostOnly:
│     └─ OrderBook::would_cross         // orderbook.rs
│           └─ best_ask / best_bid
│              price_crosses
│        ├─ cross  → push Rejected
│        └─ else   → OrderBook::rest → push Resting
├─ FOK:
│     └─ OrderBook::can_fill_fully
│           └─ can_fill_fully_inner
│                ├─ pass 1: sum level.total (upper bound)
│                └─ pass 2: per-order, skip taker_user_id
│        └─ insufficient → push Killed, return
├─ market_protection(side)         // Market only
│     └─ best_ask / best_bid
└─ OrderBook::match_order(...)
├─ loop best opposite level
│     ├─ price_crosses  (limit stop)
│     ├─ protection check (market stop)
│     └─ per-order FIFO:
│           ├─ STP: own order → pop + stp_canceled
│           └─ trade: fill, push Fill
├─ clean stp_canceled from index
└─ return MatchOutcome
└─ map outcome → Events:
├─ stp_canceled → Event::Canceled (before trades)
├─ fills        → Event::Trade
└─ remaining:
├─ Limit            → rest + Resting
└─ Market/Ioc/Fok   → Killed
└─ for ev in events: out_tx.send(ev)    ──────► (output channel)

publisher thread
└─ out_rx.iter()
└─ serde_json::to_vec(&Event)
└─ RedisOutbound::publish(topic, payload)   // mq.rs

````

## Call flow: a Cancel

```

main loop → route by symbol → Sequenced → channel
match thread
└─ Engine::handle
└─ Command::Cancel
└─ OrderBook::cancel(order_id)        // orderbook.rs
├─ index.remove → (side, price)
├─ locate level, remove order
├─ level.total -= removed.remaining
└─ drop level if empty
├─ true  → Event::Canceled
└─ false → Event::Rejected ("order not found")

````

## Module map

| File           | Responsibility                                                |
|----------------|---------------------------------------------------------------|
| `main.rs`      | Wiring: config, threads, channels, receiver/sequencer loop.   |
| `config.rs`    | TOML load + env override + startup validation.                |
| `domain.rs`    | Type aliases, `Command`, `Event`, order semantics.            |
| `mq.rs`        | `Inbound` / `Outbound` traits + Redis implementation.         |
| `engine.rs`    | Per-symbol engine: command → order book → event stream.       |
| `orderbook.rs` | The CLOB: levels, FIFO, matching/cancel core logic.           |

## Configuration

```toml
symbols = ["BTC/USD", "ETH/USD"]

[mq]
url               = "redis://127.0.0.1/"   # override with MQ_URL env var
inbound_stream    = "orders"
trades_topic      = "trades"
book_events_topic = "book"

[engine]
input_ring_capacity   = 1024   # must be a power of two
output_ring_capacity  = 4096   # must be a power of two
market_protection_bps = 500    # 5%
````

`MQ_URL` env var overrides `mq.url` (keep secrets out of the file). Ring
capacities must be powers of two; symbols must be unique — validated at startup,
the process refuses to run misconfigured.

## Running

```bash
cargo run --features redis-mq -- config.toml
cargo test --features redis-mq         # unit tests for engine + orderbook
```
## Engine performance testing
```bash
cargo bench --bench engine_bench
```

### Benchmark optimization path
- Cut outbound traffic in half. Today events are sent one-by-one via
  `out_tx.send` — two sends per order. Have the matching thread send the full
  `Vec<Event>` returned by `handle` as a single message (change the channel
  type from `Event` to `Vec<Event>` or `SmallVec<Event>`), so outbound channel
  ops drop from 2 per order to 1. That alone reduces matching-thread channel
  ops from 3 to 2, saving ~100 ns per order; retention rate should climb from
  ~28% back to ~40%. Highest-ROI change, no new dependencies. (Done)

## Roadmap

- [ ] Kafka inbound/outbound
- [ ] SQS inbound/outbound
- [ ] Snapshot + replay for cold start / recovery
- [ ] Per-shard gap detection via `shard_seq`

