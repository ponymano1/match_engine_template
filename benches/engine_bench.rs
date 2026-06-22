//! 纯引擎基准:不经 MQ、不经 serde、不经线程,直接打 Engine::handle。
//! 测的是撮合本身的吞吐与单均延迟。
//!
//! 运行:  cargo bench --bench engine_bench

use std::time::Duration;

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput,
};

// —— 把二进制 crate 里的源文件直接挂到 bench crate 根 ——
// orderbook.rs / engine.rs 内部用的是 `crate::domain` 等绝对路径,
// 这里平级同名挂载即可解析;它们的 #[cfg(test)] 单测在 bench 下不编译。
#[path = "../src/domain.rs"]
mod domain;
#[path = "../src/orderbook.rs"]
mod orderbook;
#[path = "../src/engine.rs"]
mod engine;

use domain::*;
use engine::Engine;

const SYMBOL: &str = "X";

/// 构造一个已定序的 NewOrder 命令
fn no(
    id: OrderId,
    user: u64,
    side: Side,
    ot: OrderType,
    price: Price,
    qty: Quantity,
    seq: Sequence,
) -> Sequenced {
    Sequenced {
        seq,
        shard_seq: seq,
        ts: seq,
        cmd: Command::NewOrder(NewOrder {
            order_id: id,
            symbol: SYMBOL.into(),
            side,
            order_type: ot,
            price,
            quantity: qty,
            user_id: user,
        }),
    }
}

fn cancel(id: OrderId, seq: Sequence) -> Sequenced {
    Sequenced {
        seq,
        shard_seq: seq,
        ts: seq,
        cmd: Command::Cancel {
            order_id: id,
            symbol: SYMBOL.into(),
        },
    }
}

// ============================================================
// 1) 单笔限价单挂簿(无成交)——最纯的写路径
// ============================================================
fn bench_limit_rest(c: &mut Criterion) {
    let order = no(1, 1, Side::Buy, OrderType::Limit, 100, 10, 1);
    c.bench_function("limit_rest_no_match", |b| {
        b.iter_batched_ref(
            || Engine::new(SYMBOL, 500),
            |e| black_box(e.handle(black_box(&order))),
            BatchSize::SmallInput,
        )
    });
}

// ============================================================
// 2) 单笔穿价成交(一个 maker 一个 taker)
// ============================================================
fn bench_single_cross(c: &mut Criterion) {
    let maker = no(1, 1, Side::Sell, OrderType::Limit, 100, 1, 1);
    let taker = no(2, 2, Side::Buy, OrderType::Limit, 100, 1, 2);
    c.bench_function("single_cross_trade", |b| {
        b.iter_batched_ref(
            || {
                let mut e = Engine::new(SYMBOL, 500);
                e.handle(&maker);
                e
            },
            |e| black_box(e.handle(black_box(&taker))),
            BatchSize::SmallInput,
        )
    });
}

// ============================================================
// 3) 吞吐:10000 个 1 量 taker 吃穿一个 10000 量的 maker
//    每个 taker → Accepted + Trade
// ============================================================
fn bench_10k_takers_one_maker(c: &mut Criterion) {
    const N: u64 = 10_000;
    let maker = no(1, 1, Side::Sell, OrderType::Limit, 100, N, 1);
    let takers: Vec<Sequenced> = (0..N)
        .map(|i| no(1_000_000 + i, 2, Side::Buy, OrderType::Limit, 100, 1, 100 + i))
        .collect();

    let mut g = c.benchmark_group("throughput_10k_takers");
    g.throughput(Throughput::Elements(N)); // 报告会给出 elem/s
    g.bench_function("one_deep_maker", |b| {
        b.iter_batched_ref(
            || {
                let mut e = Engine::new(SYMBOL, 500);
                e.handle(&maker); // 铺底,不计时
                e
            },
            |e| {
                for t in &takers {
                    black_box(e.handle(black_box(t)));
                }
            },
            BatchSize::SmallInput,
        )
    });
    g.finish();
}

// ============================================================
// 4) 吞吐:10000 个独立 maker(10000 个价位档),一个大 taker 一次扫穿
//    更贴近"深簿一次性吃多档"的最坏路径
// ============================================================
fn bench_sweep_10k_levels(c: &mut Criterion) {
    const N: u64 = 10_000;
    // 10000 个卖单,价位 100..10100,各 1 量
    let makers: Vec<Sequenced> = (0..N)
        .map(|i| no(i + 1, 1, Side::Sell, OrderType::Limit, 100 + i, 1, i + 1))
        .collect();
    // 一个买单,限价够高、量够大,一次吃光 10000 档
    let sweeper = no(9_999_999, 2, Side::Buy, OrderType::Limit, 100 + N, N, 10_000_000);

    let mut g = c.benchmark_group("throughput_sweep_levels");
    g.throughput(Throughput::Elements(N)); // 每个档算一个元素
    g.bench_function("sweep_10k_levels_single_taker", |b| {
        b.iter_batched_ref(
            || {
                let mut e = Engine::new(SYMBOL, 500);
                for m in &makers {
                    e.handle(m);
                }
                e
            },
            |e| black_box(e.handle(black_box(&sweeper))),
            BatchSize::SmallInput,
        )
    });
    g.finish();
}

// ============================================================
// 5) 挂单 + 撤单 配对
// ============================================================
fn bench_rest_then_cancel(c: &mut Criterion) {
    let order = no(1, 1, Side::Buy, OrderType::Limit, 100, 10, 1);
    let cxl = cancel(1, 2);
    c.bench_function("rest_then_cancel", |b| {
        b.iter_batched_ref(
            || Engine::new(SYMBOL, 500),
            |e| {
                black_box(e.handle(black_box(&order)));
                black_box(e.handle(black_box(&cxl)));
            },
            BatchSize::SmallInput,
        )
    });
}

// ============================================================
// 6) FOK 预判走快路径(流动性不足,O(档数) 上界否决)
// ============================================================
fn bench_fok_precheck_reject(c: &mut Criterion) {
    // 簿里只有 3 量,FOK 要 5 → can_fill_fully 第一遍上界就否决
    let maker = no(1, 1, Side::Sell, OrderType::Limit, 100, 3, 1);
    let fok = no(2, 2, Side::Buy, OrderType::Fok, 100, 5, 2);
    c.bench_function("fok_precheck_reject", |b| {
        b.iter_batched_ref(
            || {
                let mut e = Engine::new(SYMBOL, 500);
                e.handle(&maker);
                e
            },
            |e| black_box(e.handle(black_box(&fok))),
            BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(10))
        .sample_size(30);
    targets =
        bench_limit_rest,
        bench_single_cross,
        bench_10k_takers_one_maker,
        bench_sweep_10k_levels,
        bench_rest_then_cancel,
        bench_fok_precheck_reject,
}
criterion_main!(benches);