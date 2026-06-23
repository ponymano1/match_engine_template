//! 纯引擎基准 + channel 链路基准。
//! 前者测撮合本身;后者测「写入 crossbeam → 读取 → 撮合 → 写出」的真实 SPSC 开销,
//! 用来判断是否需要把 crossbeam_channel 换成无锁队列。
//!
//! 运行:  cargo bench --bench engine_bench

use std::thread;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};
use crossbeam_channel::bounded;

// —— 把二进制 crate 里的源文件直接挂到 bench crate 根 ——
#[path = "../src/domain.rs"]
mod domain;
#[path = "../src/engine.rs"]
mod engine;
#[path = "../src/orderbook.rs"]
mod orderbook;

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

// —— 批屏障:用 order_id = u64::MAX 的 Cancel 当哨兵 ——
// 撮合线程见到它就「重置引擎 + 回发 done」,不计入撮合工作量。
fn barrier() -> Sequenced {
    cancel(u64::MAX, 0)
}
#[inline]
fn is_barrier(s: &Sequenced) -> bool {
    matches!(&s.cmd, Command::Cancel { order_id, .. } if *order_id == u64::MAX)
}

// ============================================================
// 1) 单笔限价单挂簿(无成交)
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
// 2) 单笔穿价成交
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
// 3) 吞吐:10000 个 1 量 taker 吃穿一个 10000 量的 maker(纯引擎天花板)
// ============================================================
fn bench_10k_takers_one_maker(c: &mut Criterion) {
    const N: u64 = 10_000;
    let maker = no(1, 1, Side::Sell, OrderType::Limit, 100, N, 1);
    let takers: Vec<Sequenced> = (0..N)
        .map(|i| {
            no(
                1_000_000 + i,
                2,
                Side::Buy,
                OrderType::Limit,
                100,
                1,
                100 + i,
            )
        })
        .collect();

    let mut g = c.benchmark_group("throughput_10k_takers");
    g.throughput(Throughput::Elements(N));
    g.bench_function("one_deep_maker", |b| {
        b.iter_batched_ref(
            || {
                let mut e = Engine::new(SYMBOL, 500);
                e.handle(&maker);
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
// 4) 吞吐:一个大 taker 一次扫穿 10000 档
// ============================================================
fn bench_sweep_10k_levels(c: &mut Criterion) {
    const N: u64 = 10_000;
    let makers: Vec<Sequenced> = (0..N)
        .map(|i| no(i + 1, 1, Side::Sell, OrderType::Limit, 100 + i, 1, i + 1))
        .collect();
    let sweeper = no(
        9_999_999,
        2,
        Side::Buy,
        OrderType::Limit,
        100 + N,
        N,
        10_000_000,
    );

    let mut g = c.benchmark_group("throughput_sweep_levels");
    g.throughput(Throughput::Elements(N));
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
// 5) 挂单 + 撤单
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
// 6) FOK 预判走快路径否决
// ============================================================
fn bench_fok_precheck_reject(c: &mut Criterion) {
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

// ============================================================
// 7) channel 基础开销:同线程 send + recv,无竞争
//    量出 crossbeam_channel 每次 send/recv 的纯数据结构成本
//    (原子操作/加锁),不含跨线程唤醒、不含撮合。
// ============================================================
fn bench_channel_roundtrip(c: &mut Criterion) {
    let (tx, rx) = bounded::<Sequenced>(16);
    let cmd = no(1, 1, Side::Buy, OrderType::Limit, 100, 1, 1);
    c.bench_function("channel_send_recv_no_contention", |b| {
        b.iter_batched(
            || cmd.clone(),
            |c| {
                tx.send(c).unwrap();
                black_box(rx.recv().unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

// ============================================================
// 8) SPSC 全链路:生产者 → in_channel → 撮合线程 → out_channel → drain
//    完全照搬 main.rs::spawn_engine 的结构。
//    负载与 (3) 相同(1 maker + 10000 takers),可直接对比吞吐:
//      spsc 吞吐 / throughput_10k_takers 吞吐 = channel + 线程同步的留存率
// ============================================================
fn bench_spsc_pipeline(c: &mut Criterion) {
    const N: u64 = 10_000;
    let maker = no(1, 1, Side::Sell, OrderType::Limit, 100, N, 1);
    let takers: Vec<Sequenced> = (0..N)
        .map(|i| {
            no(
                1_000_000 + i,
                2,
                Side::Buy,
                OrderType::Limit,
                100,
                1,
                100 + i,
            )
        })
        .collect();

    // in 容量故意设小(贴近真实 ring buffer),强制生产者/撮合线程真正并发跑满。
    let (in_tx, in_rx) = bounded::<Sequenced>(1024);
    let (out_tx, out_rx) = bounded::<Event>(16_384);
    let (done_tx, done_rx) = bounded::<()>(1);

    // —— 撮合线程:从 in 读、handle、写 out;见 barrier 则重置引擎并回发 done ——
    let maker_c = maker.clone();
    let match_thread = thread::Builder::new()
        .name("bench-match".into())
        .spawn(move || {
            let mut engine = Engine::new(SYMBOL, 500);
            engine.handle(&maker_c); // 首批铺底
            for cmd in in_rx.iter() {
                if is_barrier(&cmd) {
                    engine = Engine::new(SYMBOL, 500);
                    engine.handle(&maker_c); // 为下一批重新铺底
                    let _ = done_tx.send(());
                    continue;
                }
                for ev in engine.handle(&cmd) {
                    let _ = out_tx.send(ev);
                }
            }
        })
        .unwrap();

    // —— drain 线程:消费 out,别让它成为背压瓶颈 ——
    let drain_thread = thread::Builder::new()
        .name("bench-drain".into())
        .spawn(move || {
            for _ev in out_rx.iter() {
                black_box(_ev);
            }
        })
        .unwrap();

    let mut g = c.benchmark_group("spsc_channel_pipeline");
    g.throughput(Throughput::Elements(N));
    g.bench_function("10k_takers_through_channel", |b| {
        b.iter_batched(
            || takers.clone(), // clone 不计时;模拟生产者持有 owned Sequenced
            |batch| {
                for t in batch {
                    in_tx.send(t).unwrap(); // in 满则阻塞 → 真实 SPSC 满负荷
                }
                in_tx.send(barrier()).unwrap();
                black_box(done_rx.recv().unwrap()); // 等这批撮合 + 写出全部完成
            },
            BatchSize::SmallInput,
        )
    });
    g.finish();

    // —— 收尾:关掉链路,join 线程 ——
    drop(in_tx); // 撮合线程 in_rx.iter() 结束
    match_thread.join().unwrap();
    // 撮合线程结束时 out_tx 随之 drop,drain 的 out_rx.iter() 自然结束
    drain_thread.join().unwrap();
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
        bench_channel_roundtrip,
        bench_spsc_pipeline,
}
criterion_main!(benches);
