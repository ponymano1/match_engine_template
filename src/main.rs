mod domain; mod orderbook; mod engine; mod mq;
use domain::*;
use engine::Engine;
use crossbeam_channel::{bounded, Receiver, Sender};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

/// 启动单个交易对的撮合线程，返回其 input ring buffer 入口
fn spawn_engine(symbol: String, out_tx: Sender<Event>) -> Sender<Sequenced> {
    // Input Ring Buffer：进程内有界队列（实战可换 rtrb SPSC 实现纳秒级）
    let (in_tx, in_rx): (Sender<Sequenced>, Receiver<Sequenced>) = bounded(1 << 16);
    thread::Builder::new()
        .name(format!("match-{symbol}"))
        .spawn(move || {
            let mut engine = Engine::new(symbol);
            // 撮合线程：只从 input 读、向 output 写，永不做 IO
            for seq_cmd in in_rx.iter() {
                for ev in engine.handle(&seq_cmd) {
                    let _ = out_tx.send(ev); // 推入 Output Ring Buffer
                }
            }
        })
        .unwrap();
    in_tx
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Output Ring Buffer：所有分片共享一个输出队列
    let (out_tx, out_rx) = bounded::<Event>(1 << 16);

    // —— Publisher（IO 线程）：Output Ring Buffer → External MQ ——
    {
        use mq::Outbound;
        let mut pub_mq = mq::redis_mq::RedisOutbound::new("redis://127.0.0.1/")?;
        thread::Builder::new().name("publisher".into()).spawn(move || {
            for ev in out_rx.iter() {
                // Trade → Clearing；Resting/Trade → Market Data。这里简化按类型分流 topic
                let topic = match &ev {
                    Event::Trade { .. } => "trades",
                    _ => "book_events",
                };
                let payload = serde_json::to_vec(&ev).unwrap();
                let _ = pub_mq.publish(topic, &payload);
            }
        }).unwrap();
    }

    // —— 按交易对分片：每个 symbol 一个独立引擎 + 独立线程 + 独立 Orderbook ——
    let mut routes: HashMap<String, Sender<Sequenced>> = HashMap::new();
    for sym in ["BTC/USDT", "ETH/USDT", "SOL/USDT"] {
        routes.insert(sym.into(), spawn_engine(sym.into(), out_tx.clone()));
    }

    // —— Receiver（IO 线程，主线程兼任）：External MQ → 定序 → 路由到对应 Input Ring Buffer ——
    use mq::Inbound;
    let mut in_mq = mq::redis_mq::RedisInbound::new("redis://127.0.0.1/", "orders")?;
    let seq = AtomicU64::new(1);

    loop {
        for raw in in_mq.poll()? {
            let cmd: Command = match serde_json::from_slice(&raw) {
                Ok(c) => c,
                Err(e) => { tracing::warn!("bad cmd: {e}"); continue; }
            };
            // 定序点：在这里固化 seq + ts，保证 Primary/Standby 重放完全一致
            let sequenced = Sequenced {
                seq: seq.fetch_add(1, Ordering::Relaxed),
                ts: now_nanos(),
                cmd: cmd.clone(),
            };
            let symbol = match &cmd {
                Command::NewOrder(o) => o.symbol.clone(),
                // 撤单需带 symbol 才能路由，这里假设有索引或在 NewOrder 时记录；简化处理
                Command::Cancel { .. } => continue,
            };
            if let Some(tx) = routes.get(&symbol) {
                let _ = tx.send(sequenced); // 推入对应分片的 Input Ring Buffer
            }
        }
    }
}

fn now_nanos() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}