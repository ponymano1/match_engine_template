mod config;
mod domain;
mod orderbook;
mod engine;
mod mq;

use config::Config;
use domain::*;
use engine::Engine;
use crossbeam_channel::{bounded, Receiver, Sender};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

/// 启动单个交易对的撮合线程,返回其 input ring buffer 入口
fn spawn_engine(
    symbol: String,
    protection_bps: u64,
    input_capacity: usize,
    out_tx: Sender<Event>,
) -> Sender<Sequenced> {
    let (in_tx, in_rx): (Sender<Sequenced>, Receiver<Sequenced>) = bounded(input_capacity);
    thread::Builder::new()
        .name(format!("match-{symbol}"))
        .spawn(move || {
            let mut engine = Engine::new(symbol, protection_bps);
            // 撮合线程:只从 input 读、向 output 写,永不做 IO
            for seq_cmd in in_rx.iter() {
                for ev in engine.handle(&seq_cmd) {
                    let _ = out_tx.send(ev); // 推入 Output Ring Buffer(满则背压)
                }
            }
        })
        .unwrap();
    in_tx
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // —— 配置:第一个命令行参数为路径,默认 config.toml ——
    let path = std::env::args().nth(1).unwrap_or_else(|| "config.toml".to_string());
    let cfg = Config::load(&path)?;
    tracing::info!(
        config = %path,
        symbols = cfg.symbols.len(),
        in_cap = cfg.engine.input_ring_capacity,
        out_cap = cfg.engine.output_ring_capacity,
        protection_bps = cfg.engine.market_protection_bps,
        "engine starting"
    );

    // Output Ring Buffer:所有分片共享一个输出队列
    let (out_tx, out_rx) = bounded::<Event>(cfg.engine.output_ring_capacity);

    // —— Publisher(IO 线程):Output Ring Buffer → External MQ ——
    {
        use mq::Outbound;
        let mut pub_mq = mq::redis_mq::RedisOutbound::new(&cfg.mq.url)?;
        let trades_topic = cfg.mq.trades_topic.clone();
        let book_topic = cfg.mq.book_events_topic.clone();
        thread::Builder::new()
            .name("publisher".into())
            .spawn(move || {
                for ev in out_rx.iter() {
                    let topic = match &ev {
                        Event::Trade { .. } => trades_topic.as_str(),
                        _ => book_topic.as_str(),
                    };
                    let payload = serde_json::to_vec(&ev).unwrap();
                    let _ = pub_mq.publish(topic, &payload);
                }
            })
            .unwrap();
    }

    // —— 按交易对分片:每个 symbol 一个独立引擎 + 独立线程 + 独立 Orderbook ——
    let mut routes: HashMap<String, Sender<Sequenced>> = HashMap::new();
    for sym in &cfg.symbols {
        routes.insert(
            sym.clone(),
            spawn_engine(
                sym.clone(),
                cfg.engine.market_protection_bps,
                cfg.engine.input_ring_capacity,
                out_tx.clone(),
            ),
        );
    }

    // —— Receiver(IO 线程,主线程兼任):External MQ → 定序 → 路由到对应 Input Ring Buffer ——
    use mq::Inbound;
    let mut in_mq = mq::redis_mq::RedisInbound::new(&cfg.mq.url, &cfg.mq.inbound_stream)?;
    let seq = AtomicU64::new(1);

    loop {
        for raw in in_mq.poll()? {
            let cmd: Command = match serde_json::from_slice(&raw) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("bad cmd: {e}");
                    continue;
                }
            };
            // 定序点:固化 seq + ts,保证 Primary/Standby 重放完全一致
            let sequenced = Sequenced {
                seq: seq.fetch_add(1, Ordering::Relaxed),
                ts: now_nanos(),
                cmd: cmd.clone(),
            };
            let symbol = match &cmd {
                Command::NewOrder(o) => o.symbol.clone(),
                // 撤单需带 symbol 才能路由(见下方说明),暂仍跳过
                Command::Cancel { .. } => continue,
            };
            if let Some(tx) = routes.get(&symbol) {
                let _ = tx.send(sequenced); // 推入对应分片的 Input Ring Buffer
            } else {
                tracing::warn!(%symbol, "unknown symbol, drop command");
            }
        }
    }
}

fn now_nanos() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}