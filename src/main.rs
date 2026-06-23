mod config;
mod domain;
mod engine;
mod mq;
mod orderbook;

use config::Config;
use crossbeam_channel::{Receiver, Sender, bounded};
use domain::*;
use engine::Engine;
use mq::Outbound;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

/// 启动单个交易对的撮合线程,返回其 input ring buffer 入口
fn spawn_engine(
    symbol: String,
    protection_bps: u64,
    input_capacity: usize,
    out_tx: Sender<Events>, // 该 symbol 专属的 output 队列
) -> Sender<Sequenced> {
    let (in_tx, in_rx): (Sender<Sequenced>, Receiver<Sequenced>) = bounded(input_capacity);
    thread::Builder::new()
        .name(format!("match-{symbol}"))
        .spawn(move || {
            let mut engine = Engine::new(symbol, protection_bps);
            for seq_cmd in in_rx.iter() {
                // 一单产出的整批事件,只 send 一次
                let _ = out_tx.send(engine.handle(&seq_cmd));
            }
        })
        .unwrap();
    in_tx
}

/// 启动单个交易对专属的 Publisher 线程:独立 output 队列 + 独立 Redis 连接。
/// 返回该 symbol 的 output 入口(交给对应撮合线程持有)。
/// Redis 连接在 spawn 前建立,连不上则启动即失败(fail-fast)。
fn spawn_publisher(
    symbol: &str,
    url: &str,
    output_capacity: usize,
    trades_topic: String,
    book_topic: String,
) -> anyhow::Result<Sender<Events>> {
    let (out_tx, out_rx) = bounded::<Events>(output_capacity);
    let mut pub_mq = mq::redis_mq::RedisOutbound::new(url)?;
    thread::Builder::new()
        .name(format!("publisher-{symbol}"))
        .spawn(move || {
            for batch in out_rx.iter() {
                for ev in batch {
                    let topic = match &ev {
                        Event::Trade { .. } => trades_topic.as_str(),
                        _ => book_topic.as_str(),
                    };
                    let payload = serde_json::to_vec(&ev).unwrap();
                    let _ = pub_mq.publish(topic, &payload);
                }
            }
        })?;
    Ok(out_tx)
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // —— 配置:第一个命令行参数为路径,默认 config.toml ——
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());
    let cfg = Config::load(&path)?;
    tracing::info!(
        config = %path,
        symbols = cfg.symbols.len(),
        in_cap = cfg.engine.input_ring_capacity,
        out_cap = cfg.engine.output_ring_capacity,
        protection_bps = cfg.engine.market_protection_bps,
        "engine starting"
    );

    // —— 按交易对分片:每个 symbol 一个 独立引擎 + 独立 output 队列 + 独立 publisher ——
    let mut routes: HashMap<String, Sender<Sequenced>> = HashMap::new();
    for sym in &cfg.symbols {
        // 1. 先为该 symbol 起专属 publisher,拿到它专属的 output 入口
        let out_tx = spawn_publisher(
            sym,
            &cfg.mq.url,
            cfg.engine.output_ring_capacity,
            cfg.mq.trades_topic.clone(),
            cfg.mq.book_events_topic.clone(),
        )?;

        // 2. 再起撮合线程,把专属 output 入口交给它
        let in_tx = spawn_engine(
            sym.clone(),
            cfg.engine.market_protection_bps,
            cfg.engine.input_ring_capacity,
            out_tx,
        );
        routes.insert(sym.clone(), in_tx);
    }

    // —— Receiver(IO 线程,主线程兼任):External MQ → 定序 → 路由到对应 Input Ring Buffer ——
    use mq::Inbound;
    let mut in_mq = mq::redis_mq::RedisInbound::new(&cfg.mq.url, &cfg.mq.inbound_stream)?;
    let seq = AtomicU64::new(1);
    let mut shard_seqs: HashMap<String, u64> = HashMap::new(); // symbol → 下一个分片内序号

    loop {
        for raw in in_mq.poll()? {
            let cmd: Command = match serde_json::from_slice(&raw) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("bad cmd: {e}");
                    continue;
                }
            };

            let symbol = match &cmd {
                Command::NewOrder(o) => o.symbol.clone(),
                Command::Cancel { symbol, .. } => symbol.clone(),
            };

            // 未知 symbol 先判，避免给它白白消耗一个 shard_seq（保持有效分片序号连续）
            let Some(tx) = routes.get(&symbol) else {
                tracing::warn!(%symbol, "unknown symbol, drop command");
                continue;
            };

            // 定序点：全局 seq + 分片内 shard_seq + ts 一起固化
            let shard_seq = {
                let n = shard_seqs.entry(symbol).or_insert(1);
                let cur = *n;
                *n += 1;
                cur
            };
            let sequenced = Sequenced {
                seq: seq.fetch_add(1, Ordering::Relaxed),
                shard_seq,
                ts: now_nanos(),
                cmd,
            };
            let _ = tx.send(sequenced);
        }
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
