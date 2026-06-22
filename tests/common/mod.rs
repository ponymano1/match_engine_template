//! 黑盒集成测试脚手架:用真实编译出的引擎二进制对接活的 Redis,
//! 全程只通过 Redis Stream 投命令 / 收事件。
//!
//! 线协议严格对齐 domain.rs:
//!   - Command / Event 用 serde 默认外部标签:`{"NewOrder": {...}}` / `{"Trade": {...}}`
//!   - Side -> "Buy"/"Sell",OrderType -> "Limit"/"Market"/"Ioc"/"Fok"/"PostOnly"
//!   - 入站 stream 的 JSON 放在字段 "payload";Trade 发 trades_topic,其余发 book_events_topic
//!   - 二进制名 = 包名 match_engine_template
#![allow(dead_code)]

use std::process::{Child, Command as Proc, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::Commands;
use serde::{Deserialize, Serialize};

const ENGINE_BIN: &str = env!("CARGO_BIN_EXE_match_engine_template");

pub const SYMBOL: &str = "BTC/USD";

// ── 线类型(与 domain.rs 一一对应)──────────────────────────────────────────

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrderType {
    Limit,
    Market,
    Ioc,
    Fok,
    PostOnly,
}

#[derive(Debug, Clone, Serialize)]
pub struct NewOrder {
    pub order_id: u64,
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: u64,
    pub quantity: u64,
    pub user_id: u64,
}

/// 外部标签:`{"NewOrder": {...}}` / `{"Cancel": {...}}`,与 domain.rs 完全一致
#[derive(Debug, Clone, Serialize)]
pub enum Command {
    NewOrder(NewOrder),
    Cancel { order_id: u64, symbol: String },
}

/// 外部标签反序列化。字段须与 domain.rs::Event 完全一致。
#[derive(Debug, Clone, Deserialize)]
pub enum Event {
    Accepted {
        order_id: u64,
        seq: u64,
    },
    Rejected {
        order_id: u64,
        reason: String,
    },
    Trade {
        seq: u64,
        ts: u64,
        symbol: String,
        taker_order_id: u64,
        maker_order_id: u64,
        taker_side: Side,
        price: u64,
        quantity: u64,
    },
    Resting {
        order_id: u64,
        side: Side,
        price: u64,
        remaining: u64,
    },
    Canceled {
        order_id: u64,
    },
    Killed {
        order_id: u64,
        unfilled: u64,
        reason: String,
    },
}

// ── 便捷构造 ──────────────────────────────────────────────────────────────

impl NewOrder {
    pub fn new(
        order_id: u64,
        user_id: u64,
        side: Side,
        order_type: OrderType,
        price: u64,
        quantity: u64,
    ) -> Self {
        NewOrder {
            order_id,
            symbol: SYMBOL.to_string(),
            side,
            order_type,
            price,
            quantity,
            user_id,
        }
    }
    pub fn limit(order_id: u64, user: u64, side: Side, price: u64, qty: u64) -> Self {
        Self::new(order_id, user, side, OrderType::Limit, price, qty)
    }
}

// ── Harness ──────────────────────────────────────────────────────────────────

pub struct Harness {
    con: redis::Connection,
    engine: Child,
    config_path: std::path::PathBuf,
    orders: String,
    trades: String,
    book: String,
    last_trades: String,
    last_book: String,
}

fn unique() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static C: AtomicU64 = AtomicU64::new(0);
    let n = C.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{}_{}_{}", std::process::id(), nanos, n)
}

impl Harness {
    pub fn start() -> Self {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
        let client = redis::Client::open(url.clone()).expect("invalid REDIS_URL");
        let mut con = client
            .get_connection()
            .expect("连不上 Redis —— 先跑 scripts/redis-up.sh");
        redis::cmd("PING").query::<String>(&mut con).expect("PING failed");

        let id = unique();
        let orders = format!("it_orders_{id}");
        let trades = format!("it_trades_{id}");
        let book = format!("it_book_{id}");

        // 注意:ring 容量必须是 2 的幂,否则 config.validate 会拒绝启动
        let config = format!(
            r#"symbols = ["{SYMBOL}"]

[mq]
url               = "{url}"
inbound_stream    = "{orders}"
trades_topic      = "{trades}"
book_events_topic = "{book}"

[engine]
input_ring_capacity   = 1024
output_ring_capacity  = 4096
market_protection_bps = 500
"#
        );

        let config_path = std::env::temp_dir().join(format!("me_it_{id}.toml"));
        std::fs::write(&config_path, config).expect("写临时 config 失败");

        let mut engine = Proc::new(ENGINE_BIN)
            .arg(&config_path)
            .env("MQ_URL", &url)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("启动引擎二进制失败");

        // 给引擎一点连接 + spawn 线程的时间;因为入站从 "0-0" 读,
        // 即使早投递也不会丢,这里主要是确认进程没立刻挂掉。
        std::thread::sleep(Duration::from_millis(500));
        if let Ok(Some(status)) = engine.try_wait() {
            panic!("引擎启动后立即退出: {status:?}(检查 config / Redis)");
        }

        Harness {
            con,
            engine,
            config_path,
            orders,
            trades,
            book,
            last_trades: "0".to_string(),
            last_book: "0".to_string(),
        }
    }

    pub fn send(&mut self, cmd: Command) {
        let payload = serde_json::to_string(&cmd).unwrap();
        let _: String = self
            .con
            .xadd(&self.orders, "*", &[("payload", payload)])
            .expect("XADD failed");
    }

    pub fn new_order(&mut self, o: NewOrder) {
        self.send(Command::NewOrder(o));
    }

    pub fn cancel(&mut self, order_id: u64) {
        self.send(Command::Cancel {
            order_id,
            symbol: SYMBOL.to_string(),
        });
    }

    /// 收集事件直到至少 min 个或超时。
    pub fn collect_n(&mut self, min: usize, timeout: Duration) -> Vec<Event> {
        let deadline = Instant::now() + timeout;
        let mut out = Vec::new();
        while out.len() < min {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            self.read_into(&mut out, remaining.min(Duration::from_millis(250)));
        }
        out
    }

    /// 在 window 内排空当前可读事件(用于断言"不应再有更多事件")。
    pub fn drain(&mut self, window: Duration) -> Vec<Event> {
        let mut out = Vec::new();
        self.read_into(&mut out, window);
        out
    }

    fn read_into(&mut self, out: &mut Vec<Event>, block: Duration) {
        let opts = StreamReadOptions::default()
            .block(block.as_millis() as usize)
            .count(500);

        let reply: Option<StreamReadReply> = self
            .con
            .xread_options(
                &[&self.book, &self.trades],
                &[&self.last_book, &self.last_trades],
                &opts,
            )
            .expect("XREAD failed");

        let Some(reply) = reply else { return };

        for key in reply.keys {
            for entry in key.ids {
                if let Some(v) = entry.map.get("payload") {
                    let s: String = redis::from_redis_value(v.clone()).expect("payload 不是字符串");
                    match serde_json::from_str::<Event>(&s) {
                        Ok(ev) => out.push(ev),
                        Err(e) => panic!("无法解码事件 {s:?}: {e}"),
                    }
                }
                if key.key == self.book {
                    self.last_book = entry.id.clone();
                } else {
                    self.last_trades = entry.id.clone();
                }
            }
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.engine.kill();
        let _ = self.engine.wait();
        let _: Result<(), _> = self.con.del(&[&self.orders, &self.trades, &self.book]);
        let _ = std::fs::remove_file(&self.config_path);
    }
}

// ── 断言辅助 ──────────────────────────────────────────────────────────────────

pub fn trades(evs: &[Event]) -> Vec<&Event> {
    evs.iter().filter(|e| matches!(e, Event::Trade { .. })).collect()
}
pub fn total_traded(evs: &[Event]) -> u64 {
    evs.iter()
        .filter_map(|e| match e {
            Event::Trade { quantity, .. } => Some(*quantity),
            _ => None,
        })
        .sum()
}
pub fn has_accepted(evs: &[Event], id: u64) -> bool {
    evs.iter().any(|e| matches!(e, Event::Accepted { order_id, .. } if *order_id == id))
}
pub fn has_rejected(evs: &[Event], id: u64) -> bool {
    evs.iter().any(|e| matches!(e, Event::Rejected { order_id, .. } if *order_id == id))
}
pub fn has_resting(evs: &[Event], id: u64) -> bool {
    evs.iter().any(|e| matches!(e, Event::Resting { order_id, .. } if *order_id == id))
}
pub fn has_canceled(evs: &[Event], id: u64) -> bool {
    evs.iter().any(|e| matches!(e, Event::Canceled { order_id } if *order_id == id))
}
pub fn has_killed(evs: &[Event], id: u64) -> bool {
    evs.iter().any(|e| matches!(e, Event::Killed { order_id, .. } if *order_id == id))
}