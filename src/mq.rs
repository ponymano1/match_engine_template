pub trait Inbound: Send {
    /// 阻塞拉取一批原始消息（保持顺序）
    fn poll(&mut self) -> anyhow::Result<Vec<Vec<u8>>>;
}
pub trait Outbound: Send {
    fn publish(&mut self, topic: &str, payload: &[u8]) -> anyhow::Result<()>;
}

#[cfg(feature = "redis-mq")]
pub mod redis_mq {
    use super::*;
    use redis::{Commands, Connection};

    pub struct RedisInbound { conn: Connection, stream: String, last_id: String }
    impl RedisInbound {
        pub fn new(url: &str, stream: &str) -> anyhow::Result<Self> {
            let conn = redis::Client::open(url)?.get_connection()?;
            Ok(Self { conn, stream: stream.into(), last_id: "0-0".into() })
        }
    }
    impl Inbound for RedisInbound {
        fn poll(&mut self) -> anyhow::Result<Vec<Vec<u8>>> {
            // XREAD BLOCK 1000 COUNT 256 STREAMS <stream> <last_id>
            let reply: redis::streams::StreamReadReply = self.conn.xread_options(
                &[&self.stream], &[&self.last_id],
                &redis::streams::StreamReadOptions::default().block(1000).count(256),
            )?;
            let mut out = Vec::new();
            for key in reply.keys {
                for entry in key.ids {
                    self.last_id = entry.id.clone();
                    if let Some(redis::Value::BulkString(b)) = entry.map.get("payload") {
                        out.push(b.clone());
                    }
                }
            }
            Ok(out)
        }
    }

    pub struct RedisOutbound { conn: Connection }
    impl RedisOutbound {
        pub fn new(url: &str) -> anyhow::Result<Self> {
            Ok(Self { conn: redis::Client::open(url)?.get_connection()? })
        }
    }
    impl Outbound for RedisOutbound {
        fn publish(&mut self, topic: &str, payload: &[u8]) -> anyhow::Result<()> {
            let _: String = self.conn.xadd(topic, "*", &[("payload", payload)])?;
            Ok(())
        }
    }
}