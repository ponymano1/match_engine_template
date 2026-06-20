//! 启动配置:TOML 文件加载 + 环境变量覆盖 + 启动时强校验。

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub mq: MqConfig,
    pub engine: EngineConfig,
    /// 分片交易对列表,如 ["BTC/USD", "ETH/USD"]
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqConfig {
    /// 连接串,如 "redis://127.0.0.1/"。密码建议用 MQ_URL 环境变量覆盖,不写进文件。
    pub url: String,
    /// 入站订单 stream
    pub inbound_stream: String,
    /// 成交事件 topic(发往清算)
    pub trades_topic: String,
    /// 盘口事件 topic(发往行情)
    pub book_events_topic: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    /// 每分片 Input Ring Buffer 容量
    pub input_ring_capacity: usize,
    /// 共享 Output Ring Buffer 容量
    pub output_ring_capacity: usize,
    /// 市价保护,基点(bps)。500 = 5%
    #[serde(default = "default_protection_bps")]
    pub market_protection_bps: u64,
}

fn default_protection_bps() -> u64 {
    500
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("无法读取配置文件: {path}"))?;
        let mut cfg: Config =
            toml::from_str(&raw).with_context(|| format!("配置文件解析失败: {path}"))?;

        // 敏感/部署相关项用环境变量覆盖(不进 git)
        if let Ok(url) = std::env::var("MQ_URL") {
            cfg.mq.url = url;
        }

        cfg.validate()?;
        Ok(cfg)
    }

    /// 宁可崩在启动,不带病运行。
    fn validate(&self) -> Result<()> {
        anyhow::ensure!(!self.symbols.is_empty(), "至少要配置一个交易对");
        anyhow::ensure!(
            self.engine.input_ring_capacity.is_power_of_two(),
            "input_ring_capacity 必须是 2 的幂,当前 = {}",
            self.engine.input_ring_capacity
        );
        anyhow::ensure!(
            self.engine.output_ring_capacity.is_power_of_two(),
            "output_ring_capacity 必须是 2 的幂,当前 = {}",
            self.engine.output_ring_capacity
        );
        let mut seen = std::collections::HashSet::new();
        for s in &self.symbols {
            anyhow::ensure!(seen.insert(s), "交易对重复定义: {s}");
        }
        Ok(())
    }
}