//! 撮合域模型：类型别名、命令、事件与订单语义。

use serde::{Deserialize, Serialize};

pub type OrderId = u64;
/// 定点价格：真实价格 * 10^price_scale（如 10020.50 → 1002050）
pub type Price = u64;
/// 定点数量：真实数量 * 10^qty_scale（如 0.5 BTC → 50_000_000）
pub type Quantity = u64;
pub type Timestamp = u64; // 纳秒，由定序点赋予
pub type Sequence = u64;

/// 买卖方向
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side { Buy, Sell }

/// 订单类型与成交约束
///
/// - `Limit`：限价单，未成交部分挂入簿
/// - `Market`：市价单，吃掉对手盘，剩余取消
/// - `Ioc`：立即成交或取消，能成交部分立即成交，剩余取消
/// - `Fok`：全部成交或全部取消，不能一次性全量成交则整笔拒绝
/// - `PostOnly`：只做 Maker，若会立即吃单则整笔拒绝
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType { Limit, Market, Ioc, Fok, PostOnly }

/// 撮合引擎的合法输入：已通过冻结 + Pre-trade 风控
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    NewOrder(NewOrder),
    Cancel { order_id: OrderId },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewOrder {
    pub order_id: OrderId,
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Price,          // Market 单忽略此字段
    pub quantity: Quantity,
    pub user_id: u64,
}

/// 经过定序点后的命令：seq + ts 是确定性的关键，必须随输入流固化下来
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sequenced {
    pub seq: Sequence,
    pub ts: Timestamp,
    pub cmd: Command,
}

/// 撮合输出事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    Accepted { order_id: OrderId, seq: Sequence },
    Rejected { order_id: OrderId, reason: String },
    /// 一笔成交：成交价永远是 Maker（被动方）价格
    Trade {
        seq: Sequence,
        ts: Timestamp,
        symbol: String,
        taker_order_id: OrderId,
        maker_order_id: OrderId,
        taker_side: Side,
        price: Price,
        quantity: Quantity,
    },
    /// 订单挂入簿（提供流动性）
    Resting { order_id: OrderId, side: Side, price: Price, remaining: Quantity },
    Canceled { order_id: OrderId },
    /// 未成交部分被取消（Market/IOC/FOK）
    Killed { order_id: OrderId, unfilled: Quantity, reason: String },
}

