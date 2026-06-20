//! 订单簿：维护买卖盘口、挂单索引，以及撮合/撤单核心逻辑。
//!
//! 数据结构：
//! - `bids` / `asks`：按价格分档，同档内 FIFO（时间优先）
//! - `index`：order_id → (side, price)，O(1) 撤单定位

use crate::domain::*;
use std::collections::{BTreeMap, HashMap, VecDeque};

/// 簿中一笔挂单（Maker）的剩余状态
#[derive(Debug)]
pub struct RestingOrder {
    pub order_id: OrderId,
    pub user_id: u64,
    pub remaining: Quantity,
}

/// 某一价格档位：同价订单队列 + 该档总量缓存
#[derive(Debug, Default)]
pub struct PriceLevel {
    pub orders: VecDeque<RestingOrder>, // 同价位 FIFO（时间优先）
    pub total: Quantity,                // 档位总量，FOK 预判用
}

/// 单笔成交的原始结果（seq/ts/symbol 由 engine 补全）
#[derive(Debug)]
pub struct Fill {
    pub maker_order_id: OrderId,
    pub price: Price,
    pub quantity: Quantity,
}

/// 单交易对的中央限价订单簿（CLOB）
pub struct OrderBook {
    pub symbol: String,
    bids: BTreeMap<Price, PriceLevel>, // 买盘：max key = best bid
    asks: BTreeMap<Price, PriceLevel>, // 卖盘：min key = best ask
    index: HashMap<OrderId, (Side, Price)>, // order_id → 所在侧与价位，供撤单/撮合后清理
}

impl OrderBook {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            index: HashMap::new(),
        }
    }

    /// 最优买价（最高 bid）, 取最后一个元素
    pub fn best_bid(&self) -> Option<Price> { self.bids.keys().next_back().copied() }
    /// 最优卖价（最低 ask）,  取第一个元素
    pub fn best_ask(&self) -> Option<Price> { self.asks.keys().next().copied() }

    /// 限价是否可与某对手价成交
    #[inline]
    pub fn price_crosses(taker: Side, taker_price: Price, book_price: Price) -> bool {
        match taker {
            Side::Buy => taker_price >= book_price,  // 买价 >= 卖价
            Side::Sell => taker_price <= book_price, // 卖价 <= 买价
        }
    }

    /// Post-Only 用：该限价单是否会立即成交（成为 Taker）
    pub fn would_cross(&self, side: Side, price: Price) -> bool {
        let bp = match side {
            Side::Buy => self.best_ask(),
            Side::Sell => self.best_bid(),
        };
        match bp {
            Some(bp) => Self::price_crosses(side, price, bp),
            None => false,
        }
    }

    /// FOK 用：在可接受价格内能否全量成交（只读预判，不改状态）
    pub fn can_fill_fully(&self, side: Side, price: Price, qty: Quantity) -> bool {
        let mut acc: Quantity = 0;
        match side {
            // 买：从最低卖价往上累加
            Side::Buy => {
                for (&p, lvl) in self.asks.iter() {
                    if !Self::price_crosses(side, price, p) { break; }
                    acc = acc.saturating_add(lvl.total);
                    if acc >= qty { return true; }
                }
            }
            // 卖：从最高买价往下累加
            Side::Sell => {
                for (&p, lvl) in self.bids.iter().rev() {
                    if !Self::price_crosses(side, price, p) { break; }
                    acc = acc.saturating_add(lvl.total);
                    if acc >= qty { return true; }
                }
            }
        }
        acc >= qty
    }

    /// 把剩余挂入自己一侧（提供流动性）
    pub fn rest(&mut self, order_id: OrderId, user_id: u64, side: Side, price: Price, remaining: Quantity) {
        let book = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let level = book.entry(price).or_default();
        level.total += remaining;
        level.orders.push_back(RestingOrder { order_id, user_id, remaining });
        self.index.insert(order_id, (side, price));
    }

    /// 撤单：从索引定位后从对应档位移除，空档则删除价位键
    pub fn cancel(&mut self, order_id: OrderId) -> bool {
        let Some((side, price)) = self.index.remove(&order_id) else { return false };
        let book = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if let Some(level) = book.get_mut(&price) {
            if let Some(pos) = level.orders.iter().position(|o| o.order_id == order_id) {
                let removed = level.orders.remove(pos).unwrap();
                level.total -= removed.remaining;
                if level.orders.is_empty() { book.remove(&price); }
                return true;
            }
        }
        false
    }

    /// 撮合循环：按价格优先、同价时间优先消耗对手盘。
    ///
    /// - `limit_price`：限价单的上/下限；市价单时由 `is_market` 跳过价格检查
    /// - `protection`：市价保护价，超出则停止撮合（已成交部分仍有效）
    ///
    /// 返回 `(剩余未成交量, 成交列表)`；成交价恒为 Maker（被动方）挂单价。
    pub fn match_order(
        &mut self,
        side: Side,
        limit_price: Price,
        qty: Quantity,
        is_market: bool,
        protection: Option<Price>,
    ) -> (Quantity, Vec<Fill>) {
        // 取对手盘（买单吃 asks，卖单吃 bids）
        let opposite = match side {
            Side::Buy => &mut self.asks,
            Side::Sell => &mut self.bids,
        };

        let mut remaining = qty;
        let mut fills = Vec::new();

        while remaining > 0 {
            // 1. 对手最优档
            let best_price = match side {
                Side::Buy => opposite.keys().next().copied(),       // 最低卖价
                Side::Sell => opposite.keys().next_back().copied(), // 最高买价
            };
            let Some(bp) = best_price else { break };

            //2. 如果是市价单，则直接进入撮合
            // 如果是限价单，则需要判断最优对手价是否在价格区间
            // 如果是限价单，并且当前最优对手价不在限价范围内，就直接退出撮合
            if !is_market && !Self::price_crosses(side, limit_price, bp) { break; }

            // 3. 市价保护：超出保护价停止，已成交照常返回
            // 如果市价单没有设置保护价，如果卖盘很少，可能会造成吃到很高价格的卖单，造成巨大的滑点
            if let Some(pp) = protection {
                let exceeded = match side { Side::Buy => bp > pp, Side::Sell => bp < pp };
                if exceeded { break; }
            }

            // 4. 档位内 FIFO 逐笔成交，成交价 = Maker(被动方) 价格
            let level = opposite.get_mut(&bp).expect("best_price 来自 keys(),档位必然存在——不变量被破坏");
            while remaining > 0 {
                let Some(front) = level.orders.front_mut() else { break };
                let traded = remaining.min(front.remaining);
                front.remaining -= traded;
                level.total -= traded;
                remaining -= traded;

                fills.push(Fill { maker_order_id: front.order_id, price: bp, quantity: traded });

                if front.remaining == 0 { level.orders.pop_front(); }
            }
            if level.orders.is_empty() { opposite.remove(&bp); }
        }

        (remaining, fills)
    }
}