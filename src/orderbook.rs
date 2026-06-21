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

/// 撮合结果：剩余未成交量、成交列表，以及因自成交保护被撤销的对手挂单
#[derive(Debug, Default)]
pub struct MatchOutcome {
    pub remaining: Quantity,
    pub fills: Vec<Fill>,
    pub stp_canceled: Vec<OrderId>, // STP 撤掉的 Maker，引擎据此发 Canceled
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

    pub fn can_fill_fully(
        &self,
        side: Side,
        price: Price,
        qty: Quantity,
        taker_user_id: u64,
    ) -> bool {
        match side {
            // 买单吃 asks：价格升序，只看 ask_price <= price
            Side::Buy => Self::can_fill_fully_inner(
                self.asks.range(..=price).map(|(_, lvl)| lvl),
                qty,
                taker_user_id,
            ),
            // 卖单吃 bids：价格降序，只看 bid_price >= price
            Side::Sell => Self::can_fill_fully_inner(
                self.bids.range(price..).rev().map(|(_, lvl)| lvl),
                qty,
                taker_user_id,
            ),
        }
    }

    /// 两段式：先用 level.total 做 O(档数) 上界否决，必要时再逐笔排除 taker 自身挂单。
    /// 要求迭代器可 Clone（BTreeMap 的 range/map 迭代器满足）。
    fn can_fill_fully_inner<'a, I>(levels: I, qty: Quantity, taker_user_id: u64) -> bool
    where
        I: Iterator<Item = &'a PriceLevel> + Clone,
    {
        // ---- 第一遍：O(档数) 廉价上界 ----
        // total 含自身挂单，是真实可用量的上界；连上界都不够就直接否决。
        // 绝大多数“流动性不足”的 FOK 会在这里返回，依然走快路径。
        let mut upper: Quantity = 0;
        for lvl in levels.clone() {
            upper = upper.saturating_add(lvl.total);
            if upper >= qty {
                break; // 上界已达标，停止累加，不必扫完整本簿
            }
        }
        if upper < qty {
            return false;
        }

        // ---- 第二遍：仅当上界通过才进入，逐笔排除 taker 自己的挂单 ----
        let mut avail: Quantity = 0;
        for lvl in levels {
            for order in &lvl.orders {
                if order.user_id == taker_user_id {
                    continue; // STP：自成交量不计入可用流动性
                }
                avail = avail.saturating_add(order.remaining);
                if avail >= qty {
                    return true; // 够了立刻返回，不必扫完
                }
            }
        }
        avail >= qty
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
    /// - `taker_user_id`：自成交保护用。撞到同一 user 的挂单时，**撤销该 Maker**
    ///   （Cancel Resting 策略），Taker 不与自己成交、继续吃后续挂单。
    ///
    /// 返回 `MatchOutcome`；成交价恒为 Maker（被动方）挂单价。
    pub fn match_order(
        &mut self,
        side: Side,
        limit_price: Price,
        qty: Quantity,
        is_market: bool,
        protection: Option<Price>,
        taker_user_id: u64,
    ) -> MatchOutcome {
        let opposite = match side {
            Side::Buy => &mut self.asks,
            Side::Sell => &mut self.bids,
        };

        let mut remaining = qty;
        let mut fills = Vec::new();
        let mut stp_canceled = Vec::new();

        while remaining > 0 {
            // 1. 对手最优档
            let best_price = match side {
                Side::Buy => opposite.keys().next().copied(),
                Side::Sell => opposite.keys().next_back().copied(),
            };
            let Some(bp) = best_price else { break };

            // 2. 限价单：最优对手价不在限价范围内则停止
            if !is_market && !Self::price_crosses(side, limit_price, bp) { break; }

            // 3. 市价保护：超出保护价停止
            if let Some(pp) = protection {
                let exceeded = match side { Side::Buy => bp > pp, Side::Sell => bp < pp };
                if exceeded { break; }
            }

            // 4. 档位内 FIFO 逐笔成交，成交价 = Maker 价
            let level = opposite.get_mut(&bp).expect("best_price 来自 keys(),档位必然存在——不变量被破坏");
            while remaining > 0 {
                let Some(front) = level.orders.front() else { break };

                // —— 自成交保护：撞到自己的挂单，整笔撤销，不与自己成交 ——
                if front.user_id == taker_user_id {
                    let maker = level.orders.pop_front().unwrap();
                    level.total -= maker.remaining;
                    stp_canceled.push(maker.order_id);
                    continue;
                }

                let front = level.orders.front_mut().unwrap();
                let traded = remaining.min(front.remaining);
                front.remaining -= traded;
                level.total -= traded;
                remaining -= traded;

                fills.push(Fill { maker_order_id: front.order_id, price: bp, quantity: traded });

                if front.remaining == 0 { level.orders.pop_front(); }
            }
            if level.orders.is_empty() { opposite.remove(&bp); }
        }

        // opposite 的可变借用到此结束，可安全清理被 STP 撤销的挂单索引
        for id in &stp_canceled {
            self.index.remove(id);
        }

        MatchOutcome { remaining, fills, stp_canceled }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    // 约定：挂单方用 user=1，taker 默认用 TAKER（不同用户，正常成交）
    const TAKER: u64 = 2;

    /// 遍历真实队列求和，与缓存 total 对比
    fn level_total_matches(book: &OrderBook, side: Side, price: Price) -> bool {
        let map = match side {
            Side::Buy => &book.bids,
            Side::Sell => &book.asks,
        };
        match map.get(&price) {
            Some(lvl) => lvl.orders.iter().map(|o| o.remaining).sum::<Quantity>() == lvl.total,
            None => true,
        }
    }

    #[test]
    fn empty_book_has_no_best() {
        let b = OrderBook::new("X");
        assert_eq!(b.best_bid(), None);
        assert_eq!(b.best_ask(), None);
    }

    #[test]
    fn rest_updates_best_price() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Buy, 100, 10);
        b.rest(2, 1, Side::Buy, 101, 5);
        assert_eq!(b.best_bid(), Some(101));
        b.rest(3, 1, Side::Sell, 200, 7);
        b.rest(4, 1, Side::Sell, 199, 7);
        assert_eq!(b.best_ask(), Some(199));
    }

    #[test]
    fn limit_buy_simple_fill() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Sell, 100, 10);
        let out = b.match_order(Side::Buy, 100, 6, false, None, TAKER);
        assert_eq!(out.remaining, 0);
        assert_eq!(out.fills.len(), 1);
        assert_eq!(out.fills[0].maker_order_id, 1);
        assert_eq!(out.fills[0].price, 100);
        assert_eq!(out.fills[0].quantity, 6);
        assert_eq!(b.best_ask(), Some(100));
    }

    #[test]
    fn fifo_within_same_level() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Sell, 100, 5);
        b.rest(2, 1, Side::Sell, 100, 5);
        let out = b.match_order(Side::Buy, 100, 7, false, None, TAKER);
        assert_eq!(out.remaining, 0);
        assert_eq!(out.fills.len(), 2);
        assert_eq!(out.fills[0].maker_order_id, 1);
        assert_eq!(out.fills[0].quantity, 5);
        assert_eq!(out.fills[1].maker_order_id, 2);
        assert_eq!(out.fills[1].quantity, 2);
    }

    #[test]
    fn price_priority_across_levels() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Sell, 101, 5);
        b.rest(2, 1, Side::Sell, 100, 5);
        let out = b.match_order(Side::Buy, 101, 8, false, None, TAKER);
        assert_eq!(out.remaining, 0);
        assert_eq!(out.fills[0].price, 100);
        assert_eq!(out.fills[0].quantity, 5);
        assert_eq!(out.fills[1].price, 101);
        assert_eq!(out.fills[1].quantity, 3);
    }

    #[test]
    fn limit_does_not_cross() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Sell, 100, 5);
        let out = b.match_order(Side::Buy, 99, 5, false, None, TAKER);
        assert_eq!(out.remaining, 5);
        assert!(out.fills.is_empty());
    }

    #[test]
    fn cancel_removes_and_clears_level() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Buy, 100, 5);
        assert!(b.cancel(1));
        assert_eq!(b.best_bid(), None);
        assert!(!b.cancel(1));
    }

    #[test]
    fn would_cross_detects_taker() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Sell, 100, 5);
        assert!(b.would_cross(Side::Buy, 100));
        assert!(b.would_cross(Side::Buy, 101));
        assert!(!b.would_cross(Side::Buy, 99));
    }

    #[test]
    fn can_fill_fully_checks_depth() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Sell, 100, 5);
        b.rest(2, 1, Side::Sell, 101, 5);
        assert!(b.can_fill_fully(Side::Buy, 101, 10, TAKER));
        assert!(!b.can_fill_fully(Side::Buy, 101, 11, TAKER));
        assert!(!b.can_fill_fully(Side::Buy, 100, 10, TAKER));
    }

    #[test]
    fn market_protection_stops_matching() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Sell, 100, 5);
        b.rest(2, 1, Side::Sell, 110, 5);
        let out = b.match_order(Side::Buy, 0, 10, true, Some(105), TAKER);
        assert_eq!(out.fills.len(), 1);
        assert_eq!(out.fills[0].price, 100);
        assert_eq!(out.remaining, 5);
    }

    // ===== total 缓存一致性 =====

    #[test]
    fn total_stays_consistent_after_partial_fill() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Sell, 100, 10);
        b.rest(2, 1, Side::Sell, 100, 10);
        b.match_order(Side::Buy, 100, 7, false, None, TAKER);
        assert!(level_total_matches(&b, Side::Sell, 100));
    }

    #[test]
    fn total_consistent_after_cancel_middle_order() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Buy, 100, 5);
        b.rest(2, 1, Side::Buy, 100, 7);
        b.rest(3, 1, Side::Buy, 100, 3);
        assert!(b.cancel(2));
        assert!(level_total_matches(&b, Side::Buy, 100));
        let out = b.match_order(Side::Sell, 100, 8, false, None, TAKER);
        assert_eq!(out.remaining, 0);
        assert_eq!(out.fills[0].maker_order_id, 1);
        assert_eq!(out.fills[1].maker_order_id, 3);
    }

    // ===== 同一 Maker 被分多次吃穿 =====

    #[test]
    fn maker_consumed_across_multiple_takers() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Sell, 100, 10);
        let out1 = b.match_order(Side::Buy, 100, 3, false, None, TAKER);
        assert_eq!(out1.remaining, 0);
        assert_eq!(out1.fills[0].quantity, 3);
        let out2 = b.match_order(Side::Buy, 100, 3, false, None, TAKER);
        assert_eq!(out2.remaining, 0);
        assert_eq!(out2.fills[0].quantity, 3);
        let out3 = b.match_order(Side::Buy, 100, 10, false, None, TAKER);
        assert_eq!(out3.remaining, 6);
        assert_eq!(out3.fills[0].quantity, 4);
        assert_eq!(b.best_ask(), None);
    }

    // ===== 退化输入 =====

    #[test]
    fn zero_qty_match_returns_immediately() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Sell, 100, 10);
        let out = b.match_order(Side::Buy, 100, 0, false, None, TAKER);
        assert_eq!(out.remaining, 0);
        assert!(out.fills.is_empty());
        assert_eq!(b.best_ask(), Some(100));
    }

    #[test]
    fn match_on_empty_book() {
        let mut b = OrderBook::new("X");
        let out = b.match_order(Side::Buy, 100, 5, false, None, TAKER);
        assert_eq!(out.remaining, 5);
        assert!(out.fills.is_empty());
    }

    #[test]
    fn market_match_on_empty_book_no_panic() {
        let mut b = OrderBook::new("X");
        let out = b.match_order(Side::Buy, 0, 5, true, None, TAKER);
        assert_eq!(out.remaining, 5);
        assert!(out.fills.is_empty());
    }

    // ===== 自成交保护（Cancel Resting）=====

    #[test]
    fn self_trade_cancels_resting_maker_no_fill() {
        let mut b = OrderBook::new("X");
        b.rest(1, 7, Side::Sell, 100, 5);                     // user 7 挂卖
        let out = b.match_order(Side::Buy, 100, 5, false, None, 7); // user 7 来吃
        assert!(out.fills.is_empty());            // 不与自己成交
        assert_eq!(out.stp_canceled, vec![1]);    // 自己的挂单被撤
        assert_eq!(out.remaining, 5);             // taker 一点没成交
        assert_eq!(b.best_ask(), None);           // 挂单已移除
        assert!(!b.cancel(1));                    // 索引也已清理
    }

    #[test]
    fn self_trade_skips_own_then_fills_others() {
        let mut b = OrderBook::new("X");
        b.rest(1, 7, Side::Sell, 100, 5); // 自己的，应被撤
        b.rest(2, 9, Side::Sell, 100, 5); // 别人的，应成交
        let out = b.match_order(Side::Buy, 100, 5, false, None, 7);
        assert_eq!(out.stp_canceled, vec![1]);
        assert_eq!(out.fills.len(), 1);
        assert_eq!(out.fills[0].maker_order_id, 2);
        assert_eq!(out.fills[0].quantity, 5);
        assert_eq!(out.remaining, 0);
        assert!(level_total_matches(&b, Side::Sell, 100)); // 档已空，视为一致
        assert!(!b.cancel(1)); // 被 STP 撤的单索引已清
    }

    #[test]
    fn self_trade_excluded_from_fok_precheck() {
        let mut b = OrderBook::new("X");
        b.rest(1, 7, Side::Sell, 100, 10); // 全是自己的流动性
        // 对 user 7 而言，可成交深度为 0
        assert!(!b.can_fill_fully(Side::Buy, 100, 5, 7));
        // 对别人则正常
        assert!(b.can_fill_fully(Side::Buy, 100, 5, 99));
    }

    // ===== 饱和加法 =====

    #[test]
    fn can_fill_fully_saturates_without_overflow() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Sell, 100, u64::MAX);
        b.rest(2, 1, Side::Sell, 101, u64::MAX);
        assert!(b.can_fill_fully(Side::Buy, 101, u64::MAX, TAKER));
    }

    // ===== 索引清理 =====

    #[test]
    fn index_cleared_after_full_fill() {
        let mut b = OrderBook::new("X");
        b.rest(1, 1, Side::Sell, 100, 5);
        b.match_order(Side::Buy, 100, 5, false, None, TAKER);
        assert!(!b.cancel(1));
    }
}

