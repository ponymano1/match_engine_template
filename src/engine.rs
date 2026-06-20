//! 撮合引擎：接收已定序命令，驱动订单簿并产出事件流。

use crate::domain::*;
use crate::orderbook::OrderBook;

/// 单 symbol 撮合引擎实例
pub struct Engine {
    book: OrderBook,
    /// 市价保护：超过 best * (1 ± bps/10000) 停止匹配。500 = 5%
    market_protection_bps: u64,
}

impl Engine {
    pub fn new(symbol: impl Into<String>, market_protection_bps: u64) -> Self {
        Self { book: OrderBook::new(symbol), market_protection_bps }
    }

    /// 处理一条已定序命令，返回零个或多个事件（Accepted + 后续状态变更）
    pub fn handle(&mut self, s: &Sequenced) -> Vec<Event> {
        match &s.cmd {
            Command::NewOrder(o) => self.on_new_order(o, s.seq, s.ts),
            Command::Cancel { order_id } => {
                if self.book.cancel(*order_id) {
                    vec![Event::Canceled { order_id: *order_id }]
                } else {
                    vec![Event::Rejected { order_id: *order_id, reason: "order not found".into() }]
                }
            }
        }
    }

    /// 新单路由：按 OrderType 分支（PostOnly / FOK 预判 → 撮合 → 剩余挂簿或 Kill）
    fn on_new_order(&mut self, o: &NewOrder, seq: Sequence, ts: Timestamp) -> Vec<Event> {
        let mut ev = vec![Event::Accepted { order_id: o.order_id, seq }];

        // —— Post-Only：会立即成交则整笔拒绝 ——
        if o.order_type == OrderType::PostOnly {
            if self.book.would_cross(o.side, o.price) {
                ev.push(Event::Rejected { order_id: o.order_id, reason: "post-only would take".into() });
            } else {
                self.book.rest(o.order_id, o.user_id, o.side, o.price, o.quantity);
                ev.push(Event::Resting { order_id: o.order_id, side: o.side, price: o.price, remaining: o.quantity });
            }
            return ev;
        }

        // —— FOK：预判不能全量成交则整笔取消，簿不变 ——
        if o.order_type == OrderType::Fok
            && !self.book.can_fill_fully(o.side, o.price, o.quantity)
        {
            ev.push(Event::Killed { order_id: o.order_id, unfilled: o.quantity, reason: "FOK cannot fill".into() });
            return ev;
        }

        // —— 撮合循环 ——
        let is_market = o.order_type == OrderType::Market;
        // 注意：protection 必须在 match_order 的可变借用之前算好（此处只读 self.book）
        let protection = if is_market { self.market_protection(o.side) } else { None };

        let (remaining, fills) =
            self.book.match_order(o.side, o.price, o.quantity, is_market, protection);

        for f in fills {
            ev.push(Event::Trade {
                seq, ts,
                symbol: o.symbol.clone(),
                taker_order_id: o.order_id,
                maker_order_id: f.maker_order_id,
                taker_side: o.side,
                price: f.price,        // Maker 定价 → Taker 价格改善
                quantity: f.quantity,
            });
        }

        // —— 剩余处理 ——
        if remaining > 0 {
            match o.order_type {
                OrderType::Limit => {
                    self.book.rest(o.order_id, o.user_id, o.side, o.price, remaining);
                    ev.push(Event::Resting { order_id: o.order_id, side: o.side, price: o.price, remaining });
                }
                OrderType::Market | OrderType::Ioc | OrderType::Fok => {
                    ev.push(Event::Killed { order_id: o.order_id, unfilled: remaining, reason: "remainder canceled".into() });
                }
                OrderType::PostOnly => unreachable!(),
            }
        }
        ev
    }

    /// 市价保护价：以对手最优价为基准
    fn market_protection(&self, taker: Side) -> Option<Price> {
        let bps = self.market_protection_bps;
        match taker {
            Side::Buy => self.book.best_ask().map(|p| p + p * bps / 10_000),
            Side::Sell => self.book.best_bid().map(|p| p.saturating_sub(p * bps / 10_000)),
        }
    }
}