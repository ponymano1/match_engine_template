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

/// 新单路由：qty 校验 → PostOnly / FOK 预判 → 撮合 → 剩余挂簿或 Kill
fn on_new_order(&mut self, o: &NewOrder, seq: Sequence, ts: Timestamp) -> Vec<Event> {
    let mut ev = vec![Event::Accepted { order_id: o.order_id, seq }];

    // —— 数量校验：0 量单直接拒绝（防御性，正常应被上游风控拦截）——
    if o.quantity == 0 {
        ev.push(Event::Rejected { order_id: o.order_id, reason: "quantity must be positive".into() });
        return ev;
    }

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

    // —— FOK：预判（排除自身挂单后）不能全量成交则整笔取消，簿不变 ——
    if o.order_type == OrderType::Fok
        && !self.book.can_fill_fully(o.side, o.price, o.quantity, o.user_id)
    {
        ev.push(Event::Killed { order_id: o.order_id, unfilled: o.quantity, reason: "FOK cannot fill".into() });
        return ev;
    }

    // —— 撮合循环 ——
    let is_market = o.order_type == OrderType::Market;
    let protection = if is_market { self.market_protection(o.side) } else { None };

    let outcome =
        self.book.match_order(o.side, o.price, o.quantity, is_market, protection, o.user_id);

    // 自成交保护撤销的对手挂单（统一排在成交事件之前）
    for mid in &outcome.stp_canceled {
        ev.push(Event::Canceled { order_id: *mid });
    }

    for f in outcome.fills {
        ev.push(Event::Trade {
            seq, ts,
            symbol: o.symbol.clone(),
            taker_order_id: o.order_id,
            maker_order_id: f.maker_order_id,
            taker_side: o.side,
            price: f.price,
            quantity: f.quantity,
        });
    }

    // —— 剩余处理 ——
    let remaining = outcome.remaining;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(cmd: Command, n: u64) -> Sequenced {
        Sequenced { seq: n, ts: n, cmd }
    }

    fn order_u(id: OrderId, user: u64, side: Side, ot: OrderType, price: Price, qty: Quantity) -> Command {
        Command::NewOrder(NewOrder {
            order_id: id,
            symbol: "X".into(),
            side,
            order_type: ot,
            price,
            quantity: qty,
            user_id: user,
        })
    }
    // 默认挂单方 user=1；需要成交的 taker 用 user=2，避免误触自成交保护
    fn order(id: OrderId, side: Side, ot: OrderType, price: Price, qty: Quantity) -> Command {
        order_u(id, 1, side, ot, price, qty)
    }

    fn traded_qty(events: &[Event]) -> Quantity {
        events.iter().filter_map(|e| match e {
            Event::Trade { quantity, .. } => Some(*quantity),
            _ => None,
        }).sum()
    }

    #[test]
    fn limit_rests_when_no_match() {
        let mut e = Engine::new("X", 500);
        let ev = e.handle(&seq(order(1, Side::Buy, OrderType::Limit, 100, 10), 1));
        assert!(matches!(ev[0], Event::Accepted { order_id: 1, seq: 1 }));
        assert!(ev.iter().any(|e| matches!(e, Event::Resting { order_id: 1, .. })));
    }

    #[test]
    fn limit_trades_at_maker_price() {
        let mut e = Engine::new("X", 500);
        e.handle(&seq(order(1, Side::Sell, OrderType::Limit, 100, 10), 1));
        let ev = e.handle(&seq(order_u(2, 2, Side::Buy, OrderType::Limit, 105, 4), 2));
        let trade = ev.iter().find(|e| matches!(e, Event::Trade { .. })).unwrap();
        if let Event::Trade { price, quantity, maker_order_id, taker_order_id, .. } = trade {
            assert_eq!(*price, 100);
            assert_eq!(*quantity, 4);
            assert_eq!(*maker_order_id, 1);
            assert_eq!(*taker_order_id, 2);
        }
    }

    #[test]
    fn post_only_rejected_when_crosses() {
        let mut e = Engine::new("X", 500);
        e.handle(&seq(order(1, Side::Sell, OrderType::Limit, 100, 10), 1));
        let ev = e.handle(&seq(order(2, Side::Buy, OrderType::PostOnly, 100, 5), 2));
        assert!(ev.iter().any(|e| matches!(e, Event::Rejected { .. })));
        assert!(!ev.iter().any(|e| matches!(e, Event::Resting { .. })));
    }

    #[test]
    fn post_only_rests_when_no_cross() {
        let mut e = Engine::new("X", 500);
        e.handle(&seq(order(1, Side::Sell, OrderType::Limit, 100, 10), 1));
        let ev = e.handle(&seq(order(2, Side::Buy, OrderType::PostOnly, 99, 5), 2));
        assert!(ev.iter().any(|e| matches!(e, Event::Resting { .. })));
    }

    #[test]
    fn fok_killed_when_insufficient_liquidity() {
        let mut e = Engine::new("X", 500);
        e.handle(&seq(order(1, Side::Sell, OrderType::Limit, 100, 3), 1));
        let ev = e.handle(&seq(order_u(2, 2, Side::Buy, OrderType::Fok, 100, 5), 2));
        assert!(ev.iter().any(|e| matches!(e, Event::Killed { .. })));
        assert_eq!(traded_qty(&ev), 0);
    }

    #[test]
    fn fok_fills_when_enough() {
        let mut e = Engine::new("X", 500);
        e.handle(&seq(order(1, Side::Sell, OrderType::Limit, 100, 5), 1));
        let ev = e.handle(&seq(order_u(2, 2, Side::Buy, OrderType::Fok, 100, 5), 2));
        assert_eq!(traded_qty(&ev), 5);
        assert!(!ev.iter().any(|e| matches!(e, Event::Killed { .. })));
    }

    #[test]
    fn ioc_partial_fill_kills_remainder() {
        let mut e = Engine::new("X", 500);
        e.handle(&seq(order(1, Side::Sell, OrderType::Limit, 100, 3), 1));
        let ev = e.handle(&seq(order_u(2, 2, Side::Buy, OrderType::Ioc, 100, 5), 2));
        assert_eq!(traded_qty(&ev), 3);
        assert!(ev.iter().any(|e| matches!(e, Event::Killed { unfilled: 2, .. })));
    }

    #[test]
    fn market_order_respects_protection() {
        let mut e = Engine::new("X", 500);
        e.handle(&seq(order(1, Side::Sell, OrderType::Limit, 100, 5), 1));
        e.handle(&seq(order(2, Side::Sell, OrderType::Limit, 110, 5), 2));
        let ev = e.handle(&seq(order_u(3, 2, Side::Buy, OrderType::Market, 0, 10), 3));
        assert_eq!(traded_qty(&ev), 5);
        assert!(ev.iter().any(|e| matches!(e, Event::Killed { unfilled: 5, .. })));
    }

    #[test]
    fn cancel_existing_order() {
        let mut e = Engine::new("X", 500);
        e.handle(&seq(order(1, Side::Buy, OrderType::Limit, 100, 5), 1));
        let ev = e.handle(&seq(Command::Cancel { order_id: 1 }, 2));
        assert!(matches!(ev[0], Event::Canceled { order_id: 1 }));
    }

    #[test]
    fn cancel_missing_order_rejected() {
        let mut e = Engine::new("X", 500);
        let ev = e.handle(&seq(Command::Cancel { order_id: 99 }, 1));
        assert!(matches!(ev[0], Event::Rejected { .. }));
    }

    // ===== qty = 0：拒绝 =====

    #[test]
    fn zero_qty_order_rejected() {
        let mut e = Engine::new("X", 500);
        let ev = e.handle(&seq(order(1, Side::Buy, OrderType::Limit, 100, 0), 1));
        assert!(matches!(ev[0], Event::Accepted { order_id: 1, .. }));
        assert!(matches!(ev[1], Event::Rejected { order_id: 1, .. }));
        // 不得挂簿、不得成交
        assert!(!ev.iter().any(|e| matches!(e, Event::Resting { .. })));
        assert_eq!(traded_qty(&ev), 0);
    }

    // ===== 自成交保护 =====

    #[test]
    fn self_trade_prevention_cancels_resting_maker() {
        let mut e = Engine::new("X", 500);
        e.handle(&seq(order_u(1, 7, Side::Sell, OrderType::Limit, 100, 5), 1));
        let ev = e.handle(&seq(order_u(2, 7, Side::Buy, OrderType::Limit, 100, 5), 2));
        assert!(!ev.iter().any(|e| matches!(e, Event::Trade { .. })));        // 无成交
        assert!(ev.iter().any(|e| matches!(e, Event::Canceled { order_id: 1 }))); // 自己挂单被撤
        // 限价 taker 剩余挂入买盘
        assert!(ev.iter().any(|e| matches!(e, Event::Resting { order_id: 2, remaining: 5, .. })));
    }

    #[test]
    fn self_trade_ioc_taker_killed_after_cancel() {
        let mut e = Engine::new("X", 500);
        e.handle(&seq(order_u(1, 7, Side::Sell, OrderType::Limit, 100, 5), 1));
        let ev = e.handle(&seq(order_u(2, 7, Side::Buy, OrderType::Ioc, 100, 5), 2));
        assert!(ev.iter().any(|e| matches!(e, Event::Canceled { order_id: 1 })));
        assert_eq!(traded_qty(&ev), 0);
        assert!(ev.iter().any(|e| matches!(e, Event::Killed { unfilled: 5, .. })));
    }

    #[test]
    fn self_trade_skips_own_fills_other() {
        let mut e = Engine::new("X", 500);
        e.handle(&seq(order_u(1, 7, Side::Sell, OrderType::Limit, 100, 5), 1)); // 自己的
        e.handle(&seq(order_u(2, 9, Side::Sell, OrderType::Limit, 100, 5), 2)); // 别人的
        let ev = e.handle(&seq(order_u(3, 7, Side::Buy, OrderType::Limit, 100, 5), 3));
        assert!(ev.iter().any(|e| matches!(e, Event::Canceled { order_id: 1 }))); // 撤自己
        assert_eq!(traded_qty(&ev), 5);                                           // 成交别人
        let trade = ev.iter().find(|e| matches!(e, Event::Trade { .. })).unwrap();
        if let Event::Trade { maker_order_id, .. } = trade {
            assert_eq!(*maker_order_id, 2);
        }
    }

    // ===== 其它边界 =====

    #[test]
    fn market_order_on_empty_book_killed() {
        let mut e = Engine::new("X", 500);
        let ev = e.handle(&seq(order(1, Side::Buy, OrderType::Market, 0, 5), 1));
        assert!(ev.iter().any(|e| matches!(e, Event::Killed { unfilled: 5, .. })));
    }

    #[test]
    fn fok_does_not_mutate_book_on_kill() {
        let mut e = Engine::new("X", 500);
        e.handle(&seq(order(1, Side::Sell, OrderType::Limit, 100, 3), 1));
        e.handle(&seq(order_u(2, 2, Side::Buy, OrderType::Fok, 100, 5), 2)); // 流动性不足 → Kill
        let ev = e.handle(&seq(order_u(3, 2, Side::Buy, OrderType::Ioc, 100, 3), 3));
        assert_eq!(traded_qty(&ev), 3); // 簿原封不动
    }

    #[test]
    fn trade_events_carry_taker_seq_and_ts() {
        let mut e = Engine::new("X", 500);
        e.handle(&seq(order(1, Side::Sell, OrderType::Limit, 100, 5), 1));
        e.handle(&seq(order(2, Side::Sell, OrderType::Limit, 101, 5), 2));
        let ev = e.handle(&seq(order_u(3, 2, Side::Buy, OrderType::Limit, 101, 8), 99));
        for t in ev.iter().filter(|e| matches!(e, Event::Trade { .. })) {
            if let Event::Trade { seq, ts, .. } = t {
                assert_eq!(*seq, 99);
                assert_eq!(*ts, 99);
            }
        }
    }

    #[test]
    fn limit_partial_fill_then_rest_remainder() {
        let mut e = Engine::new("X", 500);
        e.handle(&seq(order(1, Side::Sell, OrderType::Limit, 100, 3), 1));
        let ev = e.handle(&seq(order_u(2, 2, Side::Buy, OrderType::Limit, 100, 10), 2));
        assert_eq!(traded_qty(&ev), 3);
        let resting = ev.iter().find(|e| matches!(e, Event::Resting { .. })).unwrap();
        if let Event::Resting { remaining, .. } = resting {
            assert_eq!(*remaining, 7);
        }
        assert!(!ev.iter().any(|e| matches!(e, Event::Killed { .. })));
    }
}
