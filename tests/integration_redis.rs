mod common;

use std::time::Duration;
use std::time::Instant;

use common::{
    has_accepted, has_canceled, has_killed, has_rejected, has_resting, total_traded, trades,
    Event, Harness, NewOrder, OrderType, Side,
};

const T: Duration = Duration::from_secs(3);

/// 卖单挂簿,买单穿价 → 一笔成交,价为 Maker 价。
#[test]
fn limit_resting_then_cross_trades() {
    let mut h = Harness::start();

    h.new_order(NewOrder::limit(1, 100, Side::Sell, 100, 10));
    let evs = h.collect_n(2, T); // Accepted + Resting
    assert!(has_accepted(&evs, 1), "{evs:?}");
    assert!(has_resting(&evs, 1), "{evs:?}");

    h.new_order(NewOrder::limit(2, 200, Side::Buy, 105, 4));
    let evs = h.collect_n(2, T); // Accepted + Trade
    assert!(has_accepted(&evs, 2), "{evs:?}");

    let ts = trades(&evs);
    assert_eq!(ts.len(), 1, "应恰好一笔成交: {evs:?}");
    if let Event::Trade {
        taker_order_id,
        maker_order_id,
        price,
        quantity,
        ..
    } = ts[0]
    {
        assert_eq!(*taker_order_id, 2);
        assert_eq!(*maker_order_id, 1);
        assert_eq!(*price, 100, "成交价恒为 Maker 价");
        assert_eq!(*quantity, 4);
    }
}

/// 挂单可被撤销,产出 Canceled。
#[test]
fn cancel_resting_order() {
    let mut h = Harness::start();

    h.new_order(NewOrder::limit(1, 100, Side::Sell, 100, 5));
    let evs = h.collect_n(2, T);
    assert!(has_resting(&evs, 1), "{evs:?}");

    h.cancel(1);
    let evs = h.collect_n(1, T);
    assert!(has_canceled(&evs, 1), "{evs:?}");
}

/// 撤一个不存在的单 → Rejected。
#[test]
fn cancel_unknown_rejected() {
    let mut h = Harness::start();
    h.cancel(999);
    let evs = h.collect_n(1, T);
    assert!(has_rejected(&evs, 999), "{evs:?}");
}

/// Post-Only 若会穿价则整笔拒绝,不成交不挂簿。
#[test]
fn post_only_rejected_when_crossing() {
    let mut h = Harness::start();

    h.new_order(NewOrder::limit(1, 100, Side::Sell, 100, 5));
    let _ = h.collect_n(2, T);

    h.new_order(NewOrder::new(2, 200, Side::Buy, OrderType::PostOnly, 101, 1));
    let evs = h.collect_n(2, T); // Accepted + Rejected
    assert!(has_accepted(&evs, 2), "{evs:?}");
    assert!(has_rejected(&evs, 2), "{evs:?}");
    assert!(trades(&evs).is_empty(), "post-only 不得成交: {evs:?}");
}

/// Post-Only 不穿价 → 挂簿。
#[test]
fn post_only_rests_when_not_crossing() {
    let mut h = Harness::start();
    h.new_order(NewOrder::new(1, 100, Side::Buy, OrderType::PostOnly, 100, 5));
    let evs = h.collect_n(2, T);
    assert!(has_resting(&evs, 1), "{evs:?}");
    assert!(!has_rejected(&evs, 1), "{evs:?}");
}

/// FOK 流动性不足被 Kill,且订单簿原封不动。
#[test]
fn fok_killed_leaves_book_intact() {
    let mut h = Harness::start();

    h.new_order(NewOrder::limit(1, 100, Side::Sell, 100, 5));
    let _ = h.collect_n(2, T);

    h.new_order(NewOrder::new(2, 200, Side::Buy, OrderType::Fok, 100, 10));
    let evs = h.collect_n(2, T);
    assert!(has_killed(&evs, 2), "{evs:?}");
    assert!(trades(&evs).is_empty(), "被 Kill 的 FOK 不得成交: {evs:?}");

    // 证明 maker #1 仍在簿:再来个 5 量买单应当成交。
    h.new_order(NewOrder::limit(3, 300, Side::Buy, 100, 5));
    let evs = h.collect_n(2, T);
    assert_eq!(trades(&evs).len(), 1, "maker 应仍在簿: {evs:?}");
}

/// IOC 部分成交后剩余被 Kill,从不挂簿。
#[test]
fn ioc_partial_then_kill() {
    let mut h = Harness::start();

    h.new_order(NewOrder::limit(1, 100, Side::Sell, 100, 3));
    let _ = h.collect_n(2, T);

    h.new_order(NewOrder::new(2, 200, Side::Buy, OrderType::Ioc, 100, 5));
    let evs = h.collect_n(3, T); // Accepted + Trade + Killed(剩余)
    assert!(has_accepted(&evs, 2), "{evs:?}");
    assert_eq!(total_traded(&evs), 3, "{evs:?}");
    assert!(has_killed(&evs, 2), "剩余须被 Kill: {evs:?}");
    assert!(!has_resting(&evs, 2), "IOC 从不挂簿: {evs:?}");
}

/// 市价保护:超出保护价的档不吃,剩余 Kill。
#[test]
fn market_order_respects_protection() {
    let mut h = Harness::start();

    h.new_order(NewOrder::limit(1, 100, Side::Sell, 100, 5));
    h.new_order(NewOrder::limit(2, 100, Side::Sell, 110, 5));
    let _ = h.collect_n(4, T);

    // 保护 500bps = 5%,基准 best ask=100 → 上限 105,110 档超出。
    h.new_order(NewOrder::new(3, 200, Side::Buy, OrderType::Market, 0, 10));
    let evs = h.collect_n(3, T);
    assert_eq!(total_traded(&evs), 5, "只能吃到 100 档: {evs:?}");
    assert!(has_killed(&evs, 3), "{evs:?}");
}

/// 自成交保护:同 user 撞自己的挂单 → 撤 Maker,不与自己成交。
#[test]
fn self_trade_prevention_cancels_resting() {
    let mut h = Harness::start();

    h.new_order(NewOrder::limit(1, 777, Side::Sell, 100, 5));
    let _ = h.collect_n(2, T);

    h.new_order(NewOrder::limit(2, 777, Side::Buy, 100, 5));
    let evs = h.collect_n(3, T); // Accepted(2) + Canceled(1) + Resting(2)

    assert!(has_canceled(&evs, 1), "自己的 Maker 须被撤: {evs:?}");
    assert!(trades(&evs).is_empty(), "不得自成交: {evs:?}");
    assert!(has_resting(&evs, 2), "买单剩余应挂簿: {evs:?}");
}

/// 跳过自己、成交别人。
#[test]
fn self_trade_skips_own_fills_other() {
    let mut h = Harness::start();

    h.new_order(NewOrder::limit(1, 7, Side::Sell, 100, 5)); // 自己的
    h.new_order(NewOrder::limit(2, 9, Side::Sell, 100, 5)); // 别人的
    let _ = h.collect_n(4, T);

    h.new_order(NewOrder::limit(3, 7, Side::Buy, 100, 5));
    let evs = h.collect_n(3, T);
    assert!(has_canceled(&evs, 1), "{evs:?}");
    let ts = trades(&evs);
    assert_eq!(ts.len(), 1, "{evs:?}");
    if let Event::Trade { maker_order_id, .. } = ts[0] {
        assert_eq!(*maker_order_id, 2, "应成交别人的单: {evs:?}");
    }
}

/// 防御性 qty==0:先 Accepted 再 Rejected。
#[test]
fn zero_qty_accepted_then_rejected() {
    let mut h = Harness::start();

    h.new_order(NewOrder::limit(1, 100, Side::Buy, 100, 0));
    let evs = h.collect_n(2, T);
    assert!(has_accepted(&evs, 1), "{evs:?}");
    assert!(has_rejected(&evs, 1), "{evs:?}");
    assert!(trades(&evs).is_empty(), "{evs:?}");
}

/// 未知 symbol 的命令被 main 直接丢弃,引擎无任何输出。
#[test]
fn unknown_symbol_dropped() {
    let mut h = Harness::start();

    // 直接构造一个别的 symbol 的下单(绕过 SYMBOL 常量)
    let bad = NewOrder {
        order_id: 1,
        symbol: "DOGE/USD".into(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: 100,
        quantity: 5,
        user_id: 100,
    };
    h.new_order(bad);

    let evs = h.drain(Duration::from_millis(800));
    assert!(evs.is_empty(), "未知 symbol 不应产生任何事件: {evs:?}");
}

/// 测试500单逐一穿价成交,打印端到端耗时与 QPS。
#[test]
fn test_500_orders() {
    const N: u64 = 500;
    // 压测给足超时,避免 CI 慢机误杀
    let timeout = Duration::from_secs(30);

    let mut h = Harness::start();

    // —— 铺底:一个大额卖单,够 500 个 1 量买单全吃 ——
    h.new_order(NewOrder::limit(1, 1, Side::Sell, 100, N));
    let evs = h.collect_n(2, T); // Accepted + Resting
    assert!(has_resting(&evs, 1), "铺底单未挂簿: {evs:?}");

    // —— 发单 + 计时:每个 taker 产出 Accepted + Trade ——
    let start = Instant::now();
    for i in 0..N {
        let oid = 1000 + i; // 避开铺底单 id=1
        h.new_order(NewOrder::limit(oid, 2, Side::Buy, 100, 1));
    }
    // 期望事件总数:500 * (Accepted + Trade) = 1000
    let evs = h.collect_n((N * 2) as usize, timeout);
    let elapsed = start.elapsed();

    // —— 正确性校验 ——
    let traded = total_traded(&evs);
    let trade_cnt = trades(&evs).len();
    assert_eq!(traded, N, "总成交量应为 {N}: 实际 {traded}");
    assert_eq!(trade_cnt as u64, N, "应恰好 {N} 笔成交: 实际 {trade_cnt}");

    // —— 打印耗时 ——
    let secs = elapsed.as_secs_f64();
    println!("===== 500 单 =====");
    println!("订单数        : {N}");
    println!("产生事件数    : {}", evs.len());
}