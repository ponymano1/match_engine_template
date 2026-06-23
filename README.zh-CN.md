# 撮合引擎(Rust)

[English](./README.md) | 简体中文

一套确定性的、MQ 驱动的中央限价订单簿(CLOB)撮合引擎模版。设计目标是跨项目复用:
直接拿来用,或稍加改动即可快速落地。所有**不属于撮合**的业务(账户、风控、清算、
报价)都被刻意移出核心,作为上下游系统通过消息队列接入。

## 为什么做这个

工作中和多个项目里反复需要实现撮合引擎,而不同项目的撮合大同小异。于是把这部分
共性沉淀成一套模版,下个项目就能从一个能跑的核心开始,而不是从零写起。这个引擎刻意
保持"单纯":只做撮合,且只通过 MQ 与外界通信。

## 职责范围

- **属于本引擎**:订单簿维护、价格-时间优先撮合、订单类型(限价 / 市价 / IOC /
  FOK / Post-Only)、自成交保护、市价保护、确定性事件输出。
- **不属于本引擎**(作为独立服务接入):账户与余额冻结、Pre-trade 风控、
  清算/结算、报价/行情增强。

## 撮合规则:价格-时间优先

1. **价格优先** —— 更优价格先成交(最高买价 / 最低卖价)。
2. **时间优先** —— 同一价格档位内,最早的订单先成交(FIFO)。

## 系统边界

````

  输入 MQ  ──►  撮合引擎  ──►  输出 MQ

```

- **输入**:MQ(当前 Redis;计划支持 Kafka / SQS)
- **输出**:MQ
- 撮合引擎不关心订单来源。用户下单、Risk Service 回灌——在引擎眼里完全一样。
- 注意: redis 只作为测试用，生产环境最好选择其他MQ, 或者对redis做充分的高可用管理

## 上下游

```

Order Service ─┬─ Account Service(冻结余额)
               └─ Risk Service   (Pre-trade 检查)
                      │
              全部通过 → Order Service 发布到 MQ
                      │
                      ▼
                ★ 撮合引擎 ★
                      │
                      ▼
            清算 / 结算  +  行情(Market Data)

````

## 数据结构

- **价格索引**:`BTreeMap<Price, PriceLevel>`,按价格有序。
  - 买盘:`max key` = best bid。
  - 卖盘:`min key` = best ask。
- **时间队列**:每个 `PriceLevel` 持有一个 `VecDeque<RestingOrder>`(FIFO)。
- **撤单索引**:`HashMap<OrderId, (Side, Price)>`,O(1) 定位撤单。
- **档位总量缓存**:每档缓存其总量,用于 FOK 的廉价上界预判。

### 复杂度

| 操作              | 复杂度          | 说明                                |
|-------------------|-----------------|-------------------------------------|
| 读 best bid/ask   | O(1)            | `BTreeMap` 首/尾键                   |
| 插入(挂单)       | O(log N)        | 定位档位,推入 FIFO 尾部             |
| 撤单              | O(log N + k)    | 索引定位 + 单档内扫描               |
| 单笔成交          | O(1) 摊还       | 从 FIFO 头部弹出                    |
| FOK 预判          | O(档数)*        | *触发 STP 时退化为 O(订单数)        |

## 订单类型

| 类型      | 行为                                                       |
|-----------|----------------------------------------------------------|
| `Limit`   | 能成交的先成交,剩余挂入订单簿。                          |
| `Market`  | 在保护价范围内吃单,剩余取消。                            |
| `Ioc`     | 立即成交,剩余取消(从不挂簿)。                          |
| `Fok`     | 全量成交或整笔取消(取消时订单簿原封不动)。              |
| `PostOnly`| 只做 Maker;若会立即吃单则整笔拒绝。                      |

## 自成交保护(STP):撤挂单(Cancel Resting)

当 Taker 扫到自己的挂单时,把那笔 Maker **整笔撤销**(发出 `Canceled` 事件),
Taker 继续吃后面的单子,永不与自己成交。

这就是 "Cancel Oldest / Cancel Resting" 策略。注意:FOK 的 `can_fill_fully`
预判会**排除 Taker 自身的挂单流动性**,所以一笔只能靠自成交才能成交的 FOK,
会在改动订单簿之前就被 Kill。

## 防御性保障:qty == 0

风控应在上游拦截零数量订单。作为防御性兜底,引擎仍会:

1. 先发 `Accepted`(保证每笔 `NewOrder` 都被确认),再
2. 发 `Rejected`。

这只是入口处一次整数比较——不在热路径循环里——对吞吐没有任何可测影响。

## 性能说明

- **qty == 0 校验**:可忽略。引擎入口一次整数比较,不进热路径。
- **STP 在热路径里的逐笔 `user_id` 比较**:基本免费。`RestingOrder` 本来就要从
  `front()` 取出,`user_id` 和 `remaining` 在同一 cache line 上,多一次比较不会
  多一次访存。
- **真正有代价的是 `can_fill_fully`**:为排除自身挂单,它从走 `total` 缓存的
  O(档数) 退化成 O(穿越档位内订单总数)。深度簿、碎单多时,FOK 预判会明显变慢。
  这是本次设计里唯一值得认真对待的回归。
- **可利用的不对称性,把快路径救回大半**:排除自身挂单只会让可用流动性**变少**,
  不会变多。所以先用 `level.total` 做一次廉价上界判断——如果连"含自己的总量"都
  不够 `qty`,真实答案必然也不够,直接 `false` 返回,根本不用逐笔扫。只有当上界
  通过时,才退化到逐笔扫描确认。FOK 因流动性不足被拒是最常见的情况,这条优化能让
  大多数调用仍走 O(档数)。

## 高可用

引擎是确定性的:**相同输入 + 相同顺序 ⇒ 相同结果**。HA 通过并行运行多套完全相同的
引擎实现,只有 Primary 输出 trade。

```

MQ Topic
   ├─► 撮合引擎 A(Primary)  ─► Trade 输出
   ├─► 撮合引擎 B(Standby)  ─► 不输出
   └─► 撮合引擎 C(Standby)  ─► 不输出

```

Primary 挂了就把某台 Standby 提升为 Primary。**Trade 输出必须幂等**,以便下游在
切换时去重。

## 运行时架构(线程与通道)

```

                         ┌──────────────────────────────────────────────┐
                         │                   main 线程                     │
                         │             (Receiver / 定序器)                │
  Redis Stream ─poll()──►│  RedisInbound.poll()                           │
                         │     → 反序列化 Command                          │
                         │     → 按 symbol 路由                            │
                         │     → 赋予 (seq, shard_seq, ts)                 │
                         └───────────────┬───────────────┬───────────────┘
                                         │               │
                       每 symbol 一个 Sender<Sequenced>(有界环形)
                                         │               │
                          ┌──────────────▼───┐   ┌───────▼──────────┐
                          │ match-BTC/USD     │   │ match-ETH/USD    │   ...
                          │ Engine::handle()  │   │ Engine::handle() │
                          └──────────────┬───┘   └───────┬──────────┘
                                         │               │
                       Sender<Event>(有界输出环形)
                                         │               │
                                         |               |
                                         ▼               ▼
                                     ┌───────────────────────┐
                                     │     publisher 线程     │
                                     │     RedisOutbound      │
                                     │     .publish(topic,..) │
                                     └───────────┬───────────┘
                                                 ▼
                                  Redis Streams(trades / book events)

````

- **每个 symbol 一个独立线程** → 独立订单簿,无锁。
- **有界**通道在下游变慢时提供天然背压。
- 撮合线程**不做任何 IO**:只从输入环读、向输出环写,别无其他。

## 调用链:一笔 NewOrder 端到端

```

main 循环
└─ in_mq.poll()                         // mq.rs   RedisInbound::poll
└─ serde_json::from_slice::<Command>
└─ routes.get(&symbol)                  // 未知 symbol 丢弃
└─ 赋予 seq / shard_seq / ts
└─ tx.send(Sequenced)             ──────► (每 symbol 通道)

match-<symbol> 线程
└─ Engine::handle(&Sequenced)           // engine.rs
└─ on_new_order(o, seq, ts)
├─ push Event::Accepted
├─ 若 qty == 0 → push Rejected,返回
├─ PostOnly:
│     └─ OrderBook::would_cross         // orderbook.rs
│           └─ best_ask / best_bid
│              price_crosses
│        ├─ 会吃单 → push Rejected
│        └─ 否则   → OrderBook::rest → push Resting
├─ FOK:
│     └─ OrderBook::can_fill_fully
│           └─ can_fill_fully_inner
│                ├─ 第一遍:累加 level.total(上界)
│                └─ 第二遍:逐笔,跳过 taker_user_id
│        └─ 流动性不足 → push Killed,返回
├─ market_protection(side)         // 仅 Market
│     └─ best_ask / best_bid
└─ OrderBook::match_order(...)
├─ 循环对手最优档
│     ├─ price_crosses  (限价停止)
│     ├─ protection 检查 (市价停止)
│     └─ 档内 FIFO 逐笔:
│           ├─ STP:自己的单 → pop + stp_canceled
│           └─ 成交:fill,push Fill
├─ 从 index 清理 stp_canceled
└─ 返回 MatchOutcome
└─ outcome 映射为 Events:
├─ stp_canceled → Event::Canceled(排在成交前)
├─ fills        → Event::Trade
└─ remaining:
├─ Limit            → rest + Resting
└─ Market/Ioc/Fok   → Killed
└─ for ev in events: out_tx.send(ev)    ──────► (输出通道)

publisher 线程
└─ out_rx.iter()
└─ serde_json::to_vec(&Event)
└─ RedisOutbound::publish(topic, payload)   // mq.rs

````

## 调用链:一笔 Cancel

```

main 循环 → 按 symbol 路由 → Sequenced → 通道
match 线程
└─ Engine::handle
└─ Command::Cancel
└─ OrderBook::cancel(order_id)        // orderbook.rs
├─ index.remove → (side, price)
├─ 定位档位,移除订单
├─ level.total -= removed.remaining
└─ 档位空则删除价位键
├─ true  → Event::Canceled
└─ false → Event::Rejected("order not found")

````

## 模块地图

| 文件           | 职责                                                    |
|----------------|--------------------------------------------------------|
| `main.rs`      | 装配:配置、线程、通道、receiver/定序循环。             |
| `config.rs`    | TOML 加载 + 环境变量覆盖 + 启动强校验。                |
| `domain.rs`    | 类型别名、`Command`、`Event`、订单语义。               |
| `mq.rs`        | `Inbound` / `Outbound` trait + Redis 实现。            |
| `engine.rs`    | 单 symbol 引擎:命令 → 订单簿 → 事件流。               |
| `orderbook.rs` | CLOB 本体:档位、FIFO、撮合/撤单核心逻辑。             |

## 配置

```toml
symbols = ["BTC/USD", "ETH/USD"]

[mq]
url               = "redis://127.0.0.1/"   # 用 MQ_URL 环境变量覆盖
inbound_stream    = "orders"
trades_topic      = "trades"
book_events_topic = "book"

[engine]
input_ring_capacity   = 1024   # 必须是 2 的幂
output_ring_capacity  = 4096   # 必须是 2 的幂
market_protection_bps = 500    # 5%
````

`MQ_URL` 环境变量覆盖 `mq.url`(密码等敏感项别写进文件)。环形容量必须是 2 的幂;
symbol 必须唯一——均在启动时校验,配置有误则拒绝运行。

## 运行

```bash
cargo run --features redis-mq -- config.toml
cargo test --features redis-mq         # engine + orderbook 单元测试
```

## 测试
### 单元测试
```bash
cargo test
```

### 集成测试
```bash
./scripts/run-integration.sh 
```

## engine压测
```bash
cargo bench --bench engine_bench
```

### 压测优化路径
- 削掉一半的 out 流量。 现在你是逐个事件 out_tx.send,一单发 2 次。改成撮合线程把 handle 返回的整个 Vec<Event> 当一条消息发出去(channel 类型从 Event 改成 Vec<Event> 或 SmallVec<Event>),out 侧的 channel 操作就从 2 次/单降到 1 次/单。光这一步,撮合线程的 channel 操作从 3 次降到 2 次,能省掉约 100ns/单,留存率大概率能从 28% 拉回到 40% 上下。这是性价比最高的一刀,且不引入任何新依赖。(已完成)
- 目前如果纯engine调用撮合，单交易对每秒处理600万单到800万单，算上crossbeam_channel的时间，大概每秒处理300万单左右，对绝大多数交易系统，match_engine已经不是性能瓶颈了，如果想继续优化，可以替换crossbeam成无锁队列，并且可以优化内存的申请和释放。


## 路线图

* [ ] Kafka 入站/出站
* [ ] 使用无锁队列替换现有的crossbeam_channel
* [ ] SQS 入站/出站
* [ ] 快照 + 重放,用于冷启动 / 恢复
* [ ] 基于 `shard_seq` 的分片内丢包检测
