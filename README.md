# 撮合引擎(Rust)
## 目的
工作中和其他项目中多次需要实现撮合引擎，不同项目撮合引擎大同小异，于是就实现一套较为通用的撮合引擎模版，以后需要使用时，直接使用或者稍加改动就能快速使用。本撮合引擎相对单纯，把不属于撮合引擎的业务都移除了出去，比如，账号，风控，clearing 及报价，可以作为第三法系统进行接入。本撮合引擎的接入方式采用MQ进行接入。初期测试先简单支持redis, 逐步增加kafka和sqs等。  


## 前提
### Price-Time Priority
- 价格优先
- 同价位下时间优先

## 职责
- 撮合引擎不关心订单来源
    - 订单可能来自用户下单，RiskService等，对于撮合引擎来说，不用关心

## 系统边界
- 输入: MQ(redis, kafka, sqs)
- 输出: MQ

## 上下游
```
Order Service ─┬─ Account Service(冻结)
               └── Risk Service(Pre-trade 检查)
                      │
                 都通过 → Order Service 发到 MQ → ★ Matching Engine → Clearing / Market Data

```

## 数据结构
- BTreeMap (价格索引) + FIFO(每个价格档位一个FIFO, 按时间排列)

### 撮合引擎复杂度计算
- 撮合: 查询best bid和best ask, O(1), 直接去头节点即可
- 插入: 找到价格档位，然后插入队列头或者尾 O(Log(N))

### 高可用
- 撮合引擎自身保证了确定性，相同输入和相同顺序必然得到相同的结果
- 采用多套撮合引擎同时运行，只有一个作为Primary进行trade输出
- 如果Primary挂了，直接切换到standby 进行trade输出就可以(注意,Trade输出要幂等)

```
MQ Topic
    |----> Match Engine Server A (Primary) --> Trade output
    |----> Match Engine Server B (Standby) --> No trade output
    |----> Match Engine Server C (Standby) --> No trade output
```

## 注意事项
- 订单在放入mq之前要进行风控
- 风控要确保没有自成交订单和数量为0的订单 

### qty = 0,防御性保障
- 先Accepted,保证每笔NewOrder都被确认,再Reject.
- 这个是防御性保障，前期风控应该拦掉这笔单

### 自成交策略: 撤挂单(Cancel Resting/ Cancel Oldest)
- Taker 扫到自己的挂单时，把那笔Maker整比撤销，并发出Canceled事件，继续吃后面的单子

### 防御性策略对性能的影响
- qty == 0 的校验:可以忽略。就是引擎入口一次整数比较,不进热路径循环,对吞吐没有任何可测影响。

- STP 在 match_order 热路径里的逐笔 user_id 比较:也基本免费。因为 RestingOrder 本来就要从 front() 取出来,user_id 和 remaining 在同一个 cache line 上,多一次比较不会多一次访存。

- 真正有代价的是 can_fill_fully。 改动前它走的是 level.total 缓存,复杂度是 O(穿越的价格档数);改动后为了排除自身挂单,退化成 O(穿越档位内的订单总数)。深度簿、碎单多的时候,FOK 预判会明显变慢。这是这次改动里唯一值得认真对待的回归。

- 不过有个不对称性可以利用,把快路径救回来大半:排除自身挂单只会让可用流动性变少,不会变多。 所以可以先用 level.total 做一次廉价的上界判断——如果连"含自己的总量"都不够 qty,那真实答案必然也是不够,直接 false 返回,根本不用逐笔扫。只有当上界通过(总量看起来够)时,才退化到逐笔扫描确认。FOK 因流动性不足被拒是最常见的情况,这条优化能让大多数调用仍走 O(档数):


