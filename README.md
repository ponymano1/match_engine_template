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