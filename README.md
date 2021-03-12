# zolo
**Zolo For Solo, A Crypto Quantative Profolio Manager.**

> 交易是孤独者的工作，独行侠

## Zolo 设计目标及原则
- Crypto投资组合管理器
- 自动化交易框架与工具集
- 尽量只依赖标准库，少使用第三方框架(暂时最小依赖是 requests + sqlalchemy)
- 模块plugin，方便增添和删除
- 只有IO才需要异步,策略代码同步执行
- 多策略同时运行，每个策略为独立进程
- 所有策略进程共享交易所实时数据和历史数据


## Features
- 提供三类订单执行器,简化策略开发流程:
  1. 同步 (Market/IOC/FOK)
  2. 异步 (Limit/PostOnly)
  3. 渐进式下单 (使用IOC渐进多次下单完成一次大的交易)
  
- 基于观察者模式的 Plugin:
  1. Strategy 策略是 bar, tick, timer 的观察者
  2. Indicator (sma, ema 等的行情参数) 作为 bar 的观察者
  3. Benchmark (sharp, time return ) 作为 trade, tick, position 的观察者
   
- 支持三種運行模式:
  1. 线上模式 - 正式交易
  2. 回测模式 - 历史数据 + 虚拟下单
  3. Dryrun 模式 - 线上数据 + 虚拟下单
  
## TODO:
1. 需要增添虚拟仓位功能,以管理多个策略共享key的需求。
2. 增添bar数据的提供方式, 增添get_history_bars接口，同时提供dataframe转换的函数
3. 增添入库的模块,将order, fill, bar, trade, position, margin及时存储起来.