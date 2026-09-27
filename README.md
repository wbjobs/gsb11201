# 多级缓存一致性管理器 (com.gsb.cache)

JDK 8、纯标准库实现。`src/` 为主源码，`tests/` 为纯 Java 断言测试（无 JUnit），
`run-tests.sh` 用 `javac`/`java` 一键编译并跑测试。

## 编译与运行

```bash
./run-tests.sh
```

产物输出到 `build/classes`（主代码）与 `build/test-classes`（测试），
编译参数为 `-source 8 -target 8`。

## 入口

- `com.gsb.cache.CacheManager`：对外接口，含
  `register / get / put / invalidate / stats`。
- `com.gsb.cache.DefaultCacheManager`：实现，构造时注入 `Clock`
  （生产用 `SystemClock`，测试用可控时钟），可选指定 WRITE_BEHIND 队列容量。
- `com.gsb.cache.WriteMode`：`WRITE_THROUGH` / `WRITE_BEHIND`。
- `com.gsb.cache.Supplier`：回源加载器；`com.gsb.cache.CacheWriter`：
  落库写接口，通过 `setWriter(region, writer)` 注入（默认 no-op）。

## 一致性设计

- **防击穿（single-flight）**：每个 region 维护
  `key -> LoadFuture(CountDownLatch)`。未命中时只有一个调用方执行 loader，
  其余请求 `putIfAbsent` 到同一个 future 后阻塞等待同一结果。
- **失效与在途加载竞态**：每个 key 有 epoch 计数，`put`/`invalidate` 会在
  同一把 `invalidationLock` 内 bump epoch 并清空 L1/L2；加载前先记录 epoch，
  加载完成后在同一把锁内复核 epoch，未变化才发布到 L1/L2。因此 invalidate
  之后在途加载的旧值不可能写回缓存。
- **WRITE_THROUGH**：`put` 同步更新 L1/L2 并调用 writer 后才返回。
- **WRITE_BEHIND**：写先入 `pending`（key -> 最新值）map，单 worker 后台批量
  刷盘。同一 key 的多次连续写在 map 中自然合并为一次刷写。
  队列满策略为**阻塞背压**：`put` 等待 worker 腾出空间，既不覆盖也不丢弃；
  shutdown 时若仍有写则退化为同步直写，保证不丢数据。
- **L1 LRU**：访问序 `LinkedHashMap`，`removeEldestEntry` 按容量淘汰，
  `get`/L2 提升都会刷新访问顺序。`l1Capacity=0` 表示关闭 L1。
- **L2 TTL**：`expireAt = 写入时刻 + ttlMillis`，仅当 `now < expireAt`
  存活，`now == expireAt` 恰好过期；时间全部来自可注入的 `Clock`。

说明：null 值不缓存（loader 返回 null 时每次都会重新回源，`put` 不允许 null）。

## 验收用例（tests/）

1. `SingleFlightTest`：32 线程并发未命中同一 key，loader 只执行 1 次。
2. `WriteBehindCoalesceTest`：冻结刷盘后连续写同一 key 十次，只刷 1 次，
   且刷出的是最新值。
3. `WriteBehindQueueFullTest`：队列容量 2，第三个写阻塞（背压），放行后三条
   数据全部落盘、无丢失。
4. `LruEvictionTest`：断言 L1 淘汰顺序严格符合 LRU。
5. `InvalidateRaceTest`：在途加载未完成时 invalidate，断言之后永远读不到旧值。
6. `TtlBoundaryTest`：边界前 1ms 存活、边界时刻恰好过期。
7. `WriteThroughTest`：WRITE_THROUGH 同步刷盘。
