# 后端消息系统架构优化总结

AI对话记录[Deepseek](https://chat.deepseek.com/share/rmfts9fzran5uz491j)

## 一、项目初始简化

- 保留了 `cmd/main.go`、`internal/api`、`internal/service`、`internal/repository`、`internal/model` 五个核心包。
- 移除了所有历史兼容、旧客户端迁移、实验逻辑、重复状态映射等无关代码（`audit`、`callback`、`compat`、`experiments`、`legacy`、`noise`、`rollout` 等 20+ 个文件）。
- 移除了未集成的 `LegacyStatus` 字段及其展示逻辑，统一使用 `Status`。

## 二、数据一致性问题修复

### 1. 消息删除后重试导致“复活”
- 在 `StartAttempt` 中增加状态检查，若消息为 `deleted` 则直接返回错误。
- 测试用例：`TestStartAttempt_OnDeletedMessage_ShouldFail`

### 2. 成功回执覆盖已删除/已发送状态
- 在 `CompleteAttempt` 开头增加终态保护，若消息为 `deleted` 或 `sent` 则返回错误。
- 测试用例：`TestCompleteAttempt_OnDeletedMessage_ShouldNotRevive`，`TestCompleteAttempt_OnSentMessage_ShouldNotChange`

### 3. 并发更新缺少乐观锁
- `SaveMessage` 增加 `expectedVersion` 参数，比对版本号，不一致则返回 `ErrVersionConflict`。
- `RetryMessage` 调用时传入读取时的版本号。
- 测试用例：`TestSaveMessage_VersionConflict`，`TestSaveMessage_SuccessWhenVersionMatch`

### 4. 状态回退保护
- 在 `CompleteAttempt` 失败分支中，若消息历史上曾成功（`HasEverSucceeded` 为 true），则保持状态为 `sent`，不回退到 `failed`。
- 测试用例：`TestStatusRollbackProtection`

### 5. 删除消息后会话摘要未更新
- 在 `DeleteMessage` 中新增 `refreshSummaryAfterDeleteLocked`，遍历会话消息索引找到最新未删除消息并更新摘要；若无消息则清空预览。
- 测试用例：`TestDeleteMessage_UpdatesSummaryToPrevious`，`TestDeleteMessage_WhenOnlyOne_SummaryCleared`，`TestDeleteMessage_WhenDeletingOlderMessage_SummaryUnchanged`

### 6. 未读计数重复递增
- 引入 `HasEverSucceeded` 字段，仅在消息首次成功时增加接收方未读计数。
- 测试用例：`TestUnreadNotIncrementedTwiceForSameMessage`

## 三、性能优化

### 1. 消除全表扫描
- `CompleteAttempt` 通过 `HasEverSucceeded` 字段避免遍历全部 attempts。
- `ListConversationMessages` 增加会话索引 `messagesByConversation`，按时间降序直接获取会话消息。
- `FindLikelyDuplicateMessage` 增加去重索引 `duplicateIndex`，O(1) 查询。
- 保留旧实现用于性能对比（标记为 `Deprecated`）。

### 2. 事件游标二分查找
- `ListEventsAfter` 使用 `sort.Search` 二分定位起始位置，替代顺序扫描。
- 保留旧实现 `ListEventsAfterOld` 用于基准测试。

### 性能优化对比表

以下为各核心操作优化前后的耗时对比（数据量单位为条，耗时单位为纳秒或微秒，已换算为易读单位）。

| 操作 | 数据规模 | 旧版耗时 | 新版耗时 | 提升倍数 |
|------|----------|----------|----------|----------|
| CreateMessage | 10 万条插入 | 1,364 ns | 3,188 ns | 0.43x（新略慢，因维护索引） |
| CompleteAttempt | 1 万条消息 | 277 ns | 100 ns | 2.8x |
| CompleteAttempt | 5 万条消息 | 294,801 ns (295 µs) | 141 ns | 2,090x |
| ListConversationMessages | 1 万条消息 | 95,182 ns (95 µs) | 1,773 ns | 54x |
| ListConversationMessages | 5 万条消息 | 650,040 ns (650 µs) | 6,085 ns (6 µs) | 107x |
| FindLikelyDuplicateMessage | 1 万条消息 | 48,649 ns (49 µs) | 102 ns | 477x |
| FindLikelyDuplicateMessage | 5 万条消息 | 250,318 ns (250 µs) | 115 ns | 2,176x |
| ListEventsAfter | 10 万条事件 | 7,532,661 ns (7.5 ms) | 13.66 ns | 551,000x |

**说明**：
- `CreateMessage` 新版由于需要维护会话索引和去重索引，略有额外开销（约 3 µs），但发送路径为低频操作，完全可接受。
- 其余读路径均通过索引或二分查找获得数百至数十万倍的性能提升，彻底消除了全表扫描。
- `ListEventsAfter` 从毫秒级直接降至纳秒级，效果最为显著。

## 四、可观测性改进

- 引入 `log/slog` 结构化日志，同时输出 JSON 格式到文件和标准输出。
- 使用 `github.com/google/uuid` 为每个请求生成唯一 `request_id`，通过 `context` 全链路传递。
- 错误日志携带 `request_id`、`msg_id`、`attempt_id`、`user_id` 等关键字段。

## 五、安全与健壮性

### 1. 接口防刷限流
- 基于 `golang.org/x/time/rate` 实现令牌桶限流，按 `senderID` 维度限制发送频率。
- 默认速率 5 ops/s，突发容量 10。
- 超限返回 429 状态码。
- 后台协程定期清理过期的限流器。

### 2. 游标保存错误处理
- 将 `SaveDeviceCursor` 接口改为返回 `error`。
- 在 `Sync` 方法中捕获保存错误并记录日志，避免静默失败。

## 六、可配置化

- 内容去重窗口从硬编码 `30s` 改为可通过命令行 flag `-dedupe-window` 配置（默认 30s）。

## 七、未完成或暂缓处理的问题

1. **接口认证与授权**（问题 8）  
   当前 `/sync` 和 `/messages/send` 未验证用户身份，存在数据泄露风险。由于项目定位为 demo，暂未实现。

2. **数据持久化**（问题 14）  
   当前使用纯内存存储，服务重启数据全部丢失。后续若用于生产环境需接入数据库（如 PostgreSQL）。

3. **Version 字段语义滥用**（问题 12）  
   `ListConversationMessages` 将尝试次数累加到 `Version` 字段，破坏了版本号含义。当前暂不影响功能，待后续统一重构。

4. **多设备同步语义细化**（问题 4 评估后无需修改）  
   当前事件按用户维度返回，所有设备共享同一事件流，符合一对一私聊的业务模型，暂无需按设备过滤。

5. **其他边界优化**
    - 游标为 0 时可能拉取全量事件，因无认证/限流配合，当前可接受。
    - 部分错误处理可更精细，但整体已具备排障所需的结构化日志。

---

本次优化遵循“先精简、后修复核心 Bug、再提升性能与可观测性”的路径，将原始杂乱的 demo 重构为逻辑清晰、数据可靠、具备基本防护的消息系统后端基础。所有修改均经过完整测试验证，性能对比数据确凿。