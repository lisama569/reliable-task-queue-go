# 本地可靠任务队列（Go）

一个零外部依赖（仅标准库）的本地可靠任务队列：用本地 JSON 文件模拟持久队列，
用带幂等与顺序约束的内存账本（`Ledger`）模拟下游消费者。目标是让任务在
**重复投递、连续失败、进程重启** 三类异常下的最终状态都可预期。

## 目录结构

```
cmd/demo              端到端演示：提交 / 重复提交 / 重试 / 死信 / 对账
internal/model        Task 数据模型、状态与失败原因枚举
internal/storage      FileStore：原子写（临时文件 + fsync + rename）的持久层
internal/downstream   Ledger（模拟下游）+ 错误分类 + 指数退避
internal/queue        队列主体：提交确认、认领租约、投递分类、死信、租约回收
```

## 投递语义：至少一次（at-least-once）+ 下游幂等去重

- **接收确认边界 = 落库成功**。`Submit` 只有在任务记录经过
  「写临时文件 → fsync → rename → fsync 目录」完整落盘后才返回 `nil`；
  落盘失败时内存与磁盘都保持上一个已提交版本，不会对外“确认”一条丢得起的任务。
- **幂等提交**。提交按 `IdempotencyKey` 去重，重复键返回原有任务记录，
  不产生新记录、不产生任何下游副作用。
- **投递失败必须重试**。下游返回错误时，任务绝不会被静默丢弃：可重试错误
  按指数退避重新变为 `PENDING`，由调度器再次认领投递。
- **下游重复投递去重**。租约超时/崩溃恢复可能导致同一任务被投递多次。
  下游 `Ledger` 以 **任务 ID** 作为幂等键：重复投递直接按成功确认，
  余额不变，因此不会重复扣减或状态回退。
- **同业务键按提交顺序生效**。提交时为每个 `BusinessKey` 分配单调递增的
  `Seq`；调度器每个业务键每次只放行队首任务（head-of-line），下游再用
  `Seq` 做一道顺序校验（乱序到达按可重试失败拒绝）。最终下游各键余额与
  提交记录逐条对得上。

## 任务状态机

```
                提交落盘
                  │
                  ▼
              ┌────────┐ 认领（写入带 TTL 的租约）  ┌────────────┐
              │PENDING │ ───────────────────────▶ │ PROCESSING │
              └────────┘                          └────────────┘
                ▲   │                              │   │   │   │
                │   │ 不可重试 / 重试耗尽            │   │   │   │
                │   ▼                              │   │   │   └──▶ 成功
                │ ┌──────┐                         │   │   │        │
                │ │ DEAD │ ◀───────────────────────┘   │   │        ▼
                │ └──────┘   （死信，保留失败原因）      │   │   ┌───────────┐
                │                                      │   │   │ SUCCEEDED │
                └──────────────────────────────────────┘   │   └───────────┘
                  租约过期回收（崩溃/卡死的任务回到 PENDING）  │
                                                          ▼
                                            可重试错误：退避后回到 PENDING
```

## 重试与死信规则

投递一次后，根据下游错误（`downstream.DownstreamError`）分类：

| 情况 | 判定依据 | 动作 | 失败原因 |
|---|---|---|---|
| 成功（含下游识别出重复任务） | `error == nil` | `SUCCEEDED` | `NONE` |
| 永久失败 | 错误标记为不可重试 | 立即 `DEAD`，不再尝试 | `NON_RETRYABLE` |
| 临时失败且仍有预算 | 错误可重试且 `Attempts < MaxAttempts` | 退避后 `PENDING` 重投 | — |
| 临时失败且预算用尽 | 可重试但 `Attempts == MaxAttempts` | `DEAD` | `RETRY_EXHAUSTED` |

- **退避**：`Backoff.Delay(n) = min(BaseDelay × 2^(n-1), MaxDelay)`。
- **尝试次数在认领（写租约）时原子递增并落盘**，不是在投递返回后才计数。
  因此一次崩溃/卡死的投递同样消耗尝试预算，重启后不会把同一任务反复当成
  “第 1 次”，也不会因崩溃而获得无限重试。
- **死信可区分**：`Task.FailureReason` 区分 `NON_RETRYABLE` 与
  `RETRY_EXHAUSTED`，并保留 `LastError`、`Attempts`、`DeadAt`，便于排查。

## 处理中崩溃的恢复（不卡死）

- worker 认领任务时写入带 TTL 的处理租约（`LeaseHolder` + `LeaseUntil`），
  该状态变更同样先落盘。
- 进程在 `Deliver` 中途被杀死时，任务留在磁盘上的 `PROCESSING` 状态。
- 新进程启动后，回收循环扫描 `LeaseUntil < now` 的任务，将其重置为
  `PENDING` 立即重新参与调度（日志 `decision=REQUEUE reason=LEASE_EXPIRED`）。
- 由于下游按任务 ID 幂等，“旧进程可能已生效、新进程再投一次”不会重复扣减。

## 日志证据

队列每次提交、认领、投递判定、回收都会打印 **业务键、任务 ID、序号、
重试次数与判定依据**，例如：

```
[deliver] key=biz-r task=… seq=0 attempt=1 max=4 decision=RETRY backoff=5ms detail="simulated timeout"
[deliver] key=biz-r task=… seq=0 attempt=3 decision=SUCCEEDED
[deliver] key=biz-f task=… attempt=1 decision=DEAD reason=NON_RETRYABLE detail="account closed"
[reclaim] key=biz-crash task=… attempt=0 decision=REQUEUE reason=LEASE_EXPIRED holder=worker-1
[downstream] key=k task=t-dup attempt=2 decision=DEDUP already applied, balance=-100 unchanged
```

## 本地验证

要求 Go 1.25+（仅用标准库，无需外部服务）。

```bash
# 1. 全部单元测试（含重复投递 / 可重试 / 不可重试 / 崩溃重启四类场景）
go test ./... -v

# 2. 竞态检测
go test -race -count=1 ./...

# 3. 端到端演示：成功、重试后成功、不可重试死信、幂等重复提交、最终对账
go run ./cmd/demo
```

测试覆盖对应需求：

| 测试 | 场景 |
|---|---|
| `TestSubmitIdempotency` | 同幂等键重复提交不产生重复记录，序号按业务键分配 |
| `TestRetryableFailureThenSuccess` | 临时失败按退避重投，最终成功，余额正确 |
| `TestNonRetryableFailure` | 永久失败一次进入死信，原因为 `NON_RETRYABLE` |
| `TestRetryExhausted` | 持续临时失败用尽预算进入死信，原因为 `RETRY_EXHAUSTED` |
| `TestDownstreamDuplicateDeliveryDedup` | 同一任务多次投递只扣减一次 |
| `TestPerKeyOrdering` | 乱序投递被拒绝，按提交顺序生效 |
| `TestEndToEndOrderingAndIdempotency` | 并发消费下顺序、去重、最终对账 |
| `TestCrashWhileProcessingThenRestart` | 处理中“被杀”，重启后租约回收并完成投递 |
| `TestStaleDeliveryResultDoesNotOverwrite` | 投递期间租约过期被回收，陈旧投递结果不得回写覆盖新状态 |
| `TestFileStorePersistAndReload` | PROCESSING 状态跨“重启”保留 |
