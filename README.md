# Reliable Task Queue (Go, local storage simulation)

一套纯本地实现的可靠任务队列与异步投递流程。存储与下游消费者都用本地组件模拟，
目标是让任务在**重复投递、连续失败、进程重启**三种情况下的最终状态都可预期。

## 架构

```
Submitter ──▶ FileStore (内存索引 + JSON 快照落盘，模拟持久队列)
                    │ lease (带租约的抢占)
                    ▼
              Worker pool ──▶ FakeConsumer (内存下游，按任务 ID 幂等)
                    │
        失败分类 retryable / non_retryable
                    ▼
         退避重投 或 死信 (DLQ)
```

| 包 | 职责 |
| --- | --- |
| `internal/task` | 任务模型、状态、失败类别与死信原因常量 |
| `internal/store` | 持久化任务日志：幂等提交、顺序序号、租约抢占、快照恢复 |
| `internal/retry` | 指数退避 + 抖动策略 |
| `internal/downstream` | 模拟下游：按任务 ID 去重、脚本化成功/失败、按业务键记账 |
| `internal/queue` | 工作循环、失败判定、租约回收（reaper）、结构化日志 |
| `cmd/demo` | 端到端手动验证入口 |

## 投递语义

### 接收确认：先落库，后确认

- `Submit` 在**快照持久化成功之后**才返回。落盘失败会回滚内存状态并返回错误，
  调用方不得视为已接收。
- 写盘采用 `临时文件 + fsync + rename`，崩溃不会留下半截日志。

### 幂等提交：同一幂等键无重复副作用

- 每个任务带 `IdempKey`。重复提交返回**同一条任务**且 `created=false`，
  不新增记录、不触发第二次下游调用。幂等索引随快照持久化，重启后依然生效。

### 至少一次投递 + 下游去重

- 投递结果只有在 worker 收到下游响应后才会更新状态。若在
  “下游已生效、成功确认未落库”之间进程死亡，任务会被重投（at-least-once）。
- 下游以**稳定的任务 ID** 作为幂等键：重投的任务不会重复扣减、不会回退状态。
  因此重复投递对业务效果是 exactly-once 的，消息投递层面是 at-least-once 的。

### 同业务键顺序约束

- 每个 `BizKey` 内部维护单调递增 `Seq`。
- 任务仅当其所有前序任务都为 `succeeded` 时才可被抢占（同一时刻每个业务键
  最多一个在途任务），所以同键任务严格按提交顺序生效；不同业务键互不阻塞。
- 若前序任务进入死信，后续同键任务会停在队首等待（head-of-line blocking），
  从而保持“下游最终状态与提交记录一致”，不会越过失败任务继续扣减。

## 失败分类、重试与死信

下游每次返回一个失败分类：

| 分类 | 处理 |
| --- | --- |
| `retryable`（瞬时错误，如超时、连接重置） | 按退避策略计算 `NotBefore`，回到 pending 等待重投 |
| `non_retryable`（永久错误，如非法参数） | **立即**进入死信，不重试 |
| `retryable` 且 `attempts == MaxAttempts`（默认 5） | 进入死信 |

退避策略（`retry.Backoff`）：`delay = min(Base * Factor^(attempts-1), Max)`，
默认带 full jitter，避免多个任务重试同步风暴。

死信任务保留**可区分的失败原因**：

- `non_retryable`：不可重试的永久失败；
- `retry_limit_reached`：可重试但达到上限；
- 同时保留 `Attempts`（重试次数）与 `LastError`，便于排查。

### 崩溃恢复：在途任务不会永久卡死

两道防线：

1. **启动恢复**：打开快照时，所有遗留的 `inflight` 任务重置为 `pending`
   （上个进程持有的租约随进程死亡而失效）。
2. **租约回收（reaper）**：运行期 worker 卡死/假死时，超过 `LeaseDuration`
   的租约会被周期性回收，任务重新排队。重试次数（attempts）保留，计入上限。

## 任务生命周期

```
pending ──lease──▶ inflight ──成功────────▶ succeeded
   ▲                  │
   │ 退避到期          ├── non_retryable ──────▶ deadletter (non_retryable)
   └──────────────────┤── 达到 MaxAttempts ───▶ deadletter (retry_limit_reached)
                      └── retryable ──┐
   租约过期 / 重启 ◀──────────────────┘
```

## 本地验证

```bash
# 全部单测（含竞态检测）
go test -race -count=1 ./...

# 查看包含任务键、重试次数、判定依据的日志
go test -v ./internal/queue/
go test -v ./internal/store/

# 端到端演示：幂等提交、可重试退避、不可重试死信
go run ./cmd/demo
```

### 测试覆盖的场景

| 测试 | 场景与预期收敛状态 |
| --- | --- |
| `TestEndToEndIdempotentSubmit` | 重复提交 → 1 条任务、1 次下游生效、`succeeded` |
| `TestEndToEndRetryableFailure` | 连续 3 次可重试失败 → 退避后第 4 次成功，`attempts=4` |
| `TestEndToEndNonRetryableFailure` | 不可重试失败 → 立即死信，原因 `non_retryable`，不重试 |
| `TestEndToEndRetryLimit` | 持续可重试失败 → 5 次后死信，原因 `retry_limit_reached` |
| `TestEndToEndPerKeyOrdering` | 同键含失败重试 → 严格按 seq 生效，异键不被阻塞 |
| `TestEndToEndCrashRestart` | 在途崩溃（已生效未确认）→ 重启重投、下游去重、收敛 `succeeded` |
| `TestSnapshotSurvivesRestart` | 快照重启后任务与幂等索引保留，inflight 自动复位 |
| `TestReclaimExpiredLease` | 租约未过期不回收，过期后重新排队 |

单测日志每行包含 `task=<ID> idemp_key=<幂等键> biz_key=<业务键> attempt=<次数>/<上限>
class=<失败分类> decision=<SUCCEEDED|RETRY|DEADLETTER> basis=<判定依据>`，
满足任务键、重试次数与判定依据的可观测要求。

## 说明与边界

- 这是本地模拟实现：持久化用单个 JSON 快照文件，下游为内存组件；
  未实现跨进程多实例竞争（快照文件同一时刻只应由一个队列进程打开）。
- 生产化时可将 `store.Store` / `downstream.Consumer` 接口替换为真实数据库与
  HTTP/gRPC 客户端，队列与判定逻辑无需改动。
