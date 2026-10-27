# Codex overload monitor

一个不调用模型的前台小工具：监控本机活跃 Codex 根会话，在
`server_overloaded` 导致一轮结束后，等待指定时间；如果仍没有续跑，
通过 `codex queue` 发送一次 `继续`。

需要 Python 3.10+、支持 `codex queue` 的 Codex CLI，以及 Linux/macOS/Windows。
使用当前用户的本地 Codex 数据目录。已在 Linux 的 Codex CLI 0.160.0 和
Windows 的 0.164.0 数据格式上核对；数据库和 rollout 格式是内部接口，
升级后可能需要适配。

Windows 没有 `fcntl`，改用标准库 `msvcrt` 的字节范围锁。Codex 持有一个
writer lock 时，`msvcrt.locking` 报的是 `EACCES`（`[Errno 13] Permission denied`）,
而不是 `EWOULDBLOCK`，因此“锁已被占用”必须按 errno 判断；否则每个活跃会话
都会被当成致命的权限错误，启动后第一次检查就退出。

## 使用

```sh
# 一个或多个 session UUID
python3 monitor.py --session SESSION_ID --delay 200
python3 monitor.py --session SESSION_A --session SESSION_B --delay 60

# 精确匹配 cwd，包含解析符号链接后的同一路径，不包含子目录
python3 monitor.py --cwd /path/to/workspace --delay 200

# 本机全部活跃根会话；每轮自动发现新会话
python3 monitor.py --all --delay 200

# 只读检查一次：不会发送消息
python3 monitor.py --all --once --dry-run
```

Windows（PowerShell，可用 pyenv-win 的 `python`）同样只是把 `python3` 换成 `python`：

```powershell
python .\monitor.py --all --delay 200
python .\monitor.py --all --once --dry-run
```

三个选择器互斥，必须指定一个。`--poll` 是检查间隔，默认 600 秒（10 分钟）。
`--delay` 是错误发生后等待重试的时间，默认 200 秒；两者独立。
启动时立即检查一次，之后每隔 `--poll` 秒检查；在错误已满 `--delay` 秒后的
第一次检查中发送，通常在 delay 到 delay + poll 秒之间。例如默认配置下，
并不保证在错误后的第 200 秒发送，而是在满 200 秒后的下一次 10 分钟检查中发送。
启动时也会检查尚未解决的旧 overload。Ctrl-C 停止监控。
`--codex-home` 默认使用环境变量 `CODEX_HOME` 或 `~/.codex`；
`--codex` 可指定 Codex 可执行文件（按 PATH 解析；Windows 上 npm 安装的
`codex.cmd` 会经 `cmd.exe` 调用，`codex.exe` 直接调用）。

“活跃”指会话 writer lock 当前被持有；文件存在但无人持锁不算活跃。
只处理未归档的根会话。子 agent 不接受直接输入，本工具不负责其调度。
新 turn、用户输入、正常完成或中断都会取消旧 overload 的续跑资格；
队列已有消息时不发送。只认任务完成事件里的结构化 `error.codex_error_info=server_overloaded`，
或精确错误消息 `Selected model is at capacity. Please try a different model.`；
不会匹配聊天内容里的错误关键词。
Windows 数据库里的 `cwd` 带 `\\?\` 扩展长度前缀，比较前会去掉前缀并统一大小写，
所以 `--cwd E:\code\app` 能匹配到记录为 `\\?\E:\code\app` 的会话。
`codex queue` 的输出按 UTF-8 解码（`errors='replace'`），不受系统代码页影响。

## 如何识别 overload 后未续跑

从 `<codex-home>/state_5.sqlite` 查询目标 session 的 `rollout_path`，
首次读取已有记录，之后只读取新增的完整 JSONL 行。只有下面这种
结束 turn 的结构化事件才登记为待续跑错误：

```json
{
  "type": "event_msg",
  "timestamp": "2026-10-14T04:00:00Z",
  "payload": {
    "type": "task_complete",
    "turn_id": "example-turn-id",
    "error": {
      "codex_error_info": "server_overloaded"
    }
  }
}
```

聊天文本中的错误关键词和执行中的临时错误不会触发发送。
程序按记录顺序更新状态；后续出现 `task_started`、用户输入、
`turn_aborted` 或其他 `task_complete`，就取消旧错误的续跑资格。
用户输入同时从 `user_message` 事件，以及 `response_item` /
`history_mutation` 中的 user message 识别。

检查时，只有等待已满、该错误尚未发送过续跑、队列没有待处理消息的
会话才符合条件；发送前再次读取新增事件并确认 writer lock 仍被持有，
最后调用 `codex queue --thread <session-id> --message 继续`。

## 去重与边界

发送前重新读取事件和检查活跃锁。每个 overload 事件最多发送一次，
新一轮再次 overload 可以再触发；不额外实施熔断或重试次数策略。
去重记录放在 `<codex-home>/overload-monitor/sent.json`。
同一 Codex home 同时只允许运行一个监控实例，防止重复发送。
dry-run 不写入发送记录，但会创建并持有监控工具自身的锁。

发送失败、超时或读取异常时输出错误并退出，不自动重复发送。
队列发送与去重文件写入不能组成原子事务；若进程恰好在发送成功后、
记录落盘前崩溃，重启时存在重复发送的极小窗口。
发送前复查也不能完全消除用户同时手动续跑的竞争窗口。

## 验证

```sh
python3 -m unittest -v        # Windows: python -m unittest -v
```

测试使用临时 Codex 数据库、实际文件锁，并把 `subprocess.run` 换成记录器，
因此不需要真实的 Codex 可执行文件。覆盖目标选择、等待、取消、排队去重、
重启去重、发送、失败退出，以及 Windows 的锁争用（`EACCES` 视为被占用）、
批处理 shim 和 `\\?\` 路径前缀。
