# go-im-demo

一个基于 TCP 的简易即时通讯（IM）Demo，纯 Go 标准库实现。

服务端在 `127.0.0.1:9999` 监听 TCP，自带 Go 写的命令行客户端，客户端支持心跳和断线重连。

## 项目结构

```
.
├── go.mod
├── im-server/           // 服务端
│   ├── main.go          // 入口：创建并启动 Server
│   ├── server.go        // Server：监听连接、用户管理、消息广播、心跳、重连
│   └── user.go          // User：单连接消息收发、指令解析、活跃超时
└── client/              // 命令行客户端
    └── main.go          // 心跳 + 指数退避重连 + token 本地保存
```

## 功能

- TCP 长连接 + TCP keepalive
- 群发广播消息
- 在线用户列表查看
- 修改用户名
- 私聊消息
- 活跃超时踢出（默认 60 秒无消息则断开）
- 应用层心跳（`ping` / `pong`）
- 基于 token 的断线重连（默认 60 秒宽限期内可用 `reconnect|token` 恢复会话）

## 运行

### 1. 启动服务端

```bash
go run ./im-server
```

或先编译：

```bash
go build -o im-server.exe ./im-server
./im-server.exe
```

### 2. 启动客户端

开一个终端：

```bash
go run ./client
```

首次启动会提示输入昵称，连接成功后服务端回一行 `login ok,token=xxxxxxxx`，客户端把 token 保存到同目录的 `im_token.txt`，下次启动会自动用 `reconnect|token` 恢复会话。

也可以同时跑多个客户端实例测试群聊：

```bash
go run ./client   # 终端 1,昵称 alice
go run ./client   # 终端 2,昵称 bob
```

### 3. 用裸 TCP 客户端调试

不想用自带客户端的话，`nc` / telnet 也行，但需要手动发首条消息（决定走 `login` / `reconnect` / 匿名）：

```bash
# 匿名接入(以 IP:Port 当昵称,不能重连)
nc 127.0.0.1 9999
```

## 协议

每条消息以 `\n` 结尾的纯文本。首条消息决定会话模式：

| 首条消息 | 行为 |
| --- | --- |
| `login|昵称` | 注册昵称,服务端生成 token 回写 `login ok,token=xxx` |
| `reconnect|token` | 恢复已存在的会话(宽限期内),回写 `reconnect ok` |
| 其它 | 匿名模式,以地址当昵称,不能重连 |

首条消息之后，普通消息按下面的指令解析：

| 指令 | 说明 |
| --- | --- |
| `ping` | 心跳,服务端回 `pong`,不广播 |
| `who` | 查看当前在线用户 |
| `rename|新名字` | 修改自己的昵称 |
| `to|用户名|消息内容` | 向指定用户私聊 |
| 其它 | 群发给所有在线用户 |

## 实现说明

- **连接管理**：`Server.Start()` 通过 `net.Listen` 监听 TCP，每来一个连接就启动一个 goroutine 跑 `Handler`。
- **首条消息路由**：`Handler` 先读一条带 5s 超时的消息，决定走 `login` / `reconnect` / 匿名，再进入正常的读消息循环。
- **用户表**：`Server.UserList map[string]*User` 存当前在线用户，`Server.OfflineList map[string]*User` 存断线但还在宽限期内的用户（key 是 token），`sync.RWMutex` 保护并发读写。
- **消息广播**：`Server.MessageChan` 是统一的消息通道，`ListenMessage` 单 goroutine 持续监听并把消息推给每个在线用户的 `User.C`，发送用 `select { case ...: default: }` 避免慢用户阻塞整条广播。
- **用户写入**：每个 `User` 都有自己的 `C chan string` 和 `Done chan struct{}`，`ListenMessage` 用 `select` 同时监听 `C` 和 `Done`，`Offline` 时关闭 `Done` 让 goroutine 干净退出。
- **活跃检测**：`User.IsLive` 通道 + `time.NewTimer(60s)`，每次成功读到消息就重置计时；超时则关闭连接并广播下线。客户端主动发 `ping` 同样会重置计时。
- **心跳**：客户端 `client/main.go` 起一个 goroutine，每 15s 发一个 `ping`；服务端 `DoMessage` 把 `ping` 单独处理为回 `pong`，不广播。
- **重连**：服务端在 `Offline` 时把有 token 的用户挪到 `OfflineList` 并打上 `OfflineAt` 时间戳；`cleanupOffline` 每 10s 扫一次，清理超过 60s 的。客户端用 `bufio` 按行读，读到 `EOF` / 错误就触发重连；用本地 `im_token.txt` 存 token，token 失效（`reconnect failed`）会自动清掉，下次走 `login`。
- **退避**：重连间隔 1s → 2s → 4s → 8s ... 封顶 30s，每次叠加随机抖动避免雪崩。
- **TCP keepalive**：`Handler` 里 `SetKeepAlive(true)` + `SetKeepAlivePeriod(30s)`，防止中间链路/NAT 把死连接一直留着。

## 依赖

仅使用 Go 标准库（`net`、`sync`、`fmt`、`time`、`strings`、`crypto/rand`、`encoding/hex`、`bufio`、`os` 等），无第三方依赖。

`go.mod`：

```
module awesomeProject

go 1.26
```

## 已知限制

- 消息协议为纯文本以 `\n` 结尾，不支持二进制消息。
- 没有鉴权、消息持久化、离线消息推送等生产级能力，仅用于学习与演示。
- token 用本地明文文件保存，生产环境应加密或换成正经的鉴权方案。
- 服务端单进程，水平扩展需要外部存储来共享 `UserList` / `OfflineList`。
