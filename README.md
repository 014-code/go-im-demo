# go-im-demo

一个基于 TCP 的简易即时通讯（IM）服务端 Demo，纯 Go 标准库实现。

## 项目结构

```
.
├── go.mod
└── im/
    ├── main.go     // 入口：创建并启动 Server
    ├── server.go   // Server：监听连接、维护在线用户、消息广播
    └── user.go     // User：单连接消息收发、指令解析、活跃检测
```

## 功能

- TCP 长连接
- 群发广播消息
- 在线用户列表查看
- 修改用户名
- 私聊消息
- 活跃超时踢出（默认 60 秒无消息则断开）

## 运行

服务监听在 `127.0.0.1:9999`，启动方式：

```bash
go run ./im
```

或先编译再运行：

```bash
go build -o im ./im
./im
```

## 客户端接入

使用任意 TCP 客户端连接即可，例如：

```bash
# Windows PowerShell
Test-NetConnection 127.0.0.1 -Port 9999

# 或者用 nc / telnet
nc 127.0.0.1 9999
```

连上后默认以客户端地址（`IP:Port`）作为昵称，可通过 `rename|新名字` 改名。

## 指令列表

| 指令 | 说明 |
| --- | --- |
| `who` | 查看当前在线用户 |
| `rename|新名字` | 修改自己的昵称 |
| `to|用户名|消息内容` | 向指定用户私聊 |
| 其他内容 | 群发给所有在线用户 |

## 实现说明

- **连接管理**：`Server.Start()` 通过 `net.Listen` 监听 TCP，每来一个连接就启动一个 goroutine 处理。
- **用户表**：`Server.UserList` 用 `map[string]*User` 存储，配合 `sync.RWMutex` 保护并发读写。
- **消息广播**：`Server.MessageChan` 作为统一的消息通道，`ListenMessage` 单独 goroutine 持续监听并把消息推给每个在线用户的 `User.C`。
- **用户写入**：每个 `User` 都有自己的 `C chan string`，`ListenMessage` 单独 goroutine 从中读取并写回 TCP 连接，避免读写互相阻塞。
- **活跃检测**：`User.IsLive` 通道 + `time.NewTimer(60s)`，每次成功读到客户端消息就发送一次活跃信号重置计时；超时则关闭连接并广播下线。

## 依赖

仅使用 Go 标准库（`net`、`sync`、`fmt`、`time`、`strings` 等），无第三方依赖。

`go.mod`：

```
module awesomeProject

go 1.26
```

## 已知限制

- 目前没有客户端实现，需要自行用 `nc`、Telnet 或其它 TCP 工具测试。
- 消息协议为纯文本以换行结尾，不支持二进制消息。
- 目前没有重连、心跳、鉴权、消息持久化等生产级能力，仅用于学习与演示。
