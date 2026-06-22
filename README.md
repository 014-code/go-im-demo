# go-im-demo

一个基于 TCP 的简易即时通讯（IM）Demo，纯 Go 标准库实现。

服务端在 `127.0.0.1:9999` 监听 TCP，自带 Go 写的命令行客户端，客户端支持心跳和断线重连。

## 项目结构

```
.
├── go.mod
├── proto/               // 协议层
│   └── proto.go         // Msg 结构、JSON 编解码、4 字节长度前缀帧
├── im-server/           // 服务端
│   ├── main.go          // 入口:创建并启动 Server
│   ├── server.go        // Server:监听连接、用户管理、消息广播、心跳、重连
│   └── user.go          // User:单连接消息收发、指令解析、活跃超时
├── client/              // 命令行客户端
│   └── main.go          // 心跳 + 指数退避重连 + token 本地保存
└── smoketest/           // 端到端冒烟测试
    └── main.go          // 验证协议层与各业务流的正确性
```

## 功能

- TCP 长连接 + TCP keepalive
- 群发广播消息
- 在线用户列表查看
- 修改用户名
- 私聊消息
- 活跃超时踢出（默认 60 秒无消息则断开）
- 应用层心跳（`ping` / `pong`）
- 基于 token 的断线重连(默认 60 秒宽限期内可用 `reconnect` 消息带 token 恢复会话)

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

不想用自带客户端的话,自己用 `nc` / `telnet` 也可以,但因为协议是 4 字节长度头 + JSON 体,得自己拼帧,直接发裸 `login|alice\n` 是不会工作的。

最省事的调试方式就是用 `smoketest` 冒烟测试,它按帧收发,覆盖了 login / 群聊 / 私聊 / 改昵称 / 心跳 / 重连等全部路径:

```bash
# 终端 1: 启服务端
./im-server.exe

# 终端 2: 跑冒烟(会自动起两个虚拟客户端走完整流程)
go run ./smoketest
```

正常会输出 `ALL TESTS PASSED`。

如果想自己用 Go 写一个最简客户端参考,看 `smoketest/main.go` 里的 `writeFrame` / `readFrame` 即可。

## 协议

使用基于 JSON 的二进制帧协议,服务端和客户端共享 `proto/proto.go`。

### 帧格式

每个帧由 4 字节大端长度头 + JSON 消息体组成:

```
+--------+----------------+
|  4B    |  N 字节        |
| Length | JSON Body      |
+--------+----------------+
```

- `Length`: 消息体字节数(大端 uint32),合法范围 `1 ~ 64KB`
- `Body`: 任意 UTF-8 JSON

设计原因:TCP 是字节流,需要明确边界。`\n` 协议在消息体里出现换行时会断错;用长度前缀后,JSON 里可以包含任意字符,不会有歧义,也方便以后扩展二进制字段。

### 消息结构

`proto.Msg` 用 `omitempty` 序列化,字段按需出现:

| JSON 字段 | 类型   | 含义 |
| --- | --- | --- |
| `type`  | string | 消息类型,见下表 |
| `from`  | string | 发送方昵称(服务端填充) |
| `to`    | string | 私聊目标昵称 |
| `text`  | string | 文本内容 |
| `token` | string | 会话 token,用于重连 |
| `name`  | string | 登录昵称 |
| `reason`| string | 系统消息原因,如 `join` / `leave` / `kick` |
| `list`  | []string | 在线用户列表 |
| `time`  | int64  | 消息时间戳(秒) |

### 消息类型

| `type` | 方向 | 说明 |
| --- | --- | --- |
| `login`     | C→S | 首条消息,带 `name` 登录 |
| `reconnect` | C→S | 首条消息,带 `token` 恢复会话 |
| `ping`      | C→S | 心跳,服务端回 `pong` 不广播 |
| `pong`      | S→C | 心跳应答 |
| `msg`       | C↔S | 群发/接收的群消息 |
| `priv`      | C↔S | 私聊 |
| `who`       | C→S | 查询在线列表 |
| `users`     | S→C | 在线列表响应,`list` 字段带名字数组 |
| `rename`    | C→S | 改名,`text` 是新名字 |
| `system`    | S→C | 系统通知(加入/离开/踢出),`reason` 标识原因 |
| `ok`        | S→C | 操作成功,`text` 是说明 |
| `err`       | S→C | 操作失败,`text` 是原因 |

### 握手流程

客户端连上之后**第一条**消息必须是 `login` 或 `reconnect`,决定会话模式:

- 发送 `{"type":"login","name":"alice"}` → 服务端回 `{"type":"ok","text":"login","token":"xxx"}`,此后该连接属于 `alice`
- 发送 `{"type":"reconnect","token":"xxx"}` → 服务端从 `OfflineList` 找回原会话,回 `{"type":"ok","text":"reconnect"}`
- 其它首条消息:进入匿名模式,以 `IP:Port` 作为昵称,不可重连

服务端在首条消息上设了 5 秒读超时,超过未读到合法握手会主动断开。

### 帧读写

`proto` 包封装了帧编解码:

- `WriteMsg(w, m)`:把 `*Msg` 序列化为 JSON 帧写入 `w`
- `ReadMsg(r)`:从 `r` 读一帧,反序列化为 `*Msg`
- 内部用 `io.ReadFull` 按 4 字节头读取长度,再读指定长度的 body,绝不会被消息体里的特殊字符干扰
- 单帧最大 64KB,超过直接报错断连,避免恶意大包撑爆内存

## 实现说明

- **协议层**:`proto/proto.go` 定义 `Msg` 结构、`WriteFrame` / `ReadFrame` 帧读写、以及 `WriteMsg` / `ReadMsg` 业务封装。所有网络进出都走这里,服务端和客户端共用一份。
- **连接管理**:`Server.Start()` 通过 `net.Listen` 监听 TCP,每来一个连接就启动一个 goroutine 跑 `Handler`。
- **首条消息路由**:`Handler` 用 `proto.ReadMsg` 读首条握手消息(5s 超时),根据 `type` 决定走 `login` / `reconnect` / 匿名,再进入正常的读消息循环。
- **用户表**:`Server.UserList map[string]*User` 存当前在线用户,`Server.OfflineList map[string]*User` 存断线但还在宽限期内的用户(key 是 token),`sync.RWMutex` 保护并发读写。
- **消息广播**:`Server.BroadcastChan` 是统一的广播通道,`BroadcastChan` 推送编码后的 JSON body,`ListenMessage` 单 goroutine 持续监听并把消息分发给每个在线用户,发送用 `select { case ...: default: }` 避免慢用户阻塞整条广播。
- **用户写入**:每个 `User` 都有自己的 `C chan []byte` 和 `Done chan struct{}`,`ListenMessage` 用 `select` 同时监听 `C` 和 `Done`,`Offline` 时关闭 `Done` 让 goroutine 干净退出。
- **活跃检测**:`User.IsLive` 通道 + `time.NewTimer(60s)`,每次成功读到消息就重置计时;超时则关闭连接并广播下线。客户端主动发 `ping` 同样会重置计时。
- **心跳**:客户端 `client/main.go` 起一个 goroutine,每 15s 发一个 `{"type":"ping"}`;服务端 `DoMessage` 把 `ping` 单独处理为回 `{"type":"pong"}`,不广播。
- **重连**:服务端在 `Offline` 时把有 token 的用户挪到 `OfflineList` 并打上 `OfflineAt` 时间戳;`cleanupOffline` 每 10s 扫一次,清理超过 60s 的。客户端断线后用本地 `im_token.txt` 里的 token 发 `{"type":"reconnect","token":"xxx"}`;token 失效(收到 `err`)会自动清掉,下次走 `login`。
- **退避**:重连间隔 1s → 2s → 4s → 8s ... 封顶 30s,每次叠加随机抖动避免雪崩。
- **TCP keepalive**:`Handler` 里 `SetKeepAlive(true)` + `SetKeepAlivePeriod(30s)`,防止中间链路/NAT 把死连接一直留着。

## 测试

`smoketest/main.go` 是一个端到端冒烟测试,自己起两个虚拟客户端走完整流程:

1. alice 登录拿到 token
2. bob 登录
3. alice / bob 各自消费自己的 join 系统消息
4. alice 发群消息,bob 收到
5. alice 私聊 bob,bob 收到
6. bob 查在线列表,确认有 alice
7. bob 改名,确认成功
8. bob 发心跳,确认收到 pong
9. alice 关闭连接,bob 收到 leave
10. 用 token 重连 alice,bob 收到 join
11. 用无效 token 重连,确认收到 err
12. 用已被占用的名字登录,确认收到 err

跑法:

```bash
go build -o im-server.exe ./im-server
go build -o smoketest.exe ./smoketest
./im-server.exe &
./smoketest.exe
```

预期最后一行输出 `ALL TESTS PASSED`。

## 依赖

仅使用 Go 标准库（`net`、`sync`、`fmt`、`time`、`strings`、`crypto/rand`、`encoding/hex`、`bufio`、`os` 等），无第三方依赖。

`go.mod`：

```
module awesomeProject

go 1.26
```

## 已知限制

- 单帧上限 64KB,大消息(文件/图片)需要分片或走单独的传输通道。
- 没有鉴权、消息持久化、离线消息推送等生产级能力,仅用于学习与演示。
- token 用本地明文文件保存,生产环境应加密或换成正经的鉴权方案。
- 服务端单进程,水平扩展需要外部存储来共享 `UserList` / `OfflineList`。
