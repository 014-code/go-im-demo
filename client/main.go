package main

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"awesomeProject/proto"
)

const (
	//服务端地址
	serverAddr = "127.0.0.1:9999"
	//心跳周期,要小于服务端的 60s 活跃超时
	heartbeatIntv = 15 * time.Second
	//读消息超时,给心跳留出三次的余量
	readDeadline = heartbeatIntv * 3
	//重连退避下限
	minBackoff = 1 * time.Second
	//重连退避上限
	maxBackoff = 30 * time.Second
)

// token 本地存储文件名
var tokenFile = "im_token.txt"

/*
*
存 token 的文件路径,跟可执行文件放同目录
*/
func tokenPath() string {
	exe, err := os.Executable()
	if err != nil {
		return tokenFile
	}
	// 去掉文件名只留目录
	i := strings.LastIndex(exe, string(os.PathSeparator))
	if i < 0 {
		return tokenFile
	}
	return exe[:i+1] + tokenFile
}

/*
*
从本地文件读取之前保存的 token
读不到或出错返回空串,等价于首次登录
*/
func loadToken() string {
	data, err := os.ReadFile(tokenPath())
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

/*
*
把服务端下发的 token 写到本地,下次启动时优先用 token 重连
*/
func saveToken(t string) error {
	return os.WriteFile(tokenPath(), []byte(t), 0644)
}

/*
*
删掉本地 token 文件,通常在 token 失效时调用
*/
func clearToken() {
	_ = os.Remove(tokenPath())
}

/*
*
入口: 问昵称,然后循环跑会话,断线自动重连
*/
func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("昵称: ")
	name, _ := reader.ReadString('\n')
	name = strings.TrimSpace(name)
	if name == "" {
		fmt.Println("昵称不能为空")
		return
	}

	// 优先用本地 token 重连
	token := loadToken()
	backoff := minBackoff
	for {
		err := runSession(name, token)
		fmt.Printf("\n[client] 连接断开: %v\n", err)

		// login 失败(名字已存在之类)就别死循环了
		if err != nil && !shouldReconnect(err) {
			return
		}
		// 没拿到 token 说明 login 失败,直接退出
		if token == "" {
			return
		}

		// 指数退避 + 随机抖动
		jitter := time.Duration(rand.Int63n(int64(backoff)))
		sleep := backoff + jitter
		fmt.Printf("[client] %v 后重连...\n", sleep.Round(time.Millisecond))
		time.Sleep(sleep)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

/*
*
判断一个断开原因是否值得再试
只有 login 失败相关的错误不重连
*/
func shouldReconnect(err error) bool {
	if err == nil {
		return true
	}
	s := err.Error()
	return !strings.Contains(s, "login failed")
}

/*
*
单次会话: 拨号 -> 发首条 login/reconnect -> 跑三个 goroutine
任一端断开就返回,交给外层 main 决定是否重连
*/
func runSession(name, token string) error {
	// 根据有没有 token 决定首条消息走 login 还是 reconnect
	var firstMsg *proto.Msg
	if token != "" {
		firstMsg = &proto.Msg{Type: proto.TypeReconnect, Token: token}
	} else {
		firstMsg = &proto.Msg{Type: proto.TypeLogin, Name: name}
	}

	// 拨号连服务端
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// 发首条消息
	if err := proto.WriteMsg(conn, firstMsg); err != nil {
		return err
	}

	// 读首条响应(ok / err)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := proto.ReadMsg(conn)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("读首条响应失败: %w", err)
	}
	fmt.Println("[server]", resp.Type, resp.Text, resp.Token)

	// 失败
	if resp.Type == proto.TypeErr {
		// login 失败把本地 token 也清掉
		if firstMsg.Type == proto.TypeLogin {
			return fmt.Errorf("login failed: %s", resp.Text)
		}
		// reconnect 失败(token 失效)
		if firstMsg.Type == proto.TypeReconnect {
			clearToken()
			token = ""
			return fmt.Errorf("reconnect failed: %s", resp.Text)
		}
		return fmt.Errorf("%s: %s", resp.Type, resp.Text)
	}
	// login 成功,记下新 token
	if resp.Type == proto.TypeOK && resp.Token != "" {
		if err := saveToken(resp.Token); err != nil {
			fmt.Println("[client] 保存 token 失败:", err)
		} else {
			fmt.Println("[client] token 已保存到", tokenPath())
		}
		token = resp.Token
	}

	// 启动三个后台 goroutine
	var wg sync.WaitGroup
	done := make(chan struct{})

	// 收消息
	wg.Add(1)
	go func() {
		defer wg.Done()
		readLoop(conn, done)
	}()

	// 心跳
	wg.Add(1)
	go func() {
		defer wg.Done()
		heartbeatLoop(conn, done)
	}()

	// 读 stdin 发消息
	wg.Add(1)
	go func() {
		defer wg.Done()
		inputLoop(conn, done)
	}()

	// 等任一端先退出
	<-done
	close(done)
	_ = conn.Close()
	wg.Wait()
	return nil
}

/*
*
收消息循环
从 conn 读帧,根据 Type 打印到 stdout
心跳回包 / 错误等也单独提示
*/
func readLoop(conn net.Conn, done chan struct{}) {
	for {
		// 给读加个比心跳大的超时,心跳断了这里能先发现
		_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
		m, err := proto.ReadMsg(conn)
		if err != nil {
			if err == io.EOF {
				fmt.Println("\n[client] 服务端关闭连接")
			} else {
				fmt.Println("\n[client] 读消息出错:", err)
			}
			signalDone(done)
			return
		}
		_ = conn.SetReadDeadline(time.Time{})
		//按类型打印
		render(m)
	}
}

/*
*
把服务端推过来的 Msg 渲染成一行文字
不同类型前缀不一样,方便一眼区分
*/
func render(m *proto.Msg) {
	switch m.Type {
	case proto.TypeMsg:
		// 群发: [时间] alice: 文本
		fmt.Printf("[msg][%s] %s: %s\n", formatTime(m.Time), m.From, m.Text)
	case proto.TypePriv:
		// 私聊
		fmt.Printf("[priv][%s] %s -> 你: %s\n", formatTime(m.Time), m.From, m.Text)
	case proto.TypeSystem:
		// 系统通知(join/leave/kick/timeout)
		fmt.Printf("[system][%s] %s\n", formatTime(m.Time), m.Text)
	case proto.TypeUsers:
		// who 应答,带 List
		fmt.Println("[users]", strings.Join(m.List, ", "))
	case proto.TypeErr:
		// 服务端报错
		fmt.Println("[err]", m.Text)
	case proto.TypeOK:
		// 通用 ok
		fmt.Println("[ok]", m.Text)
	case proto.TypePong:
		// 心跳应答,静默不打印
	default:
		fmt.Printf("[?][%s] %s\n", m.Type, m.Text)
	}
}

/*
*
心跳循环
每隔 heartbeatIntv 给服务端发一个 ping,维持活跃状态
写失败说明连接已死,通知退出
*/
func heartbeatLoop(conn net.Conn, done chan struct{}) {
	ticker := time.NewTicker(heartbeatIntv)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			// 其它 goroutine 触发退出
			return
		case <-ticker.C:
			// 到点发心跳
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			err := proto.WriteMsg(conn, &proto.Msg{Type: proto.TypePing})
			_ = conn.SetWriteDeadline(time.Time{})
			if err != nil {
				fmt.Println("\n[client] 心跳写入失败:", err)
				signalDone(done)
				return
			}
		}
	}
}

/*
*
读 stdin 发到服务端
行内支持简写:

	to|name|text   -> priv
	who            -> who
	rename|newname -> rename
	其它           -> 群发 msg

不做本地回显,服务端广播会回带回来
EOF / 出错都退出整个会话
*/
func inputLoop(conn net.Conn, done chan struct{}) {
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			// Ctrl+D / Ctrl+Z / 终端关闭都会走这里
			if err == io.EOF {
				fmt.Println("\n[client] stdin 关闭,准备退出")
			} else {
				fmt.Println("\n[client] 读 stdin 失败:", err)
			}
			signalDone(done)
			return
		}
		// 去尾巴上的回车换行
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			// 空行忽略
			continue
		}
		// 解析成 Msg
		m := parseInput(line)
		if m == nil {
			fmt.Println("[client] 指令解析失败:", line)
			continue
		}
		// 发到服务端
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		err = proto.WriteMsg(conn, m)
		_ = conn.SetWriteDeadline(time.Time{})
		if err != nil {
			fmt.Println("\n[client] 发送失败:", err)
			signalDone(done)
			return
		}
	}
}

/*
*
把一行用户输入解析成对应的 Msg
支持的简写:

	to|name|text   -> priv
	who            -> who
	rename|newname -> rename
	其它           -> msg

解析不出来返回 nil
*/
func parseInput(line string) *proto.Msg {
	lower := strings.ToLower(line)
	// 私聊
	if strings.HasPrefix(lower, "to|") {
		parts := strings.SplitN(line, "|", 3)
		if len(parts) < 3 {
			return nil
		}
		return &proto.Msg{Type: proto.TypePriv, To: parts[1], Text: parts[2]}
	}
	// 改名
	if strings.HasPrefix(lower, "rename|") {
		parts := strings.SplitN(line, "|", 2)
		if len(parts) < 2 || parts[1] == "" {
			return nil
		}
		return &proto.Msg{Type: proto.TypeRename, Text: parts[1]}
	}
	// 在线列表
	if lower == "who" {
		return &proto.Msg{Type: proto.TypeWho}
	}
	// 默认群发
	return &proto.Msg{Type: proto.TypeMsg, Text: line}
}

/*
*
把 unix 秒转成 HH:MM:SS,空时间用 "--:--:--"
*/
func formatTime(ts int64) string {
	if ts == 0 {
		return "--:--:--"
	}
	return time.Unix(ts, 0).Format("15:04:05")
}

/*
*
幂等关闭 done,防止多次 close 同一 channel 触发 panic
*/
func signalDone(done chan struct{}) {
	select {
	case <-done:
		// 已关闭
	default:
		close(done)
	}
}
