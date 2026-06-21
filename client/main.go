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
)

const (
	serverAddr    = "127.0.0.1:9999"
	heartbeatIntv = 15 * time.Second // 心跳周期,要小于服务端的 60s 活跃超时
	readDeadline  = heartbeatIntv * 3
	minBackoff    = 1 * time.Second
	maxBackoff    = 30 * time.Second
)

var tokenFile = "im_token.txt"

// 存 token 的文件路径,跟可执行文件放同目录
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

func loadToken() string {
	data, err := os.ReadFile(tokenPath())
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func saveToken(t string) error {
	return os.WriteFile(tokenPath(), []byte(t), 0644)
}

func clearToken() {
	_ = os.Remove(tokenPath())
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("昵称: ")
	name, _ := reader.ReadString('\n')
	name = strings.TrimSpace(name)
	if name == "" {
		fmt.Println("昵称不能为空")
		return
	}

	token := loadToken()
	backoff := minBackoff
	for {
		err := runSession(name, token)
		fmt.Printf("\n[client] 连接断开: %v\n", err)

		// 第一次 login 失败就别死循环了
		if err != nil && !shouldReconnect(err) {
			return
		}
		// 拿不到 token 说明 login 失败,直接退出
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

// 哪些错误值得重连
func shouldReconnect(err error) bool {
	if err == nil {
		return true
	}
	s := err.Error()
	// login 失败(login failed / 名字已存在)不重连,其它都重连
	return !strings.Contains(s, "login failed")
}

func runSession(name, token string) error {
	var firstMsg string
	if token != "" {
		firstMsg = "reconnect|" + token
	} else {
		firstMsg = "login|" + name
	}

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// 首条消息: login 或 reconnect
	if _, err := conn.Write([]byte(firstMsg + "\n")); err != nil {
		return err
	}

	// 读首条响应(login ok / reconnect ok / 失败)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("读首条响应失败: %w", err)
	}
	resp := strings.TrimRight(line, "\r\n")
	fmt.Println("[server]", resp)

	if strings.HasPrefix(resp, "login failed") {
		return fmt.Errorf("%s", resp)
	}
	if strings.HasPrefix(resp, "login ok,token=") {
		newToken := strings.TrimPrefix(resp, "login ok,token=")
		if err := saveToken(newToken); err != nil {
			fmt.Println("[client] 保存 token 失败:", err)
		} else {
			fmt.Println("[client] token 已保存到", tokenPath())
		}
		token = newToken
	}
	if strings.HasPrefix(resp, "reconnect failed") {
		// token 失效,清掉,下次走 login
		clearToken()
		token = ""
		return fmt.Errorf("%s", resp)
	}

	// 后续按行读服务端消息
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

// 收消息循环
func readLoop(conn net.Conn, done chan struct{}) {
	reader := bufio.NewReader(conn)
	for {
		// 给读加个比心跳大的超时,心跳断了这里能先发现
		_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("\n[client] 读消息出错:", err)
			select {
			case <-done:
			default:
				signalDone(done)
			}
			return
		}
		_ = conn.SetReadDeadline(time.Time{})
		// 滤掉心跳回包,避免刷屏
		if strings.TrimRight(line, "\r\n") == "pong" {
			continue
		}
		fmt.Print(line)
	}
}

// 心跳循环
func heartbeatLoop(conn net.Conn, done chan struct{}) {
	ticker := time.NewTicker(heartbeatIntv)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			_, err := conn.Write([]byte("ping\n"))
			_ = conn.SetWriteDeadline(time.Time{})
			if err != nil {
				fmt.Println("\n[client] 心跳写入失败:", err)
				signalDone(done)
				return
			}
		}
	}
}

// 读 stdin 发到服务端(不做本地回显,服务端广播会回带回来)
func inputLoop(conn net.Conn, done chan struct{}) {
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println("\n[client] stdin 关闭,准备退出")
			} else {
				fmt.Println("\n[client] 读 stdin 失败:", err)
			}
			signalDone(done)
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err = conn.Write([]byte(line + "\n"))
		_ = conn.SetWriteDeadline(time.Time{})
		if err != nil {
			fmt.Println("\n[client] 发送失败:", err)
			signalDone(done)
			return
		}
	}
}

// 保证只关一次 done
func signalDone(done chan struct{}) {
	select {
	case <-done:
	default:
		close(done)
	}
}
