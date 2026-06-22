// 协议层端到端 smoke test
// 启服务端 -> 连两个客户端 -> 跑一轮 login / msg / priv / who / ping / rename
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

// 写一帧(测试用,直接调 proto 里的也行)
func writeFrame(w io.Writer, body []byte) {
	var h [4]byte
	binary.BigEndian.PutUint32(h[:], uint32(len(body)))
	w.Write(h[:])
	w.Write(body)
}

func readFrame(r io.Reader) ([]byte, error) {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(h[:])
	body := make([]byte, n)
	_, err := io.ReadFull(r, body)
	return body, err
}

func writeJSON(w io.Writer, m map[string]any) {
	b, _ := json.Marshal(m)
	writeFrame(w, b)
}

func readJSON(r io.Reader) map[string]any {
	b, err := readFrame(r)
	if err != nil {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil
	}
	return m
}

func mustType(t string, m map[string]any) {
	typ, _ := m["type"].(string)
	if typ != t {
		fmt.Fprintf(os.Stderr, "FAIL: expected type=%s, got %v (full=%v)\n", t, typ, m)
		os.Exit(1)
	}
	fmt.Println("  ok:", typ, m["text"], m["from"], m["list"])
}

func main() {
	// 1. 连 alice
	fmt.Println(">>> alice 连接")
	alice, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial failed:", err)
		os.Exit(1)
	}
	defer alice.Close()
	// 先发 login 帧
	writeJSON(alice, map[string]any{"type": "login", "name": "alice"})
	// 读首条响应(ok + token)
	alice.SetReadDeadline(time.Now().Add(2 * time.Second))
	loginResp := readJSON(alice)
	if loginResp == nil || loginResp["type"] != "ok" || loginResp["text"] != "login" {
		fmt.Fprintln(os.Stderr, "alice login resp 异常:", loginResp)
		os.Exit(1)
	}
	token, _ := loginResp["token"].(string)
	fmt.Println("  ok: login, token =", token)

	// 2. 连 bob
	fmt.Println(">>> bob 连接")
	bob, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial failed:", err)
		os.Exit(1)
	}
	defer bob.Close()
	writeJSON(bob, map[string]any{"type": "login", "name": "bob"})
	bob.SetReadDeadline(time.Now().Add(2 * time.Second))
	loginResp2 := readJSON(bob)
	if loginResp2 == nil || loginResp2["type"] != "ok" {
		fmt.Fprintln(os.Stderr, "bob login resp 异常:", loginResp2)
		os.Exit(1)
	}
	fmt.Println("  ok: login")

	// 3. alice 应该收到 bob 的 join 系统消息
	alice.SetReadDeadline(time.Now().Add(2 * time.Second))
	join := readJSON(alice)
	if join["type"] != "system" || join["reason"] != "join" {
		fmt.Fprintln(os.Stderr, "alice 期望 bob join 系统消息, 拿到:", join)
		os.Exit(1)
	}
	fmt.Println("  ok: bob join")

	// 3.5 bob 也收到了自己加入的系统消息,要消费掉
	bob.SetReadDeadline(time.Now().Add(2 * time.Second))
	bobJoin := readJSON(bob)
	if bobJoin["type"] != "system" || bobJoin["reason"] != "join" {
		fmt.Fprintln(os.Stderr, "bob 期望自己 join 系统消息, 拿到:", bobJoin)
		os.Exit(1)
	}
	fmt.Println("  ok: bob 看到自己 join")

	// 4. alice 发一条群消息
	fmt.Println(">>> alice 发群消息")
	writeJSON(alice, map[string]any{"type": "msg", "text": "hello all"})

	// 5. bob 应该收到 alice 的群消息
	bob.SetReadDeadline(time.Now().Add(2 * time.Second))
	m := readJSON(bob)
	if m["type"] != "msg" || m["from"] != "alice" || m["text"] != "hello all" {
		fmt.Fprintln(os.Stderr, "bob 期望 alice 群消息, 拿到:", m)
		os.Exit(1)
	}
	fmt.Println("  ok: bob 收到群消息")

	// 6. alice 发私聊给 bob
	fmt.Println(">>> alice 私聊 bob")
	writeJSON(alice, map[string]any{"type": "priv", "to": "bob", "text": "secret"})

	// 7. bob 应该收到 alice 的私聊
	bob.SetReadDeadline(time.Now().Add(2 * time.Second))
	m = readJSON(bob)
	if m["type"] != "priv" || m["from"] != "alice" || m["text"] != "secret" {
		fmt.Fprintln(os.Stderr, "bob 期望 alice 私聊, 拿到:", m)
		os.Exit(1)
	}
	fmt.Println("  ok: bob 收到私聊")

	// 8. bob 查在线
	fmt.Println(">>> bob 查在线")
	writeJSON(bob, map[string]any{"type": "who"})
	bob.SetReadDeadline(time.Now().Add(2 * time.Second))
	m = readJSON(bob)
	if m["type"] != "users" {
		fmt.Fprintln(os.Stderr, "bob 期望 users, 拿到:", m)
		os.Exit(1)
	}
	fmt.Println("  ok: 在线列表 =", m["list"])

	// 9. bob 改名
	fmt.Println(">>> bob 改名")
	writeJSON(bob, map[string]any{"type": "rename", "text": "bobby"})
	bob.SetReadDeadline(time.Now().Add(2 * time.Second))
	m = readJSON(bob)
	if m["type"] != "ok" || m["text"] != "rename ok" {
		fmt.Fprintln(os.Stderr, "bob 改名失败:", m)
		os.Exit(1)
	}
	fmt.Println("  ok: 改名成功")

	// 10. 心跳
	fmt.Println(">>> bob 发心跳")
	writeJSON(bob, map[string]any{"type": "ping"})
	bob.SetReadDeadline(time.Now().Add(2 * time.Second))
	m = readJSON(bob)
	if m["type"] != "pong" {
		fmt.Fprintln(os.Stderr, "bob 期望 pong, 拿到:", m)
		os.Exit(1)
	}
	fmt.Println("  ok: pong")

	// 11. 重连测试: 先关 alice,等服务端把她挪到 OfflineList,再拿 token 重连
	fmt.Println(">>> 关闭 alice 准备重连")
	alice.Close()
	// 等待服务端的 ReceiveMessage 读到 EOF,触发 Offline,把 alice 挪到 OfflineList
	time.Sleep(300 * time.Millisecond)

	fmt.Println(">>> 用 token 重连")
	reconn, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		fmt.Fprintln(os.Stderr, "重连 dial 失败:", err)
		os.Exit(1)
	}
	defer reconn.Close()
	writeJSON(reconn, map[string]any{"type": "reconnect", "token": token})
	reconn.SetReadDeadline(time.Now().Add(2 * time.Second))
	m = readJSON(reconn)
	if m["type"] != "ok" || m["text"] != "reconnect" {
		fmt.Fprintln(os.Stderr, "重连失败:", m)
		os.Exit(1)
	}
	fmt.Println("  ok: reconnect")

	// 11.5 alice 重新在线,bob 会收到 leave(关闭触发) + join(重连触发) 两条系统消息
	bob.SetReadDeadline(time.Now().Add(2 * time.Second))
	bobLeave := readJSON(bob)
	if bobLeave["type"] != "system" || bobLeave["reason"] != "leave" {
		fmt.Fprintln(os.Stderr, "bob 期望 alice leave 系统消息, 拿到:", bobLeave)
		os.Exit(1)
	}
	fmt.Println("  ok: bob 看到 alice leave")
	bob.SetReadDeadline(time.Now().Add(2 * time.Second))
	bobJoin2 := readJSON(bob)
	if bobJoin2["type"] != "system" || bobJoin2["reason"] != "join" {
		fmt.Fprintln(os.Stderr, "bob 期望 alice 重连 join, 拿到:", bobJoin2)
		os.Exit(1)
	}
	fmt.Println("  ok: bob 看到 alice 重连 join")

	// 12. 错误路径: 无效 token
	fmt.Println(">>> 无效 token 应该 err")
	bad, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad dial 失败:", err)
		os.Exit(1)
	}
	defer bad.Close()
	writeJSON(bad, map[string]any{"type": "reconnect", "token": "deadbeef"})
	bad.SetReadDeadline(time.Now().Add(2 * time.Second))
	m = readJSON(bad)
	if m["type"] != "err" {
		fmt.Fprintln(os.Stderr, "无效 token 应该 err, 拿到:", m)
		os.Exit(1)
	}
	fmt.Println("  ok: err =", m["text"])

	// 13. 重名登录应该 err
	fmt.Println(">>> 重名登录应该 err")
	dup, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		fmt.Fprintln(os.Stderr, "dup dial 失败:", err)
		os.Exit(1)
	}
	defer dup.Close()
	writeJSON(dup, map[string]any{"type": "login", "name": "alice"})
	dup.SetReadDeadline(time.Now().Add(2 * time.Second))
	m = readJSON(dup)
	if m["type"] != "err" {
		fmt.Fprintln(os.Stderr, "重名应该 err, 拿到:", m)
		os.Exit(1)
	}
	fmt.Println("  ok: err =", m["text"])

	fmt.Println("\nALL TESTS PASSED")
}
