package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// 离线宽限期: 断开后用户对象在 OfflineList 保留的时间,期间可用 token 重连
const offlineGracePeriod = 60 * time.Second

// 清理过期离线用户的间隔
const cleanInterval = 10 * time.Second

// TCP keepalive 周期
const kaPeriod = 30 * time.Second

// 等待客户端首条消息的超时
const firstMsgTimeout = 5 * time.Second

/**
服务器类
*/

type Server struct {
	//ip
	Ip string
	//端口
	Port int
	//当前在线用户列表map
	UserList map[string]*User
	//断线后保留的用户,token -> user,宽限期内可重连
	OfflineList map[string]*User
	//这个map的锁
	MapLock *sync.RWMutex
	//用来广播的消息管道
	MessageChan chan string
}

/*
*
新建一个服务器对象方法
*/
func NewServer(ip string, port int) *Server {
	return &Server{
		Ip:          ip,
		Port:        port,
		UserList:    make(map[string]*User),
		OfflineList: make(map[string]*User),
		MapLock:     &sync.RWMutex{},
		MessageChan: make(chan string),
	}
}

// 生成一个随机 token
func genToken() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

/*
*
消息广播到所有在线的用户chan中
*/
func (this *Server) ListenMessage() {
	for {
		msg := <-this.MessageChan
		this.MapLock.Lock()
		for _, user := range this.UserList {
			// 慢用户的 chan 满了就跳过,避免阻塞整条广播
			select {
			case user.C <- msg:
			default:
			}
		}
		this.MapLock.Unlock()
	}
}

/*
*
广播消息方法
*/
func (this *Server) Broadcast(user *User, msg string) {
	//拿到当前发消息的用户
	message := "[" + user.Addr + "]" + user.Name + ":" + msg
	//把这个消息给到消息广播器
	this.MessageChan <- message
}

/*
*
新登录: 在 UserList 中注册,生成 token 回写给客户端
name 已存在则拒绝
*/
func (this *Server) loginUser(con net.Conn, name string) *User {
	this.MapLock.Lock()
	defer this.MapLock.Unlock()
	if _, exists := this.UserList[name]; exists {
		_, _ = con.Write([]byte("login failed: 名字[" + name + "]已被使用\n"))
		_ = con.Close()
		return nil
	}
	user := NewUser(con, this, name)
	user.Token = genToken()
	this.UserList[name] = user
	_, _ = con.Write([]byte("login ok,token=" + user.Token + "\n"))
	return user
}

/*
*
重连: 在 OfflineList 中按 token 找回 user,绑到新连接上
同时重建 C / IsLive / Done 并启动对应的 goroutine
*/
func (this *Server) reconnectUser(con net.Conn, token string) *User {
	this.MapLock.Lock()
	defer this.MapLock.Unlock()
	user, ok := this.OfflineList[token]
	if !ok {
		return nil
	}
	delete(this.OfflineList, token)
	// 替换连接和通道
	user.Conn = con
	user.C = make(chan string)
	user.IsLive = make(chan bool)
	user.Done = make(chan struct{})
	// 重新放回在线列表
	this.UserList[user.Name] = user
	// 启动两个后台 goroutine
	go user.ListenMessage()
	go user.ListenIsLive()
	return user
}

/*
*
定期清理超过宽限期的离线用户
*/
func (this *Server) cleanupOffline() {
	ticker := time.NewTicker(cleanInterval)
	defer ticker.Stop()
	for now := range ticker.C {
		this.MapLock.Lock()
		for token, u := range this.OfflineList {
			if now.Sub(u.OfflineAt) > offlineGracePeriod {
				delete(this.OfflineList, token)
			}
		}
		this.MapLock.Unlock()
	}
}

/*
*
业务入口: 读首条消息决定 login / reconnect / 匿名,
然后再进入正常的读消息循环
*/
func (this *Server) Handler(con net.Conn) {
	// 打开 TCP keepalive,防止中间链路把死连接留着
	if tcp, ok := con.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(kaPeriod)
	}

	// 读首条消息(给个超时,避免慢连接占资源)
	_ = con.SetReadDeadline(time.Now().Add(firstMsgTimeout))
	buf := make([]byte, 4096)
	n, err := con.Read(buf)
	_ = con.SetReadDeadline(time.Time{})
	if err != nil || n == 0 {
		_ = con.Close()
		return
	}
	first := strings.TrimRight(string(buf[:n]), "\r\n")

	var user *User
	lower := strings.ToLower(first)
	switch {
	case strings.HasPrefix(lower, "reconnect|"):
		token := strings.SplitN(first, "|", 2)[1]
		u := this.reconnectUser(con, token)
		if u == nil {
			_, _ = con.Write([]byte("reconnect failed: token 失效或不存在\n"))
			_ = con.Close()
			return
		}
		_, _ = con.Write([]byte("reconnect ok\n"))
		user = u
		// reconnectUser 内部已启动 goroutine,这里不再启动
	case strings.HasPrefix(lower, "login|"):
		name := strings.SplitN(first, "|", 2)[1]
		if name == "" {
			_, _ = con.Write([]byte("login failed: 名字不能为空\n"))
			_ = con.Close()
			return
		}
		u := this.loginUser(con, name)
		if u == nil {
			return // loginUser 已经回写错误并 close
		}
		user = u
		go user.ListenMessage()
		go user.ListenIsLive()
	default:
		// 兼容旧的匿名连接(用地址当昵称,不生成 token)
		user = NewUser(con, this, "")
		go user.ListenMessage()
		go user.ListenIsLive()
	}

	user.Online()
	go this.ReceiveMessage(con, user)
}

/*
*
接收客户端消息方法
*/
func (this *Server) ReceiveMessage(con net.Conn, user *User) {
	for {
		bytes := make([]byte, 4096)
		n, err := con.Read(bytes)
		if err != nil && err != io.EOF {
			fmt.Println("连接读取错误:", err)
			user.Offline()
			return
		}
		if n == 0 {
			user.Offline()
			return
		}
		//对用户消息去除\n
		msg := string(bytes[:n-1])

		//重置踢出定时器
		select {
		case user.IsLive <- true:
		default:
		}

		user.DoMessage(msg)
	}
}

/*
*
开启服务器方法
*/
func (this *Server) Start() {
	les, err := net.Listen("tcp", fmt.Sprintf("%s:%d", this.Ip, this.Port))
	if err != nil {
		fmt.Println("tcp连接建立失败：", err)
		return
	}
	defer les.Close()

	// 消息广播 goroutine
	go this.ListenMessage()
	// 离线用户清理 goroutine
	go this.cleanupOffline()

	for {
		conn, err := les.Accept()
		if err != nil {
			fmt.Println("accept失败：", err)
			continue
		}
		go this.Handler(conn)
	}
}
