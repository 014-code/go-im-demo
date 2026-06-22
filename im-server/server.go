package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"awesomeProject/proto"
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
	//当前在线用户列表 map
	UserList map[string]*User
	//断线后保留的用户,token -> user,宽限期内可重连
	OfflineList map[string]*User
	//这个 map 的锁
	MapLock *sync.RWMutex
	//广播管道,放已编码的 JSON body(不是 frame,frame 头由 ListenMessage 加)
	BroadcastChan chan []byte
}

/*
*
新建一个服务器对象方法
*/
func NewServer(ip string, port int) *Server {
	return &Server{
		Ip:            ip,
		Port:          port,
		UserList:      make(map[string]*User),
		OfflineList:   make(map[string]*User),
		MapLock:       &sync.RWMutex{},
		BroadcastChan: make(chan []byte),
	}
}

/*
*
生成一个随机 token,作为客户端重连的唯一凭据
8 字节随机数 + hex 编码,碰撞概率可忽略
*/
func genToken() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

/*
*
消息广播到所有在线的用户 chan 中
body 是已经 JSON 编码好的字节(没加帧头)
ListenMessage 收到后分发给每个 user.C
*/
func (this *Server) ListenMessage() {
	for body := range this.BroadcastChan {
		this.MapLock.RLock()
		for _, user := range this.UserList {
			// 慢用户的 chan 满了就跳过,避免阻塞整条广播
			select {
			case user.C <- body:
			default:
			}
		}
		this.MapLock.RUnlock()
	}
}

/*
*
把 Msg 编码后塞进广播管道
from 是发消息的用户,会写到 Msg.From 字段
text 是消息文本
*/
func (this *Server) Broadcast(from *User, text string) {
	body, err := proto.Encode(&proto.Msg{
		Type: proto.TypeMsg,
		From: from.Name,
		Text: text,
		Time: time.Now().Unix(),
	})
	if err != nil {
		return
	}
	//可能阻塞,但听者必须保证不停从 chan 取
	this.BroadcastChan <- body
}

/*
*
广播一条系统消息(join/leave/kick/timeout)
不带 From,带 Reason 和 Text
*/
func (this *Server) BroadcastSystem(from *User, reason, text string) {
	body, err := proto.Encode(&proto.Msg{
		Type:   proto.TypeSystem,
		Reason: reason,
		Text:   text,
		Time:   time.Now().Unix(),
	})
	if err != nil {
		return
	}
	_ = from
	this.BroadcastChan <- body
}

/*
*
新登录: 在 UserList 中注册,生成 token 回写给客户端
name 已存在则拒绝
*/
func (this *Server) loginUser(con net.Conn, name string) *User {
	this.MapLock.Lock()
	defer this.MapLock.Unlock()
	//重名直接拒
	if _, exists := this.UserList[name]; exists {
		_ = proto.WriteMsg(con, &proto.Msg{Type: proto.TypeErr, Text: "名字[" + name + "]已被使用"})
		_ = con.Close()
		return nil
	}
	//建用户对象
	user := NewUser(con, this, name)
	user.Token = genToken()
	this.UserList[name] = user
	//回 ok + token
	_ = proto.WriteMsg(con, &proto.Msg{Type: proto.TypeOK, Text: "login", Token: user.Token})
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
	//从离线列表摘掉
	delete(this.OfflineList, token)
	//替换连接和通道
	user.Conn = con
	user.C = make(chan []byte, 16)
	user.IsLive = make(chan bool)
	user.Done = make(chan struct{})
	//重新放回在线列表
	this.UserList[user.Name] = user
	//启动两个后台 goroutine
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
			//过期的就清掉
			if now.Sub(u.OfflineAt) > offlineGracePeriod {
				delete(this.OfflineList, token)
			}
		}
		this.MapLock.Unlock()
	}
}

/*
*
业务入口: 读首条消息决定 login / reconnect / 匿名
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
	first, err := proto.ReadMsg(con)
	_ = con.SetReadDeadline(time.Time{})
	if err != nil {
		_ = con.Close()
		return
	}

	var user *User
	switch first.Type {
	//显式重连
	case proto.TypeReconnect:
		u := this.reconnectUser(con, first.Token)
		if u == nil {
			_ = proto.WriteMsg(con, &proto.Msg{Type: proto.TypeErr, Text: "reconnect 失败: token 失效或不存在"})
			_ = con.Close()
			return
		}
		_ = proto.WriteMsg(con, &proto.Msg{Type: proto.TypeOK, Text: "reconnect"})
		user = u
		// reconnectUser 内部已启动 goroutine,这里不再启动
	//显式登录
	case proto.TypeLogin:
		if first.Name == "" {
			_ = proto.WriteMsg(con, &proto.Msg{Type: proto.TypeErr, Text: "名字不能为空"})
			_ = con.Close()
			return
		}
		u := this.loginUser(con, first.Name)
		if u == nil {
			return
		}
		user = u
		go user.ListenMessage()
		go user.ListenIsLive()
	//其它当匿名连接处理(用地址当昵称,不生成 token)
	default:
		user = NewUser(con, this, "")
		go user.ListenMessage()
		go user.ListenIsLive()
	}

	//广播上线 + 进入读消息循环
	user.Online()
	go this.ReceiveMessage(con, user)
}

/*
*
接收客户端消息方法
按帧读取,每读一帧就分发到 DoMessage
*/
func (this *Server) ReceiveMessage(con net.Conn, user *User) {
	for {
		m, err := proto.ReadMsg(con)
		if err != nil {
			//EOF 或网络错都算断开
			if err != io.EOF {
				fmt.Println("连接读取错误:", err)
			}
			user.Offline()
			return
		}
		//成功读到一帧,重置踢人计时
		select {
		case user.IsLive <- true:
		default:
		}
		//分发
		user.DoMessage(m)
	}
}

/*
*
开启服务器方法
*/
func (this *Server) Start() {
	les, err := net.Listen("tcp", fmt.Sprintf("%s:%d", this.Ip, this.Port))
	if err != nil {
		fmt.Println("tcp连接建立失败:", err)
		return
	}
	defer les.Close()

	//消息广播 goroutine
	go this.ListenMessage()
	//离线用户清理 goroutine
	go this.cleanupOffline()

	for {
		conn, err := les.Accept()
		if err != nil {
			fmt.Println("accept失败:", err)
			continue
		}
		go this.Handler(conn)
	}
}
