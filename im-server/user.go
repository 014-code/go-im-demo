package main

import (
	"net"
	"strings"
	"time"
)

// 活跃超时时间
const activeTimeout = 60 * time.Second

/**
用户实体类
*/

type User struct {
	Name string
	//地址
	Addr string
	//对应的con连接
	Conn net.Conn
	//对应的消息canl管道
	C chan string
	//用户关联的server
	server *Server
	//当前用户是否活跃
	IsLive chan bool
	//登录后分配的 token,用于断线重连
	Token string
	//进入 OfflineList 的时间,用于宽限期清理
	OfflineAt time.Time
	//用于让 ListenMessage / ListenIsLive 退出的信号
	Done chan struct{}
}

/*
*
发送消息方法，广播给所有用户
支持指令:

	ping               心跳,服务端回 pong,不广播
	who                查看在线用户
	rename|新名字      修改用户名
	to|用户名|消息内容  私聊指定用户
	其他               群发广播
*/
func (this *User) DoMessage(msg string) {
	lower := strings.ToLower(msg)
	switch {
	//心跳: 直接回 pong,不广播不计入聊天
	case lower == "ping":
		this.Send("pong")
		return
	//私聊: to|用户名|消息内容
	case strings.HasPrefix(lower, "to|"):
		parts := strings.SplitN(msg, "|", 3)
		if len(parts) < 3 {
			this.Send("私聊格式错误,正确格式: to|用户名|消息内容")
			return
		}
		targetName := parts[1]
		content := parts[2]
		//查目标用户
		this.server.MapLock.RLock()
		target, ok := this.server.UserList[targetName]
		this.server.MapLock.RUnlock()
		if !ok {
			this.Send("用户[" + targetName + "]不在线")
			return
		}
		//只推给目标用户
		target.Send("[" + this.Name + "][私聊]:" + content)
	//查看在线用户
	case strings.Contains(lower, "who"):
		this.server.MapLock.RLock()
		defer this.server.MapLock.RUnlock()
		for _, user := range this.server.UserList {
			this.Send("[" + user.Name + "]" + user.Addr)
		}
	//改名: rename|新名字
	case strings.HasPrefix(lower, "rename|"):
		newName := strings.SplitN(msg, "|", 2)[1]
		this.server.MapLock.Lock()
		defer this.server.MapLock.Unlock()
		//名字已存在
		if _, exists := this.server.UserList[newName]; exists {
			this.Send("该用户名已被使用")
			return
		}
		//改名
		delete(this.server.UserList, this.Name)
		this.Name = newName
		this.server.UserList[newName] = this
	//默认群发
	default:
		this.server.Broadcast(this, msg)
	}
}

/*
*
新建用户对象方法(不再自动启动 goroutine,由调用方决定)
name 为空时用远端地址作为昵称
*/
func NewUser(conn net.Conn, server *Server, name string) *User {
	addr := conn.RemoteAddr().String()
	if name == "" {
		name = addr
	}
	return &User{
		Name:   name,
		Addr:   addr,
		Conn:   conn,
		C:      make(chan string),
		server: server,
		IsLive: make(chan bool),
		Done:   make(chan struct{}),
	}
}

/*
*
上线方法
*/
func (this *User) Online() {
	//加入到map在线列表中
	this.server.MapLock.Lock()
	this.server.UserList[this.Name] = this
	this.server.MapLock.Unlock()
	//将消息进行广播，这里广播上线消息
	this.server.Broadcast(this, "用户已上线")
}

/*
*
下线方法: 移入 OfflineList 保留一段时间供重连
通过 close(Done) 通知 ListenMessage / ListenIsLive 退出
*/
func (this *User) Offline() {
	this.server.MapLock.Lock()
	delete(this.server.UserList, this.Name)
	if this.Token != "" {
		this.OfflineAt = time.Now()
		this.server.OfflineList[this.Token] = this
	}
	this.server.MapLock.Unlock()
	this.server.Broadcast(this, "用户已下线")
	// 关闭 Done,让两个后台 goroutine 退出(防泄漏)
	select {
	case <-this.Done:
		// 已经关闭过
	default:
		close(this.Done)
	}
}

/*
*
给自己客户端发消息(自动加 \n)
*/
func (this *User) Send(msg string) {
	_, _ = this.Conn.Write([]byte(msg + "\n"))
}

/*
*
监听当前管道的消息方法
*/
func (this *User) ListenMessage() {
	for {
		select {
		case <-this.Done:
			return
		case msg := <-this.C:
			_, _ = this.Conn.Write([]byte(msg + "\n"))
		}
	}
}

/*
*
监听用户活跃状态,超时则踢出
每次 ReceiveMessage 成功读到消息会往 this.IsLive 推一个 true,重置定时器
如果在 activeTimeout 内没有收到信号,则强制关闭连接并广播下线
*/
func (this *User) ListenIsLive() {
	for {
		timer := time.NewTimer(activeTimeout)
		select {
		case <-this.IsLive:
			//收到活跃信号,停止本次计时,继续下一轮
			timer.Stop()
		case <-timer.C:
			//超时未活跃,踢人
			this.Send("你因超时被踢出")
			_ = this.Conn.Close()
			this.Offline()
			return
		case <-this.Done:
			timer.Stop()
			return
		}
	}
}
