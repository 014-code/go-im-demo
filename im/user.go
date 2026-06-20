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
}

/*
*
发送消息方法，广播给所有用户
支持指令:

	who                查看在线用户
	rename|新名字      修改用户名
	to|用户名|消息内容  私聊指定用户
	其他               群发广播
*/
func (this *User) DoMessage(msg string) {
	lower := strings.ToLower(msg)
	switch {
	//私聊: to|用户名|消息内容
	case strings.HasPrefix(lower, "to|"):
		parts := strings.SplitN(msg, "|", 3)
		if len(parts) < 3 {
			this.Send("私聊格式错误,正确格式: to|用户名|消息内容\n")
			return
		}
		targetName := parts[1]
		content := parts[2]
		//查目标用户
		this.server.MapLock.RLock()
		target, ok := this.server.UserList[targetName]
		this.server.MapLock.RUnlock()
		if !ok {
			this.Send("用户[" + targetName + "]不在线\n")
			return
		}
		//只推给目标用户
		target.Send("[" + this.Name + "][私聊]:" + content + "\n")
	//查看在线用户
	case strings.Contains(lower, "who"):
		this.server.MapLock.RLock()
		defer this.server.MapLock.RUnlock()
		for _, user := range this.server.UserList {
			this.Send("[" + user.Name + "]" + user.Addr + "\n")
		}
	//改名: rename|新名字
	case strings.HasPrefix(lower, "rename|"):
		newName := strings.SplitN(msg, "|", 2)[1]
		this.server.MapLock.Lock()
		defer this.server.MapLock.Unlock()
		//名字已存在
		if _, exists := this.server.UserList[newName]; exists {
			this.Send("该用户名已被使用\n")
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
新建用户对象方法
*/
func NewUser(conn net.Conn, server *Server) *User {
	userAddr := conn.RemoteAddr().String()
	user := &User{
		Name:   userAddr,
		Addr:   userAddr,
		Conn:   conn,
		C:      make(chan string),
		server: server,
		IsLive: make(chan bool),
	}
	//启动线程监听当前用户管道
	go user.ListenMessage()
	return user
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
下线方法
*/
func (this *User) Offline() {

	//加入到map在线列表中
	this.server.MapLock.Lock()
	//从map中移除
	delete(this.server.UserList, this.Name)
	this.server.MapLock.Unlock()
	//将消息进行广播，这里广播下线消息
	this.server.Broadcast(this, "用户已下线")

}

/*
*
给自己客户端发消息
*/
func (this *User) Send(msg string) {
	this.Conn.Write([]byte(msg))
}

/*
*
监听当前管道的消息方法
*/
func (this *User) ListenMessage() {
	for {
		c := <-this.C
		//write
		this.Conn.Write([]byte(c + "\n"))
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
			this.Conn.Close()
			this.Offline()
			return
		}
	}
}
