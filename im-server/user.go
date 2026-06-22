package main

import (
	"net"
	"time"

	"awesomeProject/proto"
)

// 活跃超时时间
const activeTimeout = 60 * time.Second

/**
用户实体类
*/

type User struct {
	//昵称
	Name string
	//地址
	Addr string
	//对应的 con 连接
	Conn net.Conn
	//对应的消息 chan 管道,广播时服务端往里塞已编码的 JSON 帧
	C chan []byte
	//用户关联的 server
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
处理一条来自客户端的 Msg
根据 Type 分发到具体的指令逻辑
*/
func (this *User) DoMessage(m *proto.Msg) {
	switch m.Type {
	//心跳: 直接回 pong,不广播不计入聊天
	case proto.TypePing:
		this.Send(&proto.Msg{Type: proto.TypePong, Time: time.Now().Unix()})
	//私聊
	case proto.TypePriv:
		// 目标用户必须存在
		this.server.MapLock.RLock()
		target, ok := this.server.UserList[m.To]
		this.server.MapLock.RUnlock()
		if !ok {
			this.Send(&proto.Msg{Type: proto.TypeErr, Text: "用户[" + m.To + "]不在线"})
			return
		}
		//只推给目标用户,标 From
		target.Send(&proto.Msg{
			Type: proto.TypePriv,
			From: this.Name,
			Text: m.Text,
			Time: time.Now().Unix(),
		})
	//查看在线用户
	case proto.TypeWho:
		// 在锁内拷一份名字列表,锁外再发送
		this.server.MapLock.RLock()
		names := make([]string, 0, len(this.server.UserList))
		for name := range this.server.UserList {
			names = append(names, name)
		}
		this.server.MapLock.RUnlock()
		this.Send(&proto.Msg{Type: proto.TypeUsers, List: names})
	//改名
	case proto.TypeRename:
		newName := m.Text
		this.server.MapLock.Lock()
		defer this.server.MapLock.Unlock()
		//新名字已存在
		if _, exists := this.server.UserList[newName]; exists {
			this.Send(&proto.Msg{Type: proto.TypeErr, Text: "该用户名已被使用"})
			return
		}
		//摘旧名,挂新名
		delete(this.server.UserList, this.Name)
		this.Name = newName
		this.server.UserList[newName] = this
		this.Send(&proto.Msg{Type: proto.TypeOK, Text: "rename ok"})
	//默认群发
	case proto.TypeMsg:
		this.server.Broadcast(this, m.Text)
	//登录/重连/系统等帧不该走到这里,忽略
	default:
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
		C:      make(chan []byte, 16),
		server: server,
		IsLive: make(chan bool),
		Done:   make(chan struct{}),
	}
}

/*
*
上线方法: 把 user 注册到 UserList,广播 join 系统消息
*/
func (this *User) Online() {
	//先注册到在线列表
	this.server.MapLock.Lock()
	this.server.UserList[this.Name] = this
	this.server.MapLock.Unlock()
	//再广播 join,顺序很重要:新用户得先在列表里,后面收到的 who 才会包含自己
	this.server.BroadcastSystem(this, proto.ReasonJoin, this.Name+" 加入了聊天")
}

/*
*
下线方法: 移入 OfflineList 保留一段时间供重连
通过 close(Done) 通知 ListenMessage / ListenIsLive 退出
*/
func (this *User) Offline() {
	//先从在线列表摘掉
	this.server.MapLock.Lock()
	delete(this.server.UserList, this.Name)
	//有 token 的才进宽限期,匿名用户直接释放
	if this.Token != "" {
		this.OfflineAt = time.Now()
		this.server.OfflineList[this.Token] = this
	}
	this.server.MapLock.Unlock()
	//广播 leave 系统消息
	this.server.BroadcastSystem(this, proto.ReasonLeave, this.Name+" 离开了聊天")
	// 关闭 Done,让两个后台 goroutine 退出(防泄漏)
	select {
	case <-this.Done:
		//已经关闭过
	default:
		close(this.Done)
	}
}

/*
*
发一条 Msg 给自己的连接(自动写成一帧)
*/
func (this *User) Send(m *proto.Msg) {
	_ = proto.WriteMsg(this.Conn, m)
}

/*
*
监听当前管道的消息方法
从 C 拿已编码的 JSON 帧,直接写到 conn
*/
func (this *User) ListenMessage() {
	for {
		select {
		case <-this.Done:
			//用户下线,退出
			return
		case body, ok := <-this.C:
			if !ok {
				//chan 关闭,退出
				return
			}
			//把帧写出去
			_ = proto.WriteFrame(this.Conn, body)
		}
	}
}

/*
*
监听用户活跃状态,超时则踢出
每次 ReceiveMessage 成功读到一帧会往 this.IsLive 推一个 true,重置定时器
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
			this.Send(&proto.Msg{Type: proto.TypeSystem, Reason: proto.ReasonTimeout, Text: "你因超时被踢出"})
			_ = this.Conn.Close()
			this.Offline()
			return
		case <-this.Done:
			//用户主动下线,退出
			timer.Stop()
			return
		}
	}
}
