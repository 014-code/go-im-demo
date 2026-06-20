package main

import (
	"fmt"
	"io"
	"net"
	"sync"
)

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
	server := &Server{
		Ip:          ip,
		Port:        port,
		UserList:    make(map[string]*User),
		MessageChan: make(chan string),
	}
	return server
}

/*
*
消息广播到所有在线的用户chan中
*/
func (this *Server) ListenMessage() {
	//如果管道中有数据则需要进行广播
	msg := <-this.MessageChan
	//先上锁
	this.MapLock.Lock()
	//最后释放锁
	defer this.MapLock.Unlock()
	//循环map中的所有用户进行
	for _, user := range this.UserList {
		//向对应用户管道写入
		user.C <- msg
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
* (this *Server)类似这样的把当前类本身的引用对象传入的方法就是这个类本身的方法
业务方法
*/
func (this *Server) Handler(con net.Conn) {
	//%v表示打印这个对象的所有信息类似java-toString
	//fmt.Printf("连接建立成功:%v\n", con)
	//拿到当前用户对象
	user := NewUser(con, this)
	//广播上线消息
	user.Online()

	//接收客户端消息方法
	go this.ReceiveMessage(con, user)
}

/*
*
接收客户端消息方法
*/
func (this *Server) ReceiveMessage(con net.Conn, user *User) {
	//循环读取,单次读取后 return 会导致只能收一条消息
	for {
		//建立缓冲区来存放用户消息
		bytes := make([]byte, 4096)
		//读缓冲区
		n, err := con.Read(bytes)
		//有错误
		if err != nil && err != io.EOF {
			fmt.Println("连接读取错误:", err)
			return
		}
		if n == 0 {
			//如果用户没输入消息就广播下线
			user.Offline()
			return
		}
		//对用户消息去除\n
		msg := string(bytes[:n-1])

		//发送活跃信号,重置踢出定时器
		//用 select+default 避免在 IsLive 没人接收时阻塞(例如已超时被踢)
		select {
		case user.IsLive <- true:
		default:
		}

		//广播发送的消息
		user.DoMessage(msg)
	}
}

/*
*
开启服务器方法
*/
func (this *Server) Start() {
	//先开启一个tcp连接
	les, err := net.Listen("tcp", fmt.Sprintf("%s:%d", this.Ip, this.Port))
	if err != nil {
		fmt.Println("tcp连接建立失败：", err)
	}
	//defer前置关闭该连接
	defer les.Close()
	//消息广播-在业务处理前就去监听，开一个线程来
	go this.ListenMessage()
	//循环连接
	for {
		//Accept连接
		conn, err := les.Accept()
		if err != nil {
			fmt.Println("accept失败：", err)
		}

		//开一个go程不断跑业务方法
		go this.Handler(conn)
	}
}
