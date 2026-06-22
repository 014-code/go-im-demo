// Package proto 定义了 IM 服务端与客户端之间的通信协议
//
// 帧格式:
//
//	[4 字节大端 uint32 = body 长度][body]
//
// body 是 UTF-8 JSON,对应 Msg 结构体
package proto

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// HeaderSize 是帧头大小(4 字节)
const HeaderSize = 4

// MaxFrame 是单帧 body 最大字节数(1MB),防恶意客户端塞大包
const MaxFrame = 1 << 20

/*
*
Msg 是协议层唯一的载荷结构
所有跨连接的数据都打包成 Msg,JSON 序列化后塞进帧里
*/
type Msg struct {
	//消息类型
	Type string `json:"type"`
	//发送方,由服务端在广播时填
	From string `json:"from,omitempty"`
	//私聊目标
	To string `json:"to,omitempty"`
	//文本内容
	Text string `json:"text,omitempty"`
	//会话 token
	Token string `json:"token,omitempty"`
	//登录/改名用的名字
	Name string `json:"name,omitempty"`
	//系统消息原因: join/leave/kick/timeout
	Reason string `json:"reason,omitempty"`
	//who 返回的在线用户列表
	List []string `json:"list,omitempty"`
	//服务端时间戳(秒)
	Time int64 `json:"time,omitempty"`
}

// 消息类型常量
const (
	//客户端 -> 服务端: 登录
	TypeLogin = "login"
	//客户端 -> 服务端: 重连
	TypeReconnect = "reconnect"
	//群发消息
	TypeMsg = "msg"
	//私聊消息
	TypePriv = "priv"
	//系统通知
	TypeSystem = "system"
	//心跳
	TypePing = "ping"
	//心跳应答
	TypePong = "pong"
	//查看在线列表
	TypeWho = "who"
	//修改昵称
	TypeRename = "rename"
	//who 应答,带 List
	TypeUsers = "users"
	//通用成功应答,Content 是描述
	TypeOK = "ok"
	//通用失败应答,Content 是原因
	TypeErr = "err"
)

// 系统消息 reason
const (
	//用户加入
	ReasonJoin = "join"
	//用户离开
	ReasonLeave = "leave"
	//被管理员踢出
	ReasonKick = "kick"
	//活跃超时
	ReasonTimeout = "timeout"
)

/*
*
把 body 写成一帧(4 字节长度 + body)
超出 MaxFrame 直接报错,不会写出半截
*/
func WriteFrame(w io.Writer, body []byte) error {
	if len(body) > MaxFrame {
		return fmt.Errorf("frame too large: %d > %d", len(body), MaxFrame)
	}
	// 大端 4 字节写长度
	var header [HeaderSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(body)))
	// 把头部和 body 一次性写出去,减少系统调用
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

/*
*
从 r 读一帧,返回 body
长度字段超 MaxFrame 直接报错
*/
func ReadFrame(r io.Reader) ([]byte, error) {
	var header [HeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	// 解出 body 长度
	n := binary.BigEndian.Uint32(header[:])
	if n > MaxFrame {
		return nil, fmt.Errorf("frame too large: %d > %d", n, MaxFrame)
	}
	// io.ReadFull 读到 n 字节才返回
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}

/*
*
Msg -> JSON bytes
*/
func Encode(m *Msg) ([]byte, error) {
	return json.Marshal(m)
}

/*
*
JSON bytes -> Msg
*/
func Decode(b []byte) (*Msg, error) {
	var m Msg
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

/*
*
一步到位:编码 Msg 并写成一帧
*/
func WriteMsg(w io.Writer, m *Msg) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return WriteFrame(w, body)
}

/*
*
一步到位:读一帧并解码成 Msg
*/
func ReadMsg(r io.Reader) (*Msg, error) {
	body, err := ReadFrame(r)
	if err != nil {
		return nil, err
	}
	return Decode(body)
}
