package main

import (
	"net"
	"strings"
)

type User struct {
	Name string
	Addr string
	C    chan string
	conn net.Conn

	server *Server
}

func NewUser(conn net.Conn, server *Server) *User {
	userAddr := conn.RemoteAddr().String()
	user := &User{
		Name: userAddr,
		Addr: userAddr,
		C:    make(chan string),
		conn: conn,

		server: server,
	}
	go user.ListenMessage()
	return user
}
func (this *User) Online() {
	this.server.mapLock.Lock()
	this.server.OnlineMap[this.Name] = this
	this.server.mapLock.Unlock()
	this.server.BroadcastMessage(this, "已上线")
}

func (this *User) Offline() {
	this.server.mapLock.Lock()
	delete(this.server.OnlineMap, this.Name)
	this.server.mapLock.Unlock()
	this.server.BroadcastMessage(this, "已下线")
	this.conn.Close()
}
func (this *User) SendMessage(msg string) {
	this.conn.Write([]byte(msg))
}
func (this *User) DoMessage(msg string) {

	if msg == "who" {
		this.server.mapLock.Lock()
		for _, user := range this.server.OnlineMap {
			onlineMsg := "[" + user.Addr + "]" + user.Name + ":" + "已上线\n"
			this.C <- onlineMsg
		}
		this.server.mapLock.Unlock()
		return
	} else if len(msg) > 7 && msg[:7] == "rename|" {
		newName := msg[7:]
		_, ok := this.server.OnlineMap[newName]
		if ok {
			this.SendMessage("当前用户名被使用\n")
		} else {
			this.server.mapLock.Lock()
			delete(this.server.OnlineMap, this.Name)
			this.Name = newName
			this.server.OnlineMap[newName] = this
			this.server.mapLock.Unlock()
		}
	} else if len(msg) > 4 && msg[:3] == "to|" {
		remoteName := strings.Split(msg, "|")[1]
		if remoteName == "" {
			this.SendMessage("消息格式不正确，请使用\"to|张三|你好啊\"格式。\n")
			return
		}
		remoteUser, ok := this.server.OnlineMap[remoteName]
		if !ok {
			this.SendMessage("当前用户不存在\n")
			return
		}
content := strings.Split(msg, "|")[2]
		if content == "" {
			this.SendMessage("无消息内容，请重发\n")
			return
		}
		remoteUser.SendMessage(this.Name + "对你说：" + content)
	} else {
		this.server.BroadcastMessage(this, msg)
	}
}
func (u *User) ListenMessage() {
	for msg := range u.C {
		_, err := u.conn.Write([]byte(msg + "\n"))
		if err != nil {
			return
		}
	}
}
