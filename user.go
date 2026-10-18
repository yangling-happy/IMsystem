package main

import "net"

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
func (this * User)Online () {
		this.server.mapLock.Lock()
	this.server.OnlineMap[this.Name] = this
	this.server.mapLock.Unlock()
	this.server.BroadcastMessage(this, "已上线")
}

func (this * User)Offline () {
	this.server.mapLock.Lock()
	delete(this.server.OnlineMap, this.Name)
	this.server.mapLock.Unlock()
	this.server.BroadcastMessage(this, "已下线")
	this.conn.Close()
}

func (this * User) DoMessage(msg string) {
	this.server.BroadcastMessage(this, msg)
}
func (u *User) ListenMessage() {
	for msg := range u.C {
		_, err := u.conn.Write([]byte(msg + "\n"))
		if err != nil {
			return
		}
	}
}