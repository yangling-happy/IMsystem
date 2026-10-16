package main

import ("net"
"fmt"
"sync")

type Server struct {
	Ip   string
	Port int

	OnlineMap map[string]*User
	mapLock sync.RWMutex

	Message chan string 
}
func (this *Server) ListenMessage() {
	for {
		msg := <-this.Message
		this.mapLock.Lock()
		for _, user := range this.OnlineMap {
			user.C <- msg
		}
		this.mapLock.Unlock()
	}
}
func (this *Server) BroadcastMessage(user *User, msg string) {
	sendMsg :="[" + user.Addr + "]" + user.Name + ":" + msg 
	this.Message <- sendMsg
}

func (this *Server) Handler(conn net.Conn) {
	fmt.Println("New connection from:", conn.RemoteAddr().String()) 
	user := NewUser(conn)
	this.mapLock.Lock()
	this.OnlineMap[user.Name] = user
	this.mapLock.Unlock()

	this.BroadcastMessage(user, "已上线")

	select {}
}
func NewServer(ip string, port int) *Server {
	server := &Server{
		Ip:   ip,
		Port: port,
		OnlineMap: make(map[string]*User),
		Message: make(chan string),
	}
	return server
}

func (this *Server) Start() {
	listener , err := net.Listen("tcp", fmt.Sprintf("%s:%d", this.Ip, this.Port))
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer listener.Close()

	go this.ListenMessage()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go this.Handler(conn)
	}
}