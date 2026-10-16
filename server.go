package main

import ("net"
"fmt")

type Server struct {
	Ip   string
	Port int
}
func (this *Server) Handler(conn net.Conn) {
	fmt.Println("New connection from:", conn.RemoteAddr().String()) 
}
func NewServer(ip string, port int) *Server {
	server := &Server{
		Ip:   ip,
		Port: port,
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

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go this.Handler(conn)
	}
}