package main

func main() {
	//初始化一个服务对象
	server := NewServer("127.0.0.1", 9999)
	//开启服务器
	server.Start()
}
