package main

import (
	"fmt"
	"net"
)

func main() {
	// start tcp server
	l, err := net.Listen("tcp", ":5050")
	if err != nil {
		panic(err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			continue
		}

		go func(c net.Conn) {
			defer c.Close()
			fmt.Println("Handling client: ", c.RemoteAddr())
			// for
		}(conn)
	}
}
