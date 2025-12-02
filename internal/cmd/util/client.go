package util

import (
	"fmt"
	"net"

	"github.com/spf13/pflag"
)

func ConnectToServer(flags *pflag.FlagSet) (net.Conn, error) {
	host := flags.Lookup("host").Value.String()
	port, err := flags.GetInt("port")
	if err != nil {
		panic(err)
	}

	return net.Dial("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
}
