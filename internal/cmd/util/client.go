package util

import (
	"fmt"
	"net"

	"github.com/spf13/pflag"
)

func GetAddress(flags *pflag.FlagSet) (string, error) {
	host := flags.Lookup("host").Value.String()
	port, err := flags.GetInt("port")
	if err != nil {
		return "", fmt.Errorf("invalid port: %v", err)
	}

	return net.JoinHostPort(host, fmt.Sprintf("%d", port)), nil
}
