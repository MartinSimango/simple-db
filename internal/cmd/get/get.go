/*
Copyright © 2025 Martin Simango shukomango@gmail.com
*/
package get

import (
	"fmt"

	"github.com/MartinSimango/simple-db/internal/cmd/util"
	"github.com/spf13/cobra"
)

func NewGetCmd() *cobra.Command {
	getCmd := &cobra.Command{
		Use:   "get",
		Short: "Retrieve a value by key from the database",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				fmt.Println("Error: missing key")
				cmd.Usage()
				return
			}
			conn, err := util.ConnectToServer(cmd.Flags())
			if err != nil {
				panic(err)
			}
			defer conn.Close()
			conn.Write(getMessage(args[0]))
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				fmt.Println("Error reading server response:", err)
				return
			}
			fmt.Println(string(buf[:n]))
		},
	}

	return getCmd
}

func getMessage(key string) []byte {
	return []byte(fmt.Sprintf("GET\nKey: %s\n\n", key))
}
