/*
Copyright © 2025 Martin Simango shukomango@gmail.com
*/
package del

import (
	"fmt"

	"github.com/MartinSimango/simple-db/internal/cmd/util"
	"github.com/spf13/cobra"
)

func NewDeleteCmd() *cobra.Command {
	deleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete a key-value pair from the database",
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
			conn.Write(deleteMessage(args[0]))
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				fmt.Println("Error reading server response:", err)
				return
			}
			fmt.Println(string(buf[:n]))
		},
	}

	return deleteCmd
}

func deleteMessage(key string) []byte {
	return []byte(fmt.Sprintf("DELETE\nKey: %s\n\n", key))
}
