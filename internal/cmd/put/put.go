/*
Copyright © 2025 Martin Simango shukomango@gmail.com
*/
package put

import (
	"fmt"
	"log/slog"

	"github.com/MartinSimango/simple-db/internal/cmd/util"
	"github.com/spf13/cobra"
)

func NewPutCmd() *cobra.Command {
	putCmd := &cobra.Command{
		Use:   "put <key> <value>",
		Short: "Add or update a key-value pair in the database",
		Long: `Put stores a key and value into etcd.

Arguments:
  key   - the etcd key
  value - the value to set for the key`,
		Run: func(cmd *cobra.Command, args []string) {

			if len(args) < 2 {
				fmt.Println("Error: missing key")
				cmd.Usage()
				return
			}
			conn, err := util.ConnectToServer(cmd.Flags())
			if err != nil {
				panic(err)
			}
			defer conn.Close()
			conn.Write(putMessage(args[0], args[1]))
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				slog.ErrorContext(cmd.Context(), "Failed to read server response", "error", err)
				return
			}
			fmt.Println(string(buf[:n]))
		},
	}

	return putCmd
}

func putMessage(key, value string) []byte {
	return []byte(fmt.Sprintf("PUT\nKey: %s\nLength: %d\n\n%s", key, len(value), value))
}
