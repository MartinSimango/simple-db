/*
Copyright © 2025 Martin Simango shukomango@gmail.com
*/
package put

import (
	"fmt"

	"github.com/MartinSimango/simple-db/internal/cmd/util"
	"github.com/MartinSimango/simple-db/pkg/db"
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
				fmt.Println("ERROR: missing key")
				cmd.Usage()
				return
			}
			address, err := util.GetAddress(cmd.Flags())
			if err != nil {
				fmt.Println("ERROR: invalid address:", err)
				return
			}
			client, err := db.NewSimpleDbClient(address)
			if err != nil {
				fmt.Println("ERROR: failed to connect to server:", err)
				return
			}
			defer client.Close()
			r, err := client.Put(args[0], args[1])
			if err != nil {
				fmt.Println("ERROR:", err)
				return
			}
			fmt.Println(r)
		},
	}

	return putCmd
}

func putMessage(key, value string) []byte {
	return []byte(fmt.Sprintf("PUT\nKey: %s\nLength: %d\n\n%s", key, len(value), value))
}
