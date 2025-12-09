/*
Copyright © 2025 Martin Simango shukomango@gmail.com
*/
package del

import (
	"fmt"

	"github.com/MartinSimango/simple-db/internal/cmd/util"
	"github.com/MartinSimango/simple-db/pkg/db"
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
			r, err := client.Delete(args[0])
			if err != nil {
				fmt.Println("ERROR:", err)
				return
			}
			fmt.Println(r)
		},
	}

	return deleteCmd
}
