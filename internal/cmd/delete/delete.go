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

			conn, err := util.ConnectToServer(cmd.Flags())
			if err != nil {
				panic(err)
			}
			defer conn.Close()
			fmt.Println("Connected to server from: ", conn.RemoteAddr())
		},
	}

	return deleteCmd
}
