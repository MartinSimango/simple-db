/*
Copyright © 2025 Martin Simango shukomango@gmail.com
*/
package put

import (
	"fmt"

	"github.com/MartinSimango/simple-db/pkg/cmd/util"
	"github.com/spf13/cobra"
)

func NewPutCmd() *cobra.Command {
	putCmd := &cobra.Command{
		Use:   "put",
		Short: "Add or update a key-value pair in the database",
		Run: func(cmd *cobra.Command, args []string) {

			conn, err := util.ConnectToServer(cmd.Flags())
			if err != nil {
				panic(err)
			}
			defer conn.Close()
			fmt.Println("Connected to server from: ", conn.RemoteAddr())
		},
	}

	return putCmd
}
