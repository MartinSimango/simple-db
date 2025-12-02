/*
Copyright © 2025 Martin Simango shukomango@gmail.com
*/
package get

import (
	"fmt"

	"github.com/MartinSimango/simple-db/pkg/cmd/util"
	"github.com/spf13/cobra"
)

func NewGetCmd() *cobra.Command {
	getCmd := &cobra.Command{
		Use:   "get",
		Short: "Retrieve a value by key from the database",
		Run: func(cmd *cobra.Command, args []string) {

			conn, err := util.ConnectToServer(cmd.Flags())
			if err != nil {
				panic(err)
			}
			defer conn.Close()
			fmt.Println("Connected to server from: ", conn.RemoteAddr())
		},
	}

	return getCmd
}
