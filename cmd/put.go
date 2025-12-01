/*
Copyright © 2025 Martin Simango shukomango@gmail.com
*/
package cmd

import (
	"fmt"
	"net"

	"github.com/spf13/cobra"
)

// putCmd represents the put command
var putCmd = &cobra.Command{
	Use:   "put",
	Short: "Add or update a key-value pair in the database",
	Run: func(cmd *cobra.Command, args []string) {

		host := cmd.Flag("host").Value.String()
		port, err := cmd.Flags().GetInt("port")
		if err != nil {
			panic(err)
		}

		conn, err := net.Dial("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
		if err != nil {
			panic(err)
		}

		defer conn.Close()
		fmt.Println("Connected to server from: ", conn.RemoteAddr())
	},
}

func init() {
	rootCmd.AddCommand(putCmd)

}
