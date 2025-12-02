/*
Copyright © 2025 Martin Simango shukomango@gmail.com
*/
package cmd

import (
	"github.com/spf13/cobra"
)

func NewSimpleDbCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "simple-db",
		Short: "A simple key-value database CLI application for simple-db",
	}
	cmd.PersistentFlags().String("host", "localhost", "Server host address")
	cmd.PersistentFlags().IntP("port", "p", 5050, "Server port number")
	return cmd
}
