/*
Copyright © 2025 Martin Simango shukomango@gmail.com
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "simple-db",
	Short: "A simple key-value database CLI application for simple-db",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().String("host", "localhost", "Server host address")
	rootCmd.PersistentFlags().IntP("port", "p", 5050, "Server port number")
}
