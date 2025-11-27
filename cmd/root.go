/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"net"
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "simple-db",
	Short: "A simple key-value database CLI application for simple-db",
	Long:  ``,
	// Uncomment the following line if your bare application
	// has an action associated with it:
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

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.simple-db.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().String("host", "", "Server host address")
	rootCmd.Flags().IntP("port", "p", 0, "Server port number")
	rootCmd.MarkFlagRequired("host")
	rootCmd.MarkFlagRequired("port")
}
