package main

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
)

// To start and interact with a blockchain database

// Usage:
//   bdb [command]

// Available Commands:
//   help        Help about any command
//   start       Starts a blockchain database
//   version     Print the version of blockchain database

// Flags:
//   -h, --help   help for bdb

// Use "bdb [command] --help" for more information about a command.

func main() {
	cmd := bdbCmd()
	// Define command-line flags that are valid for all peer
	// commands and subcommands.
	mainFlags := cmd.PersistentFlags()

	mainFlags.String("logging-level", "", "Legacy logging level flag")
	mainFlags.MarkHidden("logging-level")

	// On failure Cobra prints the usage message and error string, so we only
	// need to exit with a non-0 status
	if cmd.Execute() != nil {
		os.Exit(1)
	}
}

func bdbCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bdb",
		Short: "To start and interact with a blockchain database",
	}
	cmd.AddCommand(versionCmd())
	cmd.AddCommand(startCmd())
	return cmd
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version of blockchain database",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("trailing arguments detected")
			}
			cmd.SilenceUsage = true
			log.Println("bdb 0.1")
			return nil
		},
	}
}

func startCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Starts a blockchain database",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("trailing arguments detected")
			}
			cmd.SilenceUsage = true
			log.Println("starting a blockchain database")
			// need to start the server here
			return nil
		},
	}
}
