package main

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/server"
)

var (
	configPath string
)

func main() {
	cmd := bdbCmd()

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
				return fmt.Errorf("Trailing arguments detected")
			}

			cmd.SilenceUsage = true
			log.Println("bdb 0.1")

			return nil
		},
	}
}

func startCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Starts a blockchain database",
		RunE: func(cmd *cobra.Command, args []string) error {
			if configPath == "" && os.Getenv(config.PathEnv) == "" {
				log.Fatalf("Neither --configpath nor %s path environment is set", config.PathEnv)
			}

			if configPath != "" {
				if err := os.Setenv(config.PathEnv, configPath); err != nil {
					log.Fatalf("Failed to set %s due to %s", config.PathEnv, err.Error())
				}
			}

			cmd.SilenceUsage = true
			log.Println("Starting a blockchain database")
			server.Start()

			return nil
		},
	}

	cmd.PersistentFlags().StringVar(&configPath, "configpath", "", "set the absolute path of config directory")
	return cmd
}
