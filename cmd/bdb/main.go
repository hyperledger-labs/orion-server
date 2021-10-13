// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/server"
	"github.com/spf13/cobra"
)

var (
	configPath string
	// PathEnv is an environment variable that can hold
	// the absolute path of the config file
	pathEnv = "BCDB_CONFIG_PATH"
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
		Short: "To start and interact with a blockchain database server.",
	}
	cmd.AddCommand(versionCmd())
	cmd.AddCommand(startCmd())
	return cmd
}

func versionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print the version of the blockchain database server.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("Trailing arguments detected")
			}

			cmd.SilenceUsage = true
			cmd.Println("bdb 0.1")

			return nil
		},
	}

	return cmd
}

func startCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Starts a blockchain database",
		RunE: func(cmd *cobra.Command, args []string) error {
			var path string
			switch {
			case configPath != "":
				path = configPath
			case os.Getenv(pathEnv) != "":
				path = os.Getenv(pathEnv)
			default:
				log.Fatalf("Neither --configpath nor %s path environment is set", pathEnv)
			}

			conf, err := config.Read(path)
			if err != nil {
				return err
			}

			cmd.SilenceUsage = true
			log.Println("Starting a blockchain database")
			srv, err := server.New(conf)
			if err != nil {
				return err
			}

			var wg sync.WaitGroup

			wg.Add(1)
			go func() {
				if err := srv.Start(); err != nil {
					wg.Done()
					log.Fatalf("%v", err)
				}
			}()
			wg.Wait()

			return nil
		},
	}

	cmd.PersistentFlags().StringVar(&configPath, "configpath", "", "set the absolute path of config directory")
	return cmd
}
