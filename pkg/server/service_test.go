package server

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/server/mock"
)

func TestMain(m *testing.M) {
	path, err := filepath.Abs("../../config")
	if err != nil {
		log.Fatal("Failed to construct absolute path from the default config")
	}

	if err := os.Setenv(config.PathEnv, path); err != nil {
		log.Fatalf("Failed to set the config path to %s", config.PathEnv)
	}

	config.Init()
	os.Exit(m.Run())
}

func TestStart(t *testing.T) {
	dbConf := config.Database()
	defer os.RemoveAll(dbConf.LedgerDirectory)

	go Start()
	defer Stop()

	var client *mock.Client
	var err error
	createClient := func() bool {
		client, err = mock.NewRESTClient("http://localhost:6001")
		if err == nil && client != nil {
			return true
		}
		return false
	}
	require.Eventually(t, createClient, time.Second*2, time.Millisecond*100)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	valEnv, err := client.GetState(
		ctx,
		&api.GetStateQueryEnvelope{
			Payload: &api.GetStateQuery{
				UserID: "testUser",
				DBName: "db1",
				Key:    "key1",
			},
			Signature: []byte("hello"),
		},
	)
	require.Nil(t, valEnv)
	require.Error(t, err)
	require.Contains(t, err.Error(), "database db1 does not exist")
}
