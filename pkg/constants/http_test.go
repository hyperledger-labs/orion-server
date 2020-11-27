package constants

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestURLConstruction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		execute     func() string
		expectedURL string
	}{
		{
			name: "GetData",
			execute: func() string {
				return URLForGetData("db1", "key1")
			},
			expectedURL: "/data/db1/key1",
		},
		{
			name: "GetUser",
			execute: func() string {
				return URLForGetUser("user1")
			},
			expectedURL: "/user/user1",
		},
		{
			name: "GetDBStatus",
			execute: func() string {
				return URLForGetDBStatus("db1")
			},
			expectedURL: "/db/db1",
		},
		{
			name: "URLForGetConfig",
			execute: func() string {
				return URLForGetConfig()
			},
			expectedURL: "/config/tx",
		},
		{
			name: "URLForLedgerBlock",
			execute: func() string {
				return URLForLedgerBlock(10)
			},
			expectedURL: "/ledger/block/10",
		},
		{
			name: "URLForLedgerPath",
			execute: func() string {
				return URLForLedgerPath(10, 20)
			},
			expectedURL: "/ledger/path?start=10&end=20",
		},
		{
			name: "URLNodeConfigPath",
			execute: func() string {
				return URLForNodeConfigPath("node1")
			},
			expectedURL: "/config/node/node1",
		},
		{
			name: "URLNodeConfigPath all nodes",
			execute: func() string {
				return URLForNodeConfigPath("")
			},
			expectedURL: "/config/node",
		},
		{
			name: "URLTxProof",
			execute: func() string {
				return URLTxProof(1, 2)
			},
			expectedURL: "/ledger/proof/1?idx=2",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			actualURL := tt.execute()
			require.Equal(t, tt.expectedURL, actualURL)
		})
	}
}
