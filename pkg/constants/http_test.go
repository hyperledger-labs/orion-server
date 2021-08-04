// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package constants

import (
	"testing"

	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
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
			name: "ExecuteQuery",
			execute: func() string {
				return URLForDataQuery("db1")
			},
			expectedURL: "/data/db1",
		},
		{
			name: "JSONQuery",
			execute: func() string {
				return URLForJSONQuery("db1")
			},
			expectedURL: "/data/jsonquery/db1",
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
			expectedURL: "/ledger/proof/tx/1?idx=2",
		},
		{
			name: "URLDataProof deleted false",
			execute: func() string {
				return URLDataProof(1, "db1", "key", false)
			},
			expectedURL: "/ledger/proof/data/db1/key?block=1",
		},
		{
			name: "URLDataProof deleted true",
			execute: func() string {
				return URLDataProof(1, "db1", "key", true)
			},
			expectedURL: "/ledger/proof/data/db1/key?block=1&deleted=true",
		},
		{
			name: "URLForGetHistoricalData",
			execute: func() string {
				return URLForGetHistoricalData("db1", "key1")
			},
			expectedURL: "/provenance/data/history/db1/key1",
		},
		{
			name: "URLForGetHistoricalDeletedData",
			execute: func() string {
				return URLForGetHistoricalDeletedData("db1", "key1")
			},
			expectedURL: "/provenance/data/history/db1/key1?onlydeletes=true",
		},
		{
			name: "URLForGetHistoricalDataAt",
			execute: func() string {
				return URLForGetHistoricalDataAt("db2", "key2", &types.Version{
					BlockNum: 10,
					TxNum:    5,
				})
			},
			expectedURL: "/provenance/data/history/db2/key2?blocknumber=10&transactionnumber=5",
		},
		{
			name: "URLForGetHistoricalDataAtOrBelow",
			execute: func() string {
				return URLForGetHistoricalDataAtOrBelow("db2", "key2", &types.Version{
					BlockNum: 10,
					TxNum:    5,
				})
			},
			expectedURL: "/provenance/data/history/db2/key2?blocknumber=10&transactionnumber=5&mostrecent=true",
		},
		{
			name: "URLForPreviousGetHistoricalData",
			execute: func() string {
				return URLForGetPreviousHistoricalData("db3", "key3", &types.Version{
					BlockNum: 12,
					TxNum:    6,
				})
			},
			expectedURL: "/provenance/data/history/db3/key3?blocknumber=12&transactionnumber=6&direction=previous",
		},
		{
			name: "URLForNextGetHistoricalData",
			execute: func() string {
				return URLForGetNextHistoricalData("db4", "key4", &types.Version{
					BlockNum: 22,
					TxNum:    16,
				})
			},
			expectedURL: "/provenance/data/history/db4/key4?blocknumber=22&transactionnumber=16&direction=next",
		},
		{
			name: "URLForGetDataReaders",
			execute: func() string {
				return URLForGetDataReaders("db5", "key5")
			},
			expectedURL: "/provenance/data/readers/db5/key5",
		},
		{
			name: "URLForGetDataWriters",
			execute: func() string {
				return URLForGetDataWriters("db6", "key6")
			},
			expectedURL: "/provenance/data/writers/db6/key6",
		},
		{
			name: "URLForGetDataReadBy",
			execute: func() string {
				return URLForGetDataReadBy("user1")
			},
			expectedURL: "/provenance/data/read/user1",
		},
		{
			name: "URLForGetDataWrittenBy",
			execute: func() string {
				return URLForGetDataWrittenBy("user2")
			},
			expectedURL: "/provenance/data/written/user2",
		},
		{
			name: "URLForGetTransactionReceipt",
			execute: func() string {
				return URLForGetTransactionReceipt("tx1")
			},
			expectedURL: "/ledger/tx/receipt/tx1",
		},
		{
			name: "URLForGetMostRecentNodeInfo",
			execute: func() string {
				return URLForGetMostRecentNodeConfig("node1", &types.Version{
					BlockNum: 10,
					TxNum:    5,
				})
			},
			expectedURL: "/provenance/node/node1?blocknumber=10&transactionnumber=5",
		},
		{
			name: "URLForGetMostRecentUserInfo",
			execute: func() string {
				return URLForGetMostRecentUserInfo("user1", &types.Version{
					BlockNum: 10,
					TxNum:    5,
				})
			},
			expectedURL: "/provenance/user/user1?blocknumber=10&transactionnumber=5",
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
