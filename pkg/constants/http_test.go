// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package constants

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/assert"
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
			expectedURL: "/data/db1/a2V5MQ",
		},
		{
			name: "GetDataRange",
			execute: func() string {
				return URLForGetDataRange("db1", "key1", "key10", 10)
			},
			expectedURL: "/data/db1?startkey=a2V5MQ&endkey=a2V5MTA&limit=10",
		},
		{
			name: "JSONQuery",
			execute: func() string {
				return URLForJSONQuery("db1")
			},
			expectedURL: "/data/db1/jsonquery",
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
			name: "GetDBIndex",
			execute: func() string {
				return URLForGetDBIndex("db1")
			},
			expectedURL: "/db/index/db1",
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
				return URLForLedgerBlock(10, false)
			},
			expectedURL: "/ledger/block/10",
		},
		{
			name: "URLForLedgerBlock_Augmented",
			execute: func() string {
				return URLForLedgerBlock(10, true)
			},
			expectedURL: "/ledger/block/10?augmented=true",
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
			expectedURL: "/ledger/proof/data/db1/a2V5?block=1",
		},
		{
			name: "URLDataProof deleted true",
			execute: func() string {
				return URLDataProof(1, "db1", "key", true)
			},
			expectedURL: "/ledger/proof/data/db1/a2V5?block=1&deleted=true",
		},
		{
			name: "URLForGetHistoricalData",
			execute: func() string {
				return URLForGetHistoricalData("db1", "key1")
			},
			expectedURL: "/provenance/data/history/db1/a2V5MQ",
		},
		{
			name: "URLForGetHistoricalDeletedData",
			execute: func() string {
				return URLForGetHistoricalDeletedData("db1", "key1")
			},
			expectedURL: "/provenance/data/history/db1/a2V5MQ?onlydeletes=true",
		},
		{
			name: "URLForGetHistoricalDataAt",
			execute: func() string {
				return URLForGetHistoricalDataAt("db2", "key2", &types.Version{
					BlockNum: 10,
					TxNum:    5,
				})
			},
			expectedURL: "/provenance/data/history/db2/a2V5Mg?blocknumber=10&transactionnumber=5",
		},
		{
			name: "URLForGetHistoricalDataAtOrBelow",
			execute: func() string {
				return URLForGetHistoricalDataAtOrBelow("db2", "key2", &types.Version{
					BlockNum: 10,
					TxNum:    5,
				})
			},
			expectedURL: "/provenance/data/history/db2/a2V5Mg?blocknumber=10&transactionnumber=5&mostrecent=true",
		},
		{
			name: "URLForPreviousGetHistoricalData",
			execute: func() string {
				return URLForGetPreviousHistoricalData("db3", "key3", &types.Version{
					BlockNum: 12,
					TxNum:    6,
				})
			},
			expectedURL: "/provenance/data/history/db3/a2V5Mw?blocknumber=12&transactionnumber=6&direction=previous",
		},
		{
			name: "URLForNextGetHistoricalData",
			execute: func() string {
				return URLForGetNextHistoricalData("db4", "key4", &types.Version{
					BlockNum: 22,
					TxNum:    16,
				})
			},
			expectedURL: "/provenance/data/history/db4/a2V5NA?blocknumber=22&transactionnumber=16&direction=next",
		},
		{
			name: "URLForGetDataReaders",
			execute: func() string {
				return URLForGetDataReaders("db5", "key5")
			},
			expectedURL: "/provenance/data/readers/db5/a2V5NQ",
		},
		{
			name: "URLForGetDataWriters",
			execute: func() string {
				return URLForGetDataWriters("db6", "key6")
			},
			expectedURL: "/provenance/data/writers/db6/a2V5Ng",
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
		{
			name: "URLForLastLedgerBlock",
			execute: func() string {
				return URLForLastLedgerBlock()
			},
			expectedURL: "/ledger/block/last",
		},
		// URLForLastLedgerBlock
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			actualURL := tt.execute()
			require.Equal(t, tt.expectedURL, actualURL)
		})
	}
}

func TestSafeURLSegmentNZ(t *testing.T) {
	type testCase struct {
		name string
		id   string
		pass bool
	}

	// from https://www.ietf.org/rfc/rfc3986.txt
	// pchar         = unreserved / pct-encoded / sub-delims / ":" / "@"
	// unreserved  = ALPHA / DIGIT / "-" / "." / "_" / "~"
	// sub-delims  = "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "="

	num := "1234567890"
	lower := "abcdefghijklmnopqrstuvwxyz"
	upper := strings.ToUpper(lower)
	unreserverd := "-._~"
	prct := "%"
	subDelims := "!$&'()*+,;="
	subDelimsPlus := ":@"

	allSafe := num + lower + upper + unreserverd + prct + subDelims + subDelimsPlus

	testCases := []testCase{
		{
			name: "alpha-numeric",
			id:   num + lower + upper,
			pass: true,
		},
		{
			name: "unreserved",
			id:   unreserverd,
			pass: true,
		},
		{
			name: "sub-delims",
			id:   subDelims,
			pass: true,
		},
		{
			name: "sub-delims-plus",
			id:   subDelimsPlus,
			pass: true,
		},
		{
			name: "percent-encoded",
			id:   "has%20%20space",
			pass: true,
		},
		{
			name: "bad-percent-encoded",
			id:   "bad%2%2",
			pass: false,
		},
		{
			name: "non-ascii - utf8",
			id:   string([]byte{0xe2, 0x8c, 0x98}),
			pass: false,
		},
		{
			name: "non-printable",
			id:   string([]byte{0xbd, 0xb2}),
			pass: false,
		},
		{
			name: "space",
			id:   " \t\n",
			pass: false,
		},
	}

	// every byte other than the safe
	for i := 0; i <= 0xff; i++ {
		s := string([]byte{byte(i)})
		if !strings.Contains(allSafe, s) {
			testCases = append(testCases,
				testCase{
					name: fmt.Sprintf("unsafe-%q", s),
					id:   s,
					pass: false,
				})
		}
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			err := SafeURLSegmentNZ(tt.id)
			if tt.pass {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "un-safe for a URL segment:")
			}
		})
	}
}
