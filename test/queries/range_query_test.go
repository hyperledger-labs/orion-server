// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queries

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/stretchr/testify/require"
)

// Scenario:
// - Create 1 node cluster with QueryLimit = math.MaxInt64
// - Execute queries with and without pagination triggered by user limit
// - Execute open intervals queries
// - Execute queries that returns empty response:
//// - the start key is after the end key in alphabetical order
//// - key that does not exist in the database
func TestRangeQueriesWithUserLimit(t *testing.T) {
	dir, err := ioutil.TempDir("", "int-test")
	require.NoError(t, err)

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		ServersQueryLimit:   math.MaxInt64,
	}
	defer os.RemoveAll(dir)
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())
	leaderIndex := -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	s := c.Servers[0]

	insertDataAlphabet(t, s)

	// query without limit (limit=0)
	res, err := s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "key-a", "key-z", 0)
	require.NoError(t, err)
	require.NotNil(t, res)
	checkResponse(t, res.GetResponse(), 'a', "")

	// query with limit
	res, err = s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "key-a", "key-c", 3)
	require.NoError(t, err)
	require.NotNil(t, res)
	checkResponse(t, res.GetResponse(), 'a', "")

	res, err = s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "key-a", "key-c", 1)
	require.NoError(t, err)
	require.NotNil(t, res)
	checkResponse(t, res.GetResponse(), 'a', "key-b")

	// open intervals
	res, err = s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "key-a", "", 100)
	require.NoError(t, err)
	require.NotNil(t, res)
	checkResponse(t, res.GetResponse(), 'a', "")

	res, err = s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "key-a", "", 2)
	require.NoError(t, err)
	require.NotNil(t, res)
	checkResponse(t, res.GetResponse(), 'a', "key-c")

	res, err = s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "", "key-b", 10)
	require.NoError(t, err)
	require.NotNil(t, res)
	checkResponse(t, res.GetResponse(), 'a', "")

	res, err = s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "", "", 100)
	require.NoError(t, err)
	require.NotNil(t, res)
	checkResponse(t, res.GetResponse(), 'a', "")

	// A key that does not exist in the database
	res, err = s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "1", "2", 10)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Nil(t, res.GetResponse().GetKVs())

	// The start key is after the end key in alphabetical order
	res, err = s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "key-e", "key-a", 10)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 0, len(res.GetResponse().GetKVs()))
}

// Scenario:
// - Create 1 node cluster with QueryLimit = 100
// - Execute queries with and without pagination triggered by server limit
// - Execute open intervals queries
// - Execute queries that returns empty response:
//// - the start key is after the end key in alphabetical order
//// - key that does not exist in the database
func TestRangeQueriesWithServerLimit(t *testing.T) {
	dir, err := ioutil.TempDir("", "int-test")
	require.NoError(t, err)

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		ServersQueryLimit:   100,
	}
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())
	leaderIndex := -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	s := c.Servers[0]

	insertDataAlphabet(t, s)

	res, err := s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "key-a", "key-z", 0)
	require.NoError(t, err)
	require.NotNil(t, res)
	checkResponse(t, res.GetResponse(), 'a', "key-h")

	res, err = s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "key-a", "key-c", 0)
	require.NoError(t, err)
	require.NotNil(t, res)
	checkResponse(t, res.GetResponse(), 'a', "")

	// open intervals
	res, err = s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "key-a", "", 0)
	require.NoError(t, err)
	require.NotNil(t, res)
	checkResponse(t, res.GetResponse(), 'a', "key-h")

	res, err = s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "", "key-b", 0)
	require.NoError(t, err)
	require.NotNil(t, res)
	checkResponse(t, res.GetResponse(), 'a', "")

	res, err = s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "", "", 0)
	require.NoError(t, err)
	require.NotNil(t, res)
	checkResponse(t, res.GetResponse(), 'a', "key-h")

	// A key that does not exist in the database
	res, err = s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "1", "2", 0)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Nil(t, res.GetResponse().GetKVs())

	// The start key is after the end key in alphabetical order
	res, err = s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "key-e", "key-a", 0)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 0, len(res.GetResponse().GetKVs()))
}

// Scenario:
// - create cluster with QueryLimit = 10
// - Try to execute a query that returns a response size larger than the limit
func TestInvalidRangeQuery(t *testing.T) {
	dir, err := ioutil.TempDir("", "int-test")
	require.NoError(t, err)

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		ServersQueryLimit:   uint64(10),
	}
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())
	leaderIndex := -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	s := c.Servers[0]

	insertDataAlphabet(t, s)

	res, err := s.QueryDataRange(t, "admin", worldstate.DefaultDBName, "key-a", "key-z", 100)
	require.EqualError(t, err, "error while issuing /data/bdb?startkey=\"key-a\"&endkey=\"key-z\"&limit=100: "+
		"error while processing 'GET /data/bdb?startkey=\"key-a\"&endkey=\"key-z\"&limit=100' because "+
		"response size limit for queries is configured as 10 bytes but a single record size itself is 14 bytes. "+
		"Increase the query response size limit at the server")
	require.Nil(t, res)
}

func insertDataAlphabet(t *testing.T, s *setup.Server) {
	for r := 'a'; r < 'z'; r++ {
		txID, rcpt, _, err := s.WriteDataTx(t, worldstate.DefaultDBName, fmt.Sprintf("key-%c", r), []byte{uint8(r)})
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
	}
}

func checkResponse(t *testing.T, res *types.GetDataRangeResponse, expectedStartKey int32, nextKey string) {
	kvms := res.GetKVs()
	for i, r := int(expectedStartKey)-int('a'), expectedStartKey; i < len(kvms); i, r = i+1, r+1 {
		require.Equal(t, fmt.Sprintf("key-%c", r), kvms[i].GetKey())
		require.Equal(t, []byte{uint8(r)}, kvms[i].GetValue())
	}
	require.Equal(t, nextKey, res.GetNextStartKey())
}
