// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"encoding/pem"
	"net/http"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// Scenario:
// - admin adds alice to the admins' list
// - alice adds bob to the admins' list
// - bob tries to add alice again to the admins' list => the tx fails because alice is already an admin
func TestAddNewAdmins(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		CheckRedirectFunc: func(req *http.Request, via []*http.Request) error {
			return errors.Errorf("Redirect blocked in test client: url: '%s', referrer: '%s', #via: %d", req.URL, req.Referer(), len(via))
		},
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
	leaderServer := c.Servers[leaderIndex]

	cert, _, err := c.GetUser("alice")
	require.NoError(t, err)
	require.NotNil(t, cert)
	decodedCert, _ := pem.Decode(cert)

	alice := &types.Admin{
		Id:          "alice",
		Certificate: decodedCert.Bytes,
	}

	//get current cluster config
	configEnv, err := leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig := configEnv.GetResponse().GetConfig()

	//add alice to admins list
	newConfig.Admins = append(newConfig.Admins, alice)
	version := configEnv.GetResponse().GetMetadata().GetVersion()

	txID, rcpt, err := leaderServer.SetConfigTx(t, newConfig, version, c.Servers[leaderIndex].AdminSigner(), "admin")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	cert, _, err = c.GetUser("bob")
	require.NoError(t, err)
	require.NotNil(t, cert)
	decodedCert, _ = pem.Decode(cert)

	bob := &types.Admin{
		Id:          "bob",
		Certificate: decodedCert.Bytes,
	}

	//get current cluster config
	configEnv, err = leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig = configEnv.GetResponse().GetConfig()

	//alice adds bob to admins list
	newConfig.Admins = append(newConfig.Admins, bob)
	version = configEnv.GetResponse().GetMetadata().GetVersion()

	aliceCertPath, aliceKeyPath := c.GetUserCertKeyPath("alice")
	c.UpdateServersAdmin("alice", aliceKeyPath, aliceCertPath)

	txID, rcpt, err = leaderServer.SetConfigTx(t, newConfig, version, c.Servers[leaderIndex].AdminSigner(), "alice")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	//make sure alice and bob are in the admins list
	configEnv, err = leaderServer.QueryConfig(t, "alice")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	admins := configEnv.GetResponse().GetConfig().GetAdmins()
	require.Equal(t, 3, len(admins))
	require.Equal(t, alice, admins[1])
	require.Equal(t, bob, admins[2])

	bobCertPath, bobKeyPath := c.GetUserCertKeyPath("bob")
	c.UpdateServersAdmin("bob", bobKeyPath, bobCertPath)

	//bob tries to add Alice again
	newConfig = configEnv.GetResponse().GetConfig()
	newConfig.Admins = append(newConfig.Admins, alice)
	version = configEnv.GetResponse().GetMetadata().GetVersion()
	txID, rcpt, err = leaderServer.SetConfigTx(t, newConfig, version, c.Servers[leaderIndex].AdminSigner(), "bob")
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx, reason: there are two admins with the same ID [alice] in the admin config. The admin IDs must be unique")
}

func TestAddNewAdminWithInvalidCertificate(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		CheckRedirectFunc: func(req *http.Request, via []*http.Request) error {
			return errors.Errorf("Redirect blocked in test client: url: '%s', referrer: '%s', #via: %d", req.URL, req.Referer(), len(via))
		},
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
	leaderServer := c.Servers[leaderIndex]

	alice := &types.Admin{
		Id:          "alice",
		Certificate: []byte("invalid certificate"),
	}

	//get current cluster config
	configEnv, err := leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig := configEnv.GetResponse().GetConfig()

	newConfig.Admins = append(newConfig.Admins, alice)
	version := configEnv.GetResponse().GetMetadata().GetVersion()

	//add alice with invalid certificate
	_, _, err = leaderServer.SetConfigTx(t, newConfig, version, c.Servers[leaderIndex].AdminSigner(), "admin")
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx, reason: the admin [alice] has an invalid certificate: error parsing certificate: x509: malformed certificate")
}

// Scenario:
// - trying to delete the only admin in the cluster
// - config tx fails because there must be at least a single admin in the cluster
// - admin adds alice to admins list
// - alice deletes admin from the admins' list
func TestDeleteAdmin(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		CheckRedirectFunc: func(req *http.Request, via []*http.Request) error {
			return errors.Errorf("Redirect blocked in test client: url: '%s', referrer: '%s', #via: %d", req.URL, req.Referer(), len(via))
		},
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
	leaderServer := c.Servers[leaderIndex]

	//try to delete the only admin in the cluster
	configEnv, err := leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig := configEnv.GetResponse().GetConfig()
	newConfig.Admins = []*types.Admin{}
	version := configEnv.GetResponse().GetMetadata().GetVersion()

	_, _, err = leaderServer.SetConfigTx(t, newConfig, version, c.Servers[leaderIndex].AdminSigner(), "admin")
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx, reason: admin config is empty. There must be at least single admin in the cluster")

	//add alice to admins list
	cert, _, err := c.GetUser("alice")
	require.NoError(t, err)
	require.NotNil(t, cert)
	decodedCert, _ := pem.Decode(cert)

	alice := []*types.Admin{
		{
			Id:          "alice",
			Certificate: decodedCert.Bytes,
		},
	}
	//get current cluster config
	configEnv, err = leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig = configEnv.GetResponse().GetConfig()

	newConfig.Admins = append(newConfig.Admins, alice...)
	version = configEnv.GetResponse().GetMetadata().GetVersion()

	//add alice
	txID, rcpt, err := leaderServer.SetConfigTx(t, newConfig, version, c.Servers[leaderIndex].AdminSigner(), "admin")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	//make sure alice in the admins list
	configEnv, err = leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	admins := configEnv.GetResponse().GetConfig().GetAdmins()
	require.Equal(t, 2, len(admins))
	require.Equal(t, alice[0], admins[1])

	//remove original admin
	newConfig = configEnv.GetResponse().GetConfig()
	newConfig.Admins = alice
	version = configEnv.GetResponse().GetMetadata().GetVersion()

	aliceCertPath, aliceKeyPath := c.GetUserCertKeyPath("alice")
	err = c.UpdateServersAdmin("alice", aliceKeyPath, aliceCertPath)
	require.NoError(t, err)

	txID, rcpt, err = leaderServer.SetConfigTx(t, newConfig, version, c.Servers[leaderIndex].AdminSigner(), "alice")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	//make sure admin was deleted from admins list
	configEnv, err = leaderServer.QueryConfig(t, "alice")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	admins = configEnv.GetResponse().GetConfig().GetAdmins()
	require.Equal(t, 1, len(admins))
	require.Equal(t, alice[0], admins[0])

	//admin try to submit tx - admin was removed so the tx fails
	txID, rcpt, err = leaderServer.SetConfigTx(t, newConfig, version, c.Servers[leaderIndex].AdminSigner(), "admin")
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 401 Unauthorized, message: signature verification failed")
}

// Scenario:
// - admin changes his cert to the cert of alice
// - try to get config envelope with admin old signer - tx fails
// - update admin signer to new signer based on alice's private key
// - get config envelope
func TestChangeAdminCA(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		CheckRedirectFunc: func(req *http.Request, via []*http.Request) error {
			return errors.Errorf("Redirect blocked in test client: url: '%s', referrer: '%s', #via: %d", req.URL, req.Referer(), len(via))
		},
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
	leaderServer := c.Servers[leaderIndex]

	aliceCert, _, err := c.GetUser("alice")
	require.NoError(t, err)
	require.NotNil(t, aliceCert)
	decodedCert, _ := pem.Decode(aliceCert)

	//get current cluster config
	configEnv, err := leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig := configEnv.GetResponse().GetConfig()
	version := configEnv.GetResponse().GetMetadata().GetVersion()

	//change admin cert to the cert of alice
	require.Equal(t, "admin", newConfig.Admins[0].Id)
	newConfig.Admins[0].Certificate = decodedCert.Bytes

	txID, rcpt, err := leaderServer.SetConfigTx(t, newConfig, version, c.Servers[leaderIndex].AdminSigner(), "admin")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	//try to get config envelope with admin old signer
	configEnv, err = leaderServer.QueryConfig(t, "admin")
	require.EqualError(t, err, "error while issuing /config/tx: signature verification failed")

	//update admin signer
	aliceSigner, err := c.GetSigner("alice")
	require.NoError(t, err)
	require.NotNil(t, aliceSigner)
	leaderServer.SetAdminSigner(aliceSigner)

	//get config envelope with new signer
	configEnv, err = leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
}

// Scenario:
// - check that GetClusterConfig is only available to admin users
// - alice is a regular user so access to ClusterConfig is denied
// - adding alice to admins list => alice can get ClusterConfig
func TestGetClusterConfigAccessibility(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		CheckRedirectFunc: func(req *http.Request, via []*http.Request) error {
			return errors.Errorf("Redirect blocked in test client: url: '%s', referrer: '%s', #via: %d", req.URL, req.Referer(), len(via))
		},
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
	leaderServer := c.Servers[leaderIndex]

	// alice tries to get current cluster config
	configEnv, err := leaderServer.QueryConfig(t, "alice")
	require.EqualError(t, err, "error while issuing /config/tx: signature verification failed")
	require.Nil(t, configEnv)

	// admin successfully gets current cluster config
	configEnv, err = leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)

	// add alice to admins list
	cert, _, err := c.GetUser("alice")
	require.NoError(t, err)
	require.NotNil(t, cert)
	decodedCert, _ := pem.Decode(cert)
	alice := []*types.Admin{
		{
			Id:          "alice",
			Certificate: decodedCert.Bytes,
		},
	}

	newConfig := configEnv.GetResponse().GetConfig()
	newConfig.Admins = append(newConfig.Admins, alice...)

	txID, rcpt, err := leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	aliceSigner, err := c.GetSigner("alice")
	require.NoError(t, err)
	require.NotNil(t, aliceSigner)
	leaderServer.SetAdminSigner(aliceSigner)

	//alice successfully gets current cluster config
	configEnv, err = leaderServer.QueryConfig(t, "alice")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
}
