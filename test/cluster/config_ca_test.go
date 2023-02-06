// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"crypto/tls"
	"encoding/pem"
	"net/http"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// Scenario:
// - add Root CA & Intermediate CA to CertAuthConfig
func TestAddCA(t *testing.T) {
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

	// add a Root CA & Intermediate CA
	certRootCA, rootCAPrivKey, err := testutils.GenerateRootCA("Orion RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, certRootCA)
	require.NotNil(t, rootCAPrivKey)
	rootCAkeyPair, err := tls.X509KeyPair(certRootCA, rootCAPrivKey)
	require.NoError(t, err)
	require.NotNil(t, rootCAkeyPair)
	certIntCA, intCAPrivKey, err := testutils.GenerateIntermediateCA("Orion IntermediateCA", "127.0.0.1", rootCAkeyPair)
	require.NoError(t, err)
	require.NotNil(t, certIntCA)

	configEnv, err := leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig := configEnv.GetResponse().GetConfig()

	caConf := newConfig.CertAuthConfig
	decodedCertRootCA, _ := pem.Decode(certRootCA)
	caConf.Roots = append(caConf.Roots, decodedCertRootCA.Bytes)
	decodedCertIntCA, _ := pem.Decode(certIntCA)
	caConf.Intermediates = append(caConf.Intermediates, decodedCertIntCA.Bytes)

	txID, rcpt, err := leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	//create user with the new Root CA
	err = c.CreateAdditionalUserCryptoMaterials("david", certRootCA, rootCAPrivKey)
	require.NoError(t, err)

	//add david to the admins list
	cert, _, err := c.GetUser("david")
	require.NoError(t, err)
	require.NotNil(t, cert)
	decodedCert, _ := pem.Decode(cert)

	david := &types.Admin{
		Id:          "david",
		Certificate: decodedCert.Bytes,
	}

	//get current cluster config
	configEnv, err = leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig = configEnv.GetResponse().GetConfig()
	newConfig.Admins = append(newConfig.Admins, david)

	txID, rcpt, err = leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	//david submits tx as admin
	davidCertPath, davidKeyPath := c.GetUserCertKeyPath("david")
	c.UpdateServersAdmin("david", davidKeyPath, davidCertPath)

	//get current cluster config
	configEnv, err = leaderServer.QueryConfig(t, "david")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig = configEnv.GetResponse().GetConfig()

	txID, rcpt, err = leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "david")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	//create user with the new Intermediate CA
	err = c.CreateAdditionalUserCryptoMaterials("elijah", certIntCA, intCAPrivKey)
	require.NoError(t, err)

	//add elijah to the admins list
	cert, _, err = c.GetUser("elijah")
	require.NoError(t, err)
	require.NotNil(t, cert)
	decodedCert, _ = pem.Decode(cert)

	elijah := &types.Admin{
		Id:          "elijah",
		Certificate: decodedCert.Bytes,
	}

	//get current cluster config
	configEnv, err = leaderServer.QueryConfig(t, "david")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig = configEnv.GetResponse().GetConfig()
	newConfig.Admins = append(newConfig.Admins, elijah)

	txID, rcpt, err = leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "david")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	//elijah submits tx as admin
	elijahCertPath, elijahKeyPath := c.GetUserCertKeyPath("elijah")
	c.UpdateServersAdmin("elijah", elijahKeyPath, elijahCertPath)

	//get current cluster config
	configEnv, err = leaderServer.QueryConfig(t, "elijah")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig = configEnv.GetResponse().GetConfig()

	txID, rcpt, err = leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "elijah")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)
}

// Scenario:
// - adding an intermediate CA without adding the root it came from in the same tx (broken chain)
func TestInvalidCABrokenChain(t *testing.T) {
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

	// add an Intermediate CA
	certRootCA, rootCAPrivKey, err := testutils.GenerateRootCA("Orion RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, certRootCA)
	require.NotNil(t, rootCAPrivKey)
	rootCAkeyPair, err := tls.X509KeyPair(certRootCA, rootCAPrivKey)
	require.NoError(t, err)
	require.NotNil(t, rootCAkeyPair)
	certIntCA, _, err := testutils.GenerateIntermediateCA("Orion IntermediateCA", "127.0.0.1", rootCAkeyPair)
	require.NoError(t, err)
	require.NotNil(t, certIntCA)

	configEnv, err := leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig := configEnv.GetResponse().GetConfig()

	decodedCertIntCA, _ := pem.Decode(certIntCA)
	newConfig.CertAuthConfig.Intermediates = append(newConfig.CertAuthConfig.Intermediates, decodedCertIntCA.Bytes)

	_, _, err = leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to submit transaction, server returned: status: 400 Bad Request, message: "+
		"Invalid config tx, reason: CA certificate collection is invalid: error verifying CA certificate against trusted certificate "+
		"authority (CA), SN: ")
	require.Contains(t, err.Error(), ": x509: certificate signed by unknown authority")
}

// Scenario:
// - all txs returns error:
// - add a bad ֹCA
// - add an intermediate CA as a root CA
// - add a root CA as an intermediate CA
// - update to empty CA
func TestInvalidCAs(t *testing.T) {
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

	// add a bad ֹCA
	configEnv, err := leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig := configEnv.GetResponse().GetConfig()
	caConf := newConfig.CertAuthConfig
	caConf.Roots = append(caConf.Roots, []byte("bad-certificate"))
	caConf.Intermediates = append(caConf.Intermediates, []byte("bad-certificate"))

	_, _, err = leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx, reason: CA certificate collection cannot be created: x509: malformed certificate")

	// add an Intermediate CA as a root CA
	certRootCA, rootCAPrivKey, err := testutils.GenerateRootCA("Orion RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, certRootCA)
	require.NotNil(t, rootCAPrivKey)
	rootCAkeyPair, err := tls.X509KeyPair(certRootCA, rootCAPrivKey)
	require.NoError(t, err)
	require.NotNil(t, rootCAkeyPair)
	certIntCA, _, err := testutils.GenerateIntermediateCA("Orion IntermediateCA", "127.0.0.1", rootCAkeyPair)
	require.NoError(t, err)
	require.NotNil(t, certIntCA)

	configEnv, err = leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig = configEnv.GetResponse().GetConfig()

	caConf = newConfig.CertAuthConfig
	decodedCertIntCA, _ := pem.Decode(certIntCA)
	caConf.Roots = append(caConf.Roots, decodedCertIntCA.Bytes)
	caConf.Intermediates = append(caConf.Intermediates, decodedCertIntCA.Bytes)

	_, _, err = leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config"+
		" tx, reason: CA certificate collection is invalid: root CA certificate is not self-signed, SN: ")
	require.Contains(t, err.Error(), ": x509: ECDSA verification failure")

	// add a root CA as an Intermediate CA
	decodedCertRootCA, _ := pem.Decode(certRootCA)
	caConf.Roots = append(caConf.Roots, decodedCertRootCA.Bytes)
	caConf.Intermediates = append(caConf.Intermediates, decodedCertRootCA.Bytes)

	_, _, err = leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config"+
		" tx, reason: CA certificate collection is invalid: root CA certificate is not self-signed, SN: ")
	require.Contains(t, err.Error(), ": x509: ECDSA verification failure")

	// update to empty CA
	configEnv, err = leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig = configEnv.GetResponse().GetConfig()
	newConfig.CertAuthConfig = nil
	version := configEnv.GetResponse().GetMetadata().GetVersion()

	_, _, err = leaderServer.SetConfigTx(t, newConfig, version, c.Servers[leaderIndex].AdminSigner(), "admin")
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx, reason: CA config is empty. At least one root CA is required")

}

// Scenario:
// - add Root CA & Intermediate CA to CertAuthConfig
// - try to remove the original CA - tx fails
func TestRemoveCA(t *testing.T) {
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

	// add a Root CA & Intermediate CA
	certRootCA, rootCAPrivKey, err := testutils.GenerateRootCA("Orion RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, certRootCA)
	require.NotNil(t, rootCAPrivKey)
	rootCAkeyPair, err := tls.X509KeyPair(certRootCA, rootCAPrivKey)
	require.NoError(t, err)
	require.NotNil(t, rootCAkeyPair)
	certIntCA, _, err := testutils.GenerateIntermediateCA("Orion IntermediateCA", "127.0.0.1", rootCAkeyPair)
	require.NoError(t, err)
	require.NotNil(t, certIntCA)

	configEnv, err := leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig := configEnv.GetResponse().GetConfig()
	caConf := newConfig.CertAuthConfig
	decodedCertRootCA, _ := pem.Decode(certRootCA)
	caConf.Roots = append(caConf.Roots, decodedCertRootCA.Bytes)
	decodedCertIntCA, _ := pem.Decode(certIntCA)
	caConf.Intermediates = append(caConf.Intermediates, decodedCertIntCA.Bytes)

	txID, rcpt, err := leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	// try to remove the original CA
	configEnv, err = leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig = configEnv.GetResponse().GetConfig()
	caConf = newConfig.CertAuthConfig
	caConf.Roots = caConf.Roots[1:]
	caConf.Intermediates = caConf.Intermediates[1:]

	txID, rcpt, err = leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx, reason: the node [node-1] has an invalid certificate: error verifying certificate against trusted certificate authority (CA): x509: certificate signed by unknown authority")
}
