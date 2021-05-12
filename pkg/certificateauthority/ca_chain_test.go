// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package certificateauthority

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server/testutils"
)

func TestNewCACertCollection(t *testing.T) {
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"user", "node"}, true)
	userCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "user")
	caCert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.RootCAFileName)
	midCaCert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.IntermediateCAFileName)

	t.Run("valid root CA certificate", func(t *testing.T) {
		caCertCollection, err := NewCACertCollection([][]byte{caCert.Raw}, nil)
		require.NoError(t, err)
		require.NotNil(t, caCertCollection)
		err = caCertCollection.VerifyCollection()
		require.NoError(t, err)
	})

	t.Run("valid root and intermediate CA certificate", func(t *testing.T) {
		caCertCollection, err := NewCACertCollection([][]byte{caCert.Raw}, [][]byte{midCaCert.Raw})
		require.NoError(t, err)
		require.NotNil(t, caCertCollection)
		err = caCertCollection.VerifyCollection()
		require.NoError(t, err)
	})

	t.Run("not a CA certificate", func(t *testing.T) {
		caCertCollection, err := NewCACertCollection([][]byte{userCert.Raw}, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "certificate is missing the CA property, SN:")
		require.Nil(t, caCertCollection)
	})

	t.Run("invalid certificate", func(t *testing.T) {
		caCertCollection, err := NewCACertCollection([][]byte{[]byte("invalid certificate")}, nil)
		require.EqualError(t, err, "asn1: structure error: tags don't match (16 vs {class:1 tag:9 length:110 isCompound:true}) {optional:false explicit:false application:false private:false defaultValue:<nil> tag:<nil> stringType:0 timeType:0 set:false omitEmpty:false} certificate @2")
		require.Nil(t, caCertCollection)
	})

	t.Run("on empty", func(t *testing.T) {
		caCertCollection, err := NewCACertCollection(nil, nil)
		require.NoError(t, err)
		require.NotNil(t, caCertCollection)
		err = caCertCollection.VerifyCollection()
		require.NoError(t, err)
	})
}

func TestCACertCollection_VerifyLeafCert(t *testing.T) {
	// Trusted CA
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"user"})
	userCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "user")
	caCert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.RootCAFileName)

	// Generate certificates from a different CA
	untrustedCryptoDir := testutils.GenerateTestClientCrypto(t, []string{"user", "node"})
	untrustedUserCert, _ := testutils.LoadTestClientCrypto(t, untrustedCryptoDir, "user")
	untrustedCaCert, _ := testutils.LoadTestClientCA(t, untrustedCryptoDir, testutils.RootCAFileName)

	caCertCollection, err := NewCACertCollection([][]byte{caCert.Raw}, nil)
	require.NoError(t, err)
	require.NotNil(t, caCertCollection)
	err = caCertCollection.VerifyCollection()
	require.NoError(t, err)

	t.Run("valid leaf certificate", func(t *testing.T) {
		err := caCertCollection.VerifyLeafCert(userCert.Raw)
		require.NoError(t, err)
	})

	t.Run("bad leaf certificate", func(t *testing.T) {
		err := caCertCollection.VerifyLeafCert([]byte("bad-certificate"))
		require.EqualError(t, err, "error parsing certificate: asn1: structure error: tags don't match (16 vs {class:1 tag:2 length:97 isCompound:true}) {optional:false explicit:false application:false private:false defaultValue:<nil> tag:<nil> stringType:0 timeType:0 set:false omitEmpty:false} certificate @2")
	})

	t.Run("untrusted leaf certificate", func(t *testing.T) {
		err := caCertCollection.VerifyLeafCert(untrustedUserCert.Raw)
		require.EqualError(t, err, "error verifying certificate against trusted certificate authority (CA): x509: certificate signed by unknown authority (possibly because of \"x509: ECDSA verification failure\" while trying to verify candidate authority certificate \"Clients RootCA\")")
	})

	t.Run("untrusted leaf certificate (self signed)", func(t *testing.T) {
		err := caCertCollection.VerifyLeafCert(untrustedCaCert.Raw)
		require.EqualError(t, err, "error verifying certificate against trusted certificate authority (CA): x509: certificate signed by unknown authority (possibly because of \"x509: ECDSA verification failure\" while trying to verify candidate authority certificate \"Clients RootCA\")")
	})
}

// Check the internal consistency of CA chains provided to the constructor.
func TestCACertCollection_VerifyCollection(t *testing.T) {
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"}, true)
	aliceCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")
	rootCACert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.RootCAFileName)
	midCACert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.IntermediateCAFileName)

	assert.NoError(t, midCACert.CheckSignatureFrom(rootCACert))
	assert.NoError(t, rootCACert.CheckSignatureFrom(rootCACert))
	assert.Error(t, midCACert.CheckSignatureFrom(midCACert))
	assert.NoError(t, aliceCert.CheckSignatureFrom(midCACert))

	t.Logf("root: SN: %d AKI: %x SKI: %x, I: %s, S: %s", rootCACert.SerialNumber, rootCACert.AuthorityKeyId, rootCACert.SubjectKeyId, rootCACert.Issuer, rootCACert.Subject)
	t.Logf("mid: SN: %d AKI: %x SKI: %x, I: %s, S: %s", midCACert.SerialNumber, midCACert.AuthorityKeyId, midCACert.SubjectKeyId, midCACert.Issuer, midCACert.Subject)
	t.Logf("user: SN: %d AKI: %x SKI: %x, I: %s, S: %s", aliceCert.SerialNumber, aliceCert.AuthorityKeyId, aliceCert.SubjectKeyId, aliceCert.Issuer, aliceCert.Subject)

	cryptoDir2 := testutils.GenerateTestClientCrypto(t, []string{"bob"}, true)
	bobCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir2, "bob")
	rootCACert2, _ := testutils.LoadTestClientCA(t, cryptoDir2, testutils.RootCAFileName)
	midCACert2, _ := testutils.LoadTestClientCA(t, cryptoDir2, testutils.IntermediateCAFileName)

	assert.NoError(t, midCACert2.CheckSignatureFrom(rootCACert2))
	assert.NoError(t, rootCACert2.CheckSignatureFrom(rootCACert2))
	assert.Error(t, midCACert2.CheckSignatureFrom(midCACert2))
	assert.NoError(t, bobCert.CheckSignatureFrom(midCACert2))

	t.Logf("root: SN: %d AKI: %x SKI: %x, I: %s, S: %s", rootCACert2.SerialNumber, rootCACert2.AuthorityKeyId, rootCACert2.SubjectKeyId, rootCACert2.Issuer, rootCACert2.Subject)
	t.Logf("mid: SN: %d AKI: %x SKI: %x, I: %s, S: %s", midCACert2.SerialNumber, midCACert2.AuthorityKeyId, midCACert2.SubjectKeyId, midCACert2.Issuer, midCACert2.Subject)
	t.Logf("user: SN: %d AKI: %x SKI: %x, I: %s, S: %s", bobCert.SerialNumber, bobCert.AuthorityKeyId, bobCert.SubjectKeyId, bobCert.Issuer, bobCert.Subject)

	assertVerify := func(t *testing.T, caCertCollection *CACertCollection, collValid, aliceValid, bobValid bool) {
		require.NotNil(t, caCertCollection)
		err := caCertCollection.VerifyCollection()
		if collValid {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}

		err = caCertCollection.VerifyLeafCert(aliceCert.Raw)
		if aliceValid {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}

		err = caCertCollection.VerifyLeafCert(bobCert.Raw)
		if bobValid {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}

	t.Run("valid CA Collection: 1 chain", func(t *testing.T) {
		caCertCollection, err := NewCACertCollection([][]byte{rootCACert.Raw}, [][]byte{midCACert.Raw})
		require.NoError(t, err)
		assertVerify(t, caCertCollection, true, true, false)
	})

	t.Run("valid CA Collection: 2 chains", func(t *testing.T) {
		caCertCollection, err := NewCACertCollection([][]byte{rootCACert.Raw, rootCACert2.Raw}, [][]byte{midCACert.Raw, midCACert2.Raw})
		require.NoError(t, err)
		assertVerify(t, caCertCollection, true, true, true)
	})

	t.Run("invalid CA collection: intermediate as root", func(t *testing.T) {
		caCertCollection, err := NewCACertCollection([][]byte{midCACert.Raw}, nil)
		require.NoError(t, err)
		assertVerify(t, caCertCollection, false, true, false)
	})

	t.Run("invalid CA collection: root & intermediate as roots", func(t *testing.T) {
		caCertCollection, err := NewCACertCollection([][]byte{rootCACert.Raw, midCACert.Raw, midCACert2.Raw, rootCACert2.Raw}, nil)
		require.NoError(t, err)
		assertVerify(t, caCertCollection, false, true, true)
	})

	t.Run("invalid CA collection: broken chain", func(t *testing.T) {
		caCertCollection, err := NewCACertCollection([][]byte{rootCACert2.Raw}, [][]byte{midCACert.Raw})
		require.NoError(t, err)
		assertVerify(t, caCertCollection, false, false, false)
	})

	t.Run("invalid CA collection: no root", func(t *testing.T) {
		caCertCollection, err := NewCACertCollection(nil, [][]byte{midCACert.Raw})
		require.NoError(t, err)
		assertVerify(t, caCertCollection, false, false, false)
	})

	t.Run("invalid CA collection: root as intermediate", func(t *testing.T) {
		caCertCollection, err := NewCACertCollection([][]byte{rootCACert.Raw}, [][]byte{midCACert.Raw, midCACert2.Raw, rootCACert2.Raw})
		require.NoError(t, err)
		assertVerify(t, caCertCollection, false, true, false)
	})
}
