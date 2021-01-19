package certificateauthority

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
)

//TODO Currently testing on a single root CA.
// Testing of multiple root CAs and multiple intermediate CAs will be done in:
// https://github.ibm.com/blockchaindb/server/issues/358

func TestNewCACertCollection(t *testing.T) {
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"user", "node"})
	userCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "user")
	caCert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.RootCAFileName)

	t.Run("valid CA certificate", func(t *testing.T) {
		caCertCollection, err := NewCACertCollection([][]byte{caCert.Raw}, nil)
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

	t.Run("untrusted CA certificate (self signed)", func(t *testing.T) {
		err := caCertCollection.VerifyLeafCert(untrustedCaCert.Raw)
		require.EqualError(t, err, "error verifying certificate against trusted certificate authority (CA): x509: certificate signed by unknown authority (possibly because of \"x509: ECDSA verification failure\" while trying to verify candidate authority certificate \"Clients RootCA\")")
	})
}

// Check the internal consistency of CA chains provided to the constructor.
func TestCACertCollection_VerifyCollection(t *testing.T) {
	//TODO Currently testing on a single root CA.
	// Testing of multiple root CAs and multiple intermediate CAs will be done in:
	// https://github.ibm.com/blockchaindb/server/issues/358
}
