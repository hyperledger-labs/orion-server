package crypto_utils

import (
	"encoding/pem"
	"errors"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

var testNodeCrypto *testNodeCryptoProvider

func Init(t *testing.T) {
	testNodeCrypto = &testNodeCryptoProvider{rawCertificates: make(map[string][]byte, 0)}
	testNodeCrypto.rawCertificates["node1"] = loadRawCertificate(t, "../crypto/testdata/service.pem")
	testNodeCrypto.rawCertificates["node1_noca"] = loadRawCertificate(t, "../crypto/testdata/noca_service.pem")
}

func TestNewRegistry(t *testing.T) {
	Init(t)
	t.Run("NewVerifiersRegistry", func(t *testing.T) {
		opt := createOptions()
		registry, err := NewVerifiersRegistry(opt, testNodeCrypto)
		validateLoadedCrypto(t, registry, err)

		opt.CAFilePath = "../crypto/testdata/error_ca.cert"
		registry, err = NewVerifiersRegistry(opt, testNodeCrypto)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "could not read ca certificate")

		opt.CAFilePath = "../crypto/testdata/junk_ca.cert"
		registry, err = NewVerifiersRegistry(opt, testNodeCrypto)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "failed to append ca certs")
	})
}

func TestRegistryGetVerifier(t *testing.T) {
	Init(t)
	t.Run("VerifiersRegistry-GetVerifier", func(t *testing.T) {
		opt := createOptions()
		registry, err := NewVerifiersRegistry(opt, testNodeCrypto)
		validateLoadedCrypto(t, registry, err)

		require.Empty(t, registry.cache)
		verifier0, err := registry.GetVerifier("node1")
		require.NotNil(t, verifier0)
		require.NoError(t, err)
		require.Equal(t, 1, len(registry.cache))

		verifier1, err := registry.GetVerifier("node1")
		require.NotNil(t, verifier1)
		require.NoError(t, err)
		require.Equal(t, 1, len(registry.cache))
		require.Equal(t, verifier0, verifier1)

		verifier2, err := registry.GetVerifier("node1_noca")
		require.Nil(t, verifier2)
		require.Error(t, err)
		require.Equal(t, 1, len(registry.cache))

		verifier3, err := registry.GetVerifier("node2")
		require.Nil(t, verifier3)
		require.Error(t, err)
		require.Equal(t, 1, len(registry.cache))
	})
}

func validateLoadedCrypto(t *testing.T, registry *VerifiersRegistry, err error) {
	require.NoError(t, err)
	require.NotNil(t, registry)
	require.NotNil(t, registry.caCertificatesPool)
}

func createOptions() *VerificationOptions {
	return &VerificationOptions{
		CAFilePath: "../crypto/testdata/ca_service.cert",
	}
}

type testNodeCryptoProvider struct {
	rawCertificates map[string][]byte
}

func (t *testNodeCryptoProvider) GetRawCertificate(nodeID string) ([]byte, error) {
	rawCertificate, ok := t.rawCertificates[nodeID]
	if !ok {
		return nil, errors.New("can't find node crypto")
	}

	return rawCertificate, nil
}

func loadRawCertificate(t *testing.T, pemFile string) []byte {
	b, err := ioutil.ReadFile(pemFile)
	require.NoError(t, err)
	bl, _ := pem.Decode(b)
	require.NotNil(t, bl)
	return bl.Bytes
}
