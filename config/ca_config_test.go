package config

import (
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"path"
	"testing"

	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/stretchr/testify/require"
)

func TestCAConfiguration_WriteBundle(t *testing.T) {
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"user", "node"}, true)

	rootCAFileName := path.Join(cryptoDir, testutils.RootCAFileName+".pem")
	interCAFileName := path.Join(cryptoDir, testutils.IntermediateCAFileName+".pem")

	caConfig := CAConfiguration{
		RootCACertsPath:         []string{rootCAFileName},
		IntermediateCACertsPath: []string{interCAFileName},
	}

	caFile := path.Join(cryptoDir, "node", "ca-bundle.pem")
	err := caConfig.WriteBundle(caFile)
	require.NoError(t, err)

	bundleBytes, err := ioutil.ReadFile(caFile)
	require.NoError(t, err)
	require.NotNil(t, bundleBytes)

	var block *pem.Block
	for i := 0; i < 3; i++ {
		block, bundleBytes = pem.Decode(bundleBytes)
		if i < 2 {
			require.NotNil(t, block)
		} else {
			require.Nil(t, block)
			break
		}
		certRaw := block.Bytes
		cert, err := x509.ParseCertificate(certRaw)
		require.NoError(t, err)
		require.NotNil(t, cert)
		t.Logf("cert ca: %t, subject: %s, issuer: %s, serial: %v", cert.IsCA, cert.Subject, cert.Issuer, cert.SerialNumber)
	}
}
