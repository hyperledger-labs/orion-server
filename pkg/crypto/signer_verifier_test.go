// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package crypto

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewVerifierAndNewSigner(t *testing.T) {
	t.Run("NewVerifier", func(t *testing.T) {
		_, rawCert := createTestData(t)
		verifier, err := NewVerifier(rawCert)
		validateLoadedCrypto(t, verifier, err)

		// Change raw certificate bytes to make in invalid
		rawCert[0] += 1
		verifier, err = NewVerifier(rawCert)
		require.Nil(t, verifier)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "x509: malformed certificate")

	})

	t.Run("NewSigner", func(t *testing.T) {
		opt, _ := createTestData(t)

		signer, err := NewSigner(opt)
		require.NoError(t, err)
		require.NotNil(t, signer)
		require.Equal(t, opt.Identity, signer.Identity())

		// Non exist file
		opt.KeyFilePath = "testdata/error_client.key"
		signer, err = NewSigner(opt)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "no such file or directory")
		require.Nil(t, signer)

		// Non private key file
		opt.KeyFilePath = "testdata/client.pem"
		signer, err = NewSigner(opt)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "failed to find private key")
		require.Nil(t, signer)
	})
}

func TestSignAndVerify(t *testing.T) {
	t.Run("Verify Correctly", func(t *testing.T) {
		nodeSignerOpt := createSignerOptions()
		_, rawCert := createTestData(t)
		msgBytes := []byte("Test message bytes")
		loadSignAndVerify(t, rawCert, nodeSignerOpt, msgBytes)
	})

	t.Run("Verify Wrong Server Certificate Or Signature", func(t *testing.T) {
		_, rawCert := createTestData(t)
		nodeSignerOpt := createSignerOptions()
		msgBytes := []byte("Test message bytes")

		_, nodeSideSigner := loadUserSideVerifierAndNodeSideSigner(t, rawCert, nodeSignerOpt)

		signature, err := nodeSideSigner.Sign(msgBytes)
		require.NoError(t, err)
		require.NotNil(t, signature)

		// Correct certificate, but not one that sign message, for example user certificate, we get Verifier, but verification fails
		userSideVerifier, err := NewVerifier(loadRawCertificate(t, "testdata/client.pem"))
		require.NoError(t, err)
		require.NotNil(t, userSideVerifier)
		err = userSideVerifier.Verify(msgBytes, signature)
		require.Error(t, err)

		// Restore correct certificate, but we changed one byte in signature
		userSideVerifier, err = NewVerifier(rawCert)
		require.NoError(t, err)
		require.NotNil(t, userSideVerifier)
		wrongSignature := append([]byte{}, signature...)
		wrongSignature[0] += 1
		err = userSideVerifier.Verify(msgBytes, wrongSignature)
		require.Error(t, err)
	})

}

type pkcs8Key struct {
	Version    int
	Algo       []asn1.ObjectIdentifier
	PrivateKey []byte
}

func marshalPKCS8PrivKey(key *ecdsa.PrivateKey) ([]byte, error) {
	privateKey, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}

	return asn1.Marshal(pkcs8Key{
		Version: 0,
		// Taken from 	x509.go, the oidPublicKeyECDSA
		Algo:       []asn1.ObjectIdentifier{{1, 2, 840, 10045, 2, 1}},
		PrivateKey: privateKey,
	})
}

func TestKeyLoader(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)

	t.Run("checking PKCS#8", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, err)
		require.NotNil(t, key)
		keyBytes, err := marshalPKCS8PrivKey(key)
		require.NoError(t, err)
		require.NotNil(t, keyBytes)
		privatePem := pem.EncodeToMemory(
			&pem.Block{
				Type:  "PRIVATE KEY",
				Bytes: keyBytes,
			},
		)
		require.NotNil(t, privatePem)

		keyLoader := KeyLoader{}
		pKey, err := keyLoader.Load(privatePem)
		require.NoError(t, err)
		R, S, err := ecdsa.Sign(rand.Reader, pKey.(*ecdsa.PrivateKey), []byte{0})
		require.NoError(t, err)
		require.NotNil(t, R)
		require.NotNil(t, S)
		require.True(t, ecdsa.Verify(&key.PublicKey, []byte{0}, R, S))
	})

	t.Run("checking SEC1", func(t *testing.T) {
		t.Parallel()
		keyBytes, err := x509.MarshalECPrivateKey(key)
		require.NoError(t, err)
		require.NotNil(t, keyBytes)
		privatePem := pem.EncodeToMemory(
			&pem.Block{
				Type:  "PRIVATE KEY",
				Bytes: keyBytes,
			},
		)
		require.NotNil(t, privatePem)
		keyLoader := KeyLoader{}
		pKey, err := keyLoader.Load(privatePem)
		require.NoError(t, err)
		R, S, err := ecdsa.Sign(rand.Reader, pKey.(*ecdsa.PrivateKey), []byte{0})
		require.NoError(t, err)
		require.NotNil(t, R)
		require.NotNil(t, S)
		require.True(t, ecdsa.Verify(&key.PublicKey, []byte{0}, R, S))
	})
}

func validateLoadedCrypto(t *testing.T, verifier *Verifier, err error) {
	require.NoError(t, err)
	require.NotNil(t, verifier)
}

func createTestData(t *testing.T) (*SignerOptions, []byte) {
	return &SignerOptions{
			Identity:    "testUser",
			KeyFilePath: "testdata/client.key",
		},
		loadRawCertificate(t, "testdata/service.pem")
}

func loadRawCertificate(t *testing.T, pemFile string) []byte {
	b, err := ioutil.ReadFile(pemFile)
	require.NoError(t, err)
	bl, _ := pem.Decode(b)
	require.NotNil(t, bl)
	return bl.Bytes
}

func createSignerOptions() *SignerOptions {
	return &SignerOptions{
		KeyFilePath: "testdata/service.key",
	}
}

func loadUserSideVerifierAndNodeSideSigner(t *testing.T, verifierRawCert []byte, signerOpt *SignerOptions) (*Verifier, Signer) {
	userSideRegistry, err := NewVerifier(verifierRawCert)
	validateLoadedCrypto(t, userSideRegistry, err)

	nodeSideSigner, err := NewSigner(signerOpt)
	require.NoError(t, err)
	require.NotNil(t, nodeSideSigner)

	return userSideRegistry, nodeSideSigner
}

func loadSignAndVerify(t *testing.T, verifierRawCert []byte, signerOpt *SignerOptions, msgBytes []byte) {
	userSideVerifier, nodeSideSigner := loadUserSideVerifierAndNodeSideSigner(t, verifierRawCert, signerOpt)

	signature, err := nodeSideSigner.Sign(msgBytes)
	require.NoError(t, err)
	require.NotNil(t, signature)

	require.NoError(t, userSideVerifier.Verify(msgBytes, signature))
	userSideVerifier2 := &Verifier{Certificate: userSideVerifier.Certificate}
	require.NoError(t, userSideVerifier2.Verify(msgBytes, signature))
}
