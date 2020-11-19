package cryptoservice_test

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io/ioutil"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/internal/cryptoservice"
	"github.ibm.com/blockchaindb/server/internal/cryptoservice/mocks"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
)

func TestNewVerifier(t *testing.T) {
	verifier := cryptoservice.NewVerifier(&mocks.UserDBQuerier{})
	require.NotNil(t, verifier)
}

func TestSignatureVerifier_Verify(t *testing.T) {
	userData := generateUserData(t)
	userDB := &mocks.UserDBQuerier{}
	userDB.GetCertificateCalls(
		func(userID string) (*x509.Certificate, error) {
			cert, ok := userData[userID]
			if ok {
				return cert, nil
			}
			return nil, errors.New("user not found")
		},
	)
	verifier := cryptoservice.NewVerifier(userDB)
	require.NotNil(t, verifier)

	t.Run("Verify correctly", func(t *testing.T) {
		for _, name := range []string{"alice", "bob", "noca_alice", "noca_bob"} {
			msgBytes := []byte(name + " is my name!")
			signer, err := crypto.NewSigner(&crypto.SignerOptions{KeyFilePath: path.Join("testdata", name+".key")})
			require.NoError(t, err)
			sig, err := signer.Sign(msgBytes)
			require.NoError(t, err)

			err = verifier.Verify(name, sig, msgBytes)
			require.NoError(t, err)
		}
	})

	t.Run("Signature mismatch", func(t *testing.T) {
		bobSigner, err := crypto.NewSigner(&crypto.SignerOptions{KeyFilePath: path.Join("testdata", "bob.key")})
		require.NoError(t, err)
		bobSig, err := bobSigner.Sign([]byte("alice is the queen"))
		require.NoError(t, err)

		err = verifier.Verify("alice", bobSig, []byte("alice is the queen"))
		require.EqualError(t, err, "x509: ECDSA verification failure")
	})

	t.Run("Message mismatch", func(t *testing.T) {
		bobSigner, err := crypto.NewSigner(&crypto.SignerOptions{KeyFilePath: path.Join("testdata", "bob.key")})
		require.NoError(t, err)
		bobSig, err := bobSigner.Sign([]byte("bob is the king"))
		require.NoError(t, err)

		err = verifier.Verify("bob", bobSig, []byte("bob is not the king"))
		require.EqualError(t, err, "x509: ECDSA verification failure")
	})

	t.Run("Bad certificate", func(t *testing.T) {
		err := verifier.Verify("charlie", []byte{1, 2, 3, 4}, []byte("charlie's certificate is bad"))
		require.EqualError(t, err, "x509: cannot verify signature: algorithm unimplemented")
	})

	t.Run("Unknown user", func(t *testing.T) {
		aliceSigner, err := crypto.NewSigner(&crypto.SignerOptions{KeyFilePath: path.Join("testdata", "alice.key")})
		require.NoError(t, err)
		aliceSig, err := aliceSigner.Sign([]byte("alice is the queen"))
		require.NoError(t, err)
		err = verifier.Verify("unknown-user", aliceSig, []byte("alice is the queen"))
		require.EqualError(t, err, "user not found")
	})
}

func generateUserData(t *testing.T) map[string]*x509.Certificate {
	userData := make(map[string]*x509.Certificate)

	for _, name := range []string{"alice", "bob", "noca_alice", "noca_bob"} {
		rawCert := loadRawCertificate(t, path.Join("testdata", name+".pem"))
		cert, err := x509.ParseCertificate(rawCert)
		require.NoError(t, err)
		userData[name] = cert
	}

	userData["charlie"] = &x509.Certificate{}

	return userData
}

func loadRawCertificate(t *testing.T, pemFile string) []byte {
	b, err := ioutil.ReadFile(pemFile)
	require.NoError(t, err)
	bl, _ := pem.Decode(b)
	require.NotNil(t, bl)
	return bl.Bytes
}
