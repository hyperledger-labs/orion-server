package main

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/pem"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"

	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/stretchr/testify/require"
)

func TestSigner(t *testing.T) {
	t.Run("incorrect args", func(t *testing.T) {
		for _, arg := range []string{
			"",
			"-data='{\"userID\":\"admin\"}'",
			"-privatekey=../../deployment/sample/crypto/admin/admin.key",
		} {
			out, err := exec.Command("go", "run", "signer.go", arg).CombinedOutput()
			require.NoError(t, err)

			expectedOut := help + "\n  -data string\n    \tjson data to be signed. Surround that data with single quotes." +
				" An example json data is '{\"userID\":\"admin\"}'\n  -privatekey string\n    \tpath to the private key" +
				" to be used for adding a digital signature\n"
			require.Equal(t, expectedOut, string(out))
		}
	})

	t.Run("correct args", func(t *testing.T) {
		tempDir := t.TempDir()

		adminCert, adminKey := createAdminCreds(t, tempDir)
		data := "'{\"user_id\":\"admin\"}'"
		args := []string{
			"run",
			"signer.go",
			"-data=" + data,
			"-privatekey=" + adminKey,
		}

		out, err := exec.Command("go", args...).Output()
		require.NoError(t, err)

		b, err := ioutil.ReadFile(adminCert)
		require.NoError(t, err)
		bl, _ := pem.Decode(b)
		require.NotNil(t, bl)
		verifier, err := crypto.NewVerifier(bl.Bytes)
		require.NoError(t, err)

		signature, err := base64.StdEncoding.DecodeString(string(out))
		require.NoError(t, err)
		require.NoError(t, verifier.Verify([]byte(data), signature))
	})
}

func createAdminCreds(t *testing.T, tempDir string) (string, string) {
	rootCAPemCert, caPrivKey, err := testutils.GenerateRootCA("BCDB RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, rootCAPemCert)
	require.NotNil(t, caPrivKey)

	keyPair, err := tls.X509KeyPair(rootCAPemCert, caPrivKey)
	require.NoError(t, err)
	require.NotNil(t, keyPair)

	pemAdminCert, pemAdminKey, err := testutils.IssueCertificate("BCDB Admin", "127.0.0.1", keyPair)
	require.NoError(t, err)
	pemAdminCertFile, err := os.Create(path.Join(tempDir, "admin.pem"))
	require.NoError(t, err)
	_, err = pemAdminCertFile.Write(pemAdminCert)
	require.NoError(t, err)
	require.NoError(t, pemAdminCertFile.Close())

	pemAdminKeyFile, err := os.Create(path.Join(tempDir, "admin.key"))
	require.NoError(t, err)
	_, err = pemAdminKeyFile.Write(pemAdminKey)
	require.NoError(t, err)
	require.NoError(t, pemAdminKeyFile.Close())

	return filepath.Join(tempDir, "admin.pem"), filepath.Join(tempDir, "admin.key")
}
