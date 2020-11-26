package testutils

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func IssueCertificate(subjectCN string, host string, rootCAKeyPair tls.Certificate) ([]byte, []byte, error) {
	ca, err := x509.ParseCertificate(rootCAKeyPair.Certificate[0])
	if err != nil {
		return nil, nil, err
	}

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	pubKey := privKey.Public()

	ip := net.ParseIP(host)
	template, err := CertTemplate(subjectCN, []net.IP{ip})
	if err != nil {
		return nil, nil, err
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, template, ca, pubKey, rootCAKeyPair.PrivateKey)

	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	keyBytes, err := x509.MarshalECPrivateKey(privKey)
	caPvtPemByte := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	return certPem, caPvtPemByte, nil
}

func GenerateRootCA(subjectCN string, host string) ([]byte, []byte, error) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	pubKey := privKey.Public()

	ip := net.ParseIP(host)
	template, err := CertTemplate(subjectCN, []net.IP{ip})
	if err != nil {
		return nil, nil, err
	}
	template.KeyUsage |= x509.KeyUsageCertSign
	template.IsCA = true

	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, pubKey, privKey)

	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyBytes, err := x509.MarshalECPrivateKey(privKey)
	keyPem := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	return certPem, keyPem, nil
}

func CertTemplate(subjectCN string, ips []net.IP) (*x509.Certificate, error) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	SN, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, err
	}

	return &x509.Certificate{
		Subject:               pkix.Name{CommonName: subjectCN},
		SerialNumber:          SN,
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IPAddresses:           ips,
	}, nil
}

func getTestdataCert(t *testing.T, pathToCert string) *x509.Certificate {
	b, err := ioutil.ReadFile(pathToCert)
	require.NoError(t, err)
	bl, _ := pem.Decode(b)
	require.NotNil(t, bl)
	certRaw := bl.Bytes
	cert, err := x509.ParseCertificate(certRaw)
	require.NoError(t, err)
	return cert
}

func GenerateTestClientCrypto(t *testing.T, names []string) string {
	tempDir, err := ioutil.TempDir("/tmp", "handlersUnitTest")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	rootCAPemCert, caPrivKey, err := GenerateRootCA("Clients RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, rootCAPemCert)
	require.NotNil(t, caPrivKey)

	clientRootCACertFile, err := os.Create(path.Join(tempDir, "clientRootCACert.pem"))
	require.NoError(t, err)
	clientRootCACertFile.Write(rootCAPemCert)
	clientRootCACertFile.Close()

	for _, name := range names {
		keyPair, err := tls.X509KeyPair(rootCAPemCert, caPrivKey)
		require.NoError(t, err)
		require.NotNil(t, keyPair)

		pemCert, privKey, err := IssueCertificate("BCDB Client "+name, "127.0.0.1", keyPair)
		require.NoError(t, err)

		pemCertFile, err := os.Create(path.Join(tempDir, name+".pem"))
		require.NoError(t, err)
		pemCertFile.Write(pemCert)
		pemCertFile.Close()

		pemPrivKeyFile, err := os.Create(path.Join(tempDir, name+".key"))
		require.NoError(t, err)
		pemPrivKeyFile.Write(privKey)
		pemPrivKeyFile.Close()
	}

	return tempDir
}

func LoadTestClientCrypto(t *testing.T, tempDir, name string) (*x509.Certificate, crypto.Signer) {
	cert := getTestdataCert(t, path.Join(tempDir, name+".pem"))
	signer, err := crypto.NewSigner(
		&crypto.SignerOptions{KeyFilePath: path.Join(tempDir, name+".key")})
	require.NoError(t, err)

	return cert, signer
}

func SignatureFromTx(t *testing.T, signer crypto.Signer, tx interface{}) []byte {
	sig, err := cryptoservice.SignTx(signer, tx)
	require.NoError(t, err)
	return sig
}

func SignatureFromQuery(t *testing.T, signner crypto.Signer, query interface{}) []byte {
	sig, err := cryptoservice.SignQuery(signner, query)
	require.NoError(t, err)
	return sig
}

func SignatureFromQueryResponse(t *testing.T, signer crypto.Signer, queryResp interface{}) []byte {
	sig, err := cryptoservice.SignQueryResponse(signer, queryResp)
	require.NoError(t, err)
	return sig
}

func SignedDataTxEnvelope(t *testing.T, signer crypto.Signer, tx *types.DataTx) *types.DataTxEnvelope {
	env := &types.DataTxEnvelope{
		Payload:   tx,
		Signature: SignatureFromTx(t, signer, tx),
	}
	return env
}

func SignedConfigTxEnvelope(t *testing.T, signer crypto.Signer, tx *types.ConfigTx) *types.ConfigTxEnvelope {
	env := &types.ConfigTxEnvelope{
		Payload:   tx,
		Signature: SignatureFromTx(t, signer, tx),
	}
	return env
}

func SignedUserAdministrationTxEnvelope(t *testing.T, signer crypto.Signer, tx *types.UserAdministrationTx) *types.UserAdministrationTxEnvelope {
	env := &types.UserAdministrationTxEnvelope{
		Payload:   tx,
		Signature: SignatureFromTx(t, signer, tx),
	}
	return env
}

func SignedDBAdministrationTxEnvelope(t *testing.T, signer crypto.Signer, tx *types.DBAdministrationTx) *types.DBAdministrationTxEnvelope {
	env := &types.DBAdministrationTxEnvelope{
		Payload:   tx,
		Signature: SignatureFromTx(t, signer, tx),
	}
	return env
}

func VerifyPayloadSignature(t *testing.T, rawCert []byte, payload interface{}, sig []byte) {
	ver, err := crypto.NewVerifier(rawCert)
	require.NoError(t, err)
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	ver.Verify(payloadBytes, sig)
}
