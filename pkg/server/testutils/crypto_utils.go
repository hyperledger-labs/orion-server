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

const RootCAFileName = "rootCA"
const IntermediateCAFileName = "intermediateCA"

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
	if err != nil {
		return nil, nil, err
	}

	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	keyBytes, err := x509.MarshalECPrivateKey(privKey)
	if err != nil {
		return nil, nil, err
	}

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
	if err != nil {
		return nil, nil, err
	}

	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyBytes, err := x509.MarshalECPrivateKey(privKey)
	if err != nil {
		return nil, nil, err
	}

	keyPem := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	return certPem, keyPem, nil
}

func GenerateIntermediateCA(subjectCN string, host string, rootCAKeyPair tls.Certificate) ([]byte, []byte, error) {
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
	template.KeyUsage |= x509.KeyUsageCertSign
	template.IsCA = true

	certBytes, err := x509.CreateCertificate(rand.Reader, template, ca, pubKey, rootCAKeyPair.PrivateKey)
	if err != nil {
		return nil, nil, err
	}

	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	keyBytes, err := x509.MarshalECPrivateKey(privKey)
	if err != nil {
		return nil, nil, err
	}

	caPvtPemByte := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	return certPem, caPvtPemByte, nil
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

func GenerateTestClientCrypto(t *testing.T, names []string, withIntermediateCA... bool) string {
	withInterCA := false
	if len(withIntermediateCA) >0 {
		withInterCA = withIntermediateCA[0]
	}

	tempDir, err := ioutil.TempDir("/tmp", "UnitTestCrypto")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	rootCAPemCert, caPrivKey, err := GenerateRootCA("Clients RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, rootCAPemCert)
	require.NotNil(t, caPrivKey)

	rootCACertFile, err := os.Create(path.Join(tempDir, RootCAFileName+".pem"))
	require.NoError(t, err)
	_, err = rootCACertFile.Write(rootCAPemCert)
	require.NoError(t, err)
	rootCACertFile.Close()

	rootCAKeyFile, err := os.Create(path.Join(tempDir, RootCAFileName+".key"))
	require.NoError(t, err)
	_, err = rootCAKeyFile.Write(caPrivKey)
	require.NoError(t, err)
	rootCAKeyFile.Close()

	rootCAkeyPair, err := tls.X509KeyPair(rootCAPemCert, caPrivKey)
	require.NoError(t, err)
	require.NotNil(t, rootCAkeyPair)

	var keyPair tls.Certificate
	if withInterCA {
		intermediateCAPemCert, intermediateCAPrivKey, err := GenerateIntermediateCA("Clients IntermediateCA", "127.0.0.1", rootCAkeyPair)
		intermediateCACertFile, err := os.Create(path.Join(tempDir, IntermediateCAFileName+".pem"))
		require.NoError(t, err)
		_, err = intermediateCACertFile.Write(intermediateCAPemCert)
		require.NoError(t, err)
		intermediateCACertFile.Close()

		intermediateCAKeyFile, err := os.Create(path.Join(tempDir, IntermediateCAFileName+".key"))
		require.NoError(t, err)
		_, err = intermediateCAKeyFile.Write(intermediateCAPrivKey)
		require.NoError(t, err)
		intermediateCAKeyFile.Close()

		keyPair, err = tls.X509KeyPair(intermediateCAPemCert, intermediateCAPrivKey)
		require.NoError(t, err)
		require.NotNil(t, keyPair)
	} else {
		keyPair = rootCAkeyPair
	}

	for _, name := range names {
		pemCert, privKey, err := IssueCertificate("BCDB Client "+name, "127.0.0.1", keyPair)
		require.NoError(t, err)

		pemCertFile, err := os.Create(path.Join(tempDir, name+".pem"))
		require.NoError(t, err)
		_, err = pemCertFile.Write(pemCert)
		require.NoError(t, err)
		err = pemCertFile.Close()
		require.NoError(t, err)

		pemPrivKeyFile, err := os.Create(path.Join(tempDir, name+".key"))
		require.NoError(t, err)
		_, err = pemPrivKeyFile.Write(privKey)
		require.NoError(t, err)
		err = pemPrivKeyFile.Close()
		require.NoError(t, err)
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

func LoadTestClientCA(t *testing.T, tempDir, name string) (cert *x509.Certificate, key []byte) {
	cert = getTestdataCert(t, path.Join(tempDir, name+".pem"))
	require.True(t, cert.IsCA)

	keyPEMBlock, err := ioutil.ReadFile(path.Join(tempDir, name+".key"))
	require.NoError(t, err)

	keyLoader := crypto.KeyLoader{}
	_, err = keyLoader.Load(keyPEMBlock)
	require.NoError(t, err)

	return cert, keyPEMBlock
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
	err = ver.Verify(payloadBytes, sig)
	require.NoError(t, err)
}
