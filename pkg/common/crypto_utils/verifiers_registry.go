package crypto_utils

import (
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"

	"github.ibm.com/blockchaindb/server/pkg/common/crypto"
)

// VerifiersRegistry provides access to Verifiers, creates if needed and store them in cache
type VerifiersRegistry struct {
	caCertificatesPool *x509.CertPool
	fetcher            CertificateFetcher
	// TODO: Map based cache implementation, should be replaced by smarter one in future
	cache map[string]*crypto.Verifier
}

type CertificateFetcher interface {
	GetRawCertificate(id string) ([]byte, error)
}

// NewVerifiersRegistry creates new registry, fetcher interface used to access raw certificates - over REST or from local DB or whatever way we will use in future
func NewVerifiersRegistry(opt *VerificationOptions, fetcher CertificateFetcher) (*VerifiersRegistry, error) {
	caCertificatesPool := x509.NewCertPool()
	ca, err := ioutil.ReadFile(opt.CAFilePath)
	if err != nil {
		return nil, fmt.Errorf("could not read ca certificate: %s", err)
	}

	if ok := caCertificatesPool.AppendCertsFromPEM(ca); !ok {
		return nil, errors.New("failed to append ca certs")
	}
	return &VerifiersRegistry{
		caCertificatesPool: caCertificatesPool,
		fetcher:            fetcher,
		cache:              make(map[string]*crypto.Verifier, 0),
	}, nil
}

// GetVerifier gets Verifier from cache and if not in cache, creates one, including validation of raw certificate
func (r *VerifiersRegistry) GetVerifier(entityID string) (*crypto.Verifier, error) {
	verifier, ok := r.cache[entityID]
	if ok {
		return verifier, nil
	}

	rawCert, err := r.fetcher.GetRawCertificate(entityID)
	if err != nil {
		return nil, err
	}

	cert, err := x509.ParseCertificate(rawCert)
	if err != nil {
		return nil, err
	}

	// TODO: we not support chains longer than 2 at first phase, so intermediates pool can stay empty and there is no need to add already validated
	// certificate to pool
	chains, err := cert.Verify(x509.VerifyOptions{
		Intermediates: x509.NewCertPool(),
		Roots:         r.caCertificatesPool,
	})
	if err != nil {
		return nil, err
	}
	if chains == nil || len(chains) == 0 {
		return nil, errors.New("server certificate didn't pass verification")
	}

	newVerifier, err := crypto.NewVerifier(rawCert)
	if err != nil {
		return nil, err
	}

	r.cache[entityID] = newVerifier
	return newVerifier, nil
}

// VerificationOptions contains all configuration need to verify other side certificates and signatures
type VerificationOptions struct {
	CAFilePath string
}
