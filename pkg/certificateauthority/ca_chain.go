// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package certificateauthority

import (
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

type CACertCollection struct {
	roots         []*x509.Certificate
	intermediates []*x509.Certificate
	opts          x509.VerifyOptions
}

// NewCACertCollection creates a new  CACertCollection from a set of root CAs and
// intermediate CAs. The certificate are in raw format, i.e. ASN.1 DER data.
func NewCACertCollection(rootCAs [][]byte, intermediateCAs [][]byte) (*CACertCollection, error) {
	certCollection := &CACertCollection{
		opts: x509.VerifyOptions{Intermediates: x509.NewCertPool(), Roots: x509.NewCertPool()},
	}

	for _, asn1Data := range rootCAs {
		cert, err := x509.ParseCertificate(asn1Data)
		if err != nil {
			return nil, err
		}
		if !cert.IsCA {
			return nil, errors.Errorf("certificate is missing the CA property, SN: %v", cert.SerialNumber)
		}
		certCollection.roots = append(certCollection.roots, cert)
		certCollection.opts.Roots.AddCert(cert)
	}

	for _, asn1Data := range intermediateCAs {
		cert, err := x509.ParseCertificate(asn1Data)
		if err != nil {
			return nil, err
		}
		if !cert.IsCA {
			return nil, errors.Errorf("certificate is missing the CA property, SN: %v", cert.SerialNumber)
		}
		certCollection.intermediates = append(certCollection.intermediates, cert)
		certCollection.opts.Intermediates.AddCert(cert)
	}

	return certCollection, nil
}

// VerifyLeafCert verifies the given leaf certificate against the CA certificates in the collection.
func (c *CACertCollection) VerifyLeafCert(asn1Data []byte) error {
	cert, err := x509.ParseCertificate(asn1Data)
	if err != nil {
		return errors.Wrap(err, "error parsing certificate")
	}
	_, err = cert.Verify(c.opts)
	if err != nil {
		return errors.Wrap(err, "error verifying certificate against trusted certificate authority (CA)")
	}
	return nil
}

// VerifyCollection verifies each CA certificate in the collection, to make sure each one is part of a valid chain.
func (c *CACertCollection) VerifyCollection() error {
	//Make sure each root CA is self-signed
	for _, rootCert := range c.roots {
		if err := rootCert.CheckSignatureFrom(rootCert); err != nil {
			return errors.Wrapf(err, "root CA certificate is not self-signed, SN: %v", rootCert.SerialNumber)
		}
	}
	//Make sure there is a valid chain from each certificate to a root.
	allCerts := append(c.roots, c.intermediates...)
	for _, cert := range allCerts {
		_, err := cert.Verify(c.opts)
		if err != nil {
			return errors.Wrapf(err, "error verifying CA certificate against trusted certificate authority (CA), SN: %v", cert.SerialNumber)
		}
	}

	//TODO should we require a single chain?

	return nil
}

// GetCertPool combines all the CA certificates, root & intermediate, into a single x509.CertPool.
func (c *CACertCollection) GetCertPool() *x509.CertPool {
	pool := x509.NewCertPool()

	for _, cert := range c.roots {
		pool.AddCert(cert)
	}

	for _, cert := range c.intermediates {
		pool.AddCert(cert)
	}

	return pool
}

// LoadCAConfig loads the Root CA and Intermediate CA certificates defined in the configuration.
func LoadCAConfig(caConfiguration *config.CAConfiguration) (*types.CAConfig, error) {
	if len(caConfiguration.RootCACertsPath) == 0 {
		return nil, errors.New("CA configuration paths have empty RootCACertsPath")
	}

	caCerts := &types.CAConfig{}
	for _, certPath := range caConfiguration.RootCACertsPath {
		rootCACert, err := ioutil.ReadFile(certPath)
		if err != nil {
			return nil, errors.Wrapf(err, "error while reading root CA certificate %s", certPath)
		}
		caPemCert, _ := pem.Decode(rootCACert)
		caCerts.Roots = append(caCerts.Roots, caPemCert.Bytes)
	}

	for _, certPath := range caConfiguration.IntermediateCACertsPath {
		caCert, err := ioutil.ReadFile(certPath)
		if err != nil {
			return nil, errors.Wrapf(err, "error while reading intermediate CA certificate %s", certPath)
		}
		caPemCert, _ := pem.Decode(caCert)
		caCerts.Intermediates = append(caCerts.Intermediates, caPemCert.Bytes)
	}

	return caCerts, nil
}
