// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package certificateauthority

import (
	"crypto/x509"

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
