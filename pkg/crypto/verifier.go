// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package crypto

import (
	"crypto/x509"
)

// Verifier is cryptographic primitive used only to validate message signature, each node usually access multiple Verifiers.
type Verifier struct {
	Certificate *x509.Certificate
}

// NewVerifier creates Verifier from shared root certificates pool and raw verifying entity certificate.
func NewVerifier(rawCert []byte) (*Verifier, error) {
	cert, err := x509.ParseCertificate(rawCert)
	if err != nil {
		return nil, err
	}
	return &Verifier{
		Certificate: cert,
	}, nil

}

// Verify verifies signature
func (v *Verifier) Verify(msgBytes []byte, signature []byte) error {
	return v.Certificate.CheckSignature(v.Certificate.SignatureAlgorithm, msgBytes, signature)
}
