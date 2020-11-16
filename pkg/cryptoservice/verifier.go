package cryptoservice

import (
	"crypto/x509"

	"github.ibm.com/blockchaindb/server/pkg/common/crypto"
)

//go:generate counterfeiter -o mocks/user_db_querier.go --fake-name UserDBQuerier . UserDBQuerier

type UserDBQuerier interface {
	GetCertificate(userID string) (*x509.Certificate, error)
}

func NewVerifier(userQuerier UserDBQuerier) *SignatureVerifier {
	return &SignatureVerifier{
		userDBQuerier: userQuerier,
	}
}

//TODO keep a cache of user and parsed certificates to avoid going to the DB and parsing the certificate
// on every TX. Provide a mechanism to invalidate the cache when the user database changes.

type SignatureVerifier struct {
	userDBQuerier UserDBQuerier
}

func (sv *SignatureVerifier) Verify(userID string, signature, body []byte) error {
	cert, err := sv.userDBQuerier.GetCertificate(userID)
	if err != nil {
		return err
	}
	verifier := crypto.Verifier{Certificate: cert}
	return verifier.Verify(body, signature)
}
