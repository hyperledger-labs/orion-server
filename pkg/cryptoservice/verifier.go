package cryptoservice

import (
	"crypto/x509"
	"github.ibm.com/blockchaindb/server/pkg/logger"

	"github.ibm.com/blockchaindb/server/pkg/crypto"
)

//go:generate counterfeiter -o mocks/user_db_querier.go --fake-name UserDBQuerier . UserDBQuerier

type UserDBQuerier interface {
	GetCertificate(userID string) (*x509.Certificate, error)
}

func NewVerifier(userQuerier UserDBQuerier, logger *logger.SugarLogger) *SignatureVerifier {
	return &SignatureVerifier{
		userDBQuerier: userQuerier,
		logger:        logger,
	}
}

//TODO keep a cache of user and parsed certificates to avoid going to the DB and parsing the certificate
// on every TX. Provide a mechanism to invalidate the cache when the user database changes.

type SignatureVerifier struct {
	userDBQuerier UserDBQuerier
	logger        *logger.SugarLogger
}

func (sv *SignatureVerifier) Verify(userID string, signature, body []byte) error {
	cert, err := sv.userDBQuerier.GetCertificate(userID)
	if err != nil {
		sv.logger.Debugf("Error during GetCertificate: userID: %s, error: %s", userID, err)
		return err
	}
	verifier := crypto.Verifier{Certificate: cert}
	if err = verifier.Verify(body, signature); err != nil {
		sv.logger.Debugf("Failed to verify signature: userID: %s, error: %s", userID, err)
		return err
	}
	return err
}
