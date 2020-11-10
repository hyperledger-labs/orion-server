package handlers

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/crypto"
	"github.ibm.com/blockchaindb/protos/types"
)

func TestSendHTTPResponse(t *testing.T) {
	t.Parallel()

	t.Run("ok status", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		dbStatus := &types.GetDBStatusResponseEnvelope{
			Payload: &types.GetDBStatusResponse{
				Header: &types.ResponseHeader{
					NodeID: "testID",
				},
			},
		}
		SendHTTPResponse(w, http.StatusOK, dbStatus)

		require.Equal(t, http.StatusOK, w.Code)
		actualDBStatus := &types.GetDBStatusResponseEnvelope{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), actualDBStatus))
		require.True(t, proto.Equal(dbStatus, actualDBStatus))
	})

	t.Run("forbidden status", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		err := &ResponseErr{
			ErrMsg: "user does not have a read permission",
		}
		SendHTTPResponse(w, http.StatusForbidden, err)

		require.Equal(t, http.StatusForbidden, w.Code)
		actualErr := &ResponseErr{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), actualErr))
		require.Equal(t, err, actualErr)
	})
}

func signatureFromQuery(t *testing.T, querySigner *crypto.Signer, query interface{}) []byte {
	sig, err := cryptoservice.SignQuery(querySigner, query)
	require.NoError(t, err)
	return sig
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
