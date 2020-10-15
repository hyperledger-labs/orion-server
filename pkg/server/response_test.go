package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
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
			Error: "user does not have a read permission",
		}
		SendHTTPResponse(w, http.StatusForbidden, err)

		require.Equal(t, http.StatusForbidden, w.Code)
		actualErr := &ResponseErr{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), actualErr))
		require.Equal(t, err, actualErr)
	})
}
