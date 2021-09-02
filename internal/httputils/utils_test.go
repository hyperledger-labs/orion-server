// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httputils

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestSendHTTPResponse(t *testing.T) {
	t.Parallel()

	t.Run("ok status", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		dbStatus := &types.GetDBStatusResponseEnvelope{
			Response: &types.GetDBStatusResponse{
				Header: &types.ResponseHeader{
					NodeId: "testID",
				},
				Exist: false,
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
		err := &types.HttpResponseErr{
			ErrMsg: "user does not have a read permission",
		}
		SendHTTPResponse(w, http.StatusForbidden, err)

		require.Equal(t, http.StatusForbidden, w.Code)
		actualErr := &types.HttpResponseErr{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), actualErr))
		require.Equal(t, err, actualErr)
	})
}
