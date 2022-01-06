package replication

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// RaftHTTPError is used when replication needs to send an error response to a raft request
type RaftHTTPError struct {
	Message string
	Code    int
}

func (e *RaftHTTPError) Error() string {
	return fmt.Sprintf("%s (%d)", e.Message, e.Code)
}

func (e *RaftHTTPError) WriteTo(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.Code)
	b, err := json.Marshal(e)
	if err != nil {
		panic(fmt.Sprintf("marshal RaftHTTPError should never fail: %s", err.Error()))
	}

	_, _ = w.Write(b)

	return
}
