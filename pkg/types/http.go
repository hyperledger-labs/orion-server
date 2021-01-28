package types

// HttpResponseErr holds an error message. It is used as the body of an http error response.
type HttpResponseErr struct {
	ErrMsg string `json:"error,omitempty"`
}

func (e *HttpResponseErr) Error() string {
	return e.ErrMsg
}
