package errors

type NotFoundErr struct {
	Message string
}

func (e *NotFoundErr) Error() string {
	return e.Message
}

type PermissionErr struct {
	ErrMsg string
}

func (e *PermissionErr) Error() string {
	return e.ErrMsg
}

type TimeoutErr struct {
	ErrMsg string
}

func (t *TimeoutErr) Error() string {
	return t.ErrMsg
}
