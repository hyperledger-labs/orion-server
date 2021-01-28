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

// DuplicateTxIDError is an error to denote that a transaction
// has a duplicate txID
type DuplicateTxIDError struct {
	TxID string
}

func (e DuplicateTxIDError) Error() string {
	return "the transaction has a duplicate txID [" + e.TxID + "]"
}
