package errors

import "errors"

var (
	ErrNotFound          = errors.New("record not found")
	ErrBucketMissing     = errors.New("bucket does not exist")
	ErrInvalidData       = errors.New("invalid data format")
	ErrNilValue          = errors.New("nil value provided")
	ErrDatabaseNotFound  = errors.New("database not found")
	ErrDatabaseExists    = errors.New("database already exists")
	ErrNoDefaultDatabase = errors.New("no default database set")
)
