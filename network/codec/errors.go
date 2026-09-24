package codec

import "errors"

var (
	ErrInvalidOptions  = errors.New("invalid codec options")
	ErrInvalidFlag     = errors.New("codec flag must contain exactly one bit")
	ErrDuplicateFlag   = errors.New("duplicate codec flag")
	ErrUnknownFlags    = errors.New("unknown codec flags")
	ErrDecodedTooLarge = errors.New("decoded payload exceeds limit")
)
