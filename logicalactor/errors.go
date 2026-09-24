package logicalactor

import "errors"

var (
	ErrActorSystemNil              = errors.New("logical actor system is nil")
	ErrOwnerDirectoryNil           = errors.New("logical actor owner directory is nil")
	ErrActorFactoryNil             = errors.New("logical actor factory is nil")
	ErrActorFactoryReturnedNil     = errors.New("logical actor factory returned nil actor")
	ErrActorContextNil             = errors.New("logical actor context is nil")
	ErrFactoryNotFound             = errors.New("logical actor factory not found")
	ErrFactoryAlreadyExist         = errors.New("logical actor factory already registered")
	ErrOwnerMismatch               = errors.New("logical actor owner mismatch")
	ErrRouteForwardLimit           = errors.New("logical actor route forward limit exceeded")
	ErrPlacementStrategyNil        = errors.New("placement strategy is nil")
	ErrPlacementStrategyRegistered = errors.New("placement strategy already registered")
	ErrPlacementStrategyNotFound   = errors.New("placement strategy not found")
)
