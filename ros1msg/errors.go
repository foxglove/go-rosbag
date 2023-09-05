package ros1msg

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidBool indicates a bool value was not 0 or 1.
	ErrInvalidBool = errors.New("invalid bool")
	// ErrUnrecognizedPrimitive indicates a primitive type was not recognized.
	ErrUnrecognizedPrimitive = errors.New("unrecognized primitive")
)

type ErrUnknownDependency struct {
	dependency string
}

func (e ErrUnknownDependency) Error() string {
	return "unknown dependency: " + e.dependency
}

type ErrMalformedField struct {
	lineNumber int
	line       string
}

func (e ErrMalformedField) Error() string {
	return fmt.Sprintf("malformed field on line %d: %s", e.lineNumber, e.line)
}
