package schemaext

import "fmt"

// UnknownCodecError identifies a missing local registry entry before its codec
// can run. It preserves the requested model and representation. Codec callback
// failures are returned unchanged, even if they wrap ErrUnknownCodec; that
// sentinel alone does not establish a completed registry lookup refusal.
type UnknownCodecError struct {
	Kind           Kind
	Representation Representation
}

// Error names the model the selected registry cannot interpret. The zero value
// names an empty request and is not produced by a validated registry operation.
func (e *UnknownCodecError) Error() string {
	return fmt.Sprintf("%s: %q/%s", ErrUnknownCodec, e.Kind, e.Representation)
}

// Unwrap retains ErrUnknownCodec for callers using errors.Is.
func (*UnknownCodecError) Unwrap() error { return ErrUnknownCodec }
