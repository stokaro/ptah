package schemaext

import "fmt"

// InvalidModelError records a completed local rejection of a concrete model
// representation. A codec or model validator may return it after inspecting the
// supplied data. It must not stand for cancellation, unavailable services, or
// failed provider calls. Message describes the violated representation rule;
// it contains no operational cause that could be mistaken for a valid refusal.
// Errors.Is identifies ErrInvalidValue through Unwrap.
type InvalidModelError struct {
	Kind           Kind
	Representation Representation
	Message        string
}

// Error identifies the rejected model and the representation rule it violated.
func (e *InvalidModelError) Error() string {
	return fmt.Sprintf("%s model %q: %s", e.Representation, e.Kind, e.Message)
}

// Unwrap classifies a completed model rejection as an invalid feature value.
func (*InvalidModelError) Unwrap() error { return ErrInvalidValue }
