package schemadiff

// RefusalError records a completed check of common schema declarations that
// prevents comparison. It is distinct from a selected service failing to run
// or returning an invalid reply. Comparison wraps only its own deterministic
// declaration checks in this type; provider errors pass through unchanged.
type RefusalError struct {
	cause error
}

// Error preserves the declaration check's diagnostic. The zero value reports a
// refusal without a diagnostic and is not produced by the comparison pipeline.
func (e *RefusalError) Error() string {
	if e.cause == nil {
		return "schema comparison refused"
	}
	return e.cause.Error()
}

// Unwrap preserves the original schema or capability error for errors.Is and
// errors.As. The zero value unwraps to nil.
func (e *RefusalError) Unwrap() error { return e.cause }
