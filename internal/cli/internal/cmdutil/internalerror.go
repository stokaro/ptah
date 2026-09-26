package cmdutil

import "fmt"

// InternalError is the error a recovered panic becomes. The root command
// prints it and exits 2 with it, and a verb that writes a result document
// records it as the run's error, so the document and standard error carry the
// same sentence.
//
// The recovered value is formatted rather than wrapped. A panic is a defect,
// and an error value it happened to carry must not make it read as a typed
// refusal to errors.As.
func InternalError(recovered any) error {
	return fmt.Errorf("internal error: %v", recovered)
}
