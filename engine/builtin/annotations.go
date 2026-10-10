package builtin

import "ptah.run/core/annotation"

// Annotations returns the Go annotation extensions of the bundled providers:
// every owner directive the bundled runtime decodes. A caller that parses Go
// annotations for a bundled target passes it to the parse, or passes the
// Annotations of a runtime it assembled itself.
func Annotations() (annotation.Set, error) {
	runtime, err := bundled()
	if err != nil {
		return annotation.Set{}, err
	}
	return runtime.Annotations(), nil
}
