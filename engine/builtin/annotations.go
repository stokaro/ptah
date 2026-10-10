package builtin

import (
	"ptah.run/core/annotation"
	"ptah.run/engine"
)

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

// Bundled returns the runtime of the bundled providers that the package-level
// helpers share. It is frozen and safe for concurrent use. A caller that
// needs the bundled owners of every source format, rather than one format's
// set, passes it.
func Bundled() (*engine.Runtime, error) {
	return bundled()
}
