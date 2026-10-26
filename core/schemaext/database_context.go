package schemaext

import (
	"context"
	"fmt"
	"strings"
	"unicode"
)

// DatabaseContext identifies the target database and the root used to interpret
// authored absolute references. It contains no connection address or credential.
// The selected target owns both strings; a caller must not derive either from
// an object identity or rewrite one using the other's path syntax.
type DatabaseContext struct {
	Database       string `json:"database"`
	SourceDatabase string `json:"source_database"`
}

// IsZero reports that the target supplies no database interpretation context.
func (c DatabaseContext) IsZero() bool { return c == (DatabaseContext{}) }

// Validate requires both exact, nonempty, single-line root spellings. The zero
// value represents an unavailable service and is not a binding receipt.
func (c DatabaseContext) Validate() error {
	for _, root := range []string{c.Database, c.SourceDatabase} {
		if root == "" || strings.TrimSpace(root) != root || strings.ContainsFunc(root, func(r rune) bool {
			return unicode.IsControl(r) || r == '\u2028' || r == '\u2029'
		}) {
			return fmt.Errorf("%w: invalid database interpretation context", ErrInvalidValue)
		}
	}
	return nil
}

// DatabaseContextRequest asks the selected target to interpret an opaque
// connection address without connecting, provisioning, or resolving credentials.
// A refusal must not include Address, which may contain credentials.
type DatabaseContextRequest struct {
	Target  string
	Address string
}

// DatabaseContextResolver resolves one address through its selected target.
// A runtime returns the zero value when that target registers no resolver.
// A registered service must return a complete receipt or an error.
type DatabaseContextResolver interface {
	ResolveDatabaseContext(context.Context, DatabaseContextRequest) (DatabaseContext, error)
}

// ValueContextRequest binds a complete ordered batch to an established receipt.
// Binding preserves representation, payload ownership, and authored identity.
// It may complete dependency facts, but it never changes the target context of
// an already bound value. Services must not retain or mutate request values.
type ValueContextRequest struct {
	Target         string
	Context        DatabaseContext
	Representation Representation
	Values         []Value
}

// ValueContextService binds model interpretation before dependency consumers
// and scope selection. It returns the complete ordered batch or no result.
type ValueContextService interface {
	BindValueContexts(context.Context, ValueContextRequest) ([]Value, error)
}

// ValueContextRuntime supplies explicit binding and the selected model codecs.
type ValueContextRuntime interface {
	DatabaseContextResolver
	ValueContextService
	TargetResolver
	ModelRuntime
}
