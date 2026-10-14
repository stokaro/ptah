package dbschema

import (
	"context"
	"fmt"

	"ptah.run/core/platform/identifier"
)

// ResolveIdentifierSemantics resolves how the connected catalog compares the
// given identifiers. names is the finite set the caller is about to compare --
// schema and table names, typically -- and wherever the resolution consults
// it, the connection's default schema joins the set whether the caller listed
// it or not.
//
// A dialect whose identifier rules are fixed answers from its own
// deterministic local semantics without consulting names at all, so a nil
// slice is a valid call. Where the server is the only authority on the answer
// the resolution asks it: SQL Server evaluates equivalence under
// CATALOG_DEFAULT, and the result's ResolvedName entries then report which of
// the names the catalog treats as the same identifier -- names sharing a Key
// are one name to the catalog.
//
// Asking the server can fail, and a failure is reported rather than guessed
// around. A nil receiver, an identifier the dialect cannot hold, and a catalog
// that cannot answer each return an error and a zero [identifier.Semantics].
//
// The call may append to names, so it can write into the caller's backing
// array; pass a clone when that array must stay untouched.
func (dc *DatabaseConnection) ResolveIdentifierSemantics(
	ctx context.Context,
	names []string,
) (identifier.Semantics, error) {
	if dc == nil {
		return identifier.Semantics{}, fmt.Errorf("resolve identifier semantics: database connection is nil")
	}
	// The connection is the executor, so the probe a MySQL-family server is
	// asked joins whatever session and transaction the connection routes to.
	return dc.engine().ResolveIdentifierSemantics(ctx, dc, dc.Info(), names)
}
