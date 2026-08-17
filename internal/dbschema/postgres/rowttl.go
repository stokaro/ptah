package postgres

import (
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/crdbttl"
)

// rowTTLOptionsExpr renders the projection carrying a table's row-level TTL
// storage parameters, and a constant on a target that cannot have one.
//
// The parameters live in pg_class.reloptions, the same column this reader
// already decodes for index storage parameters, and they are fetched the same
// way: array_to_json rather than the array literal, because an expression
// containing a quote is stored as an escape-string literal and the array
// literal escapes it a second time. Measured on CockroachDB v26.2.5, a table
// whose expression is `expires_at + INTERVAL '1 day'` gives
//
//	array_to_json  ["ttl='on'", "ttl_expiration_expression=e'expires_at + INTERVAL \\'1 day\\''", ...]
//
// which decodes to exactly the element `SELECT unnest(reloptions)` returns.
//
// The capability gate keeps the question off targets that cannot answer it. The
// column exists on PostgreSQL too, so an ungated projection would be valid
// there — but a read that asks a target about a feature it does not have is a
// read that has to be right about a catalog nobody exercises, and the Spanner
// PostgreSQL interface has already shown that a pg_catalog column existing is
// not the same as it being readable (stokaro/ptah#942).
func (r *Reader) rowTTLOptionsExpr() string {
	if !r.caps.Has(capability.RowLevelTTL) {
		return "'[]' AS row_ttl_options"
	}
	return "COALESCE(array_to_json(c.reloptions)::text, '[]') AS row_ttl_options"
}

// readRowTTL decodes the projection above into the policy the model carries.
//
// A table with no TTL, and every table on a target without the capability,
// decodes to nil rather than to an empty spec: "no row-expiry policy" is one
// state, and the comparator must not see two.
func readRowTTL(encoded string) (*ast.RowTTLSpec, error) {
	options, err := decodePostgresNameList(encoded)
	if err != nil {
		return nil, err
	}
	return crdbttl.FromReloptions(options), nil
}
