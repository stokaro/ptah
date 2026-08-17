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

// hiddenColumnFilter excludes the columns CockroachDB creates and hides, and
// adds nothing on a target that has no such notion.
//
// PostgreSQL has no hidden columns; CockroachDB has two kinds, and both reach a
// description that does not filter them:
//
//   - `crdb_internal_expiration`, which `ttl_expire_after` creates. Measured on
//     v26.2.5, `WITH (ttl_expire_after = '3 days')` adds
//     `crdb_internal_expiration TIMESTAMPTZ NOT VISIBLE NOT NULL DEFAULT
//     current_timestamp() + '3 days'`, reported by information_schema.columns
//     with is_hidden = YES and by pg_attribute with attishidden = t.
//   - `rowid`, which a table declaring no primary key gets. This one is older
//     than row-level TTL and already leaked: measured before this change,
//     `ptah db read` against a CockroachDB table created as
//     `CREATE TABLE nokey (a INT, b STRING)` described a third column,
//     `"rowid" bigint PRIMARY KEY NOT NULL DEFAULT unique_rowid()`.
//
// Neither is a column anybody declared, and both make a description
// unreplayable: applying it back asks for a column the engine owns. Filtering
// them is what lets `ttl_expire_after` converge at all, and it fixes the older
// leak as a consequence rather than as a separate change (stokaro/ptah#1605).
//
// The predicate is capability-gated because attishidden is a CockroachDB
// column: measured, PostgreSQL 18.4 and YugabyteDB 2026.1 have neither
// pg_attribute.attishidden nor information_schema.columns.is_hidden, so naming
// it unconditionally would break every read on both. RowLevelTTL is the right
// gate rather than a dialect check because it is exactly the CockroachDB-only
// key, and the hidden columns are a CockroachDB-only shape.
//
// COALESCE covers the LEFT JOIN: a column with no pg_attribute row is not
// hidden, and a NULL there must not filter it out.
func (r *Reader) hiddenColumnFilter() string {
	if !r.caps.Has(capability.RowLevelTTL) {
		return ""
	}
	return "AND COALESCE(a.attishidden, false) = false"
}
