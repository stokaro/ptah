package postgres

import (
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbschema"
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
// The query must run inside the owning database: pg_class is per-database on
// CockroachDB as it is on PostgreSQL, and the same statement against defaultdb
// returns no row at all, which would read as a table without a TTL. Nothing
// here parses crdb_internal, which v26.2.5 restricts.
//
// The capability gate keeps the question off targets that cannot answer it. The
// column exists on PostgreSQL too, so an ungated projection would be valid
// there — but a read that asks a target about a feature it does not have is a
// read that has to be right about a catalog nobody exercises, and the Spanner
// PostgreSQL interface has already shown that a pg_catalog column existing is
// not the same as it being readable (stokaro/ptah#942).
func (r *Reader) rowTTLOptionsExpr() string {
	if !r.readsRowTTL() {
		return "'[]' AS row_ttl_options"
	}
	return "COALESCE(array_to_json(c.reloptions)::text, '[]') AS row_ttl_options"
}

// readsRowTTL reports whether this read describes CockroachDB row-level TTL.
// Only then do the tables it returns carry the owned facet and its coverage.
// The capability is CockroachDB's alone, so the facet and its coverage are
// CockroachDB's whatever dialect name the caller gave the reader.
func (r *Reader) readsRowTTL() bool {
	return r.caps.Has(capability.RowLevelTTL)
}

// rowTTLFacets decodes the projection above into the observed policy, attached
// as the owner's facet. A table without a TTL gets no facet; the coverage
// [Reader.rowTTLCoverage] records for it is what makes that an observed
// absence rather than an unread value.
func (r *Reader) rowTTLFacets(encoded string) (schemaext.Facets, error) {
	if !r.readsRowTTL() {
		return schemaext.Facets{}, nil
	}
	options, err := decodePostgresNameList(encoded)
	if err != nil {
		return schemaext.Facets{}, err
	}
	parameters := make(map[string]string, len(options))
	for _, option := range options {
		name, value, ok := strings.Cut(option, "=")
		if !ok {
			continue
		}
		parameters[strings.ToLower(strings.TrimSpace(name))] = unquoteStorageParameter(strings.TrimSpace(value))
	}
	observed := crdbschema.DecodeStored(parameters)
	if observed == nil {
		return schemaext.Facets{}, nil
	}
	if err := crdbschema.ValidateObserved(observed); err != nil {
		return schemaext.Facets{}, err
	}
	facets, err := schemaext.NewFacets(observed)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return facets.WithTargetScope(crdbschema.RowTTLKind, platform.CockroachDB)
}

// rowTTLCoverage records complete row-level TTL knowledge for exactly the
// tables this read returned. A table the read did not return is not known to
// have no TTL: a schema outside the read, or a table a later filter removes,
// says nothing about its policy.
func (r *Reader) rowTTLCoverage(schema *catalog.Database) error {
	if !r.readsRowTTL() {
		return nil
	}
	identities := objectidentity.NewBuilder(identifier.ForDialect(platform.CockroachDB))
	subjects := make([]schemaext.SubjectCoverage, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		subjects = append(subjects, schemaext.SubjectCoverage{
			Kind: crdbschema.RowTTLKind, Subject: identities.TableParts(table.Schema, table.Name),
			Knowledge: schemaext.Knowledge{State: schemaext.Complete},
		})
	}
	known, err := crdbschema.RowTTLCoverage(schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables have an inspected CockroachDB row-level TTL"}, subjects)
	if err != nil {
		return fmt.Errorf("failed to record row-level TTL coverage: %w", err)
	}
	schema.FeatureCoverage, err = schema.FeatureCoverage.Combine(known)
	return err
}

// unquoteStorageParameter reads the value half of one storage parameter.
//
// CockroachDB writes a string parameter in one of TWO forms, and which one it
// picks depends on the value, so a decoder that knows only the first silently
// corrupts an expression containing a quote -- which is not an exotic case,
// since `expires_at + INTERVAL '1 day'` is the shape the engine's own
// documentation uses. Measured on v26.2.5, reading the array element back with
// `SELECT unnest(reloptions)`:
//
//	declared expires_at                    element ttl_expiration_expression='expires_at'
//	declared expires_at + INTERVAL '1 day' element ttl_expiration_expression=e'expires_at + INTERVAL \'1 day\''
//
// The second is an escape-string literal: the value is delimited by single
// quotes, prefixed with e, and an embedded quote is BACKSLASH-escaped rather
// than doubled. So a plain literal carries no escapes at all -- any quote in
// the value forces the e form -- and the e literal is unescaped by removing one
// backslash before each escaped character.
//
// A value that is not quoted at all (every numeric and boolean knob, and
// `schema_locked=true`) is returned as it stands.
func unquoteStorageParameter(value string) string {
	value = stripTypeAnnotation(value)
	escaped := strings.HasPrefix(value, "e'")
	if escaped {
		value = value[1:]
	}
	if len(value) < 2 || !strings.HasPrefix(value, "'") || !strings.HasSuffix(value, "'") {
		return value
	}
	value = value[1 : len(value)-1]
	if !escaped {
		return value
	}
	return unescapeStorageParameter(value)
}

// unescapeStorageParameter removes one level of backslash escaping, which is
// what the escape-string form carries. A trailing lone backslash cannot appear
// in a well-formed literal and is kept rather than dropped, so a malformed value
// round-trips as itself instead of losing a character.
func unescapeStorageParameter(value string) string {
	var out strings.Builder
	out.Grow(len(value))
	for i := 0; i < len(value); i++ {
		if value[i] == '\\' && i+1 < len(value) {
			i++
		}
		out.WriteByte(value[i])
	}
	return out.String()
}

// stripTypeAnnotation removes the `:::TYPE` suffix CockroachDB writes on a
// typed storage parameter.
//
// Only one parameter carries it, and only because its value is not a string:
// measured on v26.2.5, `ttl_expire_after = '3 days'` is stored as the element
// `ttl_expire_after='3 days':::INTERVAL`, while every string-valued parameter
// beside it is stored with no annotation at all. Leaving it on would make the
// value differ from anything a declaration could write, so the parameter would
// never compare equal to itself. The suffix is removed rather than parsed: the
// type the server chose is not something Ptah models.
func stripTypeAnnotation(value string) string {
	annotation := strings.LastIndex(value, ":::")
	if annotation < 0 {
		return value
	}
	return value[:annotation]
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
	if !r.hasHiddenColumns() {
		return ""
	}
	return "AND " + hiddenAttribute("a") + " = false"
}

// hiddenKeyFilter excludes a constraint whose every column is one
// [Reader.hiddenColumnFilter] leaves out, and adds nothing on a target that has
// no hidden columns. It is a HAVING clause over the constraint read, which
// groups one constraint's key columns into one row.
//
// The shape it exists for is the key CockroachDB gives a table that declares
// none. Measured on v25.4.16, v26.2.7 and v26.3.2, `CREATE TABLE t (id INT8)`
// reports `t_pkey` in pg_constraint with contype 'p' and conkey {2}, the attnum
// of the hidden rowid, and information_schema.table_constraints lists it as a
// PRIMARY KEY. Described, it names a column the description does not have: the
// comparison plans `DROP CONSTRAINT t_pkey` against every schema that declares
// the table as it was written, and the server refuses the statement -- v26 with
// SQLSTATE 57000 because the table is schema_locked, v25.4 with 0A000 because a
// primary key cannot be dropped without adding another (stokaro/ptah#3738).
//
// Every column rather than any, and whatever the constraint type. A hash-sharded
// primary key spans the hidden shard column and the declared one, conkey {1,2}
// on v26.3.2, and is the author's key, so it stays. The CHECK CockroachDB puts
// on the shard column alone is the engine's, like the key over rowid, and goes.
// A column the author declared NOT VISIBLE is hidden too, and its key goes with
// it for the reason the column does: the description cannot hold one without
// the other.
//
// bool_and over a constraint with no key column is NULL, and COALESCE keeps it.
func (r *Reader) hiddenKeyFilter() string {
	if !r.hasHiddenColumns() {
		return ""
	}
	return "HAVING NOT COALESCE(bool_and(" + hiddenAttribute("local_column") + "), false)"
}

// hasHiddenColumns reports whether the target has CockroachDB's hidden columns,
// which is what both filters above are gated on. See [Reader.hiddenColumnFilter]
// for why the key is RowLevelTTL.
func (r *Reader) hasHiddenColumns() bool {
	return r.caps.Has(capability.RowLevelTTL)
}

// hiddenAttribute is the one spelling of "CockroachDB hides this column" over a
// pg_attribute alias. The column read and the constraint read have to agree on
// it: a constraint the second keeps over a column the first drops is a
// description naming a column it does not hold.
func hiddenAttribute(alias string) string {
	return "COALESCE(" + alias + ".attishidden, false)"
}
