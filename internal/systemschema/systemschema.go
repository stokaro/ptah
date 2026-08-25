// Package systemschema answers, from a schema NAME alone, which namespaces a
// database server owns rather than the user.
//
// It is a leaf on purpose. The question is about names and dialects, so it
// needs neither a connection nor a database URL, and the packages that render
// SQL from Go structs reach it without linking a database driver.
// [go.5x5.cz/ptah/internal/schemaselection] answers the other half -- what a
// URL and a session say about the schema a run works in -- and parses a DSN to
// do it, which is why the two questions live in two packages.
package systemschema

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
)

// PostgresDescribedSchemasPredicate is the realm predicate a DESCRIPTION uses:
// [PostgresNonSystemSchemasPredicate] plus the schemas an extension owns.
//
// An extension that installs itself into an existing schema adds no namespace
// of its own, which is the case the reader's own extension-membership comment
// measured on PostgreSQL 17.10 and the reason `public` never enters that set.
// An extension that CREATES its schemas is the other case, and it was not
// covered. Measured against TimescaleDB 2.29.2 on PostgreSQL 17.11, one
// `CREATE EXTENSION timescaledb` adds seven namespaces --
// `_timescaledb_cache`, `_timescaledb_catalog`, `_timescaledb_config`,
// `_timescaledb_functions`, `_timescaledb_internal`, `timescaledb_experimental`
// and `timescaledb_information` -- and every one of them carries a pg_depend
// row of deptype 'e' pointing at the extension.
//
// Without this arm those seven schemas and the 51 relations in them are
// described as objects Ptah owns: `schema inspect` on a database holding one
// user table answered 4003 lines, against 24 from the pinned Atlas community
// binary v1.3.0, which describes the table and `public` and nothing else. A
// document like that is not merely noisy — replayed against another database
// it asks for the extension's own catalog, table by table.
//
// The arm is gated on [capability.CatalogDependencies] because pg_depend is
// what it reads, and that key already records where the relation is absent:
// the Cloud Spanner emulator through PGAdapter answers
// `relation "pg_depend" does not exist`, which was measured again here rather
// than assumed. Omitting the arm there costs nothing, because that target has
// no CREATE EXTENSION to own a schema with.
//
// The gate that decides whether a realm is empty enough to clean keeps
// [PostgresNonSystemSchemasPredicate] instead. It answers a different question
// -- is anything here -- and an extension's schema is something that is here.
// Moving it is a parity measurement of its own.
func PostgresDescribedSchemasPredicate(dialect string, caps capability.Capabilities) string {
	predicate := PostgresNonSystemSchemasPredicate(dialect)
	if !caps.Has(capability.CatalogDependencies) {
		return predicate
	}
	return predicate + `
			  AND NOT EXISTS (
			        SELECT 1
			        FROM pg_depend d
			        WHERE d.classid = 'pg_namespace'::regclass
			          AND d.objid = n.oid
			          AND d.deptype = 'e')`
}

// PostgresNonSystemSchemasPredicate returns the WHERE predicate that keeps the
// schemas a PostgreSQL-family realm describes and drops the server's own, over
// a `pg_namespace n`.
//
// The ESCAPE clause matters: in LIKE, `_` matches any single character, so an
// unescaped 'pg\_%' would also hide a user schema named `pgapp` and describe
// less of the database than is there.
//
// The predicate is derived from the normalized dialect because PostgreSQL,
// CockroachDB and YugabyteDB share a reader but not an identical system
// namespace set. CockroachDB exposes crdb_internal through the PostgreSQL
// catalog surface, but its virtual relations are not ordinary user tables and
// PostgreSQL readers cannot inspect them as comparison input.
func PostgresNonSystemSchemasPredicate(dialect string) string {
	predicates := []string{`n.nspname <> 'information_schema'`}
	if platform.NormalizeDialect(dialect) == platform.CockroachDB {
		predicates = append(predicates, `n.nspname <> 'crdb_internal'`)
	}
	predicates = append(predicates, `n.nspname NOT LIKE 'pg\_%' ESCAPE '\'`)
	return strings.Join(predicates, `
			  AND `)
}

// IsPostgresSystemSchema reports whether name is a PostgreSQL-owned namespace
// that a user migration must not try to create. Names beginning with pg_ are
// reserved by PostgreSQL; information_schema is the other catalog namespace
// exposed by every PostgreSQL-family server. The comparison is exact because
// an authored quoted name such as "PG_APP" is a distinct user identifier.
func IsPostgresSystemSchema(name string) bool {
	return name == "information_schema" || strings.HasPrefix(name, "pg_")
}

// IsPostgresFamilySystemSchema reports whether name is owned by the concrete
// PostgreSQL-family target. CockroachDB adds crdb_internal to PostgreSQL's
// common catalog namespaces. Matching remains exact so a quoted lookalike such
// as "CRDB_INTERNAL" is still a user identifier.
func IsPostgresFamilySystemSchema(dialect, name string) bool {
	return IsPostgresSystemSchema(name) ||
		platform.NormalizeDialect(dialect) == platform.CockroachDB && name == "crdb_internal"
}

// IsUncreatableSchema reports whether the target owns name and will not create
// it, so a migration has to put objects into it without asking for it first.
//
// It is deliberately wider than [IsPostgresFamilySystemSchema] and used for a
// narrower thing. Spanner's `public` is not a catalog namespace -- it holds the
// user's own tables -- but it is implicit there and cannot be created:
// `CREATE SCHEMA IF NOT EXISTS "public"` is refused with `Schema name not
// valid: public`, measured on the PGAdapter emulator v0.55.2, and the IF NOT
// EXISTS does not help because the refusal is about the name rather than the
// existence. A document that DECLARES `schema "public"` is still accepted:
// `ptah schema inspect` writes exactly that block against Spanner, and refusing
// to read back what Ptah wrote would be the worse fault.
func IsUncreatableSchema(dialect, name string) bool {
	if IsPostgresFamilySystemSchema(dialect, name) {
		return true
	}
	if platform.NormalizeDialect(dialect) != platform.Spanner {
		return false
	}
	return name == "public" || name == "spanner_sys"
}

// ValidateDeclaredPostgresSystemSchemas refuses schema declarations that ask a
// PostgreSQL-family migration to create a server-owned namespace. Extensions
// may still name these schemas as installation placement without declaring a
// schema block.
func ValidateDeclaredPostgresSystemSchemas(dialect string, schemas []goschema.Schema) error {
	normalized := platform.NormalizeDialect(dialect)
	if !platform.IsPostgresFamily(normalized) {
		return nil
	}
	for _, schema := range schemas {
		if !IsPostgresFamilySystemSchema(normalized, schema.Name) {
			continue
		}
		return &ptaherr.RenderError{
			Dialect: normalized,
			Err:     ptaherr.ErrInvalidSchemaDiff,
			Message: fmt.Sprintf(
				"schema declares server-owned PostgreSQL schema %q; extension placement may reference it, but a migration cannot create it",
				schema.Name,
			),
		}
	}
	return nil
}
