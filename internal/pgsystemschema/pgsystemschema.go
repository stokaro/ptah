// Package pgsystemschema answers, from a schema NAME alone, which namespaces a
// PostgreSQL-family server owns rather than the user, and renders the
// predicate a realm read keeps the user's schemas with. Every member of the
// family owns information_schema and the pg_ namespaces, which this package
// holds. A member that owns more states them in its dialect descriptor
// (descriptor.SystemSchemas), and the answers here take the base set plus
// what the descriptor states, so an engine adds its schemas without editing
// this package.
//
// It is a leaf on purpose. The question is about names and dialects, so it
// needs neither a connection nor a database URL, and the packages that render
// SQL from Go structs reach it without linking a database driver.
// [ptah.run/internal/mysqlsystemdb] answers the same question for the MySQL
// family, and [ptah.run/internal/schemaselection] answers the other half --
// what a URL and a session say about the schema a run works in.
package pgsystemschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/descriptortable"
	"ptah.run/internal/sqlident"
)

// AnyMember is the dialect a caller names when it cannot tell which member of
// the PostgreSQL family it is connected to, such as a schema writer built
// without a dialect. The answers for it add the schemas every registered
// dialect states, so a schema any member owns is left alone.
const AnyMember = "*"

// stated is the schemas each dialect's descriptor adds to the base set. The
// answers are asked for each schema name a document or a plan holds, so they
// are derived once per descriptor rather than copied out of the registry for
// every name.
var stated = descriptortable.New(func(described descriptor.Dialect) descriptor.SystemSchemas {
	return described.SystemSchemas
})

// everyMember is the schemas every dialect of the registry states, each list
// in the order of the registry and without repeats: the answer for
// [AnyMember].
var everyMember = descriptortable.NewIndex(func(dialects []descriptor.Dialect) descriptor.SystemSchemas {
	var union descriptor.SystemSchemas
	for _, described := range dialects {
		union.Catalog = appendMissing(union.Catalog, described.SystemSchemas.Catalog)
		union.Reserved = appendMissing(union.Reserved, described.SystemSchemas.Reserved)
		union.Implicit = appendMissing(union.Implicit, described.SystemSchemas.Implicit)
		union.ReservedPrefixes = appendMissing(union.ReservedPrefixes, described.SystemSchemas.ReservedPrefixes)
		union.ReservedCaseVariants = appendMissing(union.ReservedCaseVariants, described.SystemSchemas.ReservedCaseVariants)
	}
	return union
})

// appendMissing appends each of names that schemas does not hold yet.
func appendMissing(schemas, names []string) []string {
	for _, name := range names {
		if !slices.Contains(schemas, name) {
			schemas = append(schemas, name)
		}
	}
	return schemas
}

// added is the schemas the dialect adds to the base set: its descriptor's,
// or every member's for [AnyMember].
func added(dialect string) descriptor.SystemSchemas {
	if dialect == AnyMember {
		return everyMember.Get()
	}
	return stated.Get(dialect)
}

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
// The gate that decides whether a realm is clean enough for a first
// `migrate apply` reads the realm through this predicate too. Its question is
// whether anything the user made is here, and the pinned binary answers it
// from the same inspection: measured on PostgreSQL 18, v1.3.0 applies against
// a database whose only schemas besides `public` are TimescaleDB's, or one an
// ALTER EXTENSION ... ADD SCHEMA handed to an extension.
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
// a `pg_namespace n`: information_schema, the pg_ namespaces, and the catalog
// schemas the dialect's descriptor states (descriptor.SystemSchemas.Catalog),
// in the order it states them.
//
// The ESCAPE clause matters: in LIKE, `_` matches any single character, so an
// unescaped 'pg\_%' would also hide a user schema named `pgapp` and describe
// less of the database than is there.
func PostgresNonSystemSchemasPredicate(dialect string) string {
	predicates := []string{`n.nspname <> 'information_schema'`}
	for _, schema := range added(dialect).Catalog {
		predicates = append(predicates, `n.nspname <> `+sqlident.StringLiteral(schema))
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
// PostgreSQL-family target: a name [IsPostgresSystemSchema] reports, or one
// the dialect's descriptor reserves (descriptor.SystemSchemas): by name, by
// prefix, or as a spelling in another case of a name the server refuses in
// every case. Matching is otherwise exact, so a quoted lookalike in another
// case is still a user identifier.
func IsPostgresFamilySystemSchema(dialect, name string) bool {
	if IsPostgresSystemSchema(name) {
		return true
	}
	schemas := added(dialect)
	return slices.Contains(schemas.Reserved, name) ||
		slices.ContainsFunc(schemas.ReservedPrefixes, func(prefix string) bool {
			return strings.HasPrefix(name, prefix)
		}) ||
		slices.ContainsFunc(schemas.ReservedCaseVariants, func(stated string) bool {
			return name != stated && strings.EqualFold(name, stated)
		})
}

// IsUncreatableSchema reports whether the target owns name and will not create
// it, so a migration has to put objects into it without asking for it first:
// a name [IsPostgresFamilySystemSchema] reports, or one the dialect's
// descriptor states every database holds (descriptor.SystemSchemas.Implicit).
//
// It is deliberately wider than [IsPostgresFamilySystemSchema] and used for a
// narrower thing. An implicit schema need not be a catalog namespace: it may
// hold the user's own tables, as Spanner's `public` does. A document that
// DECLARES such a schema is still accepted, because a schema read writes
// exactly that block, and refusing to read back what Ptah wrote would be the
// worse fault.
func IsUncreatableSchema(dialect, name string) bool {
	return IsPostgresFamilySystemSchema(dialect, name) || slices.Contains(added(dialect).Implicit, name)
}

// ValidateDeclaredPostgresSystemSchemas refuses schema declarations that ask a
// PostgreSQL-family migration to create a server-owned namespace. Extensions
// may still name these schemas as installation placement without declaring a
// schema block.
func ValidateDeclaredPostgresSystemSchemas(dialect string, schemas []schemamodel.Schema) error {
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
