// Package revisiontable holds the names of the tables Ptah's migrator records
// applied migrations in.
//
// It exists so that the migrator and the packages that must reason about the
// migrator's bookkeeping — notably internal/schemaclean, which has to name
// every object a destructive cleanup destroys — read one definition instead of
// each repeating a literal. A second copy of the literal would go stale the
// moment a default changed, and would silently under-report on any setup the
// copy was not written against.
//
// migration/migrator derives its own unexported defaults from these constants,
// so this package is the single source rather than a parallel one.
//
// It also answers WHERE the Atlas-compatible table lives, in [Schema]. That is
// a per-dialect fact rather than a constant, and it belongs beside the names
// for the same reason: the Atlas command surface and an adoption preflight both
// have to look in the one place a history was actually written, and two copies
// of the rule would send them to different schemas on the PostgreSQL family.
package revisiontable

import (
	"slices"
	"strings"

	"ptah.run/catalog"
)

const (
	// Ptah is the table Ptah's native revision layout records migrations in.
	Ptah = "schema_migrations"

	// Atlas is the table the Atlas-compatible revision layout records
	// migrations in.
	Atlas = "atlas_schema_revisions"

	// PtahLog is the table Ptah's native layout records migration attempts in.
	// The migrator derives it from whatever revision table it was given, so
	// this is the name a default configuration produces. The Atlas-compatible
	// layout has no counterpart: that contract defines the revision table and
	// nothing beside it.
	PtahLog = Ptah + LogSuffix

	// LogSuffix separates the operation log's table name from the revision
	// table's. The migrator names its log `<revision table>_log`, so an
	// operator who renamed or moved one finds the other beside it.
	LogSuffix = "_log"

	// PtahOperatorVersion is the generic operator marker for migrations without
	// a mapped source identity. Current mapped writes use the source-identity
	// marker instead, so a Flyway row with this generic value is eligible for
	// older ordering-key recovery.
	PtahOperatorVersion = "Ptah"

	// SourceBaselineOperatorVersion marks a baseline boundary that selected a
	// source migration whose provider identified it as an executed baseline.
	// The row remains Atlas's ordinary baseline type; this durable marker only
	// distinguishes it from a versioned migration baselined before a same-token
	// baseline file was introduced.
	SourceBaselineOperatorVersion = "Ptah/source-baseline"

	// SourceIdentityOperatorVersion marks a row whose version is the exact,
	// opaque source identity supplied through Atlas revision-version mapping.
	// A reader classifying a converted Flyway history uses it to tell an
	// applied versioned migration from a baseline, which the Atlas revision
	// type alone does not settle.
	SourceIdentityOperatorVersion = "Ptah/source-identity"
)

// DefaultNames returns the migrator's bookkeeping tables for every supported
// revision-table format, when no explicit table name is configured.
//
// Callers that enumerate bookkeeping tables in a live database must iterate all
// of them rather than picking the one matching the current configuration: a
// database can carry the residue of either format regardless of which format
// the invocation reading it happens to be configured for.
//
// An explicitly configured table name is deliberately absent. The dbschema
// readers leave out [NativeNames], and the SQL Server and YDB readers all of
// these, so only these can go missing from a schema snapshot; a custom name is
// read back as an ordinary table and needs no restoring.
func DefaultNames() []string {
	return []string{Atlas, Ptah, PtahLog}
}

// NativeNames returns the tables Ptah's native revision layout writes under
// their default names: the revision table and its operation log.
func NativeNames() []string {
	return []string{Ptah, PtahLog}
}

// IsNative reports whether name is one of [NativeNames].
//
// It is the predicate a schema reader leaves Ptah's own bookkeeping out with,
// by name or through [NativeSQLNames]. A reader that reports one of these tables
// hands the comparison a table no declaration has, and `migrations generate`
// then plans a DROP for the record of every applied migration. Measured on
// ClickHouse 26.9.8.3, whose reader had no such filter (stokaro/ptah#4029).
//
// The Atlas revision table is not in the set. The pinned community binary
// v1.3.0 reports both `atlas_schema_revisions` and `schema_migrations` from
// `schema inspect` as ordinary tables, measured on SQLite, and the readers of
// the dialects it supports keep the Atlas table visible so `ptah-compat
// schema inspect` lists it too. A caller that knows the configured revision
// format, such as `migrations generate`, removes it with [Configured] and
// [Without].
func IsNative(name string) bool {
	return slices.Contains(NativeNames(), name)
}

// IsDefault reports whether name is one of [DefaultNames]. The SQL Server and
// YDB readers leave all three out, since the community binary has no such
// dialect whose `schema inspect` they would have to match.
func IsDefault(name string) bool {
	return slices.Contains(DefaultNames(), name)
}

// NativeSQLNames is [NativeNames] as SQL string literals separated by commas,
// for a reader that leaves them out in its catalog query:
// `name NOT IN (` + NativeSQLNames + `)`. It is a constant so a query declared
// as one can use it; TestSQLNamesMatchTheNameLists keeps it equal to the list.
const NativeSQLNames = "'" + Ptah + "', '" + PtahLog + "'"

// DefaultSQLNames is [DefaultNames] as SQL string literals separated by commas,
// as [NativeSQLNames] is for the native set.
const DefaultSQLNames = "'" + Atlas + "', '" + Ptah + "', '" + PtahLog + "'"

// Configured returns the bookkeeping tables a migrator configured with format
// and table writes: the revision table, and for Ptah's native format the
// operation log beside it. An empty table selects the format's default name,
// and a format other than "atlas" is Ptah's native one, as it is to the
// migrator.
//
// A reader leaves only [DefaultNames] out, because a reader does not know the
// configuration. A caller that does, such as `migrations generate`, removes
// these names from what it read with [Without].
func Configured(format, table string) []string {
	table = strings.TrimSpace(table)
	if strings.EqualFold(strings.TrimSpace(format), "atlas") {
		if table == "" {
			table = Atlas
		}
		return []string{table}
	}
	if table == "" {
		table = Ptah
	}
	return []string{table, table + LogSuffix}
}

// Without returns a copy of schema with the tables named in names removed,
// together with the indexes and constraints declared on them. Names compare
// without regard to case, as the engines that fold unquoted names do.
//
// A nil schema yields an empty one, and schema itself is not changed.
func Without(schema *catalog.Database, names []string) *catalog.Database {
	if schema == nil {
		return &catalog.Database{}
	}
	named := func(table string) bool {
		return slices.ContainsFunc(names, func(name string) bool { return strings.EqualFold(name, table) })
	}
	out := *schema
	out.Tables = keep(out.Tables, func(table catalog.Table) bool { return !named(table.Name) })
	out.Indexes = keep(out.Indexes, func(index catalog.Index) bool { return !named(index.TableName) })
	out.Constraints = keep(out.Constraints, func(constraint catalog.Constraint) bool { return !named(constraint.TableName) })
	return &out
}

// keep returns the values keep answers true for, in order, in a new slice.
func keep[T any](values []T, keep func(T) bool) []T {
	out := make([]T, 0, len(values))
	for _, value := range values {
		if keep(value) {
			out = append(out, value)
		}
	}
	return out
}
