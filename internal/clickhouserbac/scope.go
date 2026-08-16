package clickhouserbac

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/sqlident"
)

// Scope is the object a ClickHouse grant applies to.
//
// ClickHouse has no object-type keyword. Where PostgreSQL writes
// `ON TABLE x` or `ON SCHEMA s`, ClickHouse writes a two-part pattern —
// `db.table`, `db.*`, or `*.*` — and the shape of the pattern IS the object
// type. So [ast.GrantPrivilegeNode]'s ObjectType is interpreted here and never
// emitted.
//
// Ptah models a ClickHouse schema as a database, which is why a declared
// `on_schema` becomes a database scope and a declared `on_table` becomes a
// table scope in that database.
type Scope struct {
	// Database is the database the grant applies to. It is never empty: the
	// global `*.*` scope is refused rather than modeled, because a privilege
	// granted server-wide reaches objects no declared schema describes.
	Database string

	// Table is the table within Database, empty for a database-wide scope.
	Table string
}

// String renders the scope as ClickHouse spells it, with both parts quoted.
//
// The wildcard is written bare because it is syntax rather than an identifier:
// `db`.* is a database scope and `db`.`*` is a table literally named `*`.
// Quoting it would silently turn one into the other, which is why every caller
// asks this function instead of assembling the pattern itself.
func (s Scope) String() string {
	database := sqlident.Quote(platform.ClickHouse, s.Database)
	if s.Table == "" {
		return database + ".*"
	}
	return database + "." + sqlident.Quote(platform.ClickHouse, s.Table)
}

// Describe renders the scope unquoted, for a diagnostic a person reads.
func (s Scope) Describe() string {
	if s.Table == "" {
		return s.Database + ".*"
	}
	return s.Database + "." + s.Table
}

// Contains reports whether s covers every object other does.
//
// This is the absorption rule the server applies, and Ptah has to know it
// because ClickHouse applies it silently. Measured on 26.7 and 24.10, in both
// orders: granting SELECT on `db`.`t` and then on `db`.* leaves ONE row,
// `database=db table=NULL`, and the table-level grant is recorded nowhere. A
// schema declaring both is therefore not round-trippable — the absorbed grant
// reads as missing on every inspection and the plan re-issues it forever.
//
// [ValidateDeclared] refuses that pair rather than emitting it.
func (s Scope) Contains(other Scope) bool {
	if s.Database != other.Database {
		return false
	}
	return s.Table == "" || s.Table == other.Table
}

// ScopeOf reads the scope a declared grant names.
//
// defaultDatabase is the database an unqualified table name belongs to — the
// connected database for a live comparison, and the declared default otherwise.
// It may be empty, in which case an unqualified table name is refused rather
// than guessed at: a grant is an access-control decision and resolving it
// against the wrong database is not a formatting mistake.
func ScopeOf(grant goschema.Grant, defaultDatabase string) (Scope, error) {
	switch {
	case grant.OnSequence != "":
		return Scope{}, fmt.Errorf(
			"grant to role %q names on_sequence %q: ClickHouse has no sequences",
			grant.Role, grant.OnSequence)
	case grant.OnSchema != "" && grant.OnTable != "":
		return Scope{}, fmt.Errorf(
			"grant to role %q names both on_schema %q and on_table %q: a ClickHouse grant has one scope",
			grant.Role, grant.OnSchema, grant.OnTable)
	case grant.OnSchema != "":
		return databaseScope(grant.Role, grant.OnSchema)
	case grant.OnTable != "":
		return tableScope(grant.Role, grant.OnTable, defaultDatabase)
	default:
		return Scope{}, fmt.Errorf(
			"grant to role %q names no object: declare on_schema for a database scope or on_table for a table scope",
			grant.Role)
	}
}

func databaseScope(role, schema string) (Scope, error) {
	if err := refuseWildcard(role, schema, "on_schema"); err != nil {
		return Scope{}, err
	}
	return Scope{Database: schema}, nil
}

func tableScope(role, table, defaultDatabase string) (Scope, error) {
	database, name, qualified := strings.Cut(table, ".")
	if !qualified {
		database, name = defaultDatabase, table
	}
	if database == "" {
		return Scope{}, fmt.Errorf(
			"grant to role %q names table %q with no database: qualify it as database.table",
			role, table)
	}
	if strings.Contains(name, ".") {
		return Scope{}, fmt.Errorf(
			"grant to role %q names table %q: a ClickHouse scope has at most two parts",
			role, table)
	}
	// A trailing dot is refused rather than read as an empty table name. An
	// empty Table is how this type spells "the whole database", so `shop.`
	// would quietly widen a table grant to `shop`.* -- a typo turning one
	// table's privilege into every table's, with no diagnostic anywhere.
	if name == "" {
		return Scope{}, fmt.Errorf(
			"grant to role %q names table %q with no table part: write database.table, or declare on_schema for a database scope",
			role, table)
	}
	if err := refuseWildcard(role, database, "database"); err != nil {
		return Scope{}, err
	}
	if err := refuseWildcard(role, name, "table"); err != nil {
		return Scope{}, err
	}
	return Scope{Database: database, Table: name}, nil
}

// refuseWildcard rejects the pattern forms Ptah does not manage.
//
// `*.*` and a wildcard database are refused because a privilege granted beyond
// one database reaches objects no declared schema describes, so Ptah could
// neither introspect it back into a declaration nor revoke it without deciding
// something the operator did not write down. The refusal is explicit rather
// than a silent narrowing, which is the acceptance criterion.
func refuseWildcard(role, value, position string) error {
	if !strings.Contains(value, "*") {
		return nil
	}
	return fmt.Errorf(
		"grant to role %q names %s %q: Ptah manages ClickHouse grants on one database or one table, not wildcard scopes",
		role, position, value)
}

// ScopeOfLive reads the scope a live grant row names.
//
// The two object types put the database in different fields, and that is the
// shared [types.DBGrant] contract rather than a ClickHouse quirk: a
// schema-scoped grant carries its target in ObjectName with Schema empty —
// which is what [types.DBGrant.QualifiedTarget] returns and what every
// comparator and converter reads — while a table-scoped grant carries the
// schema in Schema and the table in ObjectName.
//
// Reading only (Schema, ObjectName) positionally is what an earlier version of
// this function did, and it made every database-scoped grant compare unequal to
// itself.
func ScopeOfLive(grant types.DBGrant) Scope {
	if strings.EqualFold(strings.TrimSpace(grant.ObjectType), "SCHEMA") {
		return Scope{Database: grant.ObjectName}
	}
	return Scope{Database: grant.Schema, Table: grant.ObjectName}
}
