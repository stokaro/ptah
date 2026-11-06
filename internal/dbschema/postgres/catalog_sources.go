package postgres

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/pgcatalogview"
)

// readsCatalogUnfiltered reports whether a read takes its relations from
// pg_catalog rather than from information_schema.
//
// information_schema lists only what the connecting role holds a privilege on,
// and a comparison reads a missing row as a missing object, so a read through
// it can report two different databases as synced. [pgcatalogview] says what
// was measured.
//
// The two targets left on information_schema do not have this problem or cannot
// take the other read. CockroachDB's lists a relation to a role with no privilege
// on it: measured on v26.3.5, such a role is refused SELECT on a table and reads
// the same description of it as root. Spanner's pg_catalog is an emulation without
// the helper functions.
func (r *Reader) readsCatalogUnfiltered() bool {
	return r.caps.Has(capability.PostgresCatalogFunctions) && r.dialect != platform.CockroachDB
}

// tablesSource is what the table read selects its base tables from.
func (r *Reader) tablesSource() string {
	if r.readsCatalogUnfiltered() {
		return "(" + pgcatalogview.Tables + ")"
	}
	return "information_schema.tables"
}

// columnsSource is the FROM item the column read starts with, under the alias
// col. Both forms give the columns of base tables alone.
func (r *Reader) columnsSource() string {
	if r.readsCatalogUnfiltered() && r.caps.Has(capability.GeneratedColumns) {
		return "(" + pgcatalogview.Columns + ") col"
	}
	if r.readsCatalogUnfiltered() {
		return "(" + pgcatalogview.ColumnsWithoutGenerated + ") col"
	}
	return `information_schema.columns col
		JOIN information_schema.tables tbl ON tbl.table_schema = col.table_schema
			AND tbl.table_name = col.table_name
			AND tbl.table_type = 'BASE TABLE'`
}

// tableConstraintsSource is what the constraint read selects its constraints
// from.
func (r *Reader) tableConstraintsSource() string {
	if r.readsCatalogUnfiltered() {
		return "(" + pgcatalogview.TableConstraints + ")"
	}
	return "information_schema.table_constraints"
}

// viewCheckOptionExpr is a view's check option, as NONE, LOCAL or CASCADED, for
// the view read over pg_class as c. It reads what [Reader.viewCheckOptionJoin]
// joins, where that joins anything.
func (r *Reader) viewCheckOptionExpr() string {
	if r.readsCatalogUnfiltered() {
		return pgcatalogview.ViewCheckOption("c")
	}
	return "COALESCE(v.check_option, 'NONE')"
}

// viewCheckOptionJoin joins information_schema.views where
// [Reader.viewCheckOptionExpr] reads it. A LEFT JOIN, so a view the role cannot
// see in it is read with no check option.
func (r *Reader) viewCheckOptionJoin() string {
	if r.readsCatalogUnfiltered() {
		return ""
	}
	return `LEFT JOIN information_schema.views v
			ON v.table_schema = n.nspname AND v.table_name = c.relname`
}
