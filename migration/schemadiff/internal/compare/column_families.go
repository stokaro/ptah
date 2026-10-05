package compare

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbfamily"
	"ptah.run/migration/schemadiff/difftypes"
)

// AdoptHeldColumnFamilies returns desired with each table the database holds
// given the YDB column families it ends up holding, as [ydbfamily.Applied]
// reads them: every family the database holds stays, and each setting the
// declaration does not state keeps the value the database holds. desired is
// not modified.
//
// A setting a declaration leaves out means "keep what the table holds",
// because a table's defaults come from the cluster's table profile and vary
// from cluster to cluster (see package ydbfamily). Filling the declaration
// with the held values says that to every consumer at once: the comparison
// sees only what the declaration states, a table the plan rebuilds keeps the
// settings and families it held, and a rollback is the plain reverse of the
// change.
//
// A description that cannot spell a column family -- an HCL or DBML document,
// whose loader records that -- is silent about where each column sits too,
// so the table takes the database's families whole, columns included: the
// document is not asking to move every column back to the default family. A
// family keeps only the columns the desired table declares, since a column
// the document leaves out is one the plan drops.
//
// A table the database does not hold keeps what it declares.
func AdoptHeldColumnFamilies(
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
) *schemamodel.Database {
	if desired == nil || database == nil {
		return desired
	}
	held := make(map[tableIdentity]catalog.Table, len(database.Tables))
	for _, table := range database.Tables {
		if len(table.YDBColumnFamilies) > 0 {
			held[tableMapIdentity(table.Schema, table.Name, dialect, semantics)] = table
		}
	}
	if len(held) == 0 {
		return desired
	}
	var adopted *schemamodel.Database
	for i, table := range desired.Tables {
		current, holds := held[tableMapIdentity(table.Schema, table.Name, dialect, semantics)]
		if !holds {
			continue
		}
		declares := func(column string) bool {
			return slices.ContainsFunc(desired.Fields, func(field schemamodel.Field) bool {
				return field.StructName == table.StructName && field.Name == column
			})
		}
		families := ydbfamily.Applied(table.YDBColumnFamilies, current.YDBColumnFamilies)
		if !desired.NotDescribed.DescribesIn(coverage.ColumnFamily, table.Schema, familySpellings(table.Schema, table.Name)...) {
			families = ydbfamily.OnlyColumns(current.YDBColumnFamilies, declares)
		}
		if adopted == nil {
			copied := *desired
			copied.Tables = slices.Clone(desired.Tables)
			adopted = &copied
		}
		adopted.Tables[i].YDBColumnFamilies = families
	}
	if adopted == nil {
		return desired
	}
	return adopted
}

// familySpellings are the names a coverage record may give a table whose
// column families it records: qualified by the table's schema, and not.
func familySpellings(schema, table string) []string {
	return []string{tableref.Canonical(schema, table), table}
}

// columnFamiliesChange is the change a table's YDB column families make, and
// nil when the database holds everything the declaration states; see
// [ydbfamily.Satisfied].
//
// Each side's silence is read through coverage, in the direction the
// [Coverage] type gives. Families the read recorded as not described -- one
// holding a setting Ptah does not read -- are in no list, and a declaration of
// families for that table is withheld rather than planned, since a statement
// built against families the read did not see could undo what it did not
// read. [AdoptHeldColumnFamilies] has already filled what the declaration
// leaves out with what the database holds.
func columnFamiliesChange(cov Coverage, desired schemamodel.Table, current catalog.Table) *difftypes.YDBColumnFamiliesChange {
	names := familySpellings(current.Schema, current.Name)
	if !cov.PlansAddition(coverage.ColumnFamily, current.Schema, names...) {
		if len(ydbfamily.Normalize(desired.YDBColumnFamilies)) > 0 {
			cov.recordUndecidedAdditions([]coverage.Object{
				cov.withheldAddition(coverage.ColumnFamily, names[0], current.Schema, names),
			})
		}
		return nil
	}
	if !cov.PlansRemoval(coverage.ColumnFamily, current.Schema, names...) ||
		ydbfamily.Satisfied(desired.YDBColumnFamilies, current.YDBColumnFamilies) {
		return nil
	}
	return &difftypes.YDBColumnFamiliesChange{
		Desired: ast.CloneYDBColumnFamilies(desired.YDBColumnFamilies),
		Current: ast.CloneYDBColumnFamilies(current.YDBColumnFamilies),
	}
}
