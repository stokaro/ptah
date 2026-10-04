package compare

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbchangefeed"
	"ptah.run/migration/schemadiff/difftypes"
)

// AdoptUndescribedChangefeeds returns desired with each table the database
// holds given the changefeeds of it that desired neither declares nor
// describes, as the database holds them. desired is not modified.
//
// A description that cannot spell a changefeed at all -- an HCL or DBML
// document, whose loader records that -- is silent about one, not asking for
// its removal. Taking the database's changefeeds as declared says that to
// every consumer at once: the comparison finds nothing to change, and a table
// the plan rebuilds adds them to its new table, where it would otherwise drop
// them before the swap and add none back. Measured on YDB 26.2.1.14, an HCL
// document declaring a table that carries a changefeed planned `DROP
// CHANGEFEED` without the loader's record.
//
// A changefeed the table declares keeps its declaration, and so does a table
// the database does not hold.
func AdoptUndescribedChangefeeds(
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
		if len(table.Changefeeds) > 0 {
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
		var kept []ast.ChangefeedSpec
		for _, changefeed := range current.Changefeeds {
			declared := slices.ContainsFunc(table.Changefeeds, func(want ast.ChangefeedSpec) bool {
				return want.Name == changefeed.Name
			})
			if declared || desired.NotDescribed.DescribesIn(coverage.Changefeed, current.Schema,
				changefeedSpellings(current.Schema, current.Name, changefeed.Name)...) {
				continue
			}
			kept = append(kept, changefeed.Clone())
		}
		if len(kept) == 0 {
			continue
		}
		if adopted == nil {
			copied := *desired
			copied.Tables = slices.Clone(desired.Tables)
			adopted = &copied
		}
		adopted.Tables[i].Changefeeds = append(ast.CloneChangefeeds(table.Changefeeds), kept...)
	}
	if adopted == nil {
		return desired
	}
	return adopted
}

// changefeedSpellings are the names a coverage record may give the changefeed
// name of table: qualified by the table's schema, and not.
func changefeedSpellings(schema, table, name string) []string {
	return []string{tableref.Canonical(schema, table+"/"+name), table + "/" + name}
}

// changefeedsChange is the change a table's YDB changefeeds make, and nil when
// the declaration and the database hold the same ones as YDB keeps them; see
// [ydbchangefeed.Equal].
//
// Each side's silence is read through coverage, in the direction the
// [Coverage] type gives. A changefeed the read recorded as not described --
// one holding a setting Ptah does not model -- is not in the catalog, and a
// declared changefeed of its name is withheld rather than added, since ADD
// CHANGEFEED of a name the table holds fails. A changefeed the database holds
// and the declaration does not describe is kept on the desired side as it is,
// so no plan drops it.
func changefeedsChange(cov Coverage, desired schemamodel.Table, current catalog.Table) *difftypes.ChangefeedsChange {
	spellings := func(name string) []string { return changefeedSpellings(current.Schema, current.Name, name) }
	var declared []ast.ChangefeedSpec
	var withheld []coverage.Object
	for _, changefeed := range desired.Changefeeds {
		names := spellings(changefeed.Name)
		if cov.PlansAddition(coverage.Changefeed, current.Schema, names...) ||
			slices.ContainsFunc(current.Changefeeds, func(have ast.ChangefeedSpec) bool { return have.Name == changefeed.Name }) {
			declared = append(declared, changefeed.Clone())
			continue
		}
		withheld = append(withheld, cov.withheldAddition(coverage.Changefeed, names[0], current.Schema, names))
	}
	cov.recordUndecidedAdditions(withheld)
	for _, changefeed := range current.Changefeeds {
		isDeclared := slices.ContainsFunc(desired.Changefeeds, func(want ast.ChangefeedSpec) bool {
			return want.Name == changefeed.Name
		})
		if !isDeclared && !cov.PlansRemoval(coverage.Changefeed, current.Schema, spellings(changefeed.Name)...) {
			declared = append(declared, changefeed.Clone())
		}
	}
	if ydbchangefeed.ListsEqual(declared, current.Changefeeds) {
		return nil
	}
	return &difftypes.ChangefeedsChange{Desired: declared, Current: ast.CloneChangefeeds(current.Changefeeds)}
}
