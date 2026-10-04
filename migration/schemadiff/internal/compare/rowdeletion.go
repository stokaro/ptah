package compare

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// AdoptUndescribedRowDeletionPolicies returns desired with each table whose
// row deletion policy desired does not describe given the policy the database
// holds for it. desired is not modified.
//
// A description that cannot spell a policy at all -- an HCL or DBML document,
// whose loader records that -- is silent about it, not asking for its removal.
// Taking the database's policy as the declared one says that for every
// consumer at once: the comparison finds nothing to change, and a table the
// plan rebuilds writes the policy on its new table instead of losing it with
// the old one. Measured on YDB 26.2.1.14, a TTL table inspected through
// `ptah-compat schema inspect` and applied back from that HCL planned
// `ALTER TABLE ... RESET (TTL)` without this.
//
// A table that declares a policy keeps its own, whatever the description says
// about the others, and so does a table the database does not hold.
func AdoptUndescribedRowDeletionPolicies(
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
		if !table.RowDeletionPolicy.IsZero() {
			held[tableMapIdentity(table.Schema, table.Name, dialect, semantics)] = table
		}
	}
	if len(held) == 0 {
		return desired
	}
	adopted := *desired
	adopted.Tables = slices.Clone(desired.Tables)
	for i, table := range adopted.Tables {
		current, holds := held[tableMapIdentity(table.Schema, table.Name, dialect, semantics)]
		if !holds || !table.RowDeletionPolicy.IsZero() ||
			desired.NotDescribed.DescribesIn(coverage.TTL, table.Schema, tableref.Canonical(table.Schema, table.Name), table.Name) {
			continue
		}
		adopted.Tables[i].RowDeletionPolicy = current.RowDeletionPolicy.Clone()
	}
	return &adopted
}
