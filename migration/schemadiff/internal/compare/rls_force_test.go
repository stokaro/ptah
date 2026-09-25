package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// forceChange is one expected FORCE change: the table and the state it reaches.
type forceChange struct {
	Table  string
	Forced bool
}

func forceChanges(changes difftypes.RLSForceChanges) []forceChange {
	out := make([]forceChange, 0, len(changes))
	for _, change := range changes {
		out = append(out, forceChange{Table: change.Table, Forced: change.Forced})
	}
	return out
}

// TestRLSEnabledTablesWithSemantics_ComparesForce pins when a declared table's
// FORCE flag needs a statement of its own.
//
// FORCE binds the table's owner to its policies and is a flag of its own, so a
// table can stay enabled while it moves. An enablement this diff makes carries
// the declared FORCE itself, which leaves only the flag the database kept from
// an earlier enablement to turn off.
func TestRLSEnabledTablesWithSemantics_ComparesForce(t *testing.T) {
	tests := []struct {
		name        string
		declared    bool
		enabled     bool
		forced      bool
		wantAdded   []string
		wantChanges []forceChange
	}{{
		name:        "enabled table gains force",
		declared:    true,
		enabled:     true,
		forced:      false,
		wantAdded:   nil,
		wantChanges: []forceChange{{Table: "public.secured", Forced: true}},
	}, {
		name:        "enabled table loses force",
		declared:    false,
		enabled:     true,
		forced:      true,
		wantAdded:   nil,
		wantChanges: []forceChange{{Table: "public.secured", Forced: false}},
	}, {
		name:        "enabled and forced as declared",
		declared:    true,
		enabled:     true,
		forced:      true,
		wantAdded:   nil,
		wantChanges: make([]forceChange, 0),
	}, {
		name:        "enabled and unforced as declared",
		declared:    false,
		enabled:     true,
		forced:      false,
		wantAdded:   nil,
		wantChanges: make([]forceChange, 0),
	}, {
		name:        "enablement carries the declared force",
		declared:    true,
		enabled:     false,
		forced:      false,
		wantAdded:   []string{"public.secured"},
		wantChanges: make([]forceChange, 0),
	}, {
		name:        "leftover force on a disabled table goes when the declaration does not want it",
		declared:    false,
		enabled:     false,
		forced:      true,
		wantAdded:   []string{"public.secured"},
		wantChanges: []forceChange{{Table: "public.secured", Forced: false}},
	}, {
		name:        "leftover force on a disabled table stays when the declaration wants it",
		declared:    true,
		enabled:     false,
		forced:      true,
		wantAdded:   []string{"public.secured"},
		wantChanges: make([]forceChange, 0),
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{RLSEnabledTables: []schemamodel.RLSEnabledTable{
				{Table: "public.secured", Forced: test.declared},
			}}
			database := &catalog.Database{Tables: []catalog.Table{
				{Schema: "public", Name: "secured", RLSEnabled: test.enabled, RLSForced: test.forced},
			}}
			diff := &difftypes.SchemaDiff{}

			compare.RLSEnabledTablesWithSemantics(
				desired, database, diff, identifier.ForDialect(platform.Postgres), platform.Postgres)

			c.Assert(diff.RLSEnabledTablesAdded.Names(), qt.DeepEquals, test.wantAdded)
			c.Assert(forceChanges(diff.RLSForceChanged), qt.DeepEquals, test.wantChanges)
			c.Assert(diff.RLSEnabledTablesRemoved, qt.HasLen, 0)
		})
	}
}

// TestRLSEnabledTablesWithSemantics_ForceOnlyWhereTheTargetHasIt keeps FORCE
// out of a comparison whose reader cannot report it. A declaration asking for
// FORCE would otherwise differ from such a database on every run. The
// dialect-neutral comparison is the control: both of its sides come from
// Ptah's own models, which carry the flag.
func TestRLSEnabledTablesWithSemantics_ForceOnlyWhereTheTargetHasIt(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		wantChanges []forceChange
	}{
		{name: "postgres", dialect: platform.Postgres, wantChanges: []forceChange{{Table: "public.secured", Forced: true}}},
		{name: "yugabytedb", dialect: platform.YugabyteDB, wantChanges: []forceChange{{Table: "public.secured", Forced: true}}},
		{name: "dialect neutral", dialect: "", wantChanges: []forceChange{{Table: "public.secured", Forced: true}}},
		{name: "sqlserver", dialect: platform.SQLServer, wantChanges: make([]forceChange, 0)},
		{name: "mysql", dialect: platform.MySQL, wantChanges: make([]forceChange, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{RLSEnabledTables: []schemamodel.RLSEnabledTable{
				{Table: "public.secured", Forced: true},
			}}
			database := &catalog.Database{Tables: []catalog.Table{
				{Schema: "public", Name: "secured", RLSEnabled: true},
			}}
			diff := &difftypes.SchemaDiff{}

			compare.RLSEnabledTablesWithSemantics(
				desired, database, diff, identifier.ForDialect(platform.Postgres), test.dialect)

			c.Assert(forceChanges(diff.RLSForceChanged), qt.DeepEquals, test.wantChanges)
		})
	}
}
