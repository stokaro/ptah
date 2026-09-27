package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// deferralDesired is table slots with a primary key over id declared on the
// table, a UNIQUE over pos and an EXCLUDE over r, each deferring as given.
func deferralDesired(deferrable bool, initially string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "S", Name: "slots", PrimaryKey: []string{"id"},
			PrimaryKeyDeferrable: deferrable, PrimaryKeyInitially: initially,
		}},
		Fields: []schemamodel.Field{
			{StructName: "S", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "S", Name: "pos", Type: "INTEGER", Nullable: true},
			{StructName: "S", Name: "r", Type: "INTEGER", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{
			{
				StructName: "S", Table: "slots", Name: "slots_pos_key", Type: "UNIQUE", Columns: []string{"pos"},
				Deferrable: deferrable, Initially: initially,
			},
			{
				StructName: "S", Table: "slots", Name: "slots_r_excl", Type: "EXCLUDE", UsingMethod: "btree",
				ExcludeElements: "r WITH =", Deferrable: deferrable, Initially: initially,
			},
		},
	}
}

// deferralCurrent is table slots as PostgreSQL 18.6 reports it: the reader
// gives a timing only to a deferrable constraint.
func deferralCurrent(deferrable bool, initially string) *catalog.Database {
	key := func(name, kind string, columns ...string) catalog.Constraint {
		return catalog.Constraint{
			Name: name, TableName: "slots", Type: kind, ColumnNames: columns, ColumnName: columns[0],
			Deferrable: deferrable, Initially: initially,
		}
	}
	exclude := key("slots_r_excl", "EXCLUDE", "r")
	exclude.UsingMethod, exclude.ExcludeElements = new("btree"), new("r WITH =")
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "slots", Columns: []catalog.Column{
			{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "pos", DataType: "integer", IsNullable: "YES", IsUnique: true},
			{Name: "r", DataType: "integer", IsNullable: "YES"},
		}}},
		Constraints: []catalog.Constraint{key("slots_pkey", "PRIMARY KEY", "id"), key("slots_pos_key", "UNIQUE", "pos"), exclude},
	}
}

// TestCompare_KeyDeferral_Synced compares keys that defer as the database's
// do: nothing is planned. An empty timing and "immediate" are the same timing.
func TestCompare_KeyDeferral_Synced(t *testing.T) {
	tests := []struct {
		name              string
		deferrable        bool
		desiredInitially  string
		databaseInitially string
	}{
		{name: "not deferrable"},
		{name: "deferrable", deferrable: true, databaseInitially: "immediate"},
		{name: "deferred", deferrable: true, desiredInitially: "deferred", databaseInitially: "deferred"},
		{name: "immediate written out", deferrable: true, desiredInitially: "immediate", databaseInitially: "immediate"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(
				deferralDesired(test.deferrable, test.desiredInitially),
				deferralCurrent(test.deferrable, test.databaseInitially),
				platform.Postgres,
			)

			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompare_KeyDeferral_Changed plans a key whose deferral changed as a drop
// and an add. PostgreSQL 18.6 alters the deferral of a foreign key alone:
// `ALTER CONSTRAINT` on a UNIQUE or a primary key answers `is not a foreign key
// constraint` (stokaro/ptah#3824).
func TestCompare_KeyDeferral_Changed(t *testing.T) {
	tests := []struct {
		name              string
		desiredDeferrable bool
		desiredInitially  string
		dbDeferrable      bool
		dbInitially       string
		wantPlan          []string
	}{
		{
			name: "a key starts deferring", desiredDeferrable: true, desiredInitially: "deferred",
			wantPlan: []string{
				`ADD PRIMARY KEY ("id") DEFERRABLE INITIALLY DEFERRED`,
				`ADD CONSTRAINT "slots_pos_key" UNIQUE ("pos") DEFERRABLE INITIALLY DEFERRED`,
				`ADD CONSTRAINT "slots_r_excl" EXCLUDE USING btree (r WITH =) DEFERRABLE INITIALLY DEFERRED`,
			},
		},
		{
			name: "a key stops deferring", dbDeferrable: true, dbInitially: "immediate",
			wantPlan: []string{
				"DROP CONSTRAINT IF EXISTS \"slots_pkey\"\n",
				"ADD PRIMARY KEY (\"id\")\n",
				"ADD CONSTRAINT \"slots_pos_key\" UNIQUE (\"pos\")\n",
				"ADD CONSTRAINT \"slots_r_excl\" EXCLUDE USING btree (r WITH =)\n",
			},
		},
		{
			name: "a key changes its timing", desiredDeferrable: true, desiredInitially: "deferred",
			dbDeferrable: true, dbInitially: "immediate",
			wantPlan: []string{`ADD CONSTRAINT "slots_pos_key" UNIQUE ("pos") DEFERRABLE INITIALLY DEFERRED`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(
				deferralDesired(test.desiredDeferrable, test.desiredInitially),
				deferralCurrent(test.dbDeferrable, test.dbInitially),
				platform.Postgres,
			)

			plan, err := planner.GenerateSchemaDiffSQLStatements(diff, platform.Postgres)

			c.Assert(err, qt.IsNil)
			for _, want := range test.wantPlan {
				c.Assert(strings.Join(plan, "\n")+"\n", qt.Contains, want)
			}
		})
	}
}

// TestCompare_AColumnPrimaryKeyAgainstADeferrableLiveKey plans the key a
// column declares against a live key that defers: the column spelling cannot
// defer, so the key is dropped and added plain. Compared as a flag on the
// column, the two were equal and the deferral stayed forever.
func TestCompare_AColumnPrimaryKeyAgainstADeferrableLiveKey(t *testing.T) {
	c := qt.New(t)
	desired := deferralDesired(false, "")
	desired.Tables[0].PrimaryKey = nil
	desired.Constraints = nil
	current := deferralCurrent(true, "deferred")
	current.Constraints = current.Constraints[:1]
	current.Tables[0].Columns[1].IsUnique = false

	plan, err := planner.GenerateSchemaDiffSQLStatements(
		schemadiff.CompareWithDialect(desired, current, platform.Postgres), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(plan, qt.DeepEquals, []string{
		"-- ALTER statements: --\nALTER TABLE \"slots\" DROP CONSTRAINT IF EXISTS \"slots_pkey\"",
		"-- ALTER statements: --\nALTER TABLE \"slots\" ADD PRIMARY KEY (\"id\")",
	})
}

// TestCompareSchemas_KeyDeferralPairsWithItself compares a document with
// itself, as `schema diff` between two copies of one file does.
func TestCompareSchemas_KeyDeferralPairsWithItself(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareSchemas(deferralDesired(true, "deferred"), deferralDesired(true, "deferred"), platform.Postgres)

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}
