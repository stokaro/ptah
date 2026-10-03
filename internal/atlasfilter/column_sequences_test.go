package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
)

// The fixtures hold items, whose bigserial id owns items_id_seq, and other.
// app holds a grant on items_id_seq and one on the table items. The database
// side names the owner through the read's OwnedSequence; the desired side
// carries no sequence name at all, and the selection answers with the one
// PostgreSQL gives the sequence of items.id.

func columnSequenceDatabase() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "items", Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", OwnedSequence: "items_id_seq"},
				{Name: "title", DataType: "text"},
			}},
			{Name: "other", Columns: []catalog.Column{{Name: "id", DataType: "integer"}}},
		},
		Grants: []catalog.Grant{
			{Role: "app", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "items_id_seq"},
			{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "items"},
		},
	}
}

func columnSequenceDesired() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Items", Name: "items"},
			{StructName: "Other", Name: "other"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Items", Name: "id", Type: "BIGSERIAL", Primary: true},
			{StructName: "Items", Name: "title", Type: "text"},
			{StructName: "Other", Name: "id", Type: "integer", Primary: true},
		},
		Grants: []schemamodel.Grant{
			{Role: "app", Privileges: []string{"USAGE"}, OnSequence: "items_id_seq"},
			{Role: "app", Privileges: []string{"SELECT"}, OnTable: "items"},
		},
	}
}

// columnSequenceScopes are the selections both sides are projected under, with
// the sequence grants each keeps.
var columnSequenceScopes = []struct {
	name  string
	scope atlasfilter.Scope
	want  []string
}{
	{name: "no selector", scope: atlasfilter.Scope{}, want: []string{"items_id_seq"}},
	{name: "include the owning table", scope: atlasfilter.Scope{Include: []string{"items"}}, want: []string{"items_id_seq"}},
	{name: "include another table", scope: atlasfilter.Scope{Include: []string{"other"}}, want: nil},
	{name: "include the sequence by name", scope: atlasfilter.Scope{Include: []string{"items_id_seq"}}, want: []string{"items_id_seq"}},
	{
		name:  "include the sequence by type",
		scope: atlasfilter.Scope{Include: []string{"items_id_seq[type=sequence]"}},
		want:  []string{"items_id_seq"},
	},
	{name: "exclude the owning table", scope: atlasfilter.Scope{Exclude: []string{"items"}}, want: nil},
	{name: "exclude the owning column", scope: atlasfilter.Scope{Exclude: []string{"items.id"}}, want: nil},
	{name: "exclude the sequence by name", scope: atlasfilter.Scope{Exclude: []string{"items_id_seq"}}, want: nil},
	{
		name:  "exclude the sequence by type",
		scope: atlasfilter.Scope{Exclude: []string{"items_id_seq[type=sequence]"}},
		want:  nil,
	},
	{name: "exclude another table", scope: atlasfilter.Scope{Exclude: []string{"other"}}, want: []string{"items_id_seq"}},
	{name: "exclude the grant", scope: atlasfilter.Scope{Exclude: []string{"items_id_seq[type=grant]"}}, want: nil},
	{
		name:  "include the table, exclude its sequence",
		scope: atlasfilter.Scope{Include: []string{"items"}, Exclude: []string{"items_id_seq"}},
		want:  nil,
	},
}

// TestScopeDatabase_ColumnSequenceGrantFollowsItsTable projects a read under
// each selection.
func TestScopeDatabase_ColumnSequenceGrantFollowsItsTable(t *testing.T) {
	for _, test := range columnSequenceScopes {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := test.scope
			scope.DefaultSchema = "public"

			got, err := atlasfilter.ScopeDatabase(columnSequenceDatabase(), scope)

			c.Assert(err, qt.IsNil)
			c.Assert(databaseSequenceGrantTargets(got.Grants), qt.DeepEquals, test.want)
		})
	}
}

// TestScopeGenerated_ColumnSequenceGrantFollowsItsTable projects the desired
// side under the same selections, so a comparison sees one answer.
func TestScopeGenerated_ColumnSequenceGrantFollowsItsTable(t *testing.T) {
	for _, test := range columnSequenceScopes {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := test.scope
			scope.DefaultSchema = "public"

			got, err := atlasfilter.ScopeGenerated(columnSequenceDesired(), scope)

			c.Assert(err, qt.IsNil)
			c.Assert(generatedSequenceGrantTargets(got.Grants), qt.DeepEquals, test.want)
		})
	}
}

// TestExcludeSelectorNamingAColumnSequenceIsMatched reports an exclusion that
// names a column's sequence as having matched, on both sides: the sequence
// exists, so the selector is not a typo.
func TestExcludeSelectorNamingAColumnSequenceIsMatched(t *testing.T) {
	c := qt.New(t)

	selectors := []string{"items_id_seq[type=sequence]"}
	_, database, err := atlasfilter.ExcludeDatabaseReport(columnSequenceDatabase(), selectors, "public")
	c.Assert(err, qt.IsNil)
	_, desired, err := atlasfilter.ExcludeGeneratedReport(columnSequenceDesired(), selectors, "public")
	c.Assert(err, qt.IsNil)

	c.Assert(database.Unmatched, qt.HasLen, 0)
	c.Assert(desired.Unmatched, qt.HasLen, 0)
}

// TestExclude_StandaloneSequenceLeavesWithItsOwner excludes the table a
// standalone sequence is OWNED BY. The include side keeps such a sequence with
// its table, so the exclusion takes it with its table too, and the grant on it
// with it.
func TestExclude_StandaloneSequenceLeavesWithItsOwner(t *testing.T) {
	tests := []struct {
		name    string
		exclude string
	}{
		{name: "the owning table", exclude: "items"},
		{name: "the owning column", exclude: "items.n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &catalog.Database{
				Tables: []catalog.Table{{Name: "items", Columns: []catalog.Column{
					{Name: "id", DataType: "integer"},
					{Name: "n", DataType: "integer"},
				}}},
				Sequences: []catalog.Sequence{{Name: "lifecycle_seq", OwnedBy: "items.n"}},
				Grants: []catalog.Grant{
					{Role: "app", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "lifecycle_seq"},
				},
			}
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Items", Name: "items"}},
				Fields: []schemamodel.Field{
					{StructName: "Items", Name: "id", Type: "integer", Primary: true},
					{StructName: "Items", Name: "n", Type: "integer"},
				},
				Sequences: []schemamodel.Sequence{{StructName: "Seq", Name: "lifecycle_seq", OwnedBy: "items.n"}},
				Grants:    []schemamodel.Grant{{Role: "app", Privileges: []string{"USAGE"}, OnSequence: "lifecycle_seq"}},
			}

			gotDatabase, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(database, []string{test.exclude}, "public")
			c.Assert(err, qt.IsNil)
			gotDesired, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(desired, []string{test.exclude}, "public")
			c.Assert(err, qt.IsNil)

			c.Assert(gotDatabase.Sequences, qt.HasLen, 0)
			c.Assert(gotDatabase.Grants, qt.HasLen, 0)
			c.Assert(gotDesired.Sequences, qt.HasLen, 0)
			c.Assert(gotDesired.Grants, qt.HasLen, 0)
		})
	}
}

func databaseSequenceGrantTargets(grants []catalog.Grant) []string {
	var targets []string
	for _, grant := range grants {
		if grant.ObjectType == "SEQUENCE" {
			targets = append(targets, grant.ObjectName)
		}
	}
	return targets
}

func generatedSequenceGrantTargets(grants []schemamodel.Grant) []string {
	var targets []string
	for _, grant := range grants {
		if grant.OnSequence != "" {
			targets = append(targets, grant.OnSequence)
		}
	}
	return targets
}
