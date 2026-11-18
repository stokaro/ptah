package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/selectext"
	"ptah.run/internal/atlasfilter"
)

// An exclusion that leaves out a foreign key leaves out the index the server
// built for it, by the target's rule: kept alone, the index is planned for a
// DROP INDEX that MySQL and MariaDB refuse with ERROR 1553 while the key
// stands. An index the author declared stays, even where the key stands on it.
//
// integration/foreign_key_selector_e2e_test.go runs the same selectors through
// the shipped binaries against MySQL and MariaDB.

// namedAfterKey is a rule that claims the index carrying the key's name on
// the key's table, the shape of the index MySQL builds for a named key.
type namedAfterKey struct{}

func (namedAfterKey) BackingIndexes(current *catalog.Database, key catalog.Constraint, _ identifier.Semantics) []catalog.Index {
	var backing []catalog.Index
	for _, index := range current.Indexes {
		if index.QualifiedTableName() == key.QualifiedTableName() && index.Name == key.Name {
			backing = append(backing, index)
		}
	}
	return backing
}

// comparingRuntime serves scope selections and a target's comparison rules,
// as the runtime a command hands NewExclusion does.
type comparingRuntime struct{}

func (comparingRuntime) Selections() selectext.Set { return ownerSelections() }

func (comparingRuntime) ComparisonRules(string) (schemacomparison.Rules, error) {
	return schemacomparison.Rules{ForeignKeyIndexes: namedAfterKey{}}, nil
}

func observedForeignKeys() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Schema: "app", Name: "kids", Columns: []catalog.Column{
			{Name: "id", IsPrimaryKey: true}, {Name: "parent_id"}, {Name: "owner_id"},
		}}},
		Indexes: []catalog.Index{
			{Schema: "app", TableName: "kids", Name: "kids_parent_fk", Columns: []string{"parent_id"}},
			{Schema: "app", TableName: "kids", Name: "kids_owner_idx", Columns: []string{"owner_id"}},
		},
		Constraints: []catalog.Constraint{
			{
				Schema: "app", TableName: "kids", Name: "kids_parent_fk", Type: "FOREIGN KEY",
				ColumnNames: []string{"parent_id"}, ForeignTable: new("parents"),
			},
			{
				Schema: "app", TableName: "kids", Name: "kids_owner_fk", Type: "FOREIGN KEY",
				ColumnNames: []string{"owner_id"}, ForeignTable: new("owners"),
			},
		},
	}
}

func TestExcludeLeavesOutTheIndexAServerBuiltForAForeignKey_HappyPath(t *testing.T) {
	tests := []struct {
		name, exclude string
		rule          schemacomparison.ForeignKeyIndexes
		// want are the constraints the exclusion keeps, and wantIndexes the
		// indexes.
		want, wantIndexes []string
	}{
		{
			name: "the server's index leaves with its key", exclude: "kids.kids_parent_fk[type=foreign_key]", rule: namedAfterKey{},
			want: []string{"kids_owner_fk"}, wantIndexes: []string{"kids_owner_idx"},
		},
		{
			name: "an index the author declared stays", exclude: "kids.kids_owner_fk[type=foreign_key]", rule: namedAfterKey{},
			want: []string{"kids_parent_fk"}, wantIndexes: []string{"kids_parent_fk", "kids_owner_idx"},
		},
		{
			name: "a target whose server builds no index keeps every index", exclude: "kids.kids_parent_fk[type=foreign_key]",
			want: []string{"kids_owner_fk"}, wantIndexes: []string{"kids_parent_fk", "kids_owner_idx"},
		},
		{
			name: "a kept key keeps its index", exclude: "kids.owner_id[type=column]", rule: namedAfterKey{},
			want: []string{"kids_parent_fk"}, wantIndexes: []string{"kids_parent_fk"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := atlasfilter.Scope{
				Selections: ownerSelections(), Exclude: []string{test.exclude}, DefaultSchema: "app",
				Dialect: "mysql", ForeignKeyIndexes: test.rule,
			}

			current, report, err := atlasfilter.ExcludeDatabaseScopeReport(observedForeignKeys(), scope)

			c.Assert(err, qt.IsNil)
			c.Assert(constraintNames(current.Constraints), qt.DeepEquals, test.want)
			c.Assert(indexNames(current.Indexes), qt.DeepEquals, test.wantIndexes)
			c.Assert(report.Unmatched, qt.IsNil)
		})
	}
}

// An Exclusion reads the rule from the comparison rules its runtime serves
// for the dialect the naming states. A naming without a dialect reads none.
func TestExclusionLeavesOutTheIndexAServerBuiltForAForeignKey_HappyPath(t *testing.T) {
	tests := []struct {
		name, dialect string
		wantIndexes   []string
	}{
		{name: "a naming that states the dialect", dialect: "mysql", wantIndexes: []string{"kids_owner_idx"}},
		{name: "a naming that states none", wantIndexes: []string{"kids_parent_fk", "kids_owner_idx"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			exclusion, err := atlasfilter.NewExclusion(comparingRuntime{}, []string{"kids.kids_parent_fk[type=foreign_key]"})
			c.Assert(err, qt.IsNil)

			current, _, err := exclusion.Observed(observedForeignKeys(), atlasfilter.Naming{DefaultSchema: "app", Dialect: test.dialect})

			c.Assert(err, qt.IsNil)
			c.Assert(constraintNames(current.Constraints), qt.DeepEquals, []string{"kids_owner_fk"})
			c.Assert(indexNames(current.Indexes), qt.DeepEquals, test.wantIndexes)
		})
	}
}
