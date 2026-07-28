package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/compare"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// TestIndexes_SQLServerFilteredPredicateSpelling proves that filtered-index
// predicate comparison for SQL Server tolerates the catalog's canonical
// filter_definition spelling (bracket-quoted identifiers, parenthesized
// numeric literals, wrapping parentheses) while still detecting real
// predicate changes as a drop/create replacement.
func TestIndexes_SQLServerFilteredPredicateSpelling(t *testing.T) {
	replacementRefs := []difftypes.IndexRef{
		{Name: "idx_users_filtered", TableName: "dbo.users"},
	}
	tests := []struct {
		name          string
		generated     string
		database      string
		wantAdditions []difftypes.IndexRef
		wantRemovals  []difftypes.IndexRef
	}{
		{
			name:      "natural spelling matches canonical numeric filter",
			generated: "status = 1",
			database:  "([status]=(1))",
		},
		{
			name:      "IS NULL matches bracket-quoted catalog spelling",
			generated: "deleted_at IS NULL",
			database:  "([deleted_at] IS NULL)",
		},
		{
			name:      "canonical spelling matches itself",
			generated: "([status]=(2))",
			database:  "([status]=(2))",
		},
		{
			name:      "negative literal matches canonical parenthesized form",
			generated: "balance > -1.5",
			database:  "([balance]>(-1.5))",
		},
		{
			name:      "string literal comparison keeps quote escapes",
			generated: "note = 'it''s [a] (1)'",
			database:  "([note]='it''s [a] (1)')",
		},
		{
			name:      "compound predicate matches canonical spelling",
			generated: "status = 1 AND deleted_at IS NULL",
			database:  "([status]=(1) AND [deleted_at] IS NULL)",
		},
		{
			name:          "changed literal still replaces",
			generated:     "status = 2",
			database:      "([status]=(1))",
			wantAdditions: replacementRefs,
			wantRemovals:  replacementRefs,
		},
		{
			name:          "predicate added to unfiltered index replaces",
			generated:     "status = 1",
			database:      "",
			wantAdditions: replacementRefs,
			wantRemovals:  replacementRefs,
		},
		{
			name:          "predicate removed from filtered index replaces",
			generated:     "",
			database:      "([status]=(1))",
			wantAdditions: replacementRefs,
			wantRemovals:  replacementRefs,
		},
		{
			name:          "string literal content stays significant",
			generated:     "note = 'active'",
			database:      "([note]='archived')",
			wantAdditions: replacementRefs,
			wantRemovals:  replacementRefs,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{
				Indexes: []goschema.Index{{
					Name:      "idx_users_filtered",
					TableName: "dbo.users",
					Fields:    []string{"status"},
					Condition: test.generated,
				}},
			}
			database := &types.DBSchema{
				Indexes: []types.DBIndex{{
					Name:      "idx_users_filtered",
					TableName: "users",
					Schema:    "dbo",
					Columns:   []string{"status"},
					Condition: test.database,
				}},
			}
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(generated, database, diff, "sqlserver")

			c.Assert(diff.IndexAdditions(), qt.DeepEquals, test.wantAdditions)
			c.Assert(diff.IndexRemovals(), qt.DeepEquals, test.wantRemovals)
		})
	}
}

// TestIndexes_PredicateBracketSpellingStaysSignificantOutsideSQLServer guards
// that the SQL Server spelling normalization is dialect-scoped: for
// PostgreSQL, square brackets are meaningful (array subscripts) and must keep
// participating in predicate comparison.
func TestIndexes_PredicateBracketSpellingStaysSignificantOutsideSQLServer(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{
		Indexes: []goschema.Index{{
			Name:      "idx_users_filtered",
			TableName: "users",
			Fields:    []string{"tags"},
			Condition: "tags[1] = 'admin'",
		}},
	}
	database := &types.DBSchema{
		Indexes: []types.DBIndex{{
			Name:      "idx_users_filtered",
			TableName: "users",
			Columns:   []string{"tags"},
			Condition: "tags1 = 'admin'",
		}},
	}
	diff := &difftypes.SchemaDiff{}

	compare.IndexesWithDialect(generated, database, diff, "postgres")

	ref := []difftypes.IndexRef{{Name: "idx_users_filtered", TableName: "users"}}
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, ref)
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, ref)
}
