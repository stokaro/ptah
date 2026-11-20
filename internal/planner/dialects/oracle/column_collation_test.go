package oracle_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	oracleplanner "ptah.run/internal/planner/dialects/oracle"
)

// collationDiff is a diff that changes the collation of column T.C, declared
// as field, where T held observed before the change.
func collationDiff(field schemamodel.Field, change string, observed schemacapture.TableObservation) *difftypes.SchemaDiff {
	field.Name, field.StructName, field.Type = "C", "T", "VARCHAR2(20)"
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "T",
		Current:   observed,
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "C", Changes: map[string]string{"collation": change}, Desired: field,
		}},
	}}}
}

// TestPlanner_OraclePlansAChangedCollation_HappyPath plans a collation change
// as one MODIFY that states the collation is what changes. The renderer reads
// that statement to write the COLLATE clause, and the safety assessment reads
// it to rate the statement.
func TestPlanner_OraclePlansAChangedCollation_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		field   schemamodel.Field
		change  string
		current schemacapture.TableObservation
		want    string
	}{
		{name: "to a named collation", field: schemamodel.Field{Collate: "BINARY_CI"}, change: " -> BINARY_CI", want: "BINARY_CI"},
		{name: "back to none", field: schemamodel.Field{}, change: "BINARY_CI -> USING_NLS_COMP", want: ""},
		{
			name:   "a column no index of the table uses",
			field:  schemamodel.Field{Collate: "BINARY_CI"},
			change: " -> BINARY_CI",
			current: schemacapture.TableObservation{
				Indexes: []catalog.Index{{Name: "t_other", Columns: []string{"other"}}},
			},
			want: "BINARY_CI",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := oracleplanner.New().GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()), collationDiff(tc.field, tc.change, tc.current))
			c.Assert(err, qt.IsNil)
			modifies := modifyColumnOperations(nodes)
			c.Assert(modifies, qt.HasLen, 1)
			c.Assert(modifies[0].Changed, qt.Equals, ast.ColumnProperties{Collation: true})
			c.Assert(modifies[0].Column.Collate, qt.Equals, tc.want)
		})
	}
}

// TestPlanner_OracleRefusesACollationOnAKeyedColumn_FailurePath stops the plan
// before its first statement. Oracle commits each statement, so a MODIFY
// refused with ORA-43923 halfway through would leave the statements before it
// applied.
func TestPlanner_OracleRefusesACollationOnAKeyedColumn_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := collationDiff(schemamodel.Field{Collate: "BINARY_CI"}, " -> BINARY_CI", schemacapture.TableObservation{
		Indexes: []catalog.Index{{Name: "t_c_idx", Columns: []string{"C"}}},
	})

	nodes, err := oracleplanner.New().GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*collation of column "C" of table "T".*index "t_c_idx" uses the column.*`)
	c.Assert(nodes, qt.HasLen, 0)
}
