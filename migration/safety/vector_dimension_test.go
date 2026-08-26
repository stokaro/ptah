package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/migration/safety"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// vectorColumnDiff is one column whose type transition the comparator recorded.
func vectorColumnDiff(name, transition string) difftypes.ColumnDiff {
	return difftypes.ColumnDiff{
		ColumnName: name,
		Changes:    map[string]string{"type": transition},
	}
}

// modifiedVectorColumn is the statement a planner emits for one.
func modifiedVectorColumn(before, after string) ast.Node {
	return &ast.AlterTableNode{
		Name: "docs",
		Operations: []ast.AlterOperation{
			&ast.ModifyColumnOperation{
				Column:       ast.NewColumn("emb", after),
				PreviousType: before,
			},
		},
	}
}

// TestAVectorDimensionChangeIsDestructive is stokaro/ptah#2068's first
// acceptance scenario, in the layer that can answer it today.
//
// Measured on pgvector 0.8.6 / PostgreSQL 17, `ALTER TABLE docs ALTER COLUMN
// emb TYPE vector(1024)` on a populated `vector(384)` column answers "expected
// 1024 dimensions, not 384". Ptah planned exactly that statement and called it
// `columns_modified (warning)` -- the same thing it says about a widened
// VARCHAR. A reader was told a cast was planned.
//
// It is Destructive by the rule this file already applies to a narrowing type
// change: a transition that cannot keep the existing values is Destructive even
// though whether it loses any is data-dependent.
func TestAVectorDimensionChangeIsDestructive(t *testing.T) {
	tests := []struct {
		name   string
		before string
		after  string
	}{
		{name: "the epic's own transition", before: "vector(384)", after: "vector(1024)"},
		{name: "the shrinking direction", before: "vector(1024)", after: "vector(384)"},
		{name: "halfvec", before: "halfvec(3)", after: "halfvec(8)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(safety.Classify(modifiedVectorColumn(test.before, test.after)),
				qt.Equals, safety.Destructive)
		})
	}
}

// TestAVectorDimensionChangeSaysWhatItIs pins the reason, not only the
// severity.
//
// A Destructive verdict carrying "column type changes from vector(384) to
// vector(1024)" would satisfy the test above and still tell a reader nothing
// about why no migration can perform it.
func TestAVectorDimensionChangeSaysWhatItIs(t *testing.T) {
	c := qt.New(t)

	assessments := safety.Assess([]ast.Node{modifiedVectorColumn("vector(384)", "vector(1024)")})

	c.Assert(assessments, qt.HasLen, 1)
	c.Assert(assessments[0].Reason, qt.Contains, "vector dimension changes from 384 to 1024")
	c.Assert(assessments[0].Reason, qt.Contains, "recomputed")
}

// TestAVectorDimensionChangeIsItsOwnFinding is the drift path.
//
// `ptah schema drift` reports categories rather than statements, so a rule that
// lived only in the statement assessment left drift answering
// "columns_modified: 1 (warning)" for the one column modification the server
// refuses outright.
func TestAVectorDimensionChangeIsItsOwnFinding(t *testing.T) {
	c := qt.New(t)

	findings := safety.ClassifySchemaDiff(&difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName: "docs",
			ColumnsModified: []difftypes.ColumnDiff{
				vectorColumnDiff("emb", "vector(384) -> vector(1024)"),
			},
		}},
	})

	c.Assert(findings, qt.Contains, safety.Finding{
		Category: "vector_dimension_changed",
		Count:    1,
		Severity: safety.Destructive,
	})
	c.Assert(safety.Highest(findings), qt.Equals, safety.Destructive)
}

// TestAnOrdinaryColumnChangeIsStillAWarning is the control both need.
//
// A rule that answered Destructive for every modified column would satisfy
// every assertion above and would make the finding meaningless.
func TestAnOrdinaryColumnChangeIsStillAWarning(t *testing.T) {
	tests := []struct {
		name       string
		transition string
	}{
		{name: "a widened varchar", transition: "varchar(100) -> varchar(200)"},
		{name: "the same vector dimension", transition: "vector(384) -> vector(384)"},
		{name: "an unsized vector", transition: "vector(3) -> vector"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := safety.ClassifySchemaDiff(&difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{
					TableName:       "docs",
					ColumnsModified: []difftypes.ColumnDiff{vectorColumnDiff("emb", test.transition)},
				}},
			})

			c.Assert(findings, qt.Contains, safety.Finding{
				Category: "columns_modified",
				Count:    1,
				Severity: safety.Warning,
			})
			for _, finding := range findings {
				c.Assert(finding.Category, qt.Not(qt.Equals), "vector_dimension_changed")
			}
		})
	}
}
