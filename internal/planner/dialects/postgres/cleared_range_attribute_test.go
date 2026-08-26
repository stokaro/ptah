package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestGenerateMigration_AClearedRangeAttributeIsRecreatedWithoutIt is the end
// of the path a cleared attribute travels.
//
// The comparator reporting a difference is only half an answer: PostgreSQL has
// no `ALTER TYPE ... AS RANGE`, so removing an attribute means dropping the type
// and creating it again. This drives declaration and catalog through the real
// comparator and the real planner and reads the statements, so the removal is
// measured as SQL rather than as a map entry (stokaro/ptah#2223).
func TestGenerateMigration_AClearedRangeAttributeIsRecreatedWithoutIt(t *testing.T) {
	c := qt.New(t)

	sql := clearedRangeMigrationSQL(c, schemamodel.Range{
		Name:              "measurement",
		Subtype:           "int8",
		ClearedAttributes: []string{"subtype_diff"},
	})

	c.Assert(sql, qt.Contains, "DROP TYPE")
	c.Assert(sql, qt.Contains, "CREATE TYPE")
	c.Assert(sql, qt.Contains, "measurement")
	// The point of the whole path: what comes back has no subtype_diff.
	c.Assert(strings.ToLower(sql), qt.Not(qt.Contains), "int8_subdiff")
}

// TestGenerateMigration_AnOmittedRangeAttributeIsPlannedAway is the control,
// and it is the safety property the cleared spelling exists to preserve.
//
// A declaration that does not mention the attribute must plan nothing at all --
// the drop is non-CASCADE and fails while the type is in use, and rebuilds it
// without the function when it is not.
func TestGenerateMigration_AnOmittedRangeAttributeIsPlannedAway(t *testing.T) {
	c := qt.New(t)

	sql := clearedRangeMigrationSQL(c, schemamodel.Range{Name: "measurement", Subtype: "int8"})

	c.Assert(sql, qt.Equals, "")
}

// clearedRangeMigrationSQL plans the declaration against a catalog whose range
// carries a subtype_diff, and returns the statements as one string.
func clearedRangeMigrationSQL(c *qt.C, declared schemamodel.Range) string {
	c.Helper()

	target := &schemamodel.Database{Ranges: []schemamodel.Range{declared}}
	current := &catalog.Database{Ranges: []catalog.Range{{
		Name:        "measurement",
		Subtype:     "int8",
		SubtypeDiff: "int8_subdiff",
	}}}

	nodes, err := postgres.New().GenerateMigrationAST(schemadiff.Compare(target, current), target)
	c.Assert(err, qt.IsNil)

	var statements []string
	for _, node := range nodes {
		rendered, err := renderer.RenderSQL("postgres", node)
		c.Assert(err, qt.IsNil)
		statements = append(statements, rendered)
	}
	return strings.Join(statements, "\n")
}
