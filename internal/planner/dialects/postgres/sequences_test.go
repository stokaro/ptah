package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/internal/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanner_SequencesAdded_OrderedBeforeTablesWithOwnershipAfter(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		SequencesAdded: []string{"order_seq"},
		TablesAdded:    []string{"orders"},
	}
	generated := &goschema.Database{
		Sequences: []goschema.Sequence{
			{Name: "order_seq", AsType: "bigint", Cache: new(int64(20)), OwnedBy: "orders.id"},
		},
		Tables: []goschema.Table{{StructName: "Order", Name: "orders"}},
		Fields: []goschema.Field{{StructName: "Order", Name: "id", Type: "BIGINT", Primary: true}},
	}

	nodes := postgres.New().GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	createIdx := strings.Index(sql, "CREATE SEQUENCE")
	tableIdx := strings.Index(sql, "CREATE TABLE")
	ownedIdx := strings.Index(sql, "OWNED BY")

	c.Assert(createIdx >= 0, qt.IsTrue, qt.Commentf("CREATE SEQUENCE must be present:\n%s", sql))
	c.Assert(tableIdx >= 0, qt.IsTrue)
	c.Assert(ownedIdx >= 0, qt.IsTrue)
	c.Assert(createIdx < tableIdx, qt.IsTrue, qt.Commentf("CREATE SEQUENCE must precede CREATE TABLE"))
	c.Assert(tableIdx < ownedIdx, qt.IsTrue, qt.Commentf("OWNED BY must follow CREATE TABLE"))
	// The bare CREATE SEQUENCE must not carry an inline OWNED BY.
	c.Assert(sql[createIdx:tableIdx], qt.Not(qt.Contains), "OWNED BY")
}

func TestPlanner_SequencesModified_EmitsAlterForChangedOptionsOnly(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		SequencesModified: []types.SequenceDiff{
			{SequenceName: "order_seq", Changes: map[string]string{"increment": "1 -> 5", "cache": "20 -> 50"}},
		},
	}
	generated := &goschema.Database{
		Sequences: []goschema.Sequence{
			{Name: "order_seq", Increment: new(int64(5)), Cache: new(int64(50)), Start: new(int64(1))},
		},
	}

	nodes := postgres.New().GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	c.Assert(sql, qt.Contains, "ALTER SEQUENCE order_seq")
	c.Assert(sql, qt.Contains, "INCREMENT BY 5")
	c.Assert(sql, qt.Contains, "CACHE 50")
	// START was not in the change set, so it must not be emitted.
	c.Assert(sql, qt.Not(qt.Contains), "START WITH")
}
