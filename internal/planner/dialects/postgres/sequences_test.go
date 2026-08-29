package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestPlanner_SequencesAdded_OrderedBeforeTablesWithOwnershipAfter(t *testing.T) {
	c := qt.New(t)

	// The addition carries the sequence, ownership included: that is what the
	// comparator now builds, and it is what `OWNED BY` is planned from
	// (stokaro/ptah#2315).
	diff := &difftypes.SchemaDiff{
		SequencesAdded: difftypes.SequenceChanges{
			{Name: "order_seq", AsType: "bigint", Cache: new(int64(20)), OwnedBy: "orders.id"},
		},
	}
	desired := &schemamodel.Database{
		Sequences: []schemamodel.Sequence{
			{Name: "order_seq", AsType: "bigint", Cache: new(int64(20)), OwnedBy: "orders.id"},
		},
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "BIGINT", Primary: true}},
	}
	// After the schema exists: a creation carries what CREATE TABLE renders
	// from, derived from the declaration (stokaro/ptah#2315).
	diff.TablesAdded = difftypes.TableCreationsFor(desired, "orders")

	nodes, err := postgres.New().GenerateMigrationAST(withDeclaredObjects(diff, desired), desired)
	c.Assert(err, qt.IsNil)
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

	diff := &difftypes.SchemaDiff{
		SequencesModified: []difftypes.SequenceDiff{
			{
				SequenceName: "order_seq",
				Changes:      map[string]string{"increment": "1 -> 5", "cache": "20 -> 50"},
				// The option VALUES travel with the change; the map only names
				// which ones moved (stokaro/ptah#2315).
				Desired: schemamodel.Sequence{
					Name: "order_seq", Increment: new(int64(5)), Cache: new(int64(50)), Start: new(int64(1)),
				},
			},
		},
	}

	nodes, err := postgres.New().GenerateMigrationAST(diff, &schemamodel.Database{})
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	c.Assert(sql, qt.Contains, "ALTER SEQUENCE order_seq")
	c.Assert(sql, qt.Contains, "INCREMENT BY 5")
	c.Assert(sql, qt.Contains, "CACHE 50")
	// START was not in the change set, so it must not be emitted.
	c.Assert(sql, qt.Not(qt.Contains), "START WITH")
}
