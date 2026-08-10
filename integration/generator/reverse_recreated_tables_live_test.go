//go:build integration

package generator_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

const (
	rrtWidgets = "ptah_rrt_widgets"
	rrtGadgets = "ptah_rrt_gadgets"
)

// TestReverseRecreatedTable_DropTableRollbackApplies_Integration is the live
// gate for the rollback half of a migration that DROPS a table.
//
// Rendering that rollback proves a CREATE TABLE exists; it does not prove the
// server accepts what follows it. The version this replaces rendered perfectly
// and was refused at its third statement, because the re-created table already
// carried the primary key the plan then added again — PostgreSQL 17.10,
// `multiple primary keys for table "ptah_rrt_gadgets" are not allowed`. The
// same shape on MySQL 9.7 is `ERROR 1068 (42000): Multiple primary key
// defined`.
//
// The test seeds the pre-up state with Ptah's own planner, applies the up
// migration, applies the down migration, and then asserts that the dropped
// table is back with the constraints it had. Applying is the gate; the catalog
// compare is what stops a rollback that applies from restoring less than it
// should.
func TestReverseRecreatedTable_DropTableRollbackApplies_Integration(t *testing.T) {
	c := qt.New(t)

	url := revIntRequireURL(t)
	conn := revIntConnect(t, url)
	rrtDropAll(conn)
	t.Cleanup(func() { rrtDropAll(conn) })

	// 1. The pre-up state, installed through Ptah so the catalog holds what
	//    Ptah writes rather than a hand-typed approximation of it.
	prior := rrtPriorSchema()
	seedSQL, _ := generateLiveMigrationSQL(c, conn, prior)
	execScript(c, conn, seedSQL, "SEED")

	dbPrior, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	priorCatalog := rrtCatalog(dbPrior)
	c.Assert(priorCatalog, qt.Not(qt.HasLen), 0,
		qt.Commentf("the seed must leave constraints behind, or the compare proves nothing"))

	// 2. The up migration under test: the table goes away.
	target := rrtTargetSchema()
	upDiff := schemadiff.CompareWithDialect(target, dbPrior, "postgres")
	c.Assert(upDiff.TablesRemoved, qt.DeepEquals, []string{rrtGadgets})

	upSQL, downSQL := generateLiveMigrationSQL(c, conn, target)
	execScript(c, conn, upSQL, "UP")

	dbAfterUp, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(rrtHasTable(dbAfterUp), qt.IsFalse,
		qt.Commentf("the up migration must really drop the table, or the down proves nothing"))

	// 3. THE GATE. Every statement of the rollback has to be accepted.
	c.Assert(strings.ToUpper(downSQL), qt.Contains, "CREATE TABLE",
		qt.Commentf("a rollback planned against the desired state instead of the "+
			"pre-change one has nothing to re-create:\n%s", downSQL))
	execScript(c, conn, downSQL, "DOWN")

	// 4. Applying is not arriving: the table has to be back, with its
	//    constraints, and not with more of them than it started with.
	dbAfterDown, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(rrtHasTable(dbAfterDown), qt.IsTrue, qt.Commentf("down SQL:\n%s", downSQL))
	c.Assert(rrtCatalog(dbAfterDown), qt.DeepEquals, priorCatalog,
		qt.Commentf("down SQL:\n%s", downSQL))
}

func rrtTargetSchema() *goschema.Database {
	schema := rrtBaseSchema()
	goschema.Finalize(schema)
	return schema
}

func rrtBaseSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "RrtWidget", Name: rrtWidgets}},
		Fields: []goschema.Field{
			{StructName: "RrtWidget", Name: "id", Type: "BIGINT", Primary: true},
		},
	}
}

// rrtPriorSchema returns the fixture with the table the migration drops. The
// table carries one constraint of each kind the reverse has to decide about: a
// primary key and a single-column foreign key, which its own CREATE TABLE
// restores, and a CHECK, which nothing but an ALTER can restore.
func rrtPriorSchema() *goschema.Database {
	schema := rrtBaseSchema()
	schema.Tables = append(schema.Tables, goschema.Table{StructName: "RrtGadget", Name: rrtGadgets})
	schema.Fields = append(schema.Fields,
		goschema.Field{StructName: "RrtGadget", Name: "id", Type: "BIGINT", Primary: true},
		goschema.Field{StructName: "RrtGadget", Name: "qty", Type: "BIGINT"},
		goschema.Field{
			StructName: "RrtGadget", Name: "widget_id", Type: "BIGINT", Nullable: true,
			Foreign: rrtWidgets + "(id)", ForeignKeyName: "ptah_rrt_gadget_widget_fk",
		},
	)
	schema.Constraints = []goschema.Constraint{{
		StructName:      "RrtGadget",
		Table:           rrtGadgets,
		Name:            "ptah_rrt_gadget_qty_ck",
		Type:            "CHECK",
		CheckExpression: "qty > 0",
	}}
	goschema.Finalize(schema)
	return schema
}

// rrtHasTable reports whether the dropped table is present in the catalog.
func rrtHasTable(db *dbschematypes.DBSchema) bool {
	for _, table := range db.Tables {
		if table.Name == rrtGadgets {
			return true
		}
	}
	return false
}

// rrtCatalog reduces the introspected schema to the constraints on the table
// this test drops and restores, so a mismatch names the constraint rather than
// dumping a whole schema. The constraint NAME is deliberately included: a
// rollback that restores a constraint under a server-chosen name has not
// restored the catalog it started from.
//
// PostgreSQL's own NOT NULL checks are left out. They are not constraints
// anybody declared — the server synthesizes one per non-null column and names
// it after the table's OID, so a table that is dropped and re-created gets a
// new OID and new names for constraints whose meaning never changed.
func rrtCatalog(db *dbschematypes.DBSchema) []string {
	var lines []string
	for _, constraint := range db.Constraints {
		if constraint.TableName != rrtGadgets || rrtSyntheticNotNull(constraint) {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s %s (%s)",
			constraint.Type, constraint.Name, strings.Join(constraint.ColumnNamesOrDefault(), ",")))
	}
	sort.Strings(lines)
	return lines
}

// rrtSyntheticNotNull reports the server-generated per-column NOT NULL check,
// under the same rule the schema converter applies to it.
func rrtSyntheticNotNull(constraint dbschematypes.DBConstraint) bool {
	if constraint.CheckClause == nil || !strings.HasSuffix(constraint.Name, "_not_null") {
		return false
	}
	clause := strings.TrimSpace(strings.ToUpper(*constraint.CheckClause))
	return strings.HasSuffix(clause, " IS NOT NULL") && strings.Count(clause, " IS NOT NULL") == 1
}

// rrtDropAll clears this test's tables, child first, so a rerun starts from an
// empty catalog whatever the previous run left behind.
func rrtDropAll(conn *dbschema.DatabaseConnection) {
	for _, statement := range []string{
		"DROP TABLE IF EXISTS " + rrtGadgets + " CASCADE",
		"DROP TABLE IF EXISTS " + rrtWidgets + " CASCADE",
	} {
		_, _ = conn.Exec(statement)
	}
}
