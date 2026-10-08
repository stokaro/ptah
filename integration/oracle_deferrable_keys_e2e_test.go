//go:build integration

package integration_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	_ "github.com/sijms/go-ora/v3" // registers the Oracle driver for database/sql

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// oracleDeferrableKeys declares ora_slots, whose primary key and UNIQUE defer
// their checks with the timing given, or do not defer when it is empty.
func oracleDeferrableKeys(initially string) *schemamodel.Database {
	deferrable := initially != ""
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "OraSlot", Name: "ora_slots", PrimaryKey: []string{"id"},
			PrimaryKeyDeferrable: deferrable, PrimaryKeyInitially: initially,
		}},
		Fields: []schemamodel.Field{
			{StructName: "OraSlot", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "OraSlot", Name: "pos", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "OraSlot", Table: "ora_slots", Name: "ora_slots_pos_uq", Type: "UNIQUE",
			Columns: []string{"pos"}, Deferrable: deferrable,
		}},
	}
}

// oracleKeyDeferrals lists `<type> <deferrable> <initially>` for the keys of
// ORA_SLOTS the catalog holds, sorted.
func oracleKeyDeferrals(database *catalog.Database) []string {
	var keys []string
	for _, constraint := range database.Constraints {
		if constraint.TableName != "ORA_SLOTS" || constraint.Type == "CHECK" {
			continue
		}
		deferral := "not deferrable"
		if constraint.Deferrable {
			deferral = "deferrable " + constraint.Initially
		}
		keys = append(keys, constraint.Type+" "+deferral)
	}
	slices.Sort(keys)
	return keys
}

// TestOracleDeferrableKeysConvergeE2E plans a table whose primary key and
// UNIQUE defer their checks, applies the plan, reads the keys back deferrable
// from user_constraints and finds nothing left to do. Oracle Free 23 takes
// DEFERRABLE INITIALLY DEFERRED on both keys (stokaro/ptah#3824).
func TestOracleDeferrableKeysConvergeE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	defer func() {
		c.Check(conn.SchemaWriter().DropAllTables(context.WithoutCancel(ctx)), qt.IsNil)
	}()
	declared := oracleDeferrableKeys("deferred")

	applyOraclePlan(ctx, c, conn, declared)

	after, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleKeyDeferrals(after), qt.DeepEquals, []string{
		"PRIMARY KEY deferrable deferred",
		"UNIQUE deferrable immediate",
	})
	settled, err := schemadiff.CompareWithDatabase(ctx, conn, declared, after, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(settled.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(settled.ConstraintsRemoved, qt.HasLen, 0)

	// The control: the same keys without the clause are a change, and applying
	// it leaves both keys checking at once.
	plain := oracleDeferrableKeys("")
	applyOraclePlan(ctx, c, conn, plain)
	changed, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	// Oracle names the primary key itself, and a key it named that does not
	// defer is carried on the column, so the table lists the UNIQUE alone.
	c.Assert(oracleKeyDeferrals(changed), qt.DeepEquals, []string{"UNIQUE not deferrable"})
	primary := slices.IndexFunc(changed.Tables, func(table catalog.Table) bool { return table.Name == "ORA_SLOTS" })
	c.Assert(primary, qt.Not(qt.Equals), -1)
	c.Assert(changed.Tables[primary].Columns[0].IsPrimaryKey, qt.IsTrue)
}

// applyOraclePlan plans declared against the database and runs every
// statement of the plan.
func applyOraclePlan(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database) {
	c.Helper()
	before, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, before, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, platform.Oracle, planner.Options{Capabilities: conn.Info().Capabilities},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.Not(qt.HasLen), 0)
	for _, statement := range statements {
		execOracle(ctx, c, conn, strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	}
}
