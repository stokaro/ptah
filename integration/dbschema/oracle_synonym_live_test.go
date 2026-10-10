//go:build integration

package dbschema_test

import (
	"context"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/feature/synonym"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestOracleLiveSynonymLifecycle drives a declared synonym through Oracle the
// way an apply does: the first plan creates it, the read finds it, the second
// plan is empty, and a changed target is one retarget the server accepts.
//
// Oracle stores an unquoted name in upper case and records the target with its
// owner, so the declaration and the read spell the same synonym differently;
// the comparison has to pair them under the connection's rules.
func TestOracleLiveSynonymLifecycle(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	defer func() {
		c.Check(conn.SchemaWriter().DropAllTables(context.WithoutCancel(ctx)), qt.IsNil)
	}()
	for _, statement := range []string{
		"CREATE TABLE ora_syn_orders (id NUMBER PRIMARY KEY)",
		"CREATE TABLE ora_syn_archive (id NUMBER PRIMARY KEY)",
	} {
		c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("%s", statement))
	}

	declared := oracleSynonymDeclaration("ora_syn_orders")
	created := oracleSynonymPlan(c, conn, declared)
	c.Assert(created, qt.DeepEquals, []string{"CREATE SYNONYM ora_syn_alias FOR ora_syn_orders"})
	oracleApply(c, conn, created)

	c.Assert(oracleReadSynonyms(c, conn), qt.DeepEquals, map[string]string{"ORA_SYN_ALIAS": oracleOwner(conn) + ".ORA_SYN_ORDERS"})
	c.Assert(oracleSynonymPlan(c, conn, declared), qt.HasLen, 0,
		qt.Commentf("the synonym the first plan created is the one the declaration names"))

	retargeted := oracleSynonymPlan(c, conn, oracleSynonymDeclaration("ora_syn_archive"))
	c.Assert(retargeted, qt.HasLen, 2)
	oracleApply(c, conn, retargeted)
	c.Assert(oracleReadSynonyms(c, conn), qt.DeepEquals, map[string]string{"ORA_SYN_ALIAS": oracleOwner(conn) + ".ORA_SYN_ARCHIVE"})
	c.Assert(oracleSynonymPlan(c, conn, oracleSynonymDeclaration("ora_syn_archive")), qt.HasLen, 0)
}

// oracleSynonymDeclaration declares the two tables and one synonym for target,
// from a source that can declare synonyms.
func oracleSynonymDeclaration(target string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Orders", Name: "ora_syn_orders"}, {StructName: "Archive", Name: "ora_syn_archive"}},
		Fields: []schemamodel.Field{
			{StructName: "Orders", Name: "id", Type: "NUMBER", Primary: true},
			{StructName: "Archive", Name: "id", Type: "NUMBER", Primary: true},
		},
		FeatureObjects: must.Must(schemaext.NewObjects(synonym.DeclaredObject(synonym.DesiredSynonym{
			Synonym: synonym.Synonym{Name: "ora_syn_alias", Target: target},
		}))),
		FeatureCoverage: must.Must(synonym.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
}

// oracleSynonymPlan compares the declaration with the live schema and plans
// the synonym changes alone, so a difference in how the two tables are
// described cannot pass for a synonym statement.
func oracleSynonymPlan(c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database) []string {
	c.Helper()
	live, err := conn.Reader().ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, declared, live, nil, builtintest.Runtime())
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), builtintest.Runtime(),
		&difftypes.SchemaDiff{FeatureChanges: diff.FeatureChanges}, conn.Info().Dialect)
	c.Assert(err, qt.IsNil)
	return statements
}

func oracleApply(c *qt.C, conn *dbschema.DatabaseConnection, statements []string) {
	c.Helper()
	for _, statement := range statements {
		c.Assert(conn.SchemaWriter().ExecuteSQL(c.Context(), strings.TrimSuffix(statement, ";")), qt.IsNil, qt.Commentf("%s", statement))
	}
}

// oracleReadSynonyms maps each synonym the read found to its target.
func oracleReadSynonyms(c *qt.C, conn *dbschema.DatabaseConnection) map[string]string {
	c.Helper()
	live, err := conn.Reader().ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	return observedSynonymTargets(c, live)
}

func observedSynonymTargets(c *qt.C, live *catalog.Database) map[string]string {
	c.Helper()
	objects, err := live.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	targets := make(map[string]string)
	for _, object := range objects {
		if observed, ok := object.Value.(*synonym.ObservedSynonym); ok {
			targets[observed.Name] = observed.Target
		}
	}
	return targets
}

func oracleOwner(conn *dbschema.DatabaseConnection) string {
	return strings.ToUpper(conn.Info().Schema)
}
