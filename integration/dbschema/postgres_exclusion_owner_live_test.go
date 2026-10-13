//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/sqlident"
)

// The complete source/read/compare/plan path must converge after creation and
// each change. Constraint comments preserve identity; definitions replace it.
func TestPostgresExclusionOwnerConverges(t *testing.T) {
	for _, test := range []struct {
		name, elements, condition, comment, timing string
		retainsIdentity                            bool
	}{
		{name: "comment", elements: "room WITH =", condition: "room > 0", comment: "Room's schedule", retainsIdentity: true},
		{name: "clear comment", elements: "room WITH =", condition: "room > 0", retainsIdentity: true},
		{name: "expression", elements: "(room + 1) WITH =", condition: "room > 0", comment: "Expression"},
		{name: "predicate", elements: "room WITH =", condition: "room > 1", comment: "Predicate"},
		{name: "timing", elements: "room WITH =", condition: "room > 0", comment: "Deferred", timing: `deferrable initially="deferred"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schema := newFormsSchema(c)
			initial := exclusionOwnerSource(c, schema, "room WITH =", "room > 0", "Initial comment", "")
			creation := planDocumentAgainstLive(c, conn, initial, schema)
			c.Assert(strings.Join(creation, "\n"), qt.Contains, `CONSTRAINT "no_overlap" EXCLUDE`)
			c.Assert(strings.Join(creation, "\n"), qt.Contains, "ADD CONSTRAINT")
			applyExclusionPlan(c, conn, creation)
			c.Assert(planDocumentAgainstLive(c, conn, initial, schema), qt.HasLen, 0)
			before, _ := exclusionOwnerIdentity(c, conn, schema)

			live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schema})
			c.Assert(err, qt.IsNil)
			c.Assert(live.Constraints, qt.HasLen, 0)
			c.Assert(live.Indexes, qt.HasLen, 1)
			c.Assert(live.Indexes[0].ManagedBy, qt.DeepEquals, pgtable.ExclusionRef(schema+".rooms", "no_overlap", identifier.ForDialect("postgres")))
			exported, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), live, "postgres", builtintest.Runtime())
			c.Assert(err, qt.IsNil)
			c.Assert(exported.Indexes, qt.HasLen, 0)
			c.Assert(planDocumentAgainstLive(c, conn, exported, schema), qt.HasLen, 0)

			desired := exclusionOwnerSource(c, schema, test.elements, test.condition, test.comment, test.timing)
			statements := planDocumentAgainstLive(c, conn, desired, schema)
			c.Assert(statements, qt.Not(qt.HasLen), 0)
			c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "DROP INDEX")
			applyExclusionPlan(c, conn, statements)
			after, comment := exclusionOwnerIdentity(c, conn, schema)
			c.Assert(after == before, qt.Equals, test.retainsIdentity)
			c.Assert(comment, qt.Equals, test.comment)
			c.Assert(planDocumentAgainstLive(c, conn, desired, schema), qt.HasLen, 0)
		})
	}
}

func TestPostgresExclusionOwnerRemovalConverges(t *testing.T) {
	c := qt.New(t)
	conn, schema := newFormsSchema(c)
	desired := exclusionOwnerSource(c, schema, "room WITH =", "room > 0", "", "")
	applyExclusionPlan(c, conn, planDocumentAgainstLive(c, conn, desired, schema))
	desired.FeatureObjects = desired.FeatureObjects.Select(func(ref objectidentity.ID) bool { return ref.Kind != objectidentity.Kind(pgtable.ExclusionKind) })
	statements := planDocumentAgainstLive(c, conn, desired, schema)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `DROP CONSTRAINT "no_overlap"`)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "DROP INDEX")
	applyExclusionPlan(c, conn, statements)
	oid, _ := exclusionOwnerIdentity(c, conn, schema)
	c.Assert(oid, qt.Equals, int64(0))
	c.Assert(planDocumentAgainstLive(c, conn, desired, schema), qt.HasLen, 0)
}

func TestPostgresExclusionOwnerQuotedNamesConverge(t *testing.T) {
	c := qt.New(t)
	conn, parent := newFormsSchema(c)
	schema := parent + ".Space"
	_, err := conn.ExecContext(c.Context(), "CREATE SCHEMA "+sqlident.Quote("postgres", schema))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP SCHEMA "+sqlident.Quote("postgres", schema)+" CASCADE")
		c.Assert(err, qt.IsNil)
	})
	for _, comment := range []string{"Initial comment", "Changed comment"} {
		source := fmt.Sprintf(`package entities
//ptah:schema:table name="Rooms.Dot" schema=%q
//ptah:schema:exclude name="no_overlap" using="btree" elements="room WITH =" comment=%q
type Room struct {
 //ptah:schema:field name="room" type="INTEGER"
 Room int
}
`, schema, comment)
		desired, err := goschema.ParseSource(builtintest.Runtime().Annotations(), "schema.go", source)
		c.Assert(err, qt.IsNil)
		plan := planDocumentAgainstLive(c, conn, &desired, schema)
		c.Assert(plan, qt.Not(qt.HasLen), 0)
		applyExclusionPlan(c, conn, plan)
		_, actualComment := exclusionOwnerIdentity(c, conn, schema)
		c.Assert(actualComment, qt.Equals, comment)
		c.Assert(planDocumentAgainstLive(c, conn, &desired, schema), qt.HasLen, 0)
	}
}

func exclusionOwnerSource(c *qt.C, schema, elements, condition, comment, timing string) *schemamodel.Database {
	c.Helper()
	source := fmt.Sprintf(`package entities
//ptah:schema:table name="rooms" schema=%q
//ptah:schema:exclude name="no_overlap" using="btree" elements=%q condition=%q comment=%q %s
type Room struct {
 //ptah:schema:field name="room" type="INTEGER"
 Room int
}
`, schema, elements, condition, comment, timing)
	desired, err := goschema.ParseSource(builtintest.Runtime().Annotations(), "schema.go", source)
	c.Assert(err, qt.IsNil)
	return &desired
}

func applyExclusionPlan(c *qt.C, conn *dbschema.DatabaseConnection, statements []string) {
	c.Helper()
	tx, err := conn.BeginTx(c.Context(), nil)
	c.Assert(err, qt.IsNil)
	defer tx.Rollback()
	for _, statement := range statements {
		_, err = tx.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	c.Assert(tx.Commit(), qt.IsNil)
}

func exclusionOwnerIdentity(c *qt.C, conn *dbschema.DatabaseConnection, schema string) (int64, string) {
	c.Helper()
	var oid int64
	var comment string
	err := conn.QueryRowContext(c.Context(), `SELECT COALESCE(MAX(c.oid::bigint),0), COALESCE(MAX(obj_description(c.oid,'pg_constraint')),'')
 FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace
 WHERE n.nspname=$1 AND c.conname='no_overlap'`, schema).Scan(&oid, &comment)
	c.Assert(err, qt.IsNil)
	return oid, comment
}

// Different names declare separate constraints even when their definitions are
// equivalent. PostgreSQL must retain each name and its own comment.
func TestPostgresExclusionOwnerEquivalentNamesInMigration(t *testing.T) {
	for _, elements := range []string{"room WITH =", "(room::integer) WITH ="} {
		t.Run(elements, func(t *testing.T) {
			c := qt.New(t)
			conn, schema := newFormsSchema(c)
			desired := exclusionOwnerPairSource(c, schema, elements)
			statements := planDocumentAgainstLive(c, conn, desired, schema)
			verifyExclusionPair(c, conn, schema, desired, statements)
		})
	}
}

func TestPostgresExclusionOwnerEquivalentNamesInRenderedSchema(t *testing.T) {
	for _, elements := range []string{"room WITH =", "(room::integer) WITH ="} {
		t.Run(elements, func(t *testing.T) {
			c := qt.New(t)
			conn, schema := newFormsSchema(c)
			desired := exclusionOwnerPairSource(c, schema, elements)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(desired, "postgres", capability.Postgres18())
			c.Assert(err, qt.IsNil)
			verifyExclusionPair(c, conn, schema, desired, statements)
		})
	}
}

func exclusionOwnerPairSource(c *qt.C, schema, secondElements string) *schemamodel.Database {
	c.Helper()
	source := fmt.Sprintf(`package entities
//ptah:schema:table name="rooms" schema=%q
//ptah:schema:exclude name="first" using="btree" elements="room WITH =" comment="First comment"
//ptah:schema:exclude name="second" using="btree" elements=%q comment="Second comment"
type Room struct {
 //ptah:schema:field name="room" type="INTEGER"
 Room int
}
`, schema, secondElements)
	desired, err := goschema.ParseSource(builtintest.Runtime().Annotations(), "schema.go", source)
	c.Assert(err, qt.IsNil)
	return &desired
}

func verifyExclusionPair(c *qt.C, conn *dbschema.DatabaseConnection, schema string, desired *schemamodel.Database, statements []string) {
	c.Helper()
	c.Assert(strings.Count(strings.Join(statements, "\n"), "ADD CONSTRAINT"), qt.Equals, 2)
	applyExclusionPlan(c, conn, statements)
	rows, err := conn.QueryContext(c.Context(), `SELECT c.conname, COALESCE(obj_description(c.oid, 'pg_constraint'), '')
 FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace
 WHERE n.nspname=$1 AND c.contype='x' ORDER BY c.conname`, schema)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	comments := make(map[string]string)
	for rows.Next() {
		var name, comment string
		c.Assert(rows.Scan(&name, &comment), qt.IsNil)
		comments[name] = comment
	}
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(comments, qt.DeepEquals, map[string]string{"first": "First comment", "second": "Second comment"})
	c.Assert(planDocumentAgainstLive(c, conn, desired, schema), qt.HasLen, 0)
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schema})
	c.Assert(err, qt.IsNil)
	c.Assert(live.Indexes, qt.HasLen, 2)
	for _, index := range live.Indexes {
		c.Assert(index.ManagedBy.Kind, qt.Equals, objectidentity.Kind(pgtable.ExclusionKind))
	}
	exported, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), live, "postgres", builtintest.Runtime())
	c.Assert(err, qt.IsNil)
	c.Assert(exported.Indexes, qt.HasLen, 0)
	c.Assert(planDocumentAgainstLive(c, conn, exported, schema), qt.HasLen, 0)
}
