//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
)

// A declared value may carry a newline: a notice body, an address, a template.
// Inside a quoted literal a newline is ordinary SQL, and the rendered statement
// is one statement. A plan that recovers its statements by reading the script
// line by line cuts that literal in two, and the engine refuses the head with
// SQLSTATE 42601 before any row is written (stokaro/ptah#3278).

// TestDeclaredRowNewlineIsOneStatementLive plans a data-only change against a
// table that already exists, so the plan carries nothing but the declared row.
func TestDeclaredRowNewlineIsOneStatementLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn, done := declaredRowsDatabase(c, ctx, "newline_plan")
	defer done()

	applyDeclaredRows(c, ctx, conn, declaredNoticeSchema())

	plan, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{
		Desired: declaredNoticeSchema(declaredNotice("one", "first line\nsecond line")),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(declaredPlanSQL(plan), qt.DeepEquals, []string{
		"INSERT INTO \"notices\" (\"body\", \"code\") VALUES ('first line\nsecond line', 'one');",
	})
}

// TestDeclaredRowNewlineRoundTripsLive applies a row whose value carries a
// newline and reads the column back.
func TestDeclaredRowNewlineRoundTripsLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn, done := declaredRowsDatabase(c, ctx, "newline_insert")
	defer done()

	applyDeclaredRows(c, ctx, conn, declaredNoticeSchema(declaredNotice("one", "first line\nsecond line")))

	c.Assert(declaredNoticeBody(c, ctx, conn, "one"), qt.Equals, "first line\nsecond line")
}

// TestDeclaredRowNewlineUpdateLive is the same value reaching an existing row.
//
// An UPDATE cut at the newline leaves a head that files as an update and a tail
// that files as an insert, and the insert phase runs first, so the engine sees
// the tail before the head.
func TestDeclaredRowNewlineUpdateLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn, done := declaredRowsDatabase(c, ctx, "newline_update")
	defer done()

	applyDeclaredRows(c, ctx, conn, declaredNoticeSchema(declaredNotice("one", "plain")))
	applyDeclaredRows(c, ctx, conn, declaredNoticeSchema(declaredNotice("one", "first line\nsecond line")))

	c.Assert(declaredNoticeBody(c, ctx, conn, "one"), qt.Equals, "first line\nsecond line")
}

// TestDeclaredRowNewlineDeleteLive removes a row whose key carries a newline.
//
// The row is written through the driver rather than through a plan, so the
// statement the engine judges is the DELETE alone.
func TestDeclaredRowNewlineDeleteLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn, done := declaredRowsDatabase(c, ctx, "newline_delete")
	defer done()

	applyDeclaredRows(c, ctx, conn, declaredNoticeSchema())
	_, err := conn.ExecContext(ctx, `INSERT INTO "notices" ("code", "body") VALUES ($1, $2)`, "first\nsecond", "gone")
	c.Assert(err, qt.IsNil)

	applyDeclaredRows(c, ctx, conn, declaredNoticeSchema())

	c.Assert(declaredRowCodes(c, ctx, conn, "notices"), qt.DeepEquals, []string(nil))
}

// TestDeclaredRowNewlineConvergesLive is the control on the read-back side: the
// value the engine returns pairs with the declaration, so a second plan carries
// nothing.
func TestDeclaredRowNewlineConvergesLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn, done := declaredRowsDatabase(c, ctx, "newline_converge")
	defer done()

	desired := declaredNoticeSchema(declaredNotice("one", "first line\nsecond line"))
	applyDeclaredRows(c, ctx, conn, desired)

	plan, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{Desired: desired})
	c.Assert(err, qt.IsNil)
	c.Assert(declaredPlanSQL(plan), qt.DeepEquals, []string(nil))
}

func declaredNoticeSchema(rows ...schemamodel.ManagedRow) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Notice", Name: "notices"}},
		Fields: []schemamodel.Field{
			{StructName: "Notice", FieldName: "Code", Name: "code", Type: "TEXT", Primary: true},
			{StructName: "Notice", FieldName: "Body", Name: "body", Type: "TEXT"},
		},
		ManagedData: []schemamodel.ManagedData{{
			StructName: "Notice",
			Table:      "notices",
			Keys:       []string{"code"},
			File:       "notices.yaml",
			Rows:       append(make([]schemamodel.ManagedRow, 0, len(rows)), rows...),
		}},
	}
	schemamodel.Finalize(db)
	return db
}

func declaredNotice(code, body string) schemamodel.ManagedRow {
	return schemamodel.ManagedRow{
		"code": {Tag: "str", Text: code},
		"body": {Tag: "str", Text: body},
	}
}

func declaredNoticeBody(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, code string) string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, `SELECT "body" FROM "notices" WHERE "code" = $1`, code)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	body := ""
	c.Assert(rows.Next(), qt.IsTrue)
	c.Assert(rows.Scan(&body), qt.IsNil)
	c.Assert(rows.Err(), qt.IsNil)
	return body
}
