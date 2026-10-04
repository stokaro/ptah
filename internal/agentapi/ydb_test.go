package agentapi_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/agentapi"
)

// ledger is a schema with nothing YDB lacks.
const ledger = `package models

//ptah:schema:table name="entries" schema="books"
type Entry struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="memo" type="VARCHAR(255)" not_null="true"
	Memo string
}
`

// The declared-schema tools take YDB as a dialect: render_schema answers in
// YQL, with the table in its directory and the declared VARCHAR as Utf8.
func TestRenderSchema_YDB(t *testing.T) {
	c := qt.New(t)
	source := writeSchema(c, ledger)

	response, err := schemaSession(c, source).RenderSchema(context.Background(),
		agentapi.RenderSchemaRequest{Source: source, Dialect: "ydb"})

	c.Assert(err, qt.IsNil)
	c.Assert(response.Dialect, qt.Equals, "ydb")
	c.Assert(response.Statements, qt.HasLen, 1)
	c.Assert(response.Statements[0], qt.Contains, "CREATE TABLE `books/entries` (")
	c.Assert(response.Statements[0], qt.Contains, "`memo` Utf8 NOT NULL,")
}

// render_schema writes a view for YDB with the security clause YDB requires
// on every view. The bookshop schema declares one.
func TestRenderSchema_YDBView(t *testing.T) {
	c := qt.New(t)
	source := writeSchema(c, bookshop)

	response, err := schemaSession(c, source).RenderSchema(context.Background(),
		agentapi.RenderSchemaRequest{Source: source, Dialect: "ydb"})

	c.Assert(err, qt.IsNil)
	c.Assert(response.Statements, qt.Contains,
		"CREATE VIEW `author_names` WITH (security_invoker = TRUE) AS\nSELECT name FROM authors\n;\n")
}

// validate_schema for YDB reports what YDB cannot store, with the reason the
// YDB type map gives: YDB has no time-of-day type.
func TestValidateSchema_YDB(t *testing.T) {
	c := qt.New(t)
	source := writeSchema(c, `package models

//ptah:schema:table name="entries"
type Entry struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="at" type="TIME"
	At string
}
`)

	response, err := schemaSession(c, source).ValidateSchema(context.Background(),
		agentapi.ValidateSchemaRequest{Source: source, Dialect: "ydb"})

	c.Assert(err, qt.IsNil)
	c.Assert(response.Dialect, qt.Equals, "ydb")
	c.Assert(response.Problems, qt.HasLen, 1)
	c.Assert(response.Problems[0].Dialect, qt.Equals, "ydb")
	c.Assert(response.Problems[0].Message, qt.Equals,
		`column "at" of table "entries": TIME has no YDB counterpart: YDB has no time-of-day type`)
}
