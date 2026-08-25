package mcpserver_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// corpusSchema is one schema written once and asked about in every dialect.
//
// It carries the shapes a dialect is most likely to answer differently: an
// identity-ish key, a sized string, a boolean, a timestamp, a nullable column,
// a unique constraint and a foreign key. A fixture of one INTEGER column would
// agree everywhere and prove nothing.
const corpusSchema = `package models

//ptah:schema:table name="authors"
type Author struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string
	//ptah:schema:field name="active" type="BOOLEAN" not_null="true" default="true"
	Active bool
	//ptah:schema:field name="created_at" type="TIMESTAMP" not_null="true"
	CreatedAt string
	//ptah:schema:field name="bio" type="TEXT"
	Bio string
}

//ptah:schema:table name="books"
type Book struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="author_id" type="BIGINT" not_null="true" foreign="authors(id)"
	AuthorID int64
	//ptah:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string
}
`

// corpusRow is one dialect and what this document means for it.
type corpusRow struct {
	dialect string
	// refusedBecause is the fragment a refusal must carry, empty when the
	// dialect renders the document.
	refusedBecause string
}

// corpusRows are the targets this server is asked about.
//
// ClickHouse is here refusing rather than absent, and that is the row worth
// having: the document declares a foreign key, ClickHouse has none, and a
// corpus that quietly left it out would stop measuring whether a model asking
// about ClickHouse is told the truth. Measured, not assumed -- the fixture was
// written for six renderers and ClickHouse answered
// "clickhouse does not support foreign keys".
var corpusRows = []corpusRow{
	{dialect: "postgres"},
	{dialect: "mysql"},
	{dialect: "mariadb"},
	{dialect: "sqlite"},
	{dialect: "sqlserver"},
	{dialect: "clickhouse", refusedBecause: "does not support foreign keys"},
	// The four the surface accepts that this corpus did not ask about.
	// agentapi.normalizedDialect delegates to platform.NormalizeDialect, which
	// knows all four, and core/renderer has a renderer for each -- so a corpus
	// stopping at six left four targets able to answer and never asked
	// (stokaro/ptah#1490).
	{dialect: "cockroachdb"},
	{dialect: "yugabytedb"},
	{dialect: "spanner"},
	{dialect: "oracle"},
}

// renderingDialects are the rows that produce DDL, for the comparisons that
// need output rather than an answer.
func renderingDialects() []string {
	names := make([]string, 0, len(corpusRows))
	for _, row := range corpusRows {
		if row.refusedBecause != "" {
			continue
		}
		names = append(names, row.dialect)
	}
	return names
}

// TestServer_AnswersForEveryDialectItRendersFor is #1490's multi-dialect
// workflow corpus.
//
// The reading tools take a dialect and answer per dialect, and every existing
// test asks about exactly one. A dialect that started failing -- or worse,
// answering something empty -- would be invisible: the tool would still return,
// the schema would still be the schema, and only that one target would be
// wrong.
//
// The assertions are deliberately shallow and total rather than deep and
// partial. What matters at this level is that every dialect gets a real answer
// for the same document; what each answer should SAY is the renderer's own
// suite, which is far better placed to know.
func TestServer_AnswersForEveryDialectItRendersFor(t *testing.T) {
	for _, row := range corpusRows {
		t.Run(row.dialect, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(writeFile(dir, "models.go", corpusSchema), qt.IsNil)
			session := connect(c, readOnlyConfig(c, dir), nil)
			source := map[string]any{"root_dirs": []string{dir}}
			arguments := map[string]any{"source": source, "dialect": row.dialect}

			// Validation and lineage read the document rather than a target's
			// syntax, so every dialect answers them -- including the one that
			// cannot render this schema.
			c.Assert(callTool(c, session, "validate_schema", arguments), qt.Not(qt.HasLen), 0)
			c.Assert(callTool(c, session, "schema_lineage", arguments), qt.Not(qt.HasLen), 0)

			assertRendersOrRefuses(c, session, arguments, row)
		})
	}
}

// TestServer_TheDialectsAnswerDifferently is the control the corpus needs.
//
// Every assertion above is satisfied by a server that ignored the dialect
// entirely and rendered one thing six times. This asserts at least two targets
// produced different DDL for one document, so the parameter is measured as
// having an effect rather than as being accepted.
func TestServer_TheDialectsAnswerDifferently(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(writeFile(dir, "models.go", corpusSchema), qt.IsNil)
	session := connect(c, readOnlyConfig(c, dir), nil)
	source := map[string]any{"root_dirs": []string{dir}}

	answers := make(map[string]string, len(corpusRows))
	for _, dialect := range renderingDialects() {
		rendered := callTool(c, session, "render_schema",
			map[string]any{"source": source, "dialect": dialect})
		answers[dialect] = asJSONText(c, rendered)
	}

	distinct := make(map[string]bool, len(answers))
	for _, answer := range answers {
		distinct[answer] = true
	}
	c.Assert(len(distinct) > 1, qt.IsTrue,
		qt.Commentf("every dialect rendered the same bytes, so the parameter was accepted and not used"))
	c.Assert(answers["postgres"], qt.Not(qt.Equals), answers["mysql"])
	// Oracle is the row that separates "this dialect was accepted" from "this
	// dialect was used" most sharply, and it is why the corpus is worth
	// widening rather than just longer: measured on this fixture, it emits
	// `CREATE TABLE authors (` where the other nine quote every identifier. A
	// renderer selected by name but not by behavior would collapse this
	// (stokaro/ptah#1490).
	c.Assert(answers["oracle"], qt.Not(qt.Equals), answers["postgres"])
	c.Assert(answers["oracle"], qt.Contains, "CREATE TABLE authors")
}

// TestServer_ADialectItDoesNotRenderForIsRefused pins the other side.
//
// A corpus that only ever asks about targets that work cannot tell an accepted
// dialect from an ignored one. This asks about a name no dialect has and
// requires the call to fail rather than silently answer for something else.
func TestServer_ADialectItDoesNotRenderForIsRefused(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(writeFile(dir, "models.go", corpusSchema), qt.IsNil)
	session := connect(c, readOnlyConfig(c, dir), nil)

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "render_schema",
		Arguments: map[string]any{
			"source":  map[string]any{"root_dirs": []string{dir}},
			"dialect": "nonesuch",
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.IsError, qt.IsTrue)
	c.Assert(strings.ToLower(toolText(result)), qt.Contains, "nonesuch")
}

// assertRendersOrRefuses holds one dialect to what this document means for it:
// DDL naming both tables, or a refusal naming the reason.
//
// The refusal is asserted as sharply as the success. "ClickHouse errored" would
// pass for a timeout, a missing directory or a typo in the dialect name, none of
// which is what this row is about.
func assertRendersOrRefuses(c *qt.C, session *mcp.ClientSession, arguments map[string]any, row corpusRow) {
	c.Helper()
	if row.refusedBecause != "" {
		refusal := callToolError(c, session, "render_schema", arguments)
		c.Assert(refusal, qt.Contains, row.refusedBecause)
		c.Assert(refusal, qt.Contains, row.dialect)
		return
	}
	rendered := asJSONText(c, callTool(c, session, "render_schema", arguments))
	c.Assert(rendered, qt.Contains, "authors",
		qt.Commentf("%s rendered nothing that names the table it was given", row.dialect))
	c.Assert(rendered, qt.Contains, "books")
}
