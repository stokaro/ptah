package agentapi_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentapi"
)

// writeSchema puts one annotated Go file in a temporary directory and returns
// the source that names it.
func writeSchema(c *qt.C, source string) agentapi.SchemaSource {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(source), 0o600), qt.IsNil)
	return agentapi.SchemaSource{RootDirs: []string{dir}}
}

const bookshop = `package models

//ptah:schema:table name="authors"
type Author struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}

//ptah:schema:view name="author_names" body="SELECT name FROM authors"
type AuthorNames struct{}
`

// dialectRow is one dialect value and whether the contract accepts it.
type dialectRow struct {
	name    string
	dialect string
	wantErr bool
}

// TestOperations_RefuseAnUnnamedOrUnknownDialect pins that a caller who cannot
// read flag documentation is told, rather than answered about a database it did
// not ask about.
//
// On the command line a missing dialect is a usage error a person reads. Here
// the caller is a model, and a guess silently resolved to PostgreSQL would
// produce a confident answer about the wrong target.
func TestOperations_RefuseAnUnnamedOrUnknownDialect(t *testing.T) {
	rows := []dialectRow{
		{name: "empty is refused", dialect: "", wantErr: true},
		{name: "unknown is refused by name", dialect: "orackle", wantErr: true},
		{name: "a known dialect is accepted", dialect: "postgres", wantErr: false},
		{name: "an alias resolves", dialect: "postgresql", wantErr: false},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			source := writeSchema(c, bookshop)

			_, err := agentapi.RenderSchema(context.Background(),
				agentapi.RenderSchemaRequest{Source: source, Dialect: row.dialect})

			c.Assert(err != nil, qt.Equals, row.wantErr, qt.Commentf("dialect %q", row.dialect))
		})
	}
}

// TestValidateSchema_ReportsSoundnessRatherThanSilence pins that a valid schema
// says so explicitly.
//
// The response carries Valid beside the problem list because a caller that
// mishandles an empty list must not read silence as success.
func TestValidateSchema_ReportsSoundnessRatherThanSilence(t *testing.T) {
	c := qt.New(t)
	source := writeSchema(c, bookshop)

	response, err := agentapi.ValidateSchema(context.Background(),
		agentapi.ValidateSchemaRequest{Source: source, Dialect: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(response.Valid, qt.IsTrue)
	c.Assert(response.Problems, qt.HasLen, 0)
	c.Assert(response.Dialect, qt.Equals, "postgres")
}

// TestValidateSchema_TurnsAnUnreadableSourceIntoAnAnswer pins that a source
// which will not load is a validation result rather than a transport failure.
//
// The caller asked whether the schema is sound. "It does not parse for this
// target" answers that, and answering it as a protocol error would make the
// server look broken instead.
func TestValidateSchema_TurnsAnUnreadableSourceIntoAnAnswer(t *testing.T) {
	c := qt.New(t)

	response, err := agentapi.ValidateSchema(context.Background(),
		agentapi.ValidateSchemaRequest{
			Source:  agentapi.SchemaSource{RootDirs: []string{filepath.Join(c.TempDir(), "absent")}},
			Dialect: "postgres",
		})

	c.Assert(err, qt.IsNil)
	c.Assert(response.Valid, qt.IsFalse)
	c.Assert(response.Problems, qt.HasLen, 1)
	c.Assert(response.Problems[0].Kind, qt.Equals, "source")
}

// TestOperations_RefuseAnEmptySource pins that naming no source is a caller
// error rather than an empty schema, which would otherwise validate cleanly and
// render nothing.
func TestOperations_RefuseAnEmptySource(t *testing.T) {
	c := qt.New(t)

	_, err := agentapi.RenderSchema(context.Background(),
		agentapi.RenderSchemaRequest{Dialect: "postgres"})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "no schema source")
}

// TestRenderSchema_ReturnsTheStatementsInOrder pins that the DDL comes back as
// statements rather than as one blob, and that the table precedes the view that
// reads it.
func TestRenderSchema_ReturnsTheStatementsInOrder(t *testing.T) {
	c := qt.New(t)
	source := writeSchema(c, bookshop)

	response, err := agentapi.RenderSchema(context.Background(),
		agentapi.RenderSchemaRequest{Source: source, Dialect: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(len(response.Statements) >= 2, qt.IsTrue)
	c.Assert(indexOfContaining(response.Statements, "CREATE TABLE") <
		indexOfContaining(response.Statements, "CREATE VIEW"), qt.IsTrue,
		qt.Commentf("a view must not be created before the table it reads"))
}

// TestSchemaLineage_CarriesBothHalves pins that the response always has both
// lists, so a caller reading only edges cannot mistake "nothing resolved" for
// "nothing depends on anything".
func TestSchemaLineage_CarriesBothHalves(t *testing.T) {
	c := qt.New(t)
	source := writeSchema(c, bookshop)

	response, err := agentapi.SchemaLineage(context.Background(),
		agentapi.SchemaLineageRequest{Source: source, Dialect: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(response.Edges, qt.HasLen, 1)
	c.Assert(response.Edges[0].FromTable, qt.Equals, "authors")
	c.Assert(response.Edges[0].ToView, qt.Equals, "author_names")
	c.Assert(response.Undecided, qt.IsNotNil)
}

// TestReadDatabase_RefusesWithoutAURL pins the one argument it cannot default.
func TestReadDatabase_RefusesWithoutAURL(t *testing.T) {
	c := qt.New(t)

	_, err := agentapi.ReadDatabase(context.Background(), agentapi.ReadDatabaseRequest{})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "database_url is required")
}

func indexOfContaining(statements []string, needle string) int {
	for i, statement := range statements {
		if containsFold(statement, needle) {
			return i
		}
	}
	return -1
}

func containsFold(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
