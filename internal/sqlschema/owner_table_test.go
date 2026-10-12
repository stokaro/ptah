package sqlschema_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/engine"
	"ptah.run/internal/sqlschema"
)

// shelfParser reads `CREATE SHELF TABLE [IF NOT EXISTS] name`, a table its
// owner defines with no column list.
type shelfParser struct{}

func (shelfParser) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		words := strings.Fields(claim.Text)
		values = append(values, sqlext.Value{Table: &sqlext.TableDeclaration{
			Name: words[len(words)-1], IfNotExists: strings.Contains(strings.ToUpper(claim.Text), "IF NOT EXISTS"),
		}})
	}
	return sqlext.Result{Values: values}, nil
}

// shelfOwners registers target "shelves" with an owner whose statement
// declares a table.
func shelfOwners() sqlext.Set {
	return must.Must(engine.New(engine.Provider{
		ID:      "example.org/shelves",
		Targets: []engine.Target{{Name: "shelves"}},
		SQL: []sqlext.Extension{{
			Owner:    "example.org/shelves",
			Targets:  []string{"shelves"},
			Patterns: []sqlext.Pattern{{Name: "shelf", Site: sqlext.Statement, Words: []string{"CREATE", "SHELF", "TABLE"}}},
			Parser:   shelfParser{},
		}},
	})).SQL()
}

// TestReadWith_PlacesATableAnOwnersStatementDeclares reads a table an owner's
// statement declares as the CREATE TABLE every reader of a table reads,
// standing where the statement stood, with its guard, and declares it beside
// the tables of the shared grammar.
func TestReadWith_PlacesATableAnOwnersStatementDeclares(t *testing.T) {
	c := qt.New(t)
	source := "CREATE TABLE a (id INTEGER);\nCREATE SHELF TABLE IF NOT EXISTS b;\nCREATE TABLE c (id INTEGER);\n"

	database, statements, err := sqlschema.ReadWith(t.Context(), []byte(source), "shelves", nil, sqlschema.Options{Owners: shelfOwners()})

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 3)
	shelf, ok := statements.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", statements.Statements[1]))
	c.Assert(shelf.Name, qt.Equals, "b")
	c.Assert(shelf.IfNotExists, qt.IsTrue)
	c.Assert(shelf.Columns, qt.HasLen, 0)
	c.Assert(tableNames(database), qt.DeepEquals, []string{"a", "b", "c"})
}

// TestReadWith_WithoutTheTableOwnerRefusesItsSyntax is the control: the
// shared grammar has no CREATE SHELF TABLE.
func TestReadWith_WithoutTheTableOwnerRefusesItsSyntax(t *testing.T) {
	c := qt.New(t)

	database, statements, err := sqlschema.ReadWith(t.Context(), []byte("CREATE SHELF TABLE b;"), "shelves", nil,
		sqlschema.Options{Owners: sqlext.None()})

	c.Assert(err, qt.ErrorMatches, `unsupported CREATE target: SHELF at position 7`)
	c.Assert(database.Tables, qt.HasLen, 0)
	c.Assert(statements, qt.IsNil)
}

const labelKind schemaext.Kind = "example.org/shelves/label"

// label is the shelves owner's setting of a table, written after its column
// list as `LABELED '<text>'`.
type label struct {
	Text string
}

func (*label) Kind() schemaext.Kind { return labelKind }

func (l *label) Clone() schemaext.Value { return new(*l) }

func (l *label) Equal(other schemaext.Value) bool {
	o, ok := other.(*label)
	return ok && *o == *l
}

// labelParser reads `LABELED '<text>'` into the table's label facet.
type labelParser struct{}

func (labelParser) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		words := strings.Fields(claim.Text)
		facets, err := schemaext.Facets{}.With(&label{Text: strings.Trim(words[1], "'")})
		if err != nil {
			return sqlext.Result{}, err
		}
		values = append(values, sqlext.Value{Facets: facets})
	}
	return sqlext.Result{Values: values}, nil
}

// labelOwners registers target "shelves" with an owner whose table option
// labels a table.
func labelOwners() sqlext.Set {
	return must.Must(engine.New(engine.Provider{
		ID:      "example.org/shelves",
		Targets: []engine.Target{{Name: "shelves"}},
		SQL: []sqlext.Extension{{
			Owner:    "example.org/shelves",
			Targets:  []string{"shelves"},
			Facets:   []schemaext.Kind{labelKind},
			Patterns: []sqlext.Pattern{{Name: "label", Site: sqlext.TableOption, Words: []string{"LABELED"}}},
			Parser:   labelParser{},
		}},
	})).SQL()
}

// TestReadWith_AppliesAnOwnersTableOption reads an owner's option after a
// table's column list as the owner's facet of that table, beside the shared
// grammar's options, and ends the option's span at a comma.
func TestReadWith_AppliesAnOwnersTableOption(t *testing.T) {
	c := qt.New(t)
	source := "CREATE TABLE a (id INTEGER) LABELED 'archive', STRICT;\nCREATE TABLE b (id INTEGER);\n"

	_, statements, err := sqlschema.ReadWith(t.Context(), []byte(source), "shelves", nil, sqlschema.Options{Owners: labelOwners()})

	c.Assert(err, qt.IsNil)
	labeled := statements.Statements[0].(*ast.CreateTableNode)
	got, found, err := schemaext.FacetAs[*label](labeled.Facets, labelKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(got.Text, qt.Equals, "archive")
	c.Assert(labeled.Options["STRICT"], qt.Equals, "true")
	c.Assert(statements.Statements[1].(*ast.CreateTableNode).Facets.Len(), qt.Equals, 0)
}

// TestReadWith_WithoutTheOptionOwnerRefusesItsSyntax is the control: the
// shared grammar has no LABELED option.
func TestReadWith_WithoutTheOptionOwnerRefusesItsSyntax(t *testing.T) {
	c := qt.New(t)

	database, statements, err := sqlschema.ReadWith(t.Context(), []byte("CREATE TABLE a (id INTEGER) LABELED 'archive';"), "shelves", nil,
		sqlschema.Options{Owners: sqlext.None()})

	c.Assert(err, qt.ErrorMatches, `unsupported table option: LABELED at position 28`)
	c.Assert(database.Tables, qt.HasLen, 0)
	c.Assert(statements, qt.IsNil)
}

const tagKind schemaext.Kind = "example.org/shelves/tag"

// tag is the shelves owner's second setting of a column, written `TAGGED
// '<text>'`.
type tag struct {
	Text string
}

func (*tag) Kind() schemaext.Kind { return tagKind }

func (t *tag) Clone() schemaext.Value { return new(*t) }

func (t *tag) Equal(other schemaext.Value) bool {
	o, ok := other.(*tag)
	return ok && *o == *t
}

// columnLabelParser reads `LABELED '<text>'` into a label and `TAGGED
// '<text>'` into a tag of the column.
type columnLabelParser struct{}

func (columnLabelParser) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		words := strings.Fields(claim.Text)
		var value schemaext.Value = &label{Text: strings.Trim(words[1], "'")}
		if claim.Pattern == "tag" {
			value = &tag{Text: strings.Trim(words[1], "'")}
		}
		facets, err := schemaext.Facets{}.With(value)
		if err != nil {
			return sqlext.Result{}, err
		}
		values = append(values, sqlext.Value{Facets: facets})
	}
	return sqlext.Result{Values: values}, nil
}

// TestReadWith_AppliesAnOwnersColumnOptions reads an owner's options among a
// column's constraints as the owner's facets of that column, each one added
// beside the ones before it, and ends each option where the next clause of
// the column begins.
func TestReadWith_AppliesAnOwnersColumnOptions(t *testing.T) {
	c := qt.New(t)
	owners := must.Must(engine.New(engine.Provider{
		ID:      "example.org/shelves",
		Targets: []engine.Target{{Name: "shelves"}},
		SQL: []sqlext.Extension{{
			Owner:   "example.org/shelves",
			Targets: []string{"shelves"},
			Facets:  []schemaext.Kind{labelKind, tagKind},
			Patterns: []sqlext.Pattern{
				{Name: "label", Site: sqlext.ColumnOption, Words: []string{"LABELED"}},
				{Name: "tag", Site: sqlext.ColumnOption, Words: []string{"TAGGED"}},
			},
			Parser: columnLabelParser{},
		}},
	})).SQL()

	database, _, err := sqlschema.ReadWith(t.Context(), []byte("CREATE TABLE a (v int LABELED 'archive' NOT NULL TAGGED 'cold');"),
		"shelves", nil, sqlschema.Options{Owners: owners})

	c.Assert(err, qt.IsNil)
	c.Assert(database.Fields, qt.HasLen, 1)
	c.Assert(database.Fields[0].Nullable, qt.IsFalse)
	labeled, found, err := schemaext.FacetAs[*label](database.Fields[0].Facets, labelKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(labeled.Text, qt.Equals, "archive")
	tagged, found, err := schemaext.FacetAs[*tag](database.Fields[0].Facets, tagKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(tagged.Text, qt.Equals, "cold")
}
