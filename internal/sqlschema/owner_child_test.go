package sqlschema_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/engine"
	"ptah.run/internal/sqlschema"
)

const (
	markKind  schemaext.Kind = "example.org/marks/mark"
	stampKind schemaext.Kind = "example.org/marks/stamp"
)

// mark is a child object of the owner example.org/marks: declared by `CREATE
// MARK name ON table` and given a note by `NOTE ON MARK name ON table 'text'`.
type mark struct {
	Note string `json:"note,omitempty"`
}

func (*mark) Kind() schemaext.Kind { return markKind }

func (m *mark) Clone() schemaext.Value { return new(*m) }

func (m *mark) Equal(other schemaext.Value) bool {
	o, ok := other.(*mark)
	return ok && *o == *m
}

// stamp is the owner's facet of a table, set by `STAMP TABLE table 'text'`.
type stamp struct {
	Text string `json:"text"`
}

func (*stamp) Kind() schemaext.Kind { return stampKind }

func (s *stamp) Clone() schemaext.Value { return new(*s) }

func (s *stamp) Equal(other schemaext.Value) bool {
	o, ok := other.(*stamp)
	return ok && *o == *s
}

// stampStatement reports the table change that its facets describe.
type stampStatement struct{ Name string }

func (*stampStatement) Kind() schemaext.Kind { return "example.org/marks/stamp-statement" }

func (s *stampStatement) CloneExtension() ast.ExtensionPayload { return new(*s) }

func (s *stampStatement) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: ast.ExtensionModify, Name: s.Name}
}

// markRef is the identity of the mark name on table as written, which may be
// qualified.
func markRef(table, name string) objectidentity.ID {
	schema, bare, qualified := strings.Cut(table, ".")
	if !qualified {
		schema, bare = "", table
	}
	owner := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TablePartsVerbatim(schema, bare)
	return objectidentity.ID{Kind: objectidentity.Kind(markKind), Schema: owner.Schema, Parent: owner.Name,
		Name: objectidentity.Part{Source: name, Normalized: name}}
}

// markParser reads the owner's three statements.
type markParser struct{}

func (markParser) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		words := strings.Fields(strings.TrimSuffix(claim.Text, ";"))
		value := sqlext.Value{Statement: &widgetStatement{Text: claim.Text}}
		switch claim.Pattern {
		case "create":
			value.Objects = []schemaext.Object{{Ref: markRef(words[4], words[2]), Value: &mark{}}}
		case "note":
			value.Replaces = []schemaext.Object{{Ref: markRef(words[5], words[3]), Value: &mark{Note: strings.Trim(words[6], "'")}}}
		default:
			value.Statement = &stampStatement{Name: words[2]}
			value.TableFacets = &sqlext.TableFacets{Name: words[2],
				Facets: must.Must(schemaext.NewFacets(&stamp{Text: strings.Trim(words[3], "'")}))}
		}
		values = append(values, value)
	}
	return sqlext.Result{Values: values}, nil
}

// markRuntime registers target "postgres" alone, with the mark owner's
// statements, so nothing but the shared grammar and this owner reads.
func markRuntime() *engine.Runtime { return markRuntimeWithCoverage(nil) }

// markRuntimeWithCoverage adds the owner's claim to its statement reader.
func markRuntimeWithCoverage(claim func(sqlext.CoverageRequest) (schemaext.Coverage, error)) *engine.Runtime {
	codec := func(prototype schemaext.Value) schemaext.Codec {
		return schemaext.Codec{Prototype: prototype, Representation: schemaext.Desired, Version: 1,
			Definition: json.RawMessage(`{"type":"object"}`),
			Clone:      func(value schemaext.Payload) (schemaext.Payload, error) { return value.(schemaext.Value).Clone(), nil },
			Encode:     func(value schemaext.Payload) (json.RawMessage, error) { return json.Marshal(value) },
			Canonical:  func(value schemaext.Payload) (json.RawMessage, error) { return json.Marshal(value) },
			Decode: func(data json.RawMessage) (schemaext.Payload, error) {
				value := prototype.Clone()
				return value, json.Unmarshal(data, value)
			},
		}
	}
	return must.Must(engine.New(engine.Provider{
		ID:      "example.org/marks",
		Targets: []engine.Target{{Name: "postgres"}},
		Codecs:  []schemaext.Codec{codec(&mark{}), codec(&stamp{})},
		SQL: []sqlext.Extension{{
			Owner:   "example.org/marks",
			Targets: []string{"postgres"},
			Kinds:   []schemaext.Kind{markKind},
			Facets:  []schemaext.Kind{stampKind},
			Patterns: []sqlext.Pattern{
				{Name: "create", Site: sqlext.Statement, Words: []string{"CREATE", "MARK"}},
				{Name: "note", Site: sqlext.Statement, Words: []string{"NOTE", "ON", "MARK"}},
				{Name: "stamp", Site: sqlext.Statement, Words: []string{"STAMP", "TABLE"}},
			},
			Parser:   markParser{},
			Coverage: claim,
		}},
	}))
}

// readMarks reads source as a PostgreSQL document with the mark owner.
func readMarks(c *qt.C, source string) (schemamodel.Database, error) {
	c.Helper()
	runtime := markRuntime()
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(source), "postgres", nil,
		sqlschema.Options{Owners: runtime.SQL(), Relations: runtime})
	return database, err
}

// TestReadWith_KeysAChildByItsDeclaredTable pins that a child object takes
// the identity of the table the schema declares under the name its statement
// writes, a declaration and an amendment alike: `public.orders` names the
// table declared as `orders`, and the child's identity is that table's. A
// child of a table the schema does not declare, or of a table of the same
// name in another schema, stays as written.
func TestReadWith_KeysAChildByItsDeclaredTable(t *testing.T) {
	c := qt.New(t)

	database, err := readMarks(c, "CREATE TABLE orders (id int);\nCREATE MARK m ON public.orders;\n"+
		"CREATE MARK k ON orders;\nNOTE ON MARK k ON public.orders 'checked';\n"+
		"CREATE MARK n ON elsewhere;\nCREATE MARK p ON other.orders;\n")

	c.Assert(err, qt.IsNil)
	all := must.Must(database.FeatureObjects.All())
	refs := make([]objectidentity.ID, 0, len(all))
	for _, object := range all {
		refs = append(refs, object.Ref)
	}
	c.Assert(refs, qt.ContentEquals, []objectidentity.ID{
		markRef("orders", "m"), markRef("orders", "k"), markRef("elsewhere", "n"), markRef("other.orders", "p"),
	})
	noted, found, err := database.FeatureObjects.Get(markRef("orders", "k"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(noted.Value, qt.DeepEquals, schemaext.Value(&mark{Note: "checked"}))
}

// TestReadWith_SetsAnOwnersTableFacets pins that a statement's table facets
// are merged into the table the schema declares, however the statement
// writes its name.
func TestReadWith_SetsAnOwnersTableFacets(t *testing.T) {
	c := qt.New(t)

	database, err := readMarks(c, "CREATE TABLE app.orders (id int);\nCREATE TABLE app.lines (id int);\nSTAMP TABLE app.orders 'kept';\n")

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 2)
	c.Assert(database.Tables[0].Name, qt.Equals, "lines")
	c.Assert(database.Tables[0].Facets.Len(), qt.Equals, 0)
	stamped, found, err := schemaext.FacetAs[*stamp](database.Tables[1].Facets, stampKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(stamped.Text, qt.Equals, "kept")
}

// TestReadWith_ResolvesOwnerChangesAcrossDocuments applies child amendments
// and table facets to the declarations an earlier file contributes.
func TestReadWith_ResolvesOwnerChangesAcrossDocuments(t *testing.T) {
	c := qt.New(t)
	runtime := markRuntime()
	opts := sqlschema.Options{Owners: runtime.SQL(), Relations: runtime}
	earlier, err := readMarks(c, "CREATE TABLE orders (id int); CREATE MARK m ON orders;")
	c.Assert(err, qt.IsNil)
	document := sqlschema.NewDocument(&earlier)

	later, _, err := sqlschema.ReadWith(c.Context(), []byte("NOTE ON MARK m ON public.orders 'checked'; STAMP TABLE public.orders 'kept';"),
		"postgres", document, opts,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(later.Tables, qt.HasLen, 0)
	c.Assert(later.FeatureObjects.Len(), qt.Equals, 0)
	noted, found, err := earlier.FeatureObjects.Get(markRef("orders", "m"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(noted.Value, qt.DeepEquals, schemaext.Value(&mark{Note: "checked"}))
	stamped, found, err := schemaext.FacetAs[*stamp](earlier.Tables[0].Facets, stampKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(stamped.Text, qt.Equals, "kept")
}

// TestReadWith_TableFacets_FailurePath pins that table facets for a table the
// schema does not declare are refused as an undeclared object's are, and a
// second facet of a kind the table holds is refused.
func TestReadWith_TableFacets_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		wantIs  error
		wantErr string
	}{
		{name: "an undeclared table", source: "STAMP TABLE orders 'kept';\n", wantIs: sqlext.ErrUndeclared,
			wantErr: `STAMP TABLE orders 'kept': table "orders" is not declared in this schema`},
		{name: "a facet the table holds", source: "CREATE TABLE orders (id int);\nSTAMP TABLE orders 'a';\nSTAMP TABLE orders 'b';\n",
			wantIs: schemaext.ErrDuplicate, wantErr: `STAMP TABLE orders 'b': .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := readMarks(c, test.source)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}

// TestReadWith_DropTableRemovesItsChildren pins that a DROP TABLE removes the
// table's child objects and coverage, including table facets and child
// namespaces, and keeps another table's objects and coverage.
func TestReadWith_DropTableRemovesItsChildren(t *testing.T) {
	c := qt.New(t)

	database, err := readMarks(c, "CREATE TABLE orders (id int);\nCREATE TABLE lines (id int);\n"+
		"CREATE MARK m ON orders;\nCREATE MARK n ON lines;\n")
	c.Assert(err, qt.IsNil)
	runtime := markRuntime()
	var kinds []schemaext.KindCoverage
	for _, model := range runtime.Codecs().Definitions() {
		kinds = append(kinds, schemaext.KindCoverage{Model: model, Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})})
	}
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	subjects := []schemaext.SubjectCoverage{
		{Kind: markKind, Subject: markRef("orders", "m"), Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
		{Kind: markKind, Subject: builder.TablePartsVerbatim("", "orders"), Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
		{Kind: stampKind, Subject: builder.TablePartsVerbatim("", "orders"), Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
		{Kind: markKind, Subject: markRef("lines", "n"), Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
		{Kind: markKind, Subject: builder.TablePartsVerbatim("", "lines"), Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
	}
	database.FeatureCoverage = must.Must(schemaext.NewCoverage(schemaext.Desired, kinds, subjects))

	_, _, err = sqlschema.ReadWith(c.Context(), []byte("DROP TABLE orders;"), "postgres", sqlschema.NewDocument(&database),
		sqlschema.Options{Owners: runtime.SQL(), Relations: runtime},
	)

	c.Assert(err, qt.IsNil)
	refs := database.FeatureObjects.Refs()
	c.Assert(refs, qt.HasLen, 1)
	c.Assert(refs[0].Name.Source, qt.Equals, "n")
	c.Assert(database.FeatureCoverage.SubjectRecords(), qt.ContentEquals, subjects[3:])
	c.Assert(database.FeatureCoverage.KindRecords(), qt.ContentEquals, kinds)
}

// TestReadWith_CoverageUsesTheAppliedDeclarations gives the coverage owner
// the final objects, with comments applied and dropped table children absent.
func TestReadWith_CoverageUsesTheAppliedDeclarations(t *testing.T) {
	c := qt.New(t)
	var declarations [][]schemaext.Object
	runtime := markRuntimeWithCoverage(func(request sqlext.CoverageRequest) (schemaext.Coverage, error) {
		all, err := request.Declared.All()
		declarations = append(declarations, all)
		return schemaext.Coverage{}, err
	})
	source := "CREATE TABLE orders (id int); CREATE MARK m ON orders; DROP TABLE orders; " +
		"CREATE TABLE lines (id int); CREATE MARK n ON public.lines; NOTE ON MARK n ON lines 'checked';"

	database, _, err := sqlschema.ReadWith(t.Context(), []byte(source), "postgres", nil,
		sqlschema.Options{Owners: runtime.SQL(), Relations: runtime})

	c.Assert(err, qt.IsNil)
	c.Assert(declarations, qt.DeepEquals, [][]schemaext.Object{{{Ref: markRef("lines", "n"), Value: &mark{Note: "checked"}}}})
	c.Assert(database.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{markRef("lines", "n")})
}
