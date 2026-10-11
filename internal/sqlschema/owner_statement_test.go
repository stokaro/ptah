package sqlschema_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/engine"
	"ptah.run/internal/sqlschema"
)

const widgetKind schemaext.Kind = "example.org/custom/widget"

// widget is an object of the owner example.org/custom, declared by
// `CREATE WIDGET name` and commented by `COMMENT ON WIDGET name IS 'text'`.
type widget struct {
	Comment string
}

func (*widget) Kind() schemaext.Kind { return widgetKind }

func (w *widget) Clone() schemaext.Value { return new(*w) }

func (w *widget) Equal(other schemaext.Value) bool {
	o, ok := other.(*widget)
	return ok && *o == *w
}

func widgetObject(name, comment string) schemaext.Object {
	return schemaext.Object{
		Ref:   objectidentity.ID{Kind: objectidentity.Kind(widgetKind), Name: objectidentity.Part{Source: name, Normalized: name}},
		Value: &widget{Comment: comment},
	}
}

// widgetStatement is the owner's payload of one of its statements.
type widgetStatement struct {
	Text string
}

func (*widgetStatement) Kind() schemaext.Kind { return "example.org/custom/widget-statement" }

func (s *widgetStatement) CloneExtension() ast.ExtensionPayload { return new(*s) }

// widgetParser reads the owner's statements and records the requests it is
// given. A careful parser refuses a comment on a widget neither the request's
// earlier claims nor its declared objects hold; a careless one leaves that to
// the reader.
type widgetParser struct {
	requests *[]sqlext.Request
	careless bool
}

func (p widgetParser) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	*p.requests = append(*p.requests, request)
	known := make(map[string]bool)
	for _, object := range request.Declared {
		known[object.Ref.Name.Source] = true
	}
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		words := strings.Fields(strings.TrimSuffix(claim.Text, ";"))
		value := sqlext.Value{Statement: &widgetStatement{Text: claim.Text}}
		if claim.Pattern == "create" {
			known[words[2]] = true
			value.Objects = []schemaext.Object{widgetObject(words[2], "")}
			values = append(values, value)
			continue
		}
		if !known[words[3]] && !p.careless {
			return sqlext.Result{}, fmt.Errorf("%w: COMMENT ON WIDGET %s names an object this schema does not declare", sqlext.ErrUndeclared, words[3])
		}
		value.Replaces = []schemaext.Object{widgetObject(words[3], strings.Trim(words[5], "'"))}
		values = append(values, value)
	}
	return sqlext.Result{Values: values}, nil
}

// widgetOwners registers target "custom" with the widget owner's statements.
// Neither pattern is targeted, so a document that names no target reads them
// too.
func widgetOwners(parser widgetParser) sqlext.Set {
	return must.Must(engine.New(engine.Provider{
		ID:      "example.org/custom",
		Targets: []engine.Target{{Name: "custom"}},
		SQL: []sqlext.Extension{{
			Owner:   "example.org/custom",
			Targets: []string{"custom"},
			Kinds:   []schemaext.Kind{widgetKind},
			Patterns: []sqlext.Pattern{
				{Name: "create", Site: sqlext.Statement, Words: []string{"CREATE", "WIDGET"}},
				{Name: "comment", Site: sqlext.Statement, Words: []string{"COMMENT", "ON", "WIDGET"}},
			},
			Parser: parser,
		}},
	})).SQL()
}

func widgetComments(c *qt.C, objects schemaext.Objects) map[string]string {
	c.Helper()
	all, err := objects.All()
	c.Assert(err, qt.IsNil)
	comments := make(map[string]string, len(all))
	for _, object := range all {
		comments[object.Ref.Name.Source] = object.Value.(*widget).Comment
	}
	return comments
}

// TestReadWith_AppliesAnOwnersStatements reads the owner's statements through
// the runtime in one call for the document. Each stands where it stood as the
// owner's statement, a CREATE declares the owner's object, and a later
// COMMENT ON amends it.
func TestReadWith_AppliesAnOwnersStatements(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request
	source := "CREATE WIDGET w;\nCOMMENT ON WIDGET w IS 'note';\nCREATE TABLE t (id INTEGER);\n"

	database, statements, err := sqlschema.ReadWith(t.Context(), []byte(source), "custom", nil,
		sqlschema.Options{Owners: widgetOwners(widgetParser{requests: &requests})})

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].Claims, qt.HasLen, 2)
	c.Assert(requests[0].Claims[1].Text, qt.Equals, "COMMENT ON WIDGET w IS 'note'")
	c.Assert(requests[0].Declared, qt.HasLen, 0)
	c.Assert(widgetComments(c, database.FeatureObjects), qt.DeepEquals, map[string]string{"w": "note"})
	c.Assert(statements.Statements, qt.HasLen, 3)
	c.Assert(statements.Statements[0], qt.DeepEquals, ast.Node(&ast.ExtensionStatement{Payload: &widgetStatement{Text: "CREATE WIDGET w"}}))
	c.Assert(tableNames(database), qt.DeepEquals, []string{"t"})
}

// TestReadWith_AmendsWhatAnEarlierDocumentDeclared hands the owner the objects
// of its kinds an earlier document of a schema directory declared, and makes
// the amendment to the earlier document's object, in place.
func TestReadWith_AmendsWhatAnEarlierDocumentDeclared(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request
	opts := sqlschema.Options{Owners: widgetOwners(widgetParser{requests: &requests})}
	merged := &schemamodel.Database{}
	document := sqlschema.NewDocument(merged)
	first, _, err := sqlschema.ReadWith(t.Context(), []byte("CREATE WIDGET w;"), "custom", document, opts)
	c.Assert(err, qt.IsNil)
	merged.FeatureObjects = must.Must(merged.FeatureObjects.Merge(first.FeatureObjects))

	second, _, err := sqlschema.ReadWith(t.Context(), []byte("COMMENT ON WIDGET w IS 'note';"), "custom", document, opts)

	c.Assert(err, qt.IsNil)
	c.Assert(second.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(widgetComments(c, merged.FeatureObjects), qt.DeepEquals, map[string]string{"w": "note"})
	c.Assert(requests, qt.HasLen, 2)
	c.Assert(requests[1].Declared, qt.HasLen, 1)
	c.Assert(requests[1].Declared[0].Ref.Name.Source, qt.Equals, "w")
}

// TestReadWith_ReadsAnOwnersStatementOnADocumentThatNamesNoTarget consults an
// untargeted statement pattern on a document that names no target.
func TestReadWith_ReadsAnOwnersStatementOnADocumentThatNamesNoTarget(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request

	database, _, err := sqlschema.ReadWith(t.Context(), []byte("CREATE WIDGET w;"), "", nil,
		sqlschema.Options{Owners: widgetOwners(widgetParser{requests: &requests})})

	c.Assert(err, qt.IsNil)
	c.Assert(widgetComments(c, database.FeatureObjects), qt.DeepEquals, map[string]string{"w": ""})
	c.Assert(requests[0].Target, qt.Equals, "")
}

// TestReadWith_WithoutTheStatementOwnerRefusesItsSyntax is the control: the
// shared grammar has no CREATE WIDGET.
func TestReadWith_WithoutTheStatementOwnerRefusesItsSyntax(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.ReadWith(t.Context(), []byte("CREATE WIDGET w;"), "custom", nil,
		sqlschema.Options{Owners: sqlext.None()})

	c.Assert(err, qt.ErrorMatches, `unsupported CREATE target: WIDGET at position 7`)
	c.Assert(database.FeatureObjects.Len(), qt.Equals, 0)
}

// TestReadWith_RefusesAnAmendmentOfNothingDeclared reports the owner's refusal
// of a comment on a widget nothing declared, and the reader's own refusal when
// an owner amends one anyway. Both are marked as a change to an undeclared
// object.
func TestReadWith_RefusesAnAmendmentOfNothingDeclared(t *testing.T) {
	tests := []struct {
		name     string
		careless bool
		wantErr  string
	}{
		{name: "the owner refuses", careless: false,
			wantErr: `the statement changes an object its load does not declare: COMMENT ON WIDGET w names an object this schema does not declare`},
		{name: "the reader refuses", careless: true,
			wantErr: `COMMENT ON WIDGET w IS 'note': widget "w" is not declared in this schema`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []sqlext.Request

			database, _, err := sqlschema.ReadWith(t.Context(), []byte("COMMENT ON WIDGET w IS 'note';"), "custom", nil,
				sqlschema.Options{Owners: widgetOwners(widgetParser{requests: &requests, careless: test.careless})})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, sqlext.ErrUndeclared)
			c.Assert(database.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}
