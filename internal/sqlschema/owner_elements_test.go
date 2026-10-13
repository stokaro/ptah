package sqlschema_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/internal/sqlschema"
)

// childWidgetParser claims named table elements and ALTER additions in one
// document batch. Its optional parent override exercises host scope validation.
type childWidgetParser struct {
	requests *[]sqlext.Request
	parent   string
}

func (p childWidgetParser) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	*p.requests = append(*p.requests, request)
	result := sqlext.Result{}
	for _, claim := range request.Claims {
		words := strings.Fields(strings.TrimPrefix(claim.Text, "ADD "))
		parent := claim.Subject
		if p.parent != "" {
			parent = p.parent
		}
		ref := objectidentity.NewBuilder(identifier.ForDialect(request.Target)).Constraint(parent, words[1])
		ref.Kind = objectidentity.Kind(widgetKind)
		value := sqlext.Value{Objects: []schemaext.Object{{Ref: ref, Value: &widget{Comment: claim.Text}}}}
		if claim.Site == sqlext.AlterOperation {
			value.Operation = &widgetStatement{Text: claim.Text}
		}
		result.Values = append(result.Values, value)
	}
	return result, nil
}

func childWidgetOwners(parser childWidgetParser) sqlext.Set {
	return must.Must(sqlext.NewSet(sqlext.Extension{Owner: "example.org/custom", Targets: []string{"custom", "postgres"}, Kinds: []schemaext.Kind{widgetKind}, Parser: parser,
		Patterns: []sqlext.Pattern{
			{Name: "element", Site: sqlext.TableElement, Words: []string{"CONSTRAINT"}, AfterName: []string{"WIDGET"}},
			{Name: "add", Site: sqlext.AlterOperation, Words: []string{"ADD", "CONSTRAINT"}, AfterName: []string{"WIDGET"}},
		}}))
}

func TestReadWith_NamedChildElements(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request
	source := `CREATE TABLE "events.2026" (id INTEGER, CONSTRAINT z WIDGET (id), day INTEGER, CONSTRAINT a WIDGET (day), CONSTRAINT positive CHECK (id > 0));
 ALTER TABLE "events.2026" ADD CONSTRAINT later WIDGET (id, day), ADD COLUMN title TEXT;
 CREATE TABLE other (id INTEGER, CONSTRAINT z WIDGET (id));`
	database, statements, err := sqlschema.ReadWith(t.Context(), []byte(source), "custom", nil, sqlschema.Options{Owners: childWidgetOwners(childWidgetParser{requests: &requests})})
	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].Claims, qt.HasLen, 4)
	c.Assert(database.FeatureObjects.Len(), qt.Equals, 4)
	c.Assert(database.Fields, qt.HasLen, 4)
	c.Assert(database.Indexes, qt.HasLen, 0)
	c.Assert(database.Constraints, qt.HasLen, 1)
	table, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.OwnedObjects.Len(), qt.Equals, 2)
	c.Assert(table.Indexes, qt.HasLen, 0)
	c.Assert(table.Elements, qt.HasLen, 5)
	c.Assert(table.Elements[1].Objects[0].Name.Source, qt.Equals, "z")
	c.Assert(table.Elements[3].Objects[0].Name.Source, qt.Equals, "a")
	c.Assert(table.Elements[1].Objects[0].Parent.Source, qt.Equals, `"events.2026"`)
	c.Assert(table.Elements[4].Constraint.Name, qt.Equals, "positive")
	c.Assert(table.Elements[1].Index, qt.IsNil)
}

func TestReadWith_ChildOfEarlierTable(t *testing.T) {
	c := qt.New(t)
	owners := childWidgetOwners(childWidgetParser{requests: new([]sqlext.Request)})
	base, _, err := sqlschema.ReadWith(t.Context(), []byte("CREATE TABLE t (id INTEGER);"), "custom", nil, sqlschema.Options{Owners: owners})
	c.Assert(err, qt.IsNil)
	result, _, err := sqlschema.ReadWith(t.Context(), []byte("ALTER TABLE t ADD CONSTRAINT g WIDGET (id);"), "custom", sqlschema.NewDocument(&base), sqlschema.Options{Owners: owners})
	c.Assert(err, qt.IsNil)
	c.Assert(result.FeatureObjects.Len(), qt.Equals, 1)
	c.Assert(base.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(result.Tables, qt.HasLen, 0)
}

func TestReadWith_DropTableRemovesInlineChildren(t *testing.T) {
	c := qt.New(t)
	owners := childWidgetOwners(childWidgetParser{requests: new([]sqlext.Request)})
	source := `CREATE TABLE t (id INTEGER, CONSTRAINT g WIDGET (id)); DROP TABLE t; CREATE TABLE t (id INTEGER, CONSTRAINT g WIDGET (id));`
	result, _, err := sqlschema.ReadWith(t.Context(), []byte(source), "custom", nil, sqlschema.Options{Owners: owners})
	c.Assert(err, qt.IsNil)
	c.Assert(result.FeatureObjects.Len(), qt.Equals, 1)
	c.Assert(result.Tables, qt.HasLen, 1)
}

func TestReadWith_ChildScopeFailure(t *testing.T) {
	tests := []struct{ name, source, parent string }{
		{name: "create", source: `CREATE TABLE other (id INTEGER); CREATE TABLE t (id INTEGER, CONSTRAINT g WIDGET (id));`, parent: "other"},
		{name: "alter", source: `CREATE TABLE other (id INTEGER); CREATE TABLE t (id INTEGER); ALTER TABLE t ADD CONSTRAINT g WIDGET (id);`, parent: "other"},
		{name: "missing parent", source: `CREATE TABLE t (id INTEGER, CONSTRAINT g WIDGET (id));`, parent: "absent"},
		{name: "other schema", source: `CREATE TABLE app.t (id INTEGER); CREATE TABLE t (id INTEGER, CONSTRAINT g WIDGET (id));`, parent: "app.t"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owners := childWidgetOwners(childWidgetParser{requests: new([]sqlext.Request), parent: test.parent})
			_, _, err := sqlschema.ReadWith(t.Context(), []byte(test.source), "custom", nil, sqlschema.Options{Owners: owners})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, ".*owned element .* is not a child of table t")
		})
	}
}

func TestReadWith_DuplicateChildFailure(t *testing.T) {
	tests := []struct{ name, source string }{
		{name: "create", source: `CREATE TABLE t (id INTEGER, CONSTRAINT g WIDGET (id), CONSTRAINT g WIDGET (id));`},
		{name: "alter", source: `CREATE TABLE t (id INTEGER, CONSTRAINT g WIDGET (id)); ALTER TABLE t ADD CONSTRAINT g WIDGET (id);`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owners := childWidgetOwners(childWidgetParser{requests: new([]sqlext.Request)})
			_, _, err := sqlschema.ReadWith(t.Context(), []byte(test.source), "custom", nil, sqlschema.Options{Owners: owners})
			c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
		})
	}
}

func TestReadWith_DuplicateChildFromEarlierFileFailure(t *testing.T) {
	c := qt.New(t)
	owners := childWidgetOwners(childWidgetParser{requests: new([]sqlext.Request)})
	base, _, err := sqlschema.ReadWith(t.Context(), []byte("CREATE TABLE t (id INTEGER, CONSTRAINT g WIDGET (id));"), "custom", nil, sqlschema.Options{Owners: owners})
	c.Assert(err, qt.IsNil)
	_, _, err = sqlschema.ReadWith(t.Context(), []byte("ALTER TABLE t ADD CONSTRAINT g WIDGET (id);"), "custom", sqlschema.NewDocument(&base), sqlschema.Options{Owners: owners})
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(base.FeatureObjects.Len(), qt.Equals, 1)
}

func TestReadWith_ChildDefaultSchema(t *testing.T) {
	c := qt.New(t)
	owners := childWidgetOwners(childWidgetParser{requests: new([]sqlext.Request), parent: "public.t"})
	result, _, err := sqlschema.ReadWith(t.Context(), []byte("CREATE TABLE t (id INTEGER, CONSTRAINT g WIDGET (id));"), "postgres", nil, sqlschema.Options{Owners: owners})
	c.Assert(err, qt.IsNil)
	c.Assert(result.FeatureObjects.Len(), qt.Equals, 1)
	c.Assert(result.FeatureObjects.Refs()[0].Schema.Normalized, qt.Equals, "public")
	c.Assert(result.FeatureObjects.Refs()[0].Schema.Defaulted, qt.IsTrue)
}
