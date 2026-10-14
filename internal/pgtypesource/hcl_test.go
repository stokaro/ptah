package pgtypesource_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
)

// owned is a document holding a table and the type owner's objects. The
// table is what the types are placed before.
func owned(objects ...schemaext.Object) *schemamodel.Database {
	return &schemamodel.Database{
		Tables:         []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields:         []schemamodel.Field{{StructName: "T", Name: "id", Type: "INTEGER"}},
		FeatureObjects: must.Must(schemaext.NewObjects(objects...)),
	}
}

// declared is the type owner's object of kind named schema.name.
func declared(kind schemaext.Kind, schema, name string, value schemaext.Value) schemaext.Object {
	return must.Must(pgtype.DesiredObject(pgtype.Ref(kind, schema, name), value))
}

// TestHCL_WritesTypesAsTheCommonModelDoes renders each type twice through the
// runtime a command builds, once from the common model and once as the type
// owner's object, and gets the same bytes: every attribute of each kind, the
// space the common model trims, a default schema,
// and the order of several domains and composites in one document.
func TestHCL_WritesTypesAsTheCommonModelDoes(t *testing.T) {
	tests := []struct {
		name       string
		domains    []schemamodel.Domain
		composites []schemamodel.CompositeType
		objects    []schemaext.Object
		opening    string
	}{
		{name: "a domain with every attribute",
			domains: []schemamodel.Domain{{Schema: "app", Name: "email", BaseType: " text ", NotNull: true, DefaultExpr: "lower('A')",
				Check: " VALUE <> '' ", Comment: " mail "}},
			objects: []schemaext.Object{declared(pgtype.DomainKind, "app", "email", &pgtype.DesiredDomain{BaseType: " text ", NotNull: true,
				DefaultExpr: "lower('A')", Check: " VALUE <> '' ", Comment: " mail "})},
			opening: `domain "email" {`},
		{name: "a domain with a literal default in the default schema",
			domains: []schemamodel.Domain{{Name: "code", BaseType: "varchar(8)", Default: "x \"y\""}},
			objects: []schemaext.Object{declared(pgtype.DomainKind, "", "code", &pgtype.DesiredDomain{BaseType: "varchar(8)", Default: "x \"y\""})},
			opening: `domain "code" {`},
		{name: "a domain that states both defaults",
			domains: []schemamodel.Domain{{Name: "d", BaseType: "int4", Default: "1", DefaultExpr: "2"}},
			objects: []schemaext.Object{declared(pgtype.DomainKind, "", "d", &pgtype.DesiredDomain{BaseType: "int4", Default: "1", DefaultExpr: "2"})},
			opening: `domain "d" {`},
		{name: "a composite type",
			composites: []schemamodel.CompositeType{{Schema: "app", Name: "addr", Comment: " where ",
				Fields: []schemamodel.CompositeField{{Name: "zip", Type: "character varying(10)"}, {Name: "street", Type: "text"}}}},
			objects: []schemaext.Object{declared(pgtype.CompositeKind, "app", "addr", &pgtype.DesiredComposite{Comment: " where ",
				Attributes: []pgtype.Attribute{{Name: "zip", Type: "character varying(10)"}, {Name: "street", Type: "text"}}})},
			opening: `composite "addr" {`},
		{name: "several domains and composites",
			domains: []schemamodel.Domain{{Schema: "app", Name: "b", BaseType: "int4"}, {Name: "a", BaseType: "int4"}},
			composites: []schemamodel.CompositeType{{Schema: "app", Name: "c", Fields: []schemamodel.CompositeField{{Name: "f", Type: "int4"}}},
				{Schema: "other", Name: "a", Fields: []schemamodel.CompositeField{{Name: "f", Type: "int4"}}}},
			objects: []schemaext.Object{
				declared(pgtype.DomainKind, "app", "b", &pgtype.DesiredDomain{BaseType: "int4"}),
				declared(pgtype.CompositeKind, "other", "a", &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "f", Type: "int4"}}}),
				declared(pgtype.DomainKind, "", "a", &pgtype.DesiredDomain{BaseType: "int4"}),
				declared(pgtype.CompositeKind, "app", "c", &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "f", Type: "int4"}}}),
			},
			opening: `composite "c" {`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			common := owned()
			common.Domains, common.CompositeTypes = test.domains, test.composites
			options := atlashclrender.Options{HCL: builtintest.Runtime().HCL()}

			fromCommon, commonErr := atlashclrender.Render(common, options)
			fromOwner, ownerErr := atlashclrender.Render(owned(test.objects...), options)

			c.Assert(commonErr, qt.IsNil)
			c.Assert(ownerErr, qt.IsNil)
			c.Assert(string(fromCommon.Data), qt.Contains, test.opening+"\n")
			c.Assert(strings.Index(string(fromCommon.Data), test.opening) < strings.Index(string(fromCommon.Data), `table "t" {`), qt.IsTrue)
			c.Assert(string(fromOwner.Data), qt.Equals, string(fromCommon.Data))
			c.Assert(fromOwner.Diagnostics, qt.DeepEquals, fromCommon.Diagnostics)
		})
	}
}

// inTable is ref named inside table t, which no type is.
func inTable(ref objectidentity.ID) objectidentity.ID {
	ref.Parent = objectidentity.Part{Source: "t", Normalized: "t"}
	return ref
}

// TestHCL_RefusesATypeItCannotWrite refuses an observation, which a writer
// is handed only after a conversion, and a declaration that is not valid,
// rather than writing a block the reader would read as another type.
func TestHCL_RefusesATypeItCannotWrite(t *testing.T) {
	tests := []struct {
		name    string
		object  schemaext.Object
		wantErr string
	}{
		{name: "an observation", object: schemaext.Object{Ref: pgtype.Ref(pgtype.DomainKind, "", "d"), Value: &pgtype.ObservedDomain{BaseType: "int4"}},
			wantErr: `invalid feature value: HCL writes declared PostgreSQL types, not \*pgtype.ObservedDomain`},
		{name: "a domain without a base type", object: schemaext.Object{Ref: pgtype.Ref(pgtype.DomainKind, "", "d"), Value: &pgtype.DesiredDomain{}},
			wantErr: `.*a domain requires a base type`},
		{name: "a composite type without attributes", object: schemaext.Object{Ref: pgtype.Ref(pgtype.CompositeKind, "", "c"),
			Value: &pgtype.DesiredComposite{}},
			wantErr: `.*a composite type requires at least one attribute`},
		{name: "a range type without a subtype", object: schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "", "r"), Value: &pgtype.DesiredRange{}},
			wantErr: `.*a range type requires a subtype`},
		{name: "a type named inside a table", object: schemaext.Object{Ref: inTable(pgtype.Ref(pgtype.DomainKind, "", "d")),
			Value: &pgtype.DesiredDomain{BaseType: "int4"}},
			wantErr: `invalid feature value: a domain requires a schema-scoped identity with a name`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			document := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(test.object))}

			result, err := atlashclrender.Render(document, atlashclrender.Options{HCL: builtintest.Runtime().HCL()})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result.Data, qt.IsNil)
		})
	}
}
