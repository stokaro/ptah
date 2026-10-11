package sqldecl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/definedtypetest"
	"ptah.run/internal/sqldecl"
)

// TestOf_ListsAnOwnersTypeAsAType pins that a data type an owner's object
// defines is listed as a type, where an enum and a common domain are, so a
// later file declaring an enum of its name collides with it as the engine
// collides them. It is not listed again under its owner's noun. A type
// declared without a schema is listed by its bare name, as an enum declared
// without one is, so the two collide. An owner's object that defines no type
// keeps its owner's noun.
func TestOf_ListsAnOwnersTypeAsAType(t *testing.T) {
	c := qt.New(t)
	extension := schemaext.Object{Ref: objectidentity.ID{Kind: "example.org/owner/extension", Name: part("pg_trgm")},
		Value: &plainValue{}}
	db := &schemamodel.Database{
		Enums: []schemamodel.Enum{{Name: "status"}},
		FeatureObjects: must.Must(schemaext.NewObjects(definedtypetest.Object(objectidentity.KindDomain, "app", "zip", "text"),
			definedtypetest.Object(objectidentity.KindComposite, "", "pair", "int"), extension)),
	}

	declared := sqldecl.Of("one.sql", db, &ast.StatementList{})

	c.Assert(declared.Objects, qt.DeepEquals, []sqlext.Declaration{
		{Kind: "extension", Name: "pg_trgm"},
		{Kind: "type", Name: "status"},
		{Kind: "type", Name: "app.zip"},
		{Kind: "type", Name: "pair"},
	})
}

// plainValue is an owner's object that defines no type.
type plainValue struct{}

func (*plainValue) Kind() schemaext.Kind             { return "example.org/owner/extension" }
func (*plainValue) Clone() schemaext.Value           { return &plainValue{} }
func (*plainValue) Equal(other schemaext.Value) bool { _, ok := other.(*plainValue); return ok }

// part is a name component written as it is.
func part(name string) objectidentity.Part {
	return objectidentity.Part{Source: name, Normalized: name}
}

// TestOwned names an owner's object the way the ledger's refusal prints it:
// the noun its kind ends in, with hyphens as spaces, and its name qualified
// with its catalog, schema and parent where it has them, each quoted where it
// holds a dot or a quote, its signature last.
func TestOwned(t *testing.T) {
	tests := []struct {
		name string
		ref  objectidentity.ID
		want sqlext.Declaration
	}{
		{name: "a database-wide object",
			ref:  objectidentity.ID{Kind: "ptah.run/pgextension/extension", Name: part("pg_trgm")},
			want: sqlext.Declaration{Kind: "extension", Name: "pg_trgm"}},
		{name: "an object of a table",
			ref:  objectidentity.ID{Kind: "ptah.run/pgpolicy/policy", Schema: part("app"), Parent: part("users"), Name: part("own_rows")},
			want: sqlext.Declaration{Kind: "policy", Name: "app.users.own_rows"}},
		{name: "a kind of several words",
			ref:  objectidentity.ID{Kind: "example.org/owner/virtual-table", Catalog: part("main"), Name: part("docs")},
			want: sqlext.Declaration{Kind: "virtual table", Name: "main.docs"}},
		{name: "a component that holds a dot",
			ref:  objectidentity.ID{Kind: "ptah.run/ydb/changefeed", Parent: part("app.events"), Name: part(`say "hi"`)},
			want: sqlext.Declaration{Kind: "changefeed", Name: `"app.events"."say ""hi"""`}},
		{name: "an object with a signature",
			ref:  objectidentity.ID{Kind: "example.org/owner/routine", Name: part("f"), Signature: "integer"},
			want: sqlext.Declaration{Kind: "routine", Name: "f(integer)"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqldecl.Owned(test.ref), qt.Equals, test.want)
		})
	}
}
