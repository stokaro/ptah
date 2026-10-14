package oratypesource_test

import (
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/oratype"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

// readSQL reads statements as an Oracle document with the bundled owners.
func readSQL(c *qt.C, statements string) (schemamodel.Database, error) {
	c.Helper()
	db, _, err := sqlschema.ReadWith(c.Context(), []byte(statements), "oracle", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	return db, err
}

// TestSQL_ReadsEachDeclaration pins what a statement declares: each domain
// clause in any order, a string default read as the server reads it, DEFAULT
// NULL as no default, a guarded creation, a nested CHECK, a quoted and
// qualified name, and an object type in both spellings.
func TestSQL_ReadsEachDeclaration(t *testing.T) {
	tests := []struct {
		name       string
		statements string
		ref        string
		want       schemaext.Value
	}{
		{name: "every domain clause", statements: `CREATE DOMAIN d AS VARCHAR2(20) CHECK (VALUE IN ('a', 'b')) DEFAULT 'it''s' NOT NULL;`,
			ref: "d", want: &oratype.DesiredDomain{BaseType: "VARCHAR2(20)", NotNull: true, Default: "it's", Check: "VALUE IN ('a', 'b')"}},
		{name: "a default expression", statements: `CREATE DOMAIN d AS DATE DEFAULT SYSDATE NULL;`,
			ref: "d", want: &oratype.DesiredDomain{BaseType: "DATE", DefaultExpr: "SYSDATE"}},
		{name: "DEFAULT NULL", statements: `CREATE DOMAIN d AS NUMBER DEFAULT NULL;`,
			ref: "d", want: &oratype.DesiredDomain{BaseType: "NUMBER"}},
		{name: "a guarded creation", statements: `CREATE DOMAIN IF NOT EXISTS d AS NUMBER(5,2);`,
			ref: "d", want: &oratype.DesiredDomain{BaseType: "NUMBER(5,2)"}},
		{name: "a nested CHECK", statements: `CREATE DOMAIN d AS NUMBER CHECK ((VALUE > 0) AND (VALUE < 10));`,
			ref: "d", want: &oratype.DesiredDomain{BaseType: "NUMBER", Check: "(VALUE > 0) AND (VALUE < 10)"}},
		{name: "an object type in the shared spelling", statements: `CREATE TYPE "Pt" AS ("X" NUMBER(10,2), y NUMBER);`,
			ref: "Pt", want: &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "X", Type: "NUMBER(10,2)"}, {Name: "y", Type: "NUMBER"}}}},
		{name: "an object type in Oracle's spelling", statements: `CREATE OR REPLACE TYPE app.pt AS OBJECT (x NUMBER);`,
			ref: "app.pt", want: &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "x", Type: "NUMBER"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := readSQL(c, test.statements)

			c.Assert(err, qt.IsNil)
			objects := must.Must(db.FeatureObjects.All())
			c.Assert(objects, qt.HasLen, 1)
			c.Assert(oratype.QualifiedName(objects[0].Ref), qt.Equals, test.ref)
			c.Assert(objects[0].Value, qt.DeepEquals, test.want)
		})
	}
}

// TestSQL_FailurePath pins the statements the owner refuses: a clause written
// twice, a clause Ptah does not model, a domain without AS, and an object
// type with something after its attributes.
func TestSQL_FailurePath(t *testing.T) {
	tests := []struct {
		name, statements, want string
	}{
		{name: "a clause twice", statements: `CREATE DOMAIN d AS NUMBER NOT NULL NULL;`, want: `(?s).*domain d declares NULL more than once.*`},
		{name: "a named constraint", statements: `CREATE DOMAIN d AS NUMBER CONSTRAINT c CHECK (VALUE > 0);`,
			want: `(?s).*unsupported domain clause "CONSTRAINT".*`},
		{name: "a domain without AS", statements: `CREATE DOMAIN d NUMBER;`, want: `(?s).*expected AS.*`},
		{name: "a trailing clause", statements: `CREATE OR REPLACE TYPE t AS OBJECT (a NUMBER) NOT FINAL;`, want: `(?s).*unexpected NOT.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := readSQL(c, test.statements)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(db.FeatureObjects.Refs(), qt.HasLen, 0)
		})
	}
}

// TestAnnotations_FailurePath pins the declarations the owner refuses, naming
// the attribute: a NOT NULL that is no boolean and a field list with a field
// that has no type.
func TestAnnotations_FailurePath(t *testing.T) {
	tests := []struct {
		name, directive, attribute, want string
	}{
		{name: "a NOT NULL that is no boolean", directive: `//ptah:schema:domain name="d" type="NUMBER" not_null="yes" dialects="oracle"`,
			attribute: "not_null", want: `(?s).*parsing "yes": invalid syntax.*`},
		{name: "a field without a type", directive: `//ptah:schema:composite name="t" fields="a:NUMBER,b" dialects="oracle"`,
			attribute: "fields", want: `(?s).*invalid object type attribute "b".*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(builtintest.Annotations(), "fixture.go", "package fixture\n\n"+test.directive+"\ntype T struct{}\n")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			parseError, ok := errors.AsType[*ptaherr.ParseError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(parseError.Attribute, qt.Equals, test.attribute)
			c.Assert(db.FeatureObjects.Refs(), qt.HasLen, 0)
		})
	}
}

// TestHCL_RoundTripsAnOracleDocument pins that the blocks an Oracle export
// writes read back as the same declarations. A document written for Oracle
// carries no dialect attribute and reads back for Oracle; one written for no
// target names Oracle on each block and reads back for any caller.
func TestHCL_RoundTripsAnOracleDocument(t *testing.T) {
	declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		must.Must(oratype.DesiredObject(oratype.Ref(oratype.DomainKind, "", "email_d"),
			&oratype.DesiredDomain{BaseType: "VARCHAR2(320)", NotNull: true, DefaultExpr: "LOWER('X')", Check: "VALUE <> 'x'", Comment: "mail"})),
		must.Must(oratype.DesiredObject(oratype.Ref(oratype.ObjectTypeKind, "", "addr_t"),
			&oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "street", Type: "VARCHAR2(80)"}}, Comment: "address"})),
	))}
	tests := []struct {
		name, target, readFor string
		wantAttribute         bool
	}{
		{name: "written for Oracle", target: "oracle", readFor: "oracle"},
		{name: "written for no target", readFor: "", wantAttribute: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			written, err := atlashclrender.Render(declared, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Target: test.target})
			c.Assert(err, qt.IsNil)
			read, err := atlashcl.ParseWithOptions(written.Data, "schema.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL(), Dialect: test.readFor})

			c.Assert(err, qt.IsNil)
			c.Assert(containsDialect(string(written.Data)), qt.Equals, test.wantAttribute)
			c.Assert(read.FeatureObjects.Equal(declared.FeatureObjects), qt.IsTrue)
		})
	}
}

// containsDialect reports whether a document writes a dialect attribute.
func containsDialect(document string) bool {
	return strings.Contains(document, `dialect = "oracle"`)
}
