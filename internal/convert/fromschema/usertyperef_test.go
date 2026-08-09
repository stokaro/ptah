package fromschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/convert/fromschema"
)

// userTypeDocument is the IR a PostgreSQL read of one schema produces: one
// declaration of each user-type kind, each carrying the schema it lives in, and
// one table whose column type is whatever the row is about.
func userTypeDocument(columnType string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t", Schema: "app"}},
		Fields: []goschema.Field{{StructName: "T", Name: "c", Type: columnType}},
		Enums: []goschema.Enum{{
			Name: "mood", Schema: "app", Values: []string{"sad", "ok"},
		}},
		Domains: []goschema.Domain{{
			Name: "positive_int", Schema: "app", BaseType: "integer",
		}},
		CompositeTypes: []goschema.CompositeType{{
			Name:   "addr",
			Schema: "app",
			Fields: []goschema.CompositeTypeField{{Name: "street", Type: "text"}},
		}},
		Ranges: []goschema.Range{{Name: "numrng", Schema: "app", Subtype: "numeric"}},
	}
}

// TestQualifyDeclaredUserTypesNamesTheDeclarationsSchema pins the whole
// boundary: four user-type kinds times two spellings.
//
// PostgreSQL resolves a bare type name through search_path, so a column typed
// against a user type in another schema is created against whatever that name
// resolves to at replay time -- or fails outright. Measured on PostgreSQL 17.10
// by inspecting a schema `wf1138s` holding one of each and planning the result
// into a fresh database with `psql -v ON_ERROR_STOP=1`:
//
//	CREATE TYPE "wf1138s"."mood" AS ENUM ('sad', 'ok', 'happy');
//	CREATE TABLE "wf1138s"."t" (..., "enum_array" mood[], ...);
//	  -> ERROR:  type "mood[]" does not exist                exit 3
//
// One of the eight cells was already right: the scalar spelling of an enum,
// closed by stokaro/ptah#1276. The other seven are stokaro/ptah#1138, and the
// enum row makes the split visible -- the same table wrote `wf1138s.mood` for
// the scalar column and `mood[]` for the array one.
//
// The scalar enum row expects NO rewrite here on purpose. [declaredEnum] is the
// only test for "is this column an enum" and it matches the bare name, so the
// qualifier for that cell belongs to handleEnumTypes and putting it on twice
// would hide the enum from the standalone-versus-inline decision.
func TestQualifyDeclaredUserTypesNamesTheDeclarationsSchema(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		want       string
	}{
		{
			name:       "a domain, scalar",
			columnType: "positive_int",
			want:       "app.positive_int",
		},
		{
			name:       "a domain, array",
			columnType: "positive_int[]",
			want:       "app.positive_int[]",
		},
		{
			name:       "a composite type, scalar",
			columnType: "addr",
			want:       "app.addr",
		},
		{
			name:       "a composite type, array",
			columnType: "addr[]",
			want:       "app.addr[]",
		},
		{
			name:       "a range type, scalar",
			columnType: "numrng",
			want:       "app.numrng",
		},
		{
			name:       "a range type, array",
			columnType: "numrng[]",
			want:       "app.numrng[]",
		},
		{
			name:       "an enum, array",
			columnType: "mood[]",
			want:       "app.mood[]",
		},
		{
			name:       "an enum, scalar, which handleEnumTypes owns",
			columnType: "mood",
			want:       "mood",
		},
		{
			name:       "a dimensioned array keeps its dimensions",
			columnType: "positive_int[3][]",
			want:       "app.positive_int[3][]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			qualified := fromschema.QualifyDeclaredUserTypes(
				userTypeDocument(test.columnType), platform.Postgres,
			)

			c.Assert(qualified.Fields, qt.HasLen, 1)
			c.Assert(qualified.Fields[0].Type, qt.Equals, test.want)
		})
	}
}

// TestQualifyDeclaredUserTypesLeavesEverythingElseAlone is the half that keeps
// the rewrite from becoming "prefix every type with the read's schema".
//
// Each row is a spelling that must survive untouched, and each names a distinct
// way of being wrong: qualifying a built-in type, re-qualifying one the author
// already spelled, guessing between two declarations of one name, and inventing
// a schema for a declaration that has none -- which is every document an author
// wrote by hand.
func TestQualifyDeclaredUserTypesLeavesEverythingElseAlone(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		amend      func(*goschema.Database)
		want       string
	}{
		{
			name:       "a built-in scalar type",
			columnType: "varchar(100)",
			amend:      func(_ *goschema.Database) {},
			want:       "varchar(100)",
		},
		{
			name:       "a built-in array type",
			columnType: "character varying(100)[]",
			amend:      func(_ *goschema.Database) {},
			want:       "character varying(100)[]",
		},
		{
			name:       "a type the author already qualified",
			columnType: "other.positive_int",
			amend:      func(_ *goschema.Database) {},
			want:       "other.positive_int",
		},
		{
			name:       "a name two schemas both declare",
			columnType: "positive_int",
			amend: func(db *goschema.Database) {
				db.Domains = append(db.Domains, goschema.Domain{
					Name: "positive_int", Schema: "other", BaseType: "integer",
				})
			},
			want: "positive_int",
		},
		{
			name:       "an array of a name two schemas both declare",
			columnType: "positive_int[]",
			amend: func(db *goschema.Database) {
				db.Domains = append(db.Domains, goschema.Domain{
					Name: "positive_int", Schema: "other", BaseType: "integer",
				})
			},
			want: "positive_int[]",
		},
		{
			name:       "an array whose element name an enum also answers to",
			columnType: "addr[]",
			amend: func(db *goschema.Database) {
				db.Enums = append(db.Enums, goschema.Enum{
					Name: "addr", Schema: "app", Values: []string{"home"},
				})
			},
			want: "addr[]",
		},
		{
			name:       "a declaration that carries no schema, as an author writes it",
			columnType: "positive_int",
			amend: func(db *goschema.Database) {
				db.Domains = []goschema.Domain{{Name: "positive_int", BaseType: "integer"}}
			},
			want: "positive_int",
		},
		{
			name:       "an array of a declaration that carries no schema",
			columnType: "mood[]",
			amend: func(db *goschema.Database) {
				db.Enums = []goschema.Enum{{Name: "mood", Values: []string{"sad", "ok"}}}
			},
			want: "mood[]",
		},
		{
			name:       "a bracket run that is part of the type, not a dimension",
			columnType: "positive_int[a]",
			amend:      func(_ *goschema.Database) {},
			want:       "positive_int[a]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db := userTypeDocument(test.columnType)
			test.amend(db)

			qualified := fromschema.QualifyDeclaredUserTypes(db, platform.Postgres)

			c.Assert(qualified.Fields, qt.HasLen, 1)
			c.Assert(qualified.Fields[0].Type, qt.Equals, test.want)
		})
	}
}

// TestQualifyDeclaredUserTypesLeavesInlineEnumDialectsAlone pins that a dialect
// which models an enum ON the column never has its enum names rewritten here.
//
// MySQL, MariaDB, SQLite and SQL Server all reach applyInlineEnumModel, which
// finds the enum by the bare name and replaces the column type outright. A
// qualifier written before that lookup would make the enum invisible to it and
// leave `app.mood` in the DDL as a type name no such server has.
func TestQualifyDeclaredUserTypesLeavesInlineEnumDialectsAlone(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "MySQL", dialect: platform.MySQL},
		{name: "MariaDB", dialect: platform.MariaDB},
		{name: "SQLite", dialect: platform.SQLite},
		{name: "SQL Server", dialect: platform.SQLServer},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			qualified := fromschema.QualifyDeclaredUserTypes(
				userTypeDocument("mood[]"), test.dialect,
			)

			c.Assert(qualified.Fields, qt.HasLen, 1)
			c.Assert(qualified.Fields[0].Type, qt.Equals, "mood[]")
		})
	}
}

// TestQualifyDeclaredUserTypesDoesNotMutateItsInput pins that the pass is a
// clone, because both callers hand it a value they keep using afterwards.
func TestQualifyDeclaredUserTypesDoesNotMutateItsInput(t *testing.T) {
	c := qt.New(t)

	db := userTypeDocument("mood[]")

	qualified := fromschema.QualifyDeclaredUserTypes(db, platform.Postgres)

	c.Assert(qualified.Fields[0].Type, qt.Equals, "app.mood[]")
	c.Assert(db.Fields[0].Type, qt.Equals, "mood[]")
}

// TestFromDatabaseQualifiesDeclaredUserTypes pins the first of the two entry
// points the pass is wired into.
//
// Wiring it into only one of them is the plausible half-fix: FromDatabase is
// the obvious whole-schema conversion, and the planner is where a diff-driven
// CREATE TABLE is actually built. Each of the two tests fails when the pass is
// wired only into the other, so neither call site can be dropped quietly.
func TestFromDatabaseQualifiesDeclaredUserTypes(t *testing.T) {
	c := qt.New(t)

	statements := fromschema.FromDatabase(*userTypeDocument("mood[]"), platform.Postgres)

	c.Assert(statements, qt.IsNotNil)

	sql, err := renderer.RenderSQL(platform.Postgres, statements.Statements...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "\"c\" app.mood[]")
}
