package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
)

// liveKeyTable is a read of one table whose primary key and UNIQUE on code the
// server holds under the names given.
func liveKeyTable(schema, table, primary, unique string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Schema: schema, Name: table, Columns: []catalog.Column{
			{Name: "id", IsPrimaryKey: true}, {Name: "code", IsUnique: true},
		}}},
		Indexes: []catalog.Index{
			{Schema: schema, TableName: table, Name: primary, Columns: []string{"id"}, IsPrimary: true, IsUnique: true},
			{Schema: schema, TableName: table, Name: unique, Columns: []string{"code"}, IsUnique: true},
		},
		Constraints: []catalog.Constraint{
			{Schema: schema, TableName: table, Name: primary, Type: "PRIMARY KEY", ColumnNames: []string{"id"}},
			{Schema: schema, TableName: table, Name: unique, Type: "UNIQUE", ColumnNames: []string{"code"}},
		},
	}
}

// declaredKeyTable declares table with the primary key and the UNIQUE on code
// written on the columns, so the declaration names neither.
func declaredKeyTable(schema, table string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Schema: schema, Name: table, StructName: "T"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "int", Primary: true},
			{StructName: "T", Name: "code", Type: "text", Unique: true},
		},
	}
}

// The desired side names a declared primary key by the name of the key the
// database holds on the same table, so a selector leaves out the same key on
// both sides. The name the declaration predicts names a key only where the
// database holds none to pair with. A UNIQUE is paired by its name, so the
// predicted name names the declared one and the server's name names only the
// server's.
func TestExcludeNamesADeclaredKeyByTheDatabasesKey_HappyPath(t *testing.T) {
	tests := []struct {
		name, dialect, defaultSchema, exclude string
		realm                                 bool
		live                                  *catalog.Database
		desired                               *schemamodel.Database
		// wantPrimary and wantUnique are the flags of id and code the desired
		// side keeps; wantUnmatched the selectors it reports naming nothing.
		wantPrimary, wantUnique bool
		wantUnmatched           []string
	}{
		{
			name: "a primary key the server holds under another name, by that name", dialect: "postgres",
			defaultSchema: "public", realm: true, exclude: "public.kinds.pk_kinds",
			live: liveKeyTable("public", "kinds", "pk_kinds", "uq_code"), desired: declaredKeyTable("public", "kinds"),
			wantPrimary: false, wantUnique: true,
		},
		{
			name: "a primary key the server holds under another name, by the predicted name", dialect: "postgres",
			defaultSchema: "public", realm: true, exclude: "public.kinds.kinds_pkey",
			live: liveKeyTable("public", "kinds", "pk_kinds", "uq_code"), desired: declaredKeyTable("public", "kinds"),
			wantPrimary: true, wantUnique: true, wantUnmatched: []string{"public.kinds.kinds_pkey"},
		},
		{
			name: "a renamed table's key, by the name it kept", dialect: "postgres",
			defaultSchema: "public", realm: true, exclude: "public.types.kinds_pkey[type=constraint]",
			live: liveKeyTable("public", "types", "kinds_pkey", "kinds_code_key"), desired: declaredKeyTable("public", "types"),
			wantPrimary: false, wantUnique: true,
		},
		{
			name: "a renamed table's key, by the name the new table predicts", dialect: "postgres",
			defaultSchema: "public", realm: true, exclude: "public.types.types_pkey[type=constraint]",
			live: liveKeyTable("public", "types", "kinds_pkey", "kinds_code_key"), desired: declaredKeyTable("public", "types"),
			wantPrimary: true, wantUnique: true, wantUnmatched: []string{"public.types.types_pkey[type=constraint]"},
		},
		{
			name: "a UNIQUE the server holds under another name, by that name", dialect: "postgres",
			defaultSchema: "public", realm: true, exclude: "public.kinds.uq_code",
			live: liveKeyTable("public", "kinds", "kinds_pkey", "uq_code"), desired: declaredKeyTable("public", "kinds"),
			wantPrimary: true, wantUnique: true, wantUnmatched: []string{"public.kinds.uq_code"},
		},
		{
			name: "a UNIQUE the server holds under another name, by the predicted name", dialect: "postgres",
			defaultSchema: "public", realm: true, exclude: "public.kinds.kinds_code_key",
			live: liveKeyTable("public", "kinds", "kinds_pkey", "uq_code"), desired: declaredKeyTable("public", "kinds"),
			wantPrimary: true, wantUnique: false,
		},
		{
			name: "a table the database does not hold, by the predicted name", dialect: "postgres",
			defaultSchema: "public", realm: true, exclude: "public.kinds.kinds_pkey",
			live: liveKeyTable("public", "others", "pk_others", "uq_others"), desired: declaredKeyTable("public", "kinds"),
			wantPrimary: false, wantUnique: true,
		},
		{
			name: "the MySQL primary key", dialect: "mysql", defaultSchema: "app", exclude: "kinds.PRIMARY",
			live: liveKeyTable("app", "kinds", "PRIMARY", "code"), desired: declaredKeyTable("", "kinds"),
			wantPrimary: false, wantUnique: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := atlasfilter.Scope{
				Selections: ownerSelections(), Exclude: []string{test.exclude}, DefaultSchema: test.defaultSchema,
				RealmRelativePatterns: test.realm, Dialect: test.dialect, LiveKeys: atlasfilter.KeysOf(test.live),
			}

			declared, report, err := atlasfilter.ExcludeGeneratedScopeReport(test.desired, scope)

			c.Assert(err, qt.IsNil)
			c.Assert(declared.Fields[0].Primary, qt.Equals, test.wantPrimary)
			c.Assert(declared.Fields[1].Unique, qt.Equals, test.wantUnique)
			c.Assert(report.Unmatched, qt.DeepEquals, test.wantUnmatched)
		})
	}
}
