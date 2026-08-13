package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

// qualifiedKindsFixture is the PostgreSQL fixture the issue reproduces
// against, reduced to the two kinds that carried no schema at all: an enum and
// a function in the connection's own schema (empty Schema, the convention every
// reader follows) and one of each in a second schema.
//
// Tables, views and extensions ride along as controls. They already honored the
// qualified spelling before this change, so a regression that reached them
// would show up here rather than being attributed to enums and functions.
func qualifiedKindsFixture() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "users", Columns: []dbschematypes.DBColumn{{Name: "id"}}},
			{Schema: "app", Name: "orders", Columns: []dbschematypes.DBColumn{{Name: "id"}}},
		},
		Enums: []dbschematypes.DBEnum{
			{Name: "mood", Values: []string{"a", "b"}},
			{Schema: "app", Name: "color", Values: []string{"r"}},
		},
		Functions: []dbschematypes.DBFunction{
			{Name: "fn_audit", Returns: "integer"},
			{Schema: "app", Name: "fn_app", Returns: "integer"},
		},
		Views: []dbschematypes.DBView{
			{Name: "v_users"},
			{Schema: "app", Name: "v_orders"},
		},
		Extensions: []dbschematypes.DBExtension{
			{Schema: "public", Name: "pgcrypto", Version: "1.3"},
		},
	}
}

func qualifiedEnumNames(enums []dbschematypes.DBEnum) []string {
	names := make([]string, 0, len(enums))
	for _, enum := range enums {
		names = append(names, dbschematypes.QualifyTableName(enum.Schema, enum.Name))
	}
	return names
}

func qualifiedFunctionNames(functions []dbschematypes.DBFunction) []string {
	names := make([]string, 0, len(functions))
	for _, function := range functions {
		names = append(names, dbschematypes.QualifyTableName(function.Schema, function.Name))
	}
	return names
}

// TestExcludeDatabaseWithDefaultSchema_QualifiedSelectorsReachEnumsAndFunctions
// is the object-list form of the reproduction in stokaro/ptah#933: one row per
// (object kind x selector shape), asserting what survives the filter rather
// than the exit code, because a silent no-op and a correct filter share an exit
// code here — that is the whole defect.
//
// Red without the fix: every "qualified" enum and function row fails with the
// object the selector named still in the list, which is the reported bug
// verbatim. The "bare" rows are green before and after, so they separate the
// qualification axis from exclusion working at all, and the "matches nothing"
// rows are the inverse mutant: a candidate layer that qualified with a
// hard-coded schema, or ignored the schema half of the pattern, turns them red
// by removing an object no selector named.
func TestExcludeDatabaseWithDefaultSchema_QualifiedSelectorsReachEnumsAndFunctions(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		assert  func(*qt.C, *dbschematypes.DBSchema)
	}{
		{
			name:    "enum, qualified with the default schema",
			pattern: "public.mood",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(qualifiedEnumNames(got.Enums), qt.DeepEquals, []string{"app.color"})
			},
		},
		{
			name:    "enum, qualified with a non-default schema",
			pattern: "app.color",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(qualifiedEnumNames(got.Enums), qt.DeepEquals, []string{"mood"})
			},
		},
		{
			name:    "enum, bare name stays supported",
			pattern: "mood",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(qualifiedEnumNames(got.Enums), qt.DeepEquals, []string{"app.color"})
			},
		},
		{
			name:    "enum, qualified with the wrong schema matches nothing",
			pattern: "app.mood",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(qualifiedEnumNames(got.Enums), qt.DeepEquals, []string{"mood", "app.color"})
			},
		},
		{
			name:    "enum, qualified with an absent schema matches nothing",
			pattern: "nosuch.color",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(qualifiedEnumNames(got.Enums), qt.DeepEquals, []string{"mood", "app.color"})
			},
		},
		{
			name:    "function, qualified with the default schema",
			pattern: "public.fn_audit",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(qualifiedFunctionNames(got.Functions), qt.DeepEquals, []string{"app.fn_app"})
			},
		},
		{
			name:    "function, qualified with a non-default schema",
			pattern: "app.fn_app",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(qualifiedFunctionNames(got.Functions), qt.DeepEquals, []string{"fn_audit"})
			},
		},
		{
			name:    "function, bare name stays supported",
			pattern: "fn_app",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(qualifiedFunctionNames(got.Functions), qt.DeepEquals, []string{"fn_audit"})
			},
		},
		{
			name:    "function, qualified with the wrong schema matches nothing",
			pattern: "public.fn_app",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(qualifiedFunctionNames(got.Functions), qt.DeepEquals, []string{"fn_audit", "app.fn_app"})
			},
		},
		{
			name:    "function, qualified with an absent schema matches nothing",
			pattern: "nosuch.fn_app",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(qualifiedFunctionNames(got.Functions), qt.DeepEquals, []string{"fn_audit", "app.fn_app"})
			},
		},
		{
			// The qualified spelling must not widen what a type selector
			// narrows: this pattern names every enum and nothing else.
			name:    "type selector still narrows to one kind",
			pattern: "*[type=enum]",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(qualifiedEnumNames(got.Enums), qt.HasLen, 0)
				c.Assert(qualifiedFunctionNames(got.Functions), qt.DeepEquals, []string{"fn_audit", "app.fn_app"})
				c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"users", "app.orders"})
			},
		},
		{
			name:    "table control, unchanged by this fix",
			pattern: "public.users",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"app.orders"})
				c.Assert(qualifiedEnumNames(got.Enums), qt.DeepEquals, []string{"mood", "app.color"})
			},
		},
		{
			name:    "view control, unchanged by this fix",
			pattern: "public.v_users",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(viewNames(got.Views), qt.DeepEquals, []string{"app.v_orders"})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(
				qualifiedKindsFixture(), []string{test.pattern}, "public")

			c.Assert(err, qt.IsNil)
			test.assert(c, got)
		})
	}
}

// TestExcludeDatabase_EnumsAndFunctionsKeepBareOnlyCandidatesWithoutDefault is
// the inverse mutant of the table above. With no default schema an object that
// carries none is unqualified, so the qualified spelling must go back to
// matching nothing while the bare spelling still works. Without this row a
// candidate layer that hard-coded "public" would pass every row above and
// mis-filter every non-PostgreSQL target.
func TestExcludeDatabase_EnumsAndFunctionsKeepBareOnlyCandidatesWithoutDefault(t *testing.T) {
	c := qt.New(t)

	qualified, err := atlasfilter.ExcludeDatabase(qualifiedKindsFixture(), []string{"public.mood", "public.fn_audit"})
	c.Assert(err, qt.IsNil)
	c.Assert(qualifiedEnumNames(qualified.Enums), qt.DeepEquals, []string{"mood", "app.color"})
	c.Assert(qualifiedFunctionNames(qualified.Functions), qt.DeepEquals, []string{"fn_audit", "app.fn_app"})

	bare, err := atlasfilter.ExcludeDatabase(qualifiedKindsFixture(), []string{"mood", "fn_audit"})
	c.Assert(err, qt.IsNil)
	c.Assert(qualifiedEnumNames(bare.Enums), qt.DeepEquals, []string{"app.color"})
	c.Assert(qualifiedFunctionNames(bare.Functions), qt.DeepEquals, []string{"app.fn_app"})

	// The non-default schema is carried by the object itself, so it stays
	// addressable with no default in play.
	nonDefault, err := atlasfilter.ExcludeDatabase(qualifiedKindsFixture(), []string{"app.color", "app.fn_app"})
	c.Assert(err, qt.IsNil)
	c.Assert(qualifiedEnumNames(nonDefault.Enums), qt.DeepEquals, []string{"mood"})
	c.Assert(qualifiedFunctionNames(nonDefault.Functions), qt.DeepEquals, []string{"fn_audit"})
}

// generatedQualifiedKindsFixture is the desired-side mirror. Extensions carry
// installation schema separately; enums and functions retain their established
// identity representations.
func generatedQualifiedKindsFixture() *goschema.Database {
	return &goschema.Database{
		Enums: []goschema.Enum{
			{Name: "mood", Values: []string{"a", "b"}},
			{Name: "app.color", Values: []string{"r"}},
		},
		Functions: []goschema.Function{
			{Name: "fn_audit", Returns: "integer"},
			{Name: "app.fn_app", Returns: "integer"},
		},
		Extensions: []goschema.Extension{
			{Name: "pgcrypto", Version: "1.3"},
			{Name: "postgis", Schema: "app", Version: "3.4"},
		},
	}
}

func generatedFunctionNames(functions []goschema.Function) []string {
	names := make([]string, 0, len(functions))
	for _, function := range functions {
		names = append(names, function.Name)
	}
	return names
}

func generatedExtensionNames(extensions []goschema.Extension) []string {
	names := make([]string, 0, len(extensions))
	for _, extension := range extensions {
		if extension.Schema == "" {
			names = append(names, extension.Name)
		} else {
			names = append(names, extension.Schema+"."+extension.Name)
		}
	}
	return names
}

// TestExcludeGeneratedWithDefaultSchema_QualifiedSelectorsMatchDatabaseSide
// pins the symmetry the comparison depends on. A selector that subtracted an
// object from the introspected side but not from the desired side would turn a
// protected object into a CREATE, so every row that passes on the database side
// has to pass here on the same spelling.
//
// Red without the fix on every qualified row; the bare rows stay green, and the
// "matches nothing" rows redden if the candidate set is widened too far.
func TestExcludeGeneratedWithDefaultSchema_QualifiedSelectorsMatchDatabaseSide(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		assert  func(*qt.C, *goschema.Database)
	}{
		{
			name:    "enum, qualified with the default schema",
			pattern: "public.mood",
			assert: func(c *qt.C, got *goschema.Database) {
				c.Assert(generatedEnumNames(got.Enums), qt.DeepEquals, []string{"app.color"})
			},
		},
		{
			name:    "enum, qualified with a non-default schema",
			pattern: "app.color",
			assert: func(c *qt.C, got *goschema.Database) {
				c.Assert(generatedEnumNames(got.Enums), qt.DeepEquals, []string{"mood"})
			},
		},
		{
			name:    "enum, qualified with an absent schema matches nothing",
			pattern: "nosuch.mood",
			assert: func(c *qt.C, got *goschema.Database) {
				c.Assert(generatedEnumNames(got.Enums), qt.DeepEquals, []string{"mood", "app.color"})
			},
		},
		{
			name:    "function, qualified with the default schema",
			pattern: "public.fn_audit",
			assert: func(c *qt.C, got *goschema.Database) {
				c.Assert(generatedFunctionNames(got.Functions), qt.DeepEquals, []string{"app.fn_app"})
			},
		},
		{
			name:    "function, qualified with a non-default schema",
			pattern: "app.fn_app",
			assert: func(c *qt.C, got *goschema.Database) {
				c.Assert(generatedFunctionNames(got.Functions), qt.DeepEquals, []string{"fn_audit"})
			},
		},
		{
			name:    "function, qualified with an absent schema matches nothing",
			pattern: "nosuch.fn_app",
			assert: func(c *qt.C, got *goschema.Database) {
				// goschema.Finalize sorts the desired-side function list.
				c.Assert(generatedFunctionNames(got.Functions), qt.DeepEquals, []string{"app.fn_app", "fn_audit"})
			},
		},
		{
			name:    "extension, qualified with the default schema",
			pattern: "public.pgcrypto",
			assert: func(c *qt.C, got *goschema.Database) {
				c.Assert(generatedExtensionNames(got.Extensions), qt.DeepEquals, []string{"app.postgis"})
			},
		},
		{
			name:    "extension, qualified with a non-default schema",
			pattern: "app.postgis",
			assert: func(c *qt.C, got *goschema.Database) {
				c.Assert(generatedExtensionNames(got.Extensions), qt.DeepEquals, []string{"pgcrypto"})
			},
		},
		{
			name:    "extension, bare name stays supported",
			pattern: "pgcrypto",
			assert: func(c *qt.C, got *goschema.Database) {
				c.Assert(generatedExtensionNames(got.Extensions), qt.DeepEquals, []string{"app.postgis"})
			},
		},
		{
			name:    "extension, qualified with an absent schema matches nothing",
			pattern: "nosuch.postgis",
			assert: func(c *qt.C, got *goschema.Database) {
				c.Assert(generatedExtensionNames(got.Extensions), qt.DeepEquals, []string{"pgcrypto", "app.postgis"})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(
				generatedQualifiedKindsFixture(), []string{test.pattern}, "public")

			c.Assert(err, qt.IsNil)
			test.assert(c, got)
		})
	}
}

func TestScopeDatabase_ExtensionSchemaIdentityPreservesQuotedWhitespace(t *testing.T) {
	c := qt.New(t)
	database := &dbschematypes.DBSchema{
		Schemas: []dbschematypes.DBSchemaInfo{
			{Name: "Extension Store"},
			{Name: " Extension Store "},
		},
		Extensions: []dbschematypes.DBExtension{
			{Schema: "Extension Store", Name: "citext"},
			{Schema: " Extension Store ", Name: "pgcrypto"},
		},
	}

	got, err := atlasfilter.ScopeDatabase(database, atlasfilter.Scope{
		Include: []string{`" Extension Store ".pgcrypto`},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got.Extensions, qt.DeepEquals, []dbschematypes.DBExtension{{
		Schema: " Extension Store ", Name: "pgcrypto",
	}})
	c.Assert(got.Schemas, qt.DeepEquals, []dbschematypes.DBSchemaInfo{{Name: " Extension Store "}})
}

func TestScopeGenerated_ExtensionSchemaIdentityPreservesQuotedWhitespace(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Schemas: []goschema.Schema{
			{Name: "Extension Store"},
			{Name: " Extension Store "},
		},
		Extensions: []goschema.Extension{
			{Schema: "Extension Store", Name: "citext"},
			{Schema: " Extension Store ", Name: "pgcrypto"},
		},
	}

	got, err := atlasfilter.ScopeGenerated(database, atlasfilter.Scope{
		Include: []string{`" Extension Store ".pgcrypto`},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got.Extensions, qt.DeepEquals, []goschema.Extension{{
		Schema: " Extension Store ", Name: "pgcrypto",
	}})
	c.Assert(got.Schemas, qt.DeepEquals, []goschema.Schema{{Name: " Extension Store "}})
}

func TestScopeDatabase_ExtensionWhitespaceOnlySchemaIsNotTheDefault(t *testing.T) {
	c := qt.New(t)
	database := &dbschematypes.DBSchema{
		Schemas: []dbschematypes.DBSchemaInfo{{Name: " "}, {Name: "public"}},
		Extensions: []dbschematypes.DBExtension{
			{Schema: " ", Name: "pgcrypto"},
			{Schema: "public", Name: "citext"},
		},
	}

	got, err := atlasfilter.ScopeDatabase(database, atlasfilter.Scope{
		Include: []string{`" ".pgcrypto`},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got.Extensions, qt.DeepEquals, []dbschematypes.DBExtension{{
		Schema: " ", Name: "pgcrypto",
	}})
	c.Assert(got.Schemas, qt.DeepEquals, []dbschematypes.DBSchemaInfo{{Name: " "}})
}

func TestScopeGenerated_ExtensionWhitespaceOnlySchemaIsNotTheDefault(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Schemas: []goschema.Schema{{Name: " "}, {Name: "public"}},
		Extensions: []goschema.Extension{
			{Schema: " ", Name: "pgcrypto"},
			{Schema: "public", Name: "citext"},
		},
	}

	got, err := atlasfilter.ScopeGenerated(database, atlasfilter.Scope{
		Include: []string{`" ".pgcrypto`},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got.Extensions, qt.DeepEquals, []goschema.Extension{{
		Schema: " ", Name: "pgcrypto",
	}})
	c.Assert(got.Schemas, qt.DeepEquals, []goschema.Schema{{Name: " "}})
}

func TestScopeExtensionSchemasWithInternalWhitespaceUseQuotedCandidates(t *testing.T) {
	c := qt.New(t)
	scope := atlasfilter.Scope{Include: []string{`"Extension Store".pgcrypto`}}
	database, err := atlasfilter.ScopeDatabase(&dbschematypes.DBSchema{
		Schemas:    []dbschematypes.DBSchemaInfo{{Name: "Extension Store"}},
		Extensions: []dbschematypes.DBExtension{{Schema: "Extension Store", Name: "pgcrypto"}},
	}, scope)
	c.Assert(err, qt.IsNil)
	c.Assert(database.Extensions, qt.DeepEquals, []dbschematypes.DBExtension{{
		Schema: "Extension Store", Name: "pgcrypto",
	}})

	generated, err := atlasfilter.ScopeGenerated(&goschema.Database{
		Schemas:    []goschema.Schema{{Name: "Extension Store"}},
		Extensions: []goschema.Extension{{Schema: "Extension Store", Name: "pgcrypto"}},
	}, scope)
	c.Assert(err, qt.IsNil)
	c.Assert(generated.Extensions, qt.DeepEquals, []goschema.Extension{{
		Schema: "Extension Store", Name: "pgcrypto",
	}})
}
