package atlasschema

// White-box testing required: scopeDiffState composes the generated desired
// projection with an authoritative catalog selection before either command
// adapter sees the result. Pure fixtures are needed to distinguish those two
// representations: database filtering loses body dependencies and a column's
// domain identity, while the shared schema model preserves catalog identity.
// The qualified-extension case is also covered through the compat command and
// live PostgreSQL in
// TestSchemaDiffIncludeMatchesLiveExtensionOutsideTheDefaultSchemaPostgres.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

func TestScopeDiffStatePreservesGeneratedDependencyValidation(t *testing.T) {
	c := qt.New(t)
	database := &types.DBSchema{
		Tables: []types.DBTable{{Name: "users"}},
		Views:  []types.DBView{{Name: "active_users", Body: "SELECT * FROM users"}},
	}
	state := diffDatabaseState(database)

	got := scopeDiffState(state, atlasfilter.Scope{
		Include:       []string{"active_users"},
		DefaultSchema: state.DefaultSchema,
	}, "--to schema", "postgres")

	var crossScope *atlasfilter.CrossScopeError
	c.Assert(got.err, qt.ErrorAs, &crossScope)
	c.Assert(crossScope.Diagnostics, qt.DeepEquals, []string{
		`view "active_users" references "users", but "users" is not selected`,
	})
}

func TestScopeDiffStatePreservesGeneratedDomainDependencies(t *testing.T) {
	c := qt.New(t)
	database := &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "measurements",
			Columns: []types.DBColumn{{
				Name:          "value",
				DataType:      "integer",
				UDTName:       "int4",
				DomainName:    "positive",
				FormattedType: "doms.positive",
			}},
		}},
		Domains: []types.DBDomain{{
			Name:     "positive",
			Schema:   "doms",
			BaseType: "integer",
			Check:    "VALUE > 0",
		}},
	}
	state := diffDatabaseState(database)

	got := scopeDiffState(state, atlasfilter.Scope{
		Include:       []string{"measurements"},
		DefaultSchema: state.DefaultSchema,
	}, "--to schema", "postgres")

	c.Assert(got.err, qt.IsNil)
	c.Assert(got.schema.Fields, qt.HasLen, 1)
	c.Assert(got.schema.Fields[0].Type, qt.Equals, "doms.positive")
	c.Assert(got.schema.Domains, qt.DeepEquals, []goschema.Domain{{
		Name:     "positive",
		Schema:   "doms",
		BaseType: "integer",
		Check:    "VALUE > 0",
	}})
}

func TestScopeDiffStatePreservesQualifiedDatabaseFunctionIdentity(t *testing.T) {
	c := qt.New(t)
	database := &types.DBSchema{
		Schemas: []types.DBSchemaInfo{{Name: "extra"}},
		Functions: []types.DBFunction{{
			Schema:   "extra",
			Name:     "fn",
			Returns:  "integer",
			Language: "sql",
			Body:     "SELECT 1",
		}},
	}
	state := diffDatabaseState(database)

	got := scopeDiffState(state, atlasfilter.Scope{
		Include:       []string{"extra.fn"},
		DefaultSchema: state.DefaultSchema,
	}, "--to schema", "postgres")

	c.Assert(got.err, qt.IsNil)
	c.Assert(got.selectionErr, qt.IsNil)
	c.Assert(got.database.Functions, qt.DeepEquals, database.Functions)
	c.Assert(got.database.Schemas, qt.DeepEquals, database.Schemas)
	c.Assert(got.schema.Functions, qt.HasLen, 1)
	c.Assert(got.schema.Functions[0].Name, qt.Equals, "extra.fn")
	c.Assert(got.schema.Schemas, qt.DeepEquals, []goschema.Schema{{Name: "extra"}})
}

func TestScopeDiffStateExcludesQualifiedDatabaseEnumSymmetrically(t *testing.T) {
	c := qt.New(t)
	database := &types.DBSchema{
		Schemas: []types.DBSchemaInfo{{Name: "app"}},
		Enums:   []types.DBEnum{{Schema: "app", Name: "color", Values: []string{"red"}}},
	}
	state := diffDatabaseState(database)

	got := scopeDiffState(state, atlasfilter.Scope{
		Include:       []string{"app.color"},
		Exclude:       []string{"app.color"},
		DefaultSchema: state.DefaultSchema,
	}, "--to schema", "postgres")

	c.Assert(got.err, qt.IsNil)
	c.Assert(got.selectionErr, qt.IsNil)
	c.Assert(got.database.Enums, qt.HasLen, 0)
	c.Assert(got.schema.Enums, qt.HasLen, 0)
	c.Assert(got.database.Schemas, qt.DeepEquals, database.Schemas)
	c.Assert(got.schema.Schemas, qt.DeepEquals, []goschema.Schema{{Name: "app"}})
}

func TestScopeDiffStatesBareExcludePreservesCatalogSchemaSpelling(t *testing.T) {
	c := qt.New(t)
	current := &types.DBSchema{
		Schemas: []types.DBSchemaInfo{{Name: "Sales"}},
		Tables:  []types.DBTable{{Schema: "Sales", Name: "orders"}},
	}
	scope := atlasfilter.Scope{Exclude: []string{"Sales"}, DefaultSchema: "dbo"}

	from, to := scopeDiffStates(
		diffDatabaseState(current),
		atlassource.State{Schema: &goschema.Database{}, DefaultSchema: "dbo"},
		scope,
		platform.SQLServer,
	)

	c.Assert(from.err, qt.IsNil)
	c.Assert(to.err, qt.IsNil)
	c.Assert(from.report.Unmatched, qt.IsNil)
	c.Assert(from.database.Schemas, qt.HasLen, 0)
	c.Assert(from.database.Tables, qt.HasLen, 0)
	diff := schemadiff.CompareWithDialect(to.schema, from.database, platform.SQLServer)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

func TestScopeDiffStateDoesNotMergeUnrelatedCatalogType(t *testing.T) {
	c := qt.New(t)
	database := &types.DBSchema{
		Tables: []types.DBTable{{
			Name:   "users",
			Schema: "public",
			Columns: []types.DBColumn{{
				Name:     "id",
				DataType: "integer",
				UDTName:  "int4",
			}},
		}},
		Domains: []types.DBDomain{{
			Name:     "int4",
			Schema:   "app",
			BaseType: "integer",
			Check:    "VALUE > 0",
		}},
	}
	state := diffDatabaseState(database)

	got := scopeDiffState(state, atlasfilter.Scope{
		Include:       []string{"public.users"},
		DefaultSchema: state.DefaultSchema,
	}, "--to schema", "postgres")

	c.Assert(got.err, qt.IsNil)
	c.Assert(got.selectionErr, qt.IsNil)
	c.Assert(got.schema.Tables, qt.HasLen, 1)
	c.Assert(got.schema.Domains, qt.HasLen, 0)
}

func TestScopeDiffStateUsesDatabaseIdentityForSelectionOutcome(t *testing.T) {
	c := qt.New(t)
	database := &types.DBSchema{
		Extensions: []types.DBExtension{{
			Name:    "pgcrypto",
			Schema:  "extensions",
			Version: "1.3",
		}},
		NotDescribed: coverage.Set{}.WithKind(coverage.Sequence),
	}
	state := diffDatabaseState(database)

	matched := scopeDiffState(state, atlasfilter.Scope{
		Include:       []string{"extensions.pgcrypto"},
		DefaultSchema: state.DefaultSchema,
	}, "--from schema", "postgres")
	c.Assert(matched.err, qt.IsNil)
	c.Assert(matched.selectionErr, qt.IsNil)
	c.Assert(matched.database.Extensions, qt.DeepEquals, database.Extensions)
	c.Assert(matched.schema.Extensions, qt.HasLen, 1)
	c.Assert(matched.schema.Extensions[0].Name, qt.Equals, "pgcrypto")
	c.Assert(matched.schema.Extensions[0].Schema, qt.Equals, "extensions")
	c.Assert(matched.schema.NotDescribed, qt.DeepEquals, coverage.Set{}.WithKind(coverage.Sequence))

	missed := scopeDiffState(state, atlasfilter.Scope{
		Include:       []string{"extensions.typo"},
		DefaultSchema: state.DefaultSchema,
	}, "--from schema", "postgres")
	c.Assert(missed.err, qt.IsNil)
	var empty *atlasfilter.EmptySelectionError
	c.Assert(missed.selectionErr, qt.ErrorAs, &empty)
}

func TestScopeDiffStatePreservesDatabaseWideExtensionsAcrossSchemaUniverse(t *testing.T) {
	c := qt.New(t)
	database := &types.DBSchema{
		Schemas: []types.DBSchemaInfo{{Name: "app"}, {Name: "extensions"}, {Name: "other"}},
		Tables: []types.DBTable{{
			Schema: "app",
			Name:   "users",
			Columns: []types.DBColumn{{
				Name: "email", DataType: "USER-DEFINED", UDTName: "citext", FormattedType: "extensions.citext",
			}},
		}},
		Extensions: []types.DBExtension{
			{Name: "pgcrypto"},
			{Schema: "extensions", Name: "citext"},
			{Schema: "other", Name: "unrelated"},
		},
	}
	state := diffDatabaseState(database)

	got := scopeDiffState(state, atlasfilter.Scope{
		Schemas:       []string{"app"},
		Include:       []string{"app.users", "other.unrelated"},
		DefaultSchema: state.DefaultSchema,
	}, "--to schema", "postgres")

	c.Assert(got.err, qt.IsNil)
	c.Assert(got.selectionErr, qt.IsNil)
	c.Assert(got.database.Extensions, qt.DeepEquals, database.Extensions)
	c.Assert(got.schema.Extensions, qt.DeepEquals, []goschema.Extension{
		{Schema: "extensions", Name: "citext", IfNotExists: true},
		{Name: "pgcrypto", IfNotExists: true},
		{Schema: "other", Name: "unrelated", IfNotExists: true},
	})
	c.Assert(got.selection.NonExtensionMatched, qt.IsTrue)
	c.Assert(got.schema.Fields, qt.HasLen, 1)
	c.Assert(got.schema.Fields[0].Type, qt.Equals, "extensions.citext")

	diff := schemadiff.CompareWithDialect(got.schema, &types.DBSchema{}, platform.Postgres)
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, got.schema, platform.Postgres)
	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, ";\n")
	c.Assert(sql, qt.Contains, `CREATE SCHEMA IF NOT EXISTS "extensions"`)
	c.Assert(sql, qt.Contains, `CREATE EXTENSION IF NOT EXISTS "citext" WITH SCHEMA "extensions"`)
	c.Assert(strings.Index(sql, `CREATE SCHEMA IF NOT EXISTS "extensions"`) <
		strings.Index(sql, `CREATE EXTENSION IF NOT EXISTS "citext" WITH SCHEMA "extensions"`), qt.IsTrue)
}

func TestNonExtensionScopeDoesNotRemoveUnmentionedCurrentExtension(t *testing.T) {
	c := qt.New(t)
	current := &types.DBSchema{
		Tables: []types.DBTable{{Schema: "app", Name: "users"}},
		Extensions: []types.DBExtension{
			{Schema: "extensions", Name: "citext"},
			{Name: "pgcrypto"},
		},
	}
	desired := &goschema.Database{
		Tables:     []goschema.Table{{StructName: "User", Schema: "app", Name: "users"}},
		Extensions: []goschema.Extension{{Schema: "extensions", Name: "citext"}},
	}
	scope := atlasfilter.Scope{Include: []string{"app.users"}, DefaultSchema: "public"}

	from, to := scopeDiffStates(
		diffDatabaseState(current),
		atlassource.State{Schema: desired, DefaultSchema: "public"},
		scope,
		"postgres",
	)

	c.Assert(from.err, qt.IsNil)
	c.Assert(to.err, qt.IsNil)
	c.Assert(from.database.Extensions, qt.DeepEquals, current.Extensions)
	c.Assert(to.schema.Extensions, qt.DeepEquals, desired.Extensions)
	applyExtensionSupportCoverage(to.schema, from.selection, to.selection)
	c.Assert(to.schema.NotDescribed, qt.DeepEquals, coverage.Set{}.WithKind(coverage.Extension))
	diff := schemadiff.CompareWithDialect(to.schema, from.database, platform.Postgres)
	c.Assert(diff.ExtensionsAdded, qt.HasLen, 0)
	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0)
}

func TestCurrentOnlyNonExtensionMatchDoesNotRemoveUnrelatedExtension(t *testing.T) {
	c := qt.New(t)
	current := &types.DBSchema{
		Tables:     []types.DBTable{{Schema: "app", Name: "users"}},
		Extensions: []types.DBExtension{{Name: "pgcrypto"}},
	}
	scope := atlasfilter.Scope{Include: []string{"app.users"}, DefaultSchema: "public"}

	from, to := scopeDiffStates(
		diffDatabaseState(current),
		atlassource.State{Schema: &goschema.Database{}, DefaultSchema: "public"},
		scope,
		"postgres",
	)

	c.Assert(from.err, qt.IsNil)
	c.Assert(to.err, qt.IsNil)
	c.Assert(from.selection.NonExtensionMatched, qt.IsTrue)
	c.Assert(to.selection.NonExtensionMatched, qt.IsFalse)
	applyExtensionSupportCoverage(to.schema, from.selection, to.selection)
	diff := schemadiff.CompareWithDialect(to.schema, from.database, platform.Postgres)
	c.Assert(diff.TablesRemoved, qt.HasLen, 1)
	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0)
}

func TestDesiredOnlyNonExtensionMatchStillAddsDeclaredExtension(t *testing.T) {
	c := qt.New(t)
	current := &types.DBSchema{}
	desired := &goschema.Database{
		Tables:     []goschema.Table{{StructName: "User", Schema: "app", Name: "users"}},
		Extensions: []goschema.Extension{{Schema: "extensions", Name: "citext"}},
	}
	scope := atlasfilter.Scope{Include: []string{"app.users"}, DefaultSchema: "public"}

	from, to := scopeDiffStates(
		diffDatabaseState(current),
		atlassource.State{Schema: desired, DefaultSchema: "public"},
		scope,
		"postgres",
	)

	c.Assert(from.err, qt.IsNil)
	c.Assert(to.err, qt.IsNil)
	c.Assert(from.database.Extensions, qt.DeepEquals, current.Extensions)
	c.Assert(to.schema.Extensions, qt.DeepEquals, desired.Extensions)
	applyExtensionSupportCoverage(to.schema, from.selection, to.selection)
	diff := schemadiff.CompareWithDialect(to.schema, from.database, platform.Postgres)
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"citext"})
	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesAdded, qt.HasLen, 1)
}

func TestDesiredOnlyNonExtensionMatchDoesNotReAddCurrentSupportExtension(t *testing.T) {
	c := qt.New(t)
	current := &types.DBSchema{Extensions: []types.DBExtension{
		{Schema: "extensions", Name: "citext"},
		{Name: "pgcrypto"},
	}}
	desired := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Schema: "app", Name: "users"}},
		Extensions: []goschema.Extension{
			{Schema: "extensions", Name: "citext"},
			{Name: "pgcrypto"},
		},
	}
	scope := atlasfilter.Scope{
		Include:       []string{"app.users", "extensions.citext"},
		DefaultSchema: "public",
	}

	from, to := scopeDiffStates(
		diffDatabaseState(current),
		atlassource.State{Schema: desired, DefaultSchema: "public"},
		scope,
		"postgres",
	)

	c.Assert(from.err, qt.IsNil)
	c.Assert(to.err, qt.IsNil)
	c.Assert(from.database.Extensions, qt.DeepEquals, current.Extensions)
	c.Assert(to.schema.Extensions, qt.DeepEquals, desired.Extensions)
	applyExtensionSupportCoverage(to.schema, from.selection, to.selection)
	diff := schemadiff.CompareWithDialect(to.schema, from.database, platform.Postgres)
	c.Assert(diff.ExtensionsAdded, qt.HasLen, 0)
	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesAdded, qt.HasLen, 1)
}

func TestExtensionOnlyScopeStillRemovesSelectedExtension(t *testing.T) {
	c := qt.New(t)
	current := &types.DBSchema{Extensions: []types.DBExtension{{Name: "pgcrypto"}}}
	desired := &goschema.Database{}
	scope := atlasfilter.Scope{Include: []string{"pgcrypto"}, DefaultSchema: "public"}

	from, to := scopeDiffStates(
		diffDatabaseState(current),
		atlassource.State{Schema: desired, DefaultSchema: "public"},
		scope,
		"postgres",
	)

	c.Assert(from.err, qt.IsNil)
	c.Assert(to.err, qt.IsNil)
	var empty *atlasfilter.EmptySelectionError
	c.Assert(to.selectionErr, qt.ErrorAs, &empty)
	applyExtensionSupportCoverage(to.schema, from.selection, to.selection)
	c.Assert(to.schema.NotDescribed.IsZero(), qt.IsTrue)
	diff := schemadiff.CompareWithDialect(to.schema, from.database, platform.Postgres)
	c.Assert(diff.ExtensionsAdded, qt.HasLen, 0)
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"pgcrypto"})
}

func TestValidateDiffSystemSchemaStatesRefusesAuthoredStates(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		fromState atlassource.State
		toState   atlassource.State
		wantFlag  string
		wantName  string
	}{
		{
			name:    "PostgreSQL current document",
			dialect: platform.Postgres,
			fromState: atlassource.State{Schema: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "pg_catalog"}},
			}},
			toState:  atlassource.State{Schema: &goschema.Database{}},
			wantFlag: "--from",
			wantName: "pg_catalog",
		},
		{
			name:      "PostgreSQL desired document",
			dialect:   platform.Postgres,
			fromState: atlassource.State{Schema: &goschema.Database{}},
			toState: atlassource.State{Schema: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "information_schema"}},
			}},
			wantFlag: "--to",
			wantName: "information_schema",
		},
		{
			name:      "CockroachDB desired document",
			dialect:   platform.CockroachDB,
			fromState: atlassource.State{Schema: &goschema.Database{}},
			toState: atlassource.State{Schema: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "crdb_internal"}},
			}},
			wantFlag: "--to",
			wantName: "crdb_internal",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := validateDiffSystemSchemaStates(test.fromState, test.toState, test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches,
				`validate `+test.wantFlag+` schema: .*declares server-owned PostgreSQL schema "`+
					test.wantName+`".*`)
		})
	}
}

func TestValidateDiffSystemSchemaStatesRefusesUnsafeObservedStates(t *testing.T) {
	pgCatalog := diffDatabaseState(&types.DBSchema{Schemas: []types.DBSchemaInfo{{Name: "pg_catalog"}}})
	pgCatalog.Kind = atlassource.KindDatabase
	informationSchema := diffDatabaseState(&types.DBSchema{Schemas: []types.DBSchemaInfo{{Name: "information_schema"}}})
	informationSchema.Kind = atlassource.KindDatabase
	crdbInternal := diffDatabaseState(&types.DBSchema{Schemas: []types.DBSchemaInfo{{Name: "crdb_internal"}}})
	crdbInternal.Kind = atlassource.KindDatabase
	replayedCatalog := diffDatabaseState(&types.DBSchema{Schemas: []types.DBSchemaInfo{{Name: "pg_catalog"}}})
	replayedCatalog.Kind = atlassource.KindMigrationDir
	empty := atlassource.State{Schema: &goschema.Database{}}
	tests := []struct {
		name      string
		dialect   string
		fromState atlassource.State
		toState   atlassource.State
		schema    string
		wantFlag  string
	}{
		{name: "PostgreSQL current database pg_catalog", dialect: platform.Postgres, fromState: pgCatalog, toState: empty, schema: "pg_catalog", wantFlag: "--from"},
		{name: "PostgreSQL desired database information_schema", dialect: platform.Postgres, fromState: empty, toState: informationSchema, schema: "information_schema", wantFlag: "--to"},
		{name: "CockroachDB database crdb_internal", dialect: platform.CockroachDB, fromState: crdbInternal, toState: empty, schema: "crdb_internal", wantFlag: "--from"},
		{name: "migration replay snapshot", dialect: platform.Postgres, fromState: replayedCatalog, toState: empty, schema: "pg_catalog", wantFlag: "--from"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := validateDiffSystemSchemaStates(test.fromState, test.toState, test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches,
				`validate `+test.wantFlag+` database schema: observed server-owned PostgreSQL schema "`+
					test.schema+`" cannot be compared safely; its catalog objects are not migration-managed state`)
		})
	}
}

func TestValidateDiffSystemSchemaStatesAllowsOrdinaryObservedStates(t *testing.T) {
	tests := []struct {
		name    string
		kind    atlassource.Kind
		dialect string
		schema  string
	}{
		{name: "database user schema", kind: atlassource.KindDatabase, dialect: platform.Postgres, schema: "app"},
		{name: "PostgreSQL quoted lookalike", kind: atlassource.KindDatabase, dialect: platform.Postgres, schema: "PG_CATALOG"},
		{name: "CockroachDB quoted lookalike", kind: atlassource.KindDatabase, dialect: platform.CockroachDB, schema: "CRDB_INTERNAL"},
		{name: "ordinary migration replay", kind: atlassource.KindMigrationDir, dialect: platform.Postgres, schema: "app"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &types.DBSchema{Schemas: []types.DBSchemaInfo{{Name: test.schema}}}
			state := diffDatabaseState(database)
			state.Kind = test.kind

			err := validateDiffSystemSchemaStates(state, state, test.dialect)

			c.Assert(err, qt.IsNil)
		})
	}
}

func diffDatabaseState(database *types.DBSchema) atlassource.State {
	return atlassource.State{
		Schema:        dbschematogo.ConvertDBSchemaToGoSchema(database),
		DB:            database,
		DefaultSchema: "public",
	}
}
