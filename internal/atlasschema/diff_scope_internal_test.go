package atlasschema

// White-box testing required: scopeDiffState composes the generated desired
// projection with an authoritative catalog selection before either command
// adapter sees the result. Pure fixtures are needed to distinguish those two
// representations: goschema loses an extension's installation schema, while
// database filtering loses body dependencies and a column's domain identity.
// The qualified-extension case is also covered through the compat command and
// live PostgreSQL in
// TestSchemaDiffIncludeMatchesLiveExtensionOutsideTheDefaultSchemaPostgres.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
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
	c.Assert(matched.schema.NotDescribed, qt.DeepEquals, coverage.Set{}.WithKind(coverage.Sequence))

	missed := scopeDiffState(state, atlasfilter.Scope{
		Include:       []string{"extensions.typo"},
		DefaultSchema: state.DefaultSchema,
	}, "--from schema", "postgres")
	c.Assert(missed.err, qt.IsNil)
	var empty *atlasfilter.EmptySelectionError
	c.Assert(missed.selectionErr, qt.ErrorAs, &empty)
}

func TestValidateDesiredExtensionSchemas(t *testing.T) {
	tests := []struct {
		name string
		from []types.DBExtension
		to   []types.DBExtension
	}{
		{
			name: "identical non-default placement",
			from: []types.DBExtension{{Name: "pgcrypto", Schema: "extensions"}},
			to:   []types.DBExtension{{Name: "pgcrypto", Schema: "extensions"}},
		},
		{
			name: "default schema create",
			to:   []types.DBExtension{{Name: "pgcrypto", Schema: "public"}},
		},
		{
			name: "non-default drop",
			from: []types.DBExtension{{Name: "pgcrypto", Schema: "extensions"}},
		},
		{
			name: "ignored non-default create",
			to:   []types.DBExtension{{Name: "plpgsql", Schema: "pg_catalog"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(validateDesiredExtensionSchemas(
				&types.DBSchema{Extensions: test.from},
				&types.DBSchema{Extensions: test.to},
				"public",
				config.DefaultCompareOptions(),
			), qt.IsNil)
		})
	}
}

func TestValidateDesiredExtensionSchemasRefusesUnrepresentablePlacement(t *testing.T) {
	tests := []struct {
		name    string
		from    []types.DBExtension
		to      []types.DBExtension
		wantErr string
	}{
		{
			name:    "non-default schema create",
			to:      []types.DBExtension{{Name: "pgcrypto", Schema: "extensions"}},
			wantErr: `cannot create extension "pgcrypto" in schema "extensions": schema diff cannot represent PostgreSQL extension installation schemas`,
		},
		{
			name:    "ignored name prefix does not suppress user extension",
			to:      []types.DBExtension{{Name: "plpgsql_extra", Schema: "extensions"}},
			wantErr: `cannot create extension "plpgsql_extra" in schema "extensions": schema diff cannot represent PostgreSQL extension installation schemas`,
		},
		{
			name: "placement change",
			from: []types.DBExtension{{Name: "pgcrypto", Schema: "public"}},
			to:   []types.DBExtension{{Name: "pgcrypto", Schema: "extensions"}},
			wantErr: `cannot move extension "pgcrypto" from schema "public" to schema "extensions": ` +
				`schema diff cannot represent PostgreSQL extension installation schemas`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(validateDesiredExtensionSchemas(
				&types.DBSchema{Extensions: test.from},
				&types.DBSchema{Extensions: test.to},
				"public",
				config.DefaultCompareOptions(),
			), qt.ErrorMatches, test.wantErr)
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
