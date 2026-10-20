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
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

func TestScopeDiffStatePreservesGeneratedDependencyValidation(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{
		Tables: []catalog.Table{{Name: "users"}},
		Views:  []catalog.View{{Name: "active_users", Body: "SELECT * FROM users"}},
	}
	state := diffDatabaseState(database)

	got := scopeDiffState(t.Context(), state, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Include:       []string{"active_users"},
		DefaultSchema: state.DefaultSchema,
	}, "--to schema", "postgres", must.Must(builtin.New()))

	var crossScope *atlasfilter.CrossScopeError
	c.Assert(got.err, qt.ErrorAs, &crossScope)
	c.Assert(crossScope.Diagnostics, qt.DeepEquals, []string{
		`view "active_users" references "users", but "users" is not selected`,
	})
}

func TestScopeDiffStatePreservesGeneratedDomainDependencies(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "measurements",
			Columns: []catalog.Column{{
				Name:          "value",
				DataType:      "integer",
				UDTName:       "int4",
				DomainName:    "positive",
				FormattedType: "doms.positive",
			}},
		}},
		FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{
			Ref: pgtype.Ref(pgtype.DomainKind, "doms", "positive"), Value: &pgtype.ObservedDomain{BaseType: "integer", Check: "VALUE > 0"},
		})),
	}
	state := diffDatabaseState(database)

	got := scopeDiffState(t.Context(), state, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Include:       []string{"measurements"},
		DefaultSchema: state.DefaultSchema,
	}, "--to schema", "postgres", must.Must(builtin.New()))

	c.Assert(got.err, qt.IsNil)
	c.Assert(got.schema.Fields, qt.HasLen, 1)
	c.Assert(got.schema.Fields[0].Type, qt.Equals, "doms.positive")
	domain, found, err := got.schema.FeatureObjects.Get(pgtype.Ref(pgtype.DomainKind, "doms", "positive"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(domain.Value, qt.DeepEquals, &pgtype.DesiredDomain{BaseType: "integer", Check: "VALUE > 0"})
}

func TestScopeDiffStatePreservesQualifiedDatabaseFunctionIdentity(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{
		Schemas: []catalog.Schema{{Name: "extra"}},
		Functions: []catalog.Function{{
			Schema:   "extra",
			Name:     "fn",
			Returns:  "integer",
			Language: "sql",
			Body:     "SELECT 1",
		}},
	}
	state := diffDatabaseState(database)

	got := scopeDiffState(t.Context(), state, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Include:       []string{"extra.fn"},
		DefaultSchema: state.DefaultSchema,
	}, "--to schema", "postgres", must.Must(builtin.New()))

	c.Assert(got.err, qt.IsNil)
	c.Assert(got.selectionErr, qt.IsNil)
	c.Assert(got.database.Functions, qt.DeepEquals, database.Functions)
	c.Assert(got.database.Schemas, qt.DeepEquals, database.Schemas)
	c.Assert(got.schema.Functions, qt.HasLen, 1)
	c.Assert(got.schema.Functions[0].Name, qt.Equals, "extra.fn")
	c.Assert(got.schema.Schemas, qt.DeepEquals, []schemamodel.Schema{{Name: "extra"}})
}

func TestScopeDiffStateExcludesQualifiedDatabaseEnumSymmetrically(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{
		Schemas: []catalog.Schema{{Name: "app"}},
		Enums:   []catalog.Enum{{Schema: "app", Name: "color", Values: []string{"red"}}},
	}
	state := diffDatabaseState(database)

	got := scopeDiffState(t.Context(), state, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Include:       []string{"app.color"},
		Exclude:       []string{"app.color"},
		DefaultSchema: state.DefaultSchema,
	}, "--to schema", "postgres", must.Must(builtin.New()))

	c.Assert(got.err, qt.IsNil)
	c.Assert(got.selectionErr, qt.IsNil)
	c.Assert(got.database.Enums, qt.HasLen, 0)
	c.Assert(got.schema.Enums, qt.HasLen, 0)
	c.Assert(got.database.Schemas, qt.DeepEquals, database.Schemas)
	c.Assert(got.schema.Schemas, qt.DeepEquals, []schemamodel.Schema{{Name: "app"}})
}

func TestScopeDiffStatesBareExcludePreservesCatalogSchemaSpelling(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		Schemas: []catalog.Schema{{Name: "Sales"}},
		Tables:  []catalog.Table{{Schema: "Sales", Name: "orders"}},
	}
	scope := atlasfilter.Scope{Selections: ownerSelections(), Exclude: []string{"Sales"}, DefaultSchema: "dbo"}

	from, to := scopeDiffStates(
		t.Context(), diffDatabaseState(current),
		atlassource.State{Schema: &schemamodel.Database{}, DefaultSchema: "dbo"},
		scope,
		platform.SQLServer, must.Must(builtin.New()),
	)

	c.Assert(from.err, qt.IsNil)
	c.Assert(to.err, qt.IsNil)
	c.Assert(from.report.Unmatched, qt.IsNil)
	c.Assert(from.database.Schemas, qt.HasLen, 0)
	c.Assert(from.database.Tables, qt.HasLen, 0)
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), to.schema, from.database, platform.SQLServer, must.Must(builtin.New())))
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

func TestScopeDiffStateDoesNotMergeUnrelatedCatalogType(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name:   "users",
			Schema: "public",
			Columns: []catalog.Column{{
				Name:     "id",
				DataType: "integer",
				UDTName:  "int4",
			}},
		}},
		FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{
			Ref: pgtype.Ref(pgtype.DomainKind, "app", "int4"), Value: &pgtype.ObservedDomain{BaseType: "integer", Check: "VALUE > 0"},
		})),
	}
	state := diffDatabaseState(database)

	got := scopeDiffState(t.Context(), state, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Include:       []string{"public.users"},
		DefaultSchema: state.DefaultSchema,
	}, "--to schema", "postgres", must.Must(builtin.New()))

	c.Assert(got.err, qt.IsNil)
	c.Assert(got.selectionErr, qt.IsNil)
	c.Assert(got.schema.Tables, qt.HasLen, 1)
	c.Assert(got.schema.FeatureObjects.Refs(), qt.HasLen, 0)
}

func TestScopeDiffStateUsesDatabaseIdentityForSelectionOutcome(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{
		FeatureObjects:  installedExtensions(installedExtension("pgcrypto", "extensions", "1.3")),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
		NotDescribed:    coverage.Set{}.WithKind(coverage.Sequence),
	}
	state := diffDatabaseState(database)

	matched := scopeDiffState(t.Context(), state, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Include:       []string{"extensions.pgcrypto"},
		DefaultSchema: state.DefaultSchema,
	}, "--from schema", "postgres", must.Must(builtin.New()))
	c.Assert(matched.err, qt.IsNil)
	c.Assert(matched.selectionErr, qt.IsNil)
	c.Assert(pgextension.Names(matched.database.FeatureObjects), qt.DeepEquals, []string{"pgcrypto"})
	c.Assert(extensionPlacements(c, matched.schema.FeatureObjects), qt.DeepEquals, map[string]string{"pgcrypto": "extensions"})
	c.Assert(matched.schema.NotDescribed, qt.DeepEquals, coverage.Set{}.WithKind(coverage.Sequence))

	missed := scopeDiffState(t.Context(), state, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Include:       []string{"extensions.typo"},
		DefaultSchema: state.DefaultSchema,
	}, "--from schema", "postgres", must.Must(builtin.New()))
	c.Assert(missed.err, qt.IsNil)
	var empty *atlasfilter.EmptySelectionError
	c.Assert(missed.selectionErr, qt.ErrorAs, &empty)
}

func TestScopeDiffStatePreservesDatabaseWideExtensionsAcrossSchemaUniverse(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{
		Schemas: []catalog.Schema{{Name: "app"}, {Name: "extensions"}, {Name: "other"}},
		Tables: []catalog.Table{{
			Schema: "app",
			Name:   "users",
			Columns: []catalog.Column{{
				Name: "email", DataType: "USER-DEFINED", UDTName: "citext", FormattedType: "extensions.citext",
			}},
		}},
		FeatureObjects: installedExtensions(
			installedExtension("pgcrypto", "", ""),
			installedExtension("citext", "extensions", ""),
			installedExtension("unrelated", "other", ""),
		),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
	}
	state := diffDatabaseState(database)

	got := scopeDiffState(t.Context(), state, atlasfilter.Scope{
		Selections:    ownerSelections(),
		Schemas:       []string{"app"},
		Include:       []string{"app.users", "other.unrelated"},
		DefaultSchema: state.DefaultSchema,
	}, "--to schema", "postgres", must.Must(builtin.New()))

	c.Assert(got.err, qt.IsNil)
	c.Assert(got.selectionErr, qt.IsNil)
	c.Assert(pgextension.Names(got.database.FeatureObjects), qt.DeepEquals, []string{"citext", "pgcrypto", "unrelated"})
	c.Assert(extensionPlacements(c, got.schema.FeatureObjects), qt.DeepEquals,
		map[string]string{"citext": "extensions", "pgcrypto": "", "unrelated": "other"})
	c.Assert(got.selection.NonExtensionMatched, qt.IsTrue)
	c.Assert(got.schema.Fields, qt.HasLen, 1)
	c.Assert(got.schema.Fields[0].Type, qt.Equals, "extensions.citext")

	empty := &catalog.Database{FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed))}
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), got.schema, empty, platform.Postgres, must.Must(builtin.New())))
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		context.Background(), must.Must(builtin.New()),
		diff, platform.Postgres,
	)
	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, ";\n")
	c.Assert(sql, qt.Contains, `CREATE SCHEMA IF NOT EXISTS "extensions"`)
	c.Assert(sql, qt.Contains, `CREATE EXTENSION IF NOT EXISTS "citext" WITH SCHEMA "extensions"`)
	c.Assert(strings.Index(sql, `CREATE SCHEMA IF NOT EXISTS "extensions"`) <
		strings.Index(sql, `CREATE EXTENSION IF NOT EXISTS "citext" WITH SCHEMA "extensions"`), qt.IsTrue)
}

func TestNonExtensionScopeDoesNotRemoveUnmentionedCurrentExtension(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		Tables:          []catalog.Table{{Schema: "app", Name: "users"}},
		FeatureObjects:  installedExtensions(installedExtension("citext", "extensions", ""), installedExtension("pgcrypto", "", "")),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
	}
	desired := &schemamodel.Database{
		Tables:          []schemamodel.Table{{StructName: "User", Schema: "app", Name: "users"}},
		FeatureObjects:  declaredExtensions(declaredExtension("citext", "extensions")),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired)),
	}
	scope := atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"app.users"}, DefaultSchema: "public"}

	from, to := scopeDiffStates(
		t.Context(), diffDatabaseState(current),
		atlassource.State{Schema: desired, DefaultSchema: "public"},
		scope,
		"postgres", must.Must(builtin.New()),
	)

	c.Assert(from.err, qt.IsNil)
	c.Assert(to.err, qt.IsNil)
	c.Assert(pgextension.Names(from.database.FeatureObjects), qt.DeepEquals, []string{"citext", "pgcrypto"})
	c.Assert(pgextension.Names(to.schema.FeatureObjects), qt.DeepEquals, []string{"citext"})
	applyExtensionSupportCoverage(to.schema, from.selection, to.selection)
	c.Assert(to.schema.NotDescribed, qt.DeepEquals, coverage.Set{}.With(coverage.Object{
		Kind:       coverage.Extension,
		Reason:     coverage.OutsideScope,
		Provenance: coverage.Configured,
	}))
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), to.schema, from.database, platform.Postgres, must.Must(builtin.New())))
	added, removed := extensionChanges(diff)
	c.Assert(added, qt.HasLen, 0)
	c.Assert(removed, qt.HasLen, 0)
}

func TestCurrentOnlyNonExtensionMatchDoesNotRemoveUnrelatedExtension(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		Tables:          []catalog.Table{{Schema: "app", Name: "users"}},
		FeatureObjects:  installedExtensions(installedExtension("pgcrypto", "", "")),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
	}
	scope := atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"app.users"}, DefaultSchema: "public"}

	from, to := scopeDiffStates(
		t.Context(), diffDatabaseState(current),
		atlassource.State{Schema: &schemamodel.Database{FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired))},
			DefaultSchema: "public"},
		scope,
		"postgres", must.Must(builtin.New()),
	)

	c.Assert(from.err, qt.IsNil)
	c.Assert(to.err, qt.IsNil)
	c.Assert(from.selection.NonExtensionMatched, qt.IsTrue)
	c.Assert(to.selection.NonExtensionMatched, qt.IsFalse)
	applyExtensionSupportCoverage(to.schema, from.selection, to.selection)
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), to.schema, from.database, platform.Postgres, must.Must(builtin.New())))
	c.Assert(diff.TablesRemoved.Names(), qt.HasLen, 1)
	_, removed := extensionChanges(diff)
	c.Assert(removed, qt.HasLen, 0)
}

func TestDesiredOnlyNonExtensionMatchStillAddsDeclaredExtension(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed))}
	desired := &schemamodel.Database{
		Tables:          []schemamodel.Table{{StructName: "User", Schema: "app", Name: "users"}},
		FeatureObjects:  declaredExtensions(declaredExtension("citext", "extensions")),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired)),
	}
	scope := atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"app.users"}, DefaultSchema: "public"}

	from, to := scopeDiffStates(
		t.Context(), diffDatabaseState(current),
		atlassource.State{Schema: desired, DefaultSchema: "public"},
		scope,
		"postgres", must.Must(builtin.New()),
	)

	c.Assert(from.err, qt.IsNil)
	c.Assert(to.err, qt.IsNil)
	c.Assert(pgextension.Names(from.database.FeatureObjects), qt.HasLen, 0)
	c.Assert(pgextension.Names(to.schema.FeatureObjects), qt.DeepEquals, []string{"citext"})
	applyExtensionSupportCoverage(to.schema, from.selection, to.selection)
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), to.schema, from.database, platform.Postgres, must.Must(builtin.New())))
	added, removed := extensionChanges(diff)
	c.Assert(added, qt.DeepEquals, []string{"citext"})
	c.Assert(removed, qt.HasLen, 0)
	c.Assert(diff.TablesAdded, qt.HasLen, 1)
}

func TestDesiredOnlyNonExtensionMatchDoesNotReAddCurrentSupportExtension(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		FeatureObjects:  installedExtensions(installedExtension("citext", "extensions", ""), installedExtension("pgcrypto", "", "")),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
	}
	desired := &schemamodel.Database{
		Tables:          []schemamodel.Table{{StructName: "User", Schema: "app", Name: "users"}},
		FeatureObjects:  declaredExtensions(declaredExtension("citext", "extensions"), declaredExtension("pgcrypto", "")),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired)),
	}
	scope := atlasfilter.Scope{
		Selections:    ownerSelections(),
		Include:       []string{"app.users", "extensions.citext"},
		DefaultSchema: "public",
	}

	from, to := scopeDiffStates(
		t.Context(), diffDatabaseState(current),
		atlassource.State{Schema: desired, DefaultSchema: "public"},
		scope,
		"postgres", must.Must(builtin.New()),
	)

	c.Assert(from.err, qt.IsNil)
	c.Assert(to.err, qt.IsNil)
	c.Assert(pgextension.Names(from.database.FeatureObjects), qt.DeepEquals, []string{"citext", "pgcrypto"})
	c.Assert(pgextension.Names(to.schema.FeatureObjects), qt.DeepEquals, []string{"citext", "pgcrypto"})
	applyExtensionSupportCoverage(to.schema, from.selection, to.selection)
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), to.schema, from.database, platform.Postgres, must.Must(builtin.New())))
	added, removed := extensionChanges(diff)
	c.Assert(added, qt.HasLen, 0)
	c.Assert(removed, qt.HasLen, 0)
	c.Assert(diff.TablesAdded, qt.HasLen, 1)
}

func TestExtensionOnlyScopeStillRemovesSelectedExtension(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		FeatureObjects:  installedExtensions(installedExtension("pgcrypto", "", "")),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
	}
	desired := &schemamodel.Database{FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired))}
	scope := atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"pgcrypto"}, DefaultSchema: "public"}

	from, to := scopeDiffStates(
		t.Context(), diffDatabaseState(current),
		atlassource.State{Schema: desired, DefaultSchema: "public"},
		scope,
		"postgres", must.Must(builtin.New()),
	)

	c.Assert(from.err, qt.IsNil)
	c.Assert(to.err, qt.IsNil)
	var empty *atlasfilter.EmptySelectionError
	c.Assert(to.selectionErr, qt.ErrorAs, &empty)
	applyExtensionSupportCoverage(to.schema, from.selection, to.selection)
	c.Assert(to.schema.NotDescribed.IsZero(), qt.IsTrue)
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), to.schema, from.database, platform.Postgres, must.Must(builtin.New())))
	added, removed := extensionChanges(diff)
	c.Assert(added, qt.HasLen, 0)
	c.Assert(removed, qt.DeepEquals, []string{"pgcrypto"})
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
			fromState: atlassource.State{Schema: &schemamodel.Database{
				Schemas: []schemamodel.Schema{{Name: "pg_catalog"}},
			}},
			toState:  atlassource.State{Schema: &schemamodel.Database{}},
			wantFlag: "--from",
			wantName: "pg_catalog",
		},
		{
			name:      "PostgreSQL desired document",
			dialect:   platform.Postgres,
			fromState: atlassource.State{Schema: &schemamodel.Database{}},
			toState: atlassource.State{Schema: &schemamodel.Database{
				Schemas: []schemamodel.Schema{{Name: "information_schema"}},
			}},
			wantFlag: "--to",
			wantName: "information_schema",
		},
		{
			name:      "CockroachDB desired document",
			dialect:   platform.CockroachDB,
			fromState: atlassource.State{Schema: &schemamodel.Database{}},
			toState: atlassource.State{Schema: &schemamodel.Database{
				Schemas: []schemamodel.Schema{{Name: "crdb_internal"}},
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
	pgCatalog := diffDatabaseState(&catalog.Database{Schemas: []catalog.Schema{{Name: "pg_catalog"}}})
	pgCatalog.Kind = atlassource.KindDatabase
	informationSchema := diffDatabaseState(&catalog.Database{Schemas: []catalog.Schema{{Name: "information_schema"}}})
	informationSchema.Kind = atlassource.KindDatabase
	crdbInternal := diffDatabaseState(&catalog.Database{Schemas: []catalog.Schema{{Name: "crdb_internal"}}})
	crdbInternal.Kind = atlassource.KindDatabase
	replayedCatalog := diffDatabaseState(&catalog.Database{Schemas: []catalog.Schema{{Name: "pg_catalog"}}})
	replayedCatalog.Kind = atlassource.KindMigrationDir
	empty := atlassource.State{Schema: &schemamodel.Database{}}
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
			database := &catalog.Database{Schemas: []catalog.Schema{{Name: test.schema}}}
			state := diffDatabaseState(database)
			state.Kind = test.kind

			err := validateDiffSystemSchemaStates(state, state, test.dialect)

			c.Assert(err, qt.IsNil)
		})
	}
}

func diffDatabaseState(current *catalog.Database) atlassource.State {
	return atlassource.State{
		Schema:        must.Must(dbschematogo.ConvertDBSchemaToGoSchema(context.Background(), current, "postgres", must.Must(builtin.New()))),
		DB:            current,
		DefaultSchema: "public",
	}
}

// installedExtension is an extension a PostgreSQL read hands to the extension
// owner.
func installedExtension(name, schema, version string) schemaext.Object {
	return must.Must(pgextension.ObservedObject(pgextension.Ref(name), pgextension.ObservedExtension{Schema: schema, Version: version}))
}

func installedExtensions(objects ...schemaext.Object) schemaext.Objects {
	return must.Must(schemaext.NewObjects(objects...))
}

// declaredExtension is an extension a source declares to the extension owner.
func declaredExtension(name, schema string) schemaext.Object {
	return must.Must(pgextension.DesiredObject(pgextension.Ref(name), pgextension.DesiredExtension{Schema: schema}))
}

func declaredExtensions(objects ...schemaext.Object) schemaext.Objects {
	return must.Must(schemaext.NewObjects(objects...))
}

// extensionPlacements is the installation schema of each declared extension,
// by name.
func extensionPlacements(c *qt.C, objects schemaext.Objects) map[string]string {
	c.Helper()
	all, err := objects.All()
	c.Assert(err, qt.IsNil)
	placements := make(map[string]string)
	for _, object := range all {
		if extension, ok := object.Value.(*pgextension.DesiredExtension); ok {
			placements[pgextension.Name(object.Ref)] = extension.Schema
		}
	}
	return placements
}

// extensionChanges names the extensions a diff installs and drops, from the
// extension owner's changes.
func extensionChanges(diff *difftypes.SchemaDiff) (added, removed []string) {
	for _, record := range diff.FeatureChanges {
		change, ok := record.Value.(*pgextension.Change)
		switch {
		case !ok:
		case change.Before == nil:
			added = append(added, pgextension.Name(record.Subject))
		case change.After == nil:
			removed = append(removed, pgextension.Name(record.Subject))
		}
	}
	return added, removed
}
