package builtin_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// These tests build the extension owner's models themselves, rather than read
// them from a source, and drive them through the bundled runtime the commands
// use.

// docsWithTrigrams declares pg_trgm in a schema of its own, at a version and
// with a comment, beside one table.
func docsWithTrigrams(extension pgextension.DesiredExtension) *schemamodel.Database {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{{StructName: "Doc", Name: "id", Type: "INTEGER", Primary: true}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgextension.DesiredObject(pgextension.Ref("pg_trgm"),
			extension)))),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired)),
	}
	schemamodel.Finalize(database)
	return database
}

// emptyPostgres is a database that holds no extension, and says so.
func emptyPostgres() *catalog.Database {
	return &catalog.Database{FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed))}
}

// TestPostgresDeclaredExtensionComesFirst pins a declared extension on the
// render surface and on the plan surface: the schema it is installed into is
// created first, then the extension and its comment, then the table that may
// use it.
func TestPostgresDeclaredExtensionComesFirst(t *testing.T) {
	c := qt.New(t)
	declared := docsWithTrigrams(pgextension.DesiredExtension{Schema: "ext", Version: "1.6", Comment: "trigram matching"})
	runtime := must.Must(builtin.New())

	rendered, err := builtin.GetOrderedCreateStatements(declared, platform.Postgres)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, emptyPostgres(), catalog.ServerInfo{Dialect: platform.Postgres}, nil, runtime)
	c.Assert(err, qt.IsNil)
	planned, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, platform.Postgres, planner.Options{})
	c.Assert(err, qt.IsNil)

	for _, statements := range [][]string{rendered, planned} {
		script := strings.Join(statements, "\n")
		order := []int{
			strings.Index(script, `CREATE SCHEMA IF NOT EXISTS "ext"`),
			strings.Index(script, `CREATE EXTENSION "pg_trgm" WITH SCHEMA "ext" VERSION '1.6'`),
			strings.Index(script, `COMMENT ON EXTENSION "pg_trgm" IS 'trigram matching'`),
			strings.Index(script, `CREATE TABLE "docs" (`),
		}
		c.Assert(order[0] >= 0 && slices.IsSorted(order), qt.IsTrue, qt.Commentf("%v\n%s", order, script))
		c.Assert(strings.Count(script, "EXTENSION"), qt.Equals, 2, qt.Commentf("%s", script))
	}
}

// TestPostgresRemovedExtensionGoesLast pins a drop: the extension goes after
// the table that may have used it.
func TestPostgresRemovedExtensionGoesLast(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired := &schemamodel.Database{FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired))}
	current := emptyPostgres()
	current.Tables = []catalog.Table{{Name: "docs", Schema: "public", Columns: []catalog.Column{{Name: "id", DataType: "integer"}}}}
	current.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(pgextension.ObservedObject(pgextension.Ref("pg_trgm"),
		pgextension.ObservedExtension{Schema: "public", Version: "1.6", Relocatable: true}))))

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current, catalog.ServerInfo{Dialect: platform.Postgres}, nil, runtime)
	c.Assert(err, qt.IsNil)
	planned, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, platform.Postgres, planner.Options{})

	c.Assert(err, qt.IsNil)
	script := strings.Join(planned, "\n")
	table := strings.Index(script, `DROP TABLE IF EXISTS "public"."docs"`)
	extension := strings.Index(script, `DROP EXTENSION IF EXISTS "pg_trgm"`)
	c.Assert([]bool{table >= 0, extension > table}, qt.DeepEquals, []bool{true, true}, qt.Commentf("%s", script))
}

// TestPostgresFamilyRendersDeclaredExtensions pins every target of the family:
// each installs a declared extension that names no schema.
func TestPostgresFamilyRendersDeclaredExtensions(t *testing.T) {
	for _, target := range []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(docsWithTrigrams(pgextension.DesiredExtension{IfNotExists: true}), target)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, `CREATE EXTENSION IF NOT EXISTS "pg_trgm"`)
		})
	}
}

// TestDeclaredExtensionRefusals pins where a declared extension cannot go: an
// installation schema on a target of the family without one, and YDB, which
// refuses what the owner skips elsewhere outside the family.
func TestDeclaredExtensionRefusals(t *testing.T) {
	tests := []struct {
		name   string
		target string
		want   string
	}{
		{name: "a schema on CockroachDB", target: platform.CockroachDB,
			want: `(?s).*cockroachdb does not support PostgreSQL extension installation schema "ext" for extension "pg_trgm".*`},
		{name: "a schema on Spanner", target: platform.Spanner,
			want: `(?s).*spanner does not support PostgreSQL extension installation schema "ext" for extension "pg_trgm".*`},
		{name: "YDB", target: platform.YDB, want: `PostgreSQL extension pg_trgm cannot be planned on ydb; scope its declaration to the ` +
			`targets that host it, as dialects="cockroachdb,postgres,spanner,yugabytedb" does in a Go annotation`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(docsWithTrigrams(pgextension.DesiredExtension{Schema: "ext"}), test.target)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestDeclaredExtensionRefusalsFailValidation pins that validation refuses
// what the render refuses: the owner's refusal of an installation schema on a
// target of the family without one, and YDB's of the extension. The control
// is PostgreSQL, which installs the extension where it is declared.
func TestDeclaredExtensionRefusalsFailValidation(t *testing.T) {
	tests := []struct {
		name   string
		target string
		want   string
	}{
		{name: "a schema on CockroachDB", target: platform.CockroachDB,
			want: `(?s).*cockroachdb does not support PostgreSQL extension installation schema "ext" for extension "pg_trgm".*`},
		{name: "a schema on Spanner", target: platform.Spanner,
			want: `(?s).*spanner does not support PostgreSQL extension installation schema "ext" for extension "pg_trgm".*`},
		{name: "YDB", target: platform.YDB, want: `(?s).*PostgreSQL extension pg_trgm cannot be planned on ydb.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := builtin.ValidateSchema(docsWithTrigrams(pgextension.DesiredExtension{Schema: "ext"}), test.target)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
	t.Run("PostgreSQL", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(builtin.ValidateSchema(docsWithTrigrams(pgextension.DesiredExtension{Schema: "ext"}), platform.Postgres), qt.IsNil)
	})
}

// TestOffFamilyTargetsSkipADeclaredExtension pins every target outside the
// family that skips a declared extension, on the render and the plan: one
// comment says so ahead of the statements, and the table is created as
// before. A comparison that differs only in the extension has no changes.
func TestOffFamilyTargetsSkipADeclaredExtension(t *testing.T) {
	for _, target := range []string{platform.MySQL, platform.MariaDB, platform.SQLite, platform.ClickHouse, platform.SQLServer, platform.Oracle} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			declared := docsWithTrigrams(pgextension.DesiredExtension{})
			skipped := "-- " + strings.ToUpper(target) + `: PostgreSQL extension pg_trgm is not supported by this target; skipped.`

			rendered, err := builtin.GetOrderedCreateStatements(declared, target)
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, &catalog.Database{}, catalog.ServerInfo{Dialect: target}, nil, runtime)
			c.Assert(err, qt.IsNil)
			planned, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, target, planner.Options{})
			c.Assert(err, qt.IsNil)

			c.Assert(rendered[0], qt.Equals, skipped)
			c.Assert(strings.Join(rendered, "\n"), qt.Contains, "docs")
			c.Assert(strings.SplitN(planned[0], "\n", 2)[0], qt.Equals, skipped)
			c.Assert(strings.Join(planned, "\n"), qt.Contains, "docs")
			c.Assert(diff.SkippedFeatures, qt.DeepEquals, []schemaext.SkippedObject{{Subject: pgextension.Ref("pg_trgm"),
				Label: "PostgreSQL extension", Target: target}})
		})
	}
}

// TestOffFamilyTargetSkipIsNotAChange pins that an extension a target skips
// is not a difference: against a database that holds the table, nothing
// changes, and the skip is still reported.
func TestOffFamilyTargetSkipIsNotAChange(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	current := &catalog.Database{Tables: []catalog.Table{{Name: "docs", Columns: []catalog.Column{{Name: "id", DataType: "int",
		IsPrimaryKey: true}}}}}

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), docsWithTrigrams(pgextension.DesiredExtension{}), current,
		catalog.ServerInfo{Dialect: platform.MySQL}, nil, runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff))
	c.Assert(diff.SkippedFeatures, qt.HasLen, 1)
}
