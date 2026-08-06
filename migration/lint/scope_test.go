package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

// analyzeScoped analyzes one selected up migration under a schema scope.
//
// The base file is version 1 and is never selected, so every object it creates
// exists before the analyzed statement runs. That is the only shape where an
// out-of-scope drop is distinguishable from a same-file create-then-drop, which
// is exempt for an unrelated reason.
func analyzeScoped(c *qt.C, base, sql, scope string) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(map[string]string{
		"1.sql": base,
		"2.sql": sql,
	}), lint.Options{
		Compatibility: lint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
		Dialect:       "postgres",
		SchemaScope:   scope,
		Selection:     lint.VersionSelection{Versions: []int64{2}, Restricted: true},
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// TestAnalyzeFS_SchemaScopeFiltersFindings pins which destructive statements a
// scoped lint run reports (stokaro/ptah#1074 S1).
//
// A run that validates against a dev database reviews the objects that database
// holds. When the dev URL names one schema, an object in a different schema was
// never in the before-state, so destroying it is not a covered change.
//
// Measured against the pinned community binary v1.3.0 on PostgreSQL 16 with
// `?search_path=public`: two `app` tables created in an earlier file and
// dropped in the next produce no diagnostic and exit 0, and so does the
// single-target form and the `app` DROP COLUMN form. The `public` rows are the
// controls that must not move -- both tools report them, and an unrestricted
// scope reports the `app` rows too, which is what proves the filter is the
// schema boundary rather than a blanket silencer.
func TestAnalyzeFS_SchemaScopeFiltersFindings(t *testing.T) {
	const appBase = "CREATE SCHEMA app;\nCREATE TABLE app.\"Users\" (id int);\nCREATE TABLE app.audit_log (id int);\n"
	const publicBase = "CREATE TABLE users (id int);\nCREATE TABLE audit_log (id int);\n"
	const mixedBase = "CREATE SCHEMA app;\nCREATE TABLE users (id int);\nCREATE TABLE app.audit_log (id int);\n"
	const appColumnBase = "CREATE SCHEMA app;\nCREATE TABLE app.users (id int, nick text);\n"

	tests := []struct {
		name  string
		base  string
		sql   string
		scope string
		want  []string
	}{
		{
			name:  "multi-target drop outside the scope is not reported",
			base:  appBase,
			sql:   "DROP TABLE app.\"Users\", app.audit_log;\n",
			scope: "public",
			want:  []string{},
		},
		{
			name:  "single-target drop outside the scope is not reported",
			base:  appBase,
			sql:   "DROP TABLE app.\"Users\";\n",
			scope: "public",
			want:  []string{},
		},
		{
			name:  "column drop outside the scope is not reported",
			base:  appColumnBase,
			sql:   "ALTER TABLE app.users DROP COLUMN nick;\n",
			scope: "public",
			want:  []string{},
		},
		{
			name:  "an unrestricted scope reports the same out-of-schema drop",
			base:  appBase,
			sql:   "DROP TABLE app.\"Users\", app.audit_log;\n",
			scope: "",
			want:  []string{"DS101", "DS101"},
		},
		{
			name:  "unqualified targets in the scope are reported",
			base:  publicBase,
			sql:   "DROP TABLE users, audit_log;\n",
			scope: "public",
			want:  []string{"DS101", "DS101"},
		},
		{
			name:  "a single unqualified target in the scope is reported",
			base:  publicBase,
			sql:   "DROP TABLE users;\n",
			scope: "public",
			want:  []string{"DS101"},
		},
		{
			name:  "targets qualified with the scope itself are reported",
			base:  "CREATE TABLE public.users (id int);\nCREATE TABLE public.audit_log (id int);\n",
			sql:   "DROP TABLE public.users, public.audit_log;\n",
			scope: "public",
			want:  []string{"DS101", "DS101"},
		},
		{
			name:  "only the in-scope target of a mixed drop is reported",
			base:  mixedBase,
			sql:   "DROP TABLE users, app.audit_log;\n",
			scope: "public",
			want:  []string{"DS101"},
		},
		{
			name:  "the scope itself is the reviewed schema, not always public",
			base:  appBase,
			sql:   "DROP TABLE app.\"Users\", app.audit_log;\n",
			scope: "app",
			want:  []string{"DS101", "DS101"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			analysis := analyzeScoped(c, test.base, test.sql, test.scope)

			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_SchemaScopeFiltersChanges pins the semantic schema-change count
// a scoped run reports, which is a separate output from the findings: it drives
// the `-- N schema changes` report line even on versions that raise no
// diagnostic at all.
//
// Measured against the pinned community binary v1.3.0 with
// `?search_path=public`: `CREATE SCHEMA app2; CREATE TABLE app2.t (id int);
// CREATE TABLE keep (id int);` counts one schema change, and the two-table `app`
// drop counts none. The `CREATE SCHEMA` row is what separates "a schema is
// measured by its own name" from "a schema is an unqualified object and
// therefore always in scope"; without it, the count would come out two.
func TestAnalyzeFS_SchemaScopeFiltersChanges(t *testing.T) {
	tests := []struct {
		name  string
		base  string
		sql   string
		scope string
		want  []changeProjection
	}{
		{
			name:  "creating a schema and its table counts only the in-scope table",
			base:  "CREATE TABLE seed (id int);\n",
			sql:   "CREATE SCHEMA app2;\nCREATE TABLE app2.t (id int);\nCREATE TABLE keep (id int);\n",
			scope: "public",
			want:  []changeProjection{{lint.SchemaChangeAdd, "keep"}},
		},
		{
			name:  "an unrestricted scope counts every one of them",
			base:  "CREATE TABLE seed (id int);\n",
			sql:   "CREATE SCHEMA app2;\nCREATE TABLE app2.t (id int);\nCREATE TABLE keep (id int);\n",
			scope: "",
			want: []changeProjection{
				{lint.SchemaChangeAdd, "app2"},
				{lint.SchemaChangeAdd, "app2.t"},
				{lint.SchemaChangeAdd, "keep"},
			},
		},
		{
			name:  "creating the reviewed schema itself is in scope",
			base:  "CREATE TABLE seed (id int);\n",
			sql:   "CREATE SCHEMA app2;\n",
			scope: "app2",
			want:  []changeProjection{{lint.SchemaChangeAdd, "app2"}},
		},
		{
			name:  "dropping tables outside the scope counts no change",
			base:  "CREATE SCHEMA app;\nCREATE TABLE app.a (id int);\nCREATE TABLE app.b (id int);\n",
			sql:   "DROP TABLE app.a, app.b;\n",
			scope: "public",
			want:  []changeProjection{},
		},
		{
			name:  "a mixed drop counts only its in-scope target",
			base:  "CREATE SCHEMA app;\nCREATE TABLE users (id int);\nCREATE TABLE app.audit_log (id int);\n",
			sql:   "DROP TABLE users, app.audit_log;\n",
			scope: "public",
			want:  []changeProjection{{lint.SchemaChangeDrop, "users"}},
		},
		{
			name:  "an ALTER TABLE is measured by its table, not by its column",
			base:  "CREATE SCHEMA app;\nCREATE TABLE app.users (id int, nick text);\n",
			sql:   "ALTER TABLE app.users DROP COLUMN nick;\n",
			scope: "public",
			want:  []changeProjection{},
		},
		{
			name:  "an in-scope ALTER TABLE still counts its column change",
			base:  "CREATE TABLE users (id int, nick text);\n",
			sql:   "ALTER TABLE users DROP COLUMN nick;\n",
			scope: "public",
			want:  []changeProjection{{lint.SchemaChangeDrop, "nick"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			analysis := analyzeScoped(c, test.base, test.sql, test.scope)

			c.Assert(projectChanges(fileByName(c, analysis, "2.sql").Changes), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_SchemaScopeKeepsUnattributableFindings proves the filter never
// silences a hazard whose object it cannot read.
//
// Statement rules report the statement rather than the objects in it, so they
// carry no subjects and there is nothing to measure a scope against. Dropping
// them would silence a destructive change on the strength of a boundary that was
// never established, which is the failure mode a scope filter is most likely to
// introduce.
func TestAnalyzeFS_SchemaScopeKeepsUnattributableFindings(t *testing.T) {
	tests := []struct {
		name string
		base string
		sql  string
		want []string
	}{
		{
			name: "TRUNCATE of an out-of-scope table stays reported",
			base: "CREATE SCHEMA app;\nCREATE TABLE app.users (id int);\n",
			sql:  "TRUNCATE TABLE app.users;\n",
			want: []string{"DS108"},
		},
		{
			name: "dropping an out-of-scope database object stays reported",
			base: "CREATE SCHEMA app;\nCREATE TABLE app.users (id int);\n",
			sql:  "DROP FUNCTION app.recalc();\n",
			want: []string{"DS107"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			analysis := analyzeScoped(c, test.base, test.sql, "public")

			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}
