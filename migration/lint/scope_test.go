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

// TestAnalyzeFS_SchemaScopeDecidesOncePerStatement pins that the reported
// change count and the reported findings describe the same set of statements
// (stokaro/ptah#1249).
//
// Two shapes disagreed before. An `ALTER TABLE app.users ADD CONSTRAINT ...`
// contributed no change under a run reviewing `public` and still raised PG105,
// because that rule attaches no subject and the finding filter deliberately
// keeps a finding it cannot place. A `CREATE INDEX idx ON app.users (id)`
// contributed a change and raised PG101, because an index was measured by its
// own name, which never carries a schema.
//
// Each row asserts both outputs, because either alone would accept a fix that
// silenced one side. The in-scope rows are the controls that separate "correctly
// scoped out" from "never analyzed": without the `public` index row a filter
// that dropped every index would pass, and without the unrestricted rows a fix
// that narrowed the native surface too would pass.
func TestAnalyzeFS_SchemaScopeDecidesOncePerStatement(t *testing.T) {
	const appTable = "CREATE SCHEMA app;\nCREATE TABLE app.users (id int, nick text);\n"
	const publicTable = "CREATE TABLE public.t (id int, c text);\n"

	tests := []struct {
		name         string
		base         string
		sql          string
		scope        string
		wantChanges  []changeProjection
		wantFindings []string
	}{
		{
			name:         "a constraint added outside the scope reports neither",
			base:         appTable,
			sql:          "ALTER TABLE app.users ADD CONSTRAINT users_id_key UNIQUE (id);\n",
			scope:        "public",
			wantChanges:  []changeProjection{},
			wantFindings: []string{},
		},
		{
			name:         "an index on a table outside the scope reports neither",
			base:         appTable,
			sql:          "CREATE INDEX idx ON app.users (id);\n",
			scope:        "public",
			wantChanges:  []changeProjection{},
			wantFindings: []string{},
		},
		{
			name:         "a column dropped inside the scope reports both",
			base:         publicTable,
			sql:          "ALTER TABLE public.t DROP COLUMN c;\n",
			scope:        "public",
			wantChanges:  []changeProjection{{lint.SchemaChangeDrop, "c"}},
			wantFindings: []string{"DS102"},
		},
		{
			name:         "a column dropped outside the scope reports neither",
			base:         "CREATE SCHEMA app;\nCREATE TABLE app.t (id int, c text);\n",
			sql:          "ALTER TABLE app.t DROP COLUMN c;\n",
			scope:        "public",
			wantChanges:  []changeProjection{},
			wantFindings: []string{},
		},
		{
			name:         "an index on a table inside the scope reports both",
			base:         "CREATE TABLE public.users (id int);\n",
			sql:          "CREATE INDEX idx ON public.users (id);\n",
			scope:        "public",
			wantChanges:  []changeProjection{{lint.SchemaChangeAdd, "idx"}},
			wantFindings: []string{"PG101"},
		},
		{
			name:         "an index on an unqualified table stays under review",
			base:         "CREATE TABLE users (id int);\n",
			sql:          "CREATE INDEX idx ON users (id);\n",
			scope:        "public",
			wantChanges:  []changeProjection{{lint.SchemaChangeAdd, "idx"}},
			wantFindings: []string{"PG101"},
		},
		{
			name:         "an unrestricted run still reports the out-of-schema index",
			base:         appTable,
			sql:          "CREATE INDEX idx ON app.users (id);\n",
			scope:        "",
			wantChanges:  []changeProjection{{lint.SchemaChangeAdd, "idx"}},
			wantFindings: []string{"PG101"},
		},
		{
			name:         "an unrestricted run still reports the out-of-schema constraint",
			base:         appTable,
			sql:          "ALTER TABLE app.users ADD CONSTRAINT users_id_key UNIQUE (id);\n",
			scope:        "",
			wantChanges:  []changeProjection{{lint.SchemaChangeAdd, "users_id_key"}},
			wantFindings: []string{"PG105"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			analysis := analyzeScoped(c, test.base, test.sql, test.scope)

			c.Assert(projectChanges(fileByName(c, analysis, "2.sql").Changes), qt.DeepEquals, test.wantChanges)
			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.wantFindings)
		})
	}
}

// TestAnalyzeFS_SchemaScopeRemovesOnlyStatementsNamingNothingUnderReview pins
// that a statement is removed only when every table it names is out of review,
// not only the one the altered table's name carries (stokaro/ptah#1300).
//
// `ALTER TABLE app.child ADD CONSTRAINT ... FOREIGN KEY (pid) REFERENCES
// public.parent (id)` names two tables. PostgreSQL validates every existing row
// and holds a SHARE ROW EXCLUSIVE lock on `public.parent` for the duration,
// which is exactly what `PG306` reports, so under a run reviewing `public` the
// hazard lands on a table the run is responsible for. Measured on the pinned
// community binary v1.3.0 with `?search_path=public`: it reports no diagnostic
// and exits 0, so keeping `PG306` is Ptah being stricter at exit 0 rather than
// looser, and it is the behavior the base branch already had.
//
// Both outputs are asserted on every row because they answer different
// questions here: the constraint lands on `app.child`, so the change count stays
// zero on the in-scope rows too. Not being scoped out is not the same as
// contributing a change, and a row that asserted only the count would accept a
// fix that silenced the diagnostic.
func TestAnalyzeFS_SchemaScopeRemovesOnlyStatementsNamingNothingUnderReview(t *testing.T) {
	const base = "CREATE TABLE public.parent (id int PRIMARY KEY);\n" +
		"CREATE SCHEMA app;\n" +
		"CREATE TABLE app.parent (id int PRIMARY KEY);\n" +
		"CREATE TABLE app.child (id int, pid int);\n" +
		"CREATE TABLE app.users (id int);\n"

	tests := []struct {
		name         string
		sql          string
		wantChanges  []changeProjection
		wantFindings []string
	}{
		{
			name:         "a foreign key referencing the reviewed schema keeps its statement under review",
			sql:          "ALTER TABLE app.child ADD CONSTRAINT child_pid_fkey FOREIGN KEY (pid) REFERENCES public.parent (id);\n",
			wantChanges:  []changeProjection{},
			wantFindings: []string{"PG306"},
		},
		{
			name:         "a foreign key referencing only the unreviewed schema reports neither",
			sql:          "ALTER TABLE app.child ADD CONSTRAINT child_pid_fkey FOREIGN KEY (pid) REFERENCES app.parent (id);\n",
			wantChanges:  []changeProjection{},
			wantFindings: []string{},
		},
		{
			name:         "an unqualified foreign key reference resolves into the reviewed schema",
			sql:          "ALTER TABLE app.child ADD CONSTRAINT child_pid_fkey FOREIGN KEY (pid) REFERENCES parent (id);\n",
			wantChanges:  []changeProjection{},
			wantFindings: []string{"PG306"},
		},
		{
			name:         "a foreign key written inline on an added column is the same reference",
			sql:          "ALTER TABLE app.users ADD COLUMN pid int REFERENCES public.parent (id), ADD CONSTRAINT users_id_key UNIQUE (id);\n",
			wantChanges:  []changeProjection{},
			wantFindings: []string{"PG105"},
		},
		{
			name:         "an inline foreign key outside the reviewed schema reports neither",
			sql:          "ALTER TABLE app.users ADD COLUMN pid int REFERENCES app.parent (id), ADD CONSTRAINT users_id_key UNIQUE (id);\n",
			wantChanges:  []changeProjection{},
			wantFindings: []string{},
		},
		{
			name:         "a constraint naming no other table is still removed with its statement",
			sql:          "ALTER TABLE app.users ADD CONSTRAINT users_id_key UNIQUE (id);\n",
			wantChanges:  []changeProjection{},
			wantFindings: []string{},
		},
		{
			name:         "a foreign key added inside the reviewed schema reports both",
			sql:          "ALTER TABLE public.parent ADD CONSTRAINT parent_self_fkey FOREIGN KEY (id) REFERENCES public.parent (id);\n",
			wantChanges:  []changeProjection{{lint.SchemaChangeAdd, "parent_self_fkey"}},
			wantFindings: []string{"PG306"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			analysis := analyzeScoped(c, base, test.sql, "public")

			c.Assert(projectChanges(fileByName(c, analysis, "2.sql").Changes), qt.DeepEquals, test.wantChanges)
			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.wantFindings)
		})
	}
}

// TestAnalyzeFS_SchemaScopeExcludesOnlyTheScopedOutStatement pins that the
// exclusion applies to the statement the scope removed and to no other.
//
// The set is keyed by statement index, and every other fixture here analyzes a
// single-statement version, so nothing else can tell an off-by-one in that
// lookup from correct behavior: a filter that dropped the statement before or
// after a scoped-out one would pass the rest of this file.
//
// Measured with `?search_path=public`: the two-statement version reports one
// change and one diagnostic both on the pinned community binary v1.3.0 (`MF101`)
// and here (`PG105`), where the base branch reported two of each.
func TestAnalyzeFS_SchemaScopeExcludesOnlyTheScopedOutStatement(t *testing.T) {
	const base = "CREATE SCHEMA app;\nCREATE TABLE app.users (id int);\nCREATE TABLE public.t (id int);\n"
	const inScope = "ALTER TABLE public.t ADD CONSTRAINT t_id_key UNIQUE (id);\n"
	const scopedOut = "CREATE INDEX idx ON app.users (id);\n"

	tests := []struct {
		name         string
		sql          string
		wantChanges  []changeProjection
		wantFindings []string
	}{
		{
			name:         "the statement before a scoped-out one keeps its change and its finding",
			sql:          inScope + scopedOut,
			wantChanges:  []changeProjection{{lint.SchemaChangeAdd, "t_id_key"}},
			wantFindings: []string{"PG105"},
		},
		{
			name:         "the statement after a scoped-out one keeps its change and its finding",
			sql:          scopedOut + inScope,
			wantChanges:  []changeProjection{{lint.SchemaChangeAdd, "t_id_key"}},
			wantFindings: []string{"PG105"},
		},
		{
			name:         "a scoped-out statement between two in-scope ones removes only itself",
			sql:          inScope + scopedOut + "ALTER TABLE public.t ADD CONSTRAINT t_id_check CHECK (id > 0);\n",
			wantChanges:  []changeProjection{{lint.SchemaChangeAdd, "t_id_key"}, {lint.SchemaChangeAdd, "t_id_check"}},
			wantFindings: []string{"PG105", "PG305"},
		},
		{
			name:         "two scoped-out statements report neither",
			sql:          scopedOut + "ALTER TABLE app.users ADD CONSTRAINT users_id_key UNIQUE (id);\n",
			wantChanges:  []changeProjection{},
			wantFindings: []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			analysis := analyzeScoped(c, base, test.sql, "public")

			c.Assert(projectChanges(fileByName(c, analysis, "2.sql").Changes), qt.DeepEquals, test.wantChanges)
			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.wantFindings)
		})
	}
}

// TestAnalyzeFS_SchemaScopeLeavesUnmodeledStatementsAlone pins that a statement
// Ptah's SQL parser cannot model keeps its findings under every scope, and that
// the scope is not what suppresses its change count.
//
// `DROP INDEX` is the measured case. stokaro/ptah#1249 read its `0 changes /
// 1 finding` as the scope filter reaching the diagnostic but not the change;
// it is not. `parser.NewParser("DROP INDEX app.idx").Parse()` returns
// `unsupported DROP target: INDEX at position 5`, so the statement contributes
// no change in the reviewed schema either. The `public` row is what says so: it
// is the reviewed schema and it still counts zero, while the `DROP TABLE`
// control on the same schema counts one. That missing change is a real defect
// and a wider one, and it belongs to the parser rather than to the scope.
//
// The rows are also the guard on the exclusion rule: a statement is excluded
// only when the scope rejected something it named, never merely because it
// produced no change. Reading it the other way would silence PG106 here.
func TestAnalyzeFS_SchemaScopeLeavesUnmodeledStatementsAlone(t *testing.T) {
	tests := []struct {
		name         string
		base         string
		sql          string
		wantChanges  []changeProjection
		wantFindings []string
	}{
		{
			name:         "dropping an index in the reviewed schema counts no change and still reports",
			base:         "CREATE TABLE public.t (id int);\nCREATE INDEX idx ON public.t (id);\n",
			sql:          "DROP INDEX public.idx;\n",
			wantChanges:  []changeProjection{},
			wantFindings: []string{"PG106"},
		},
		{
			name:         "dropping an index outside the reviewed schema behaves identically",
			base:         "CREATE SCHEMA app;\nCREATE TABLE app.t (id int);\nCREATE INDEX idx ON app.t (id);\n",
			sql:          "DROP INDEX app.idx;\n",
			wantChanges:  []changeProjection{},
			wantFindings: []string{"PG106"},
		},
		{
			name:         "a statement the parser models as no schema object keeps its finding",
			base:         "CREATE TABLE public.t (id int);\n",
			sql:          "DELETE FROM pg_enum WHERE enumtypid = 1;\n",
			wantChanges:  []changeProjection{},
			wantFindings: []string{"DS106"},
		},
		{
			name:         "the modeled destructive control on the same schema counts its change",
			base:         "CREATE TABLE public.t (id int);\n",
			sql:          "DROP TABLE public.t;\n",
			wantChanges:  []changeProjection{{lint.SchemaChangeDrop, "public.t"}},
			wantFindings: []string{"DS101"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			analysis := analyzeScoped(c, test.base, test.sql, "public")

			c.Assert(projectChanges(fileByName(c, analysis, "2.sql").Changes), qt.DeepEquals, test.wantChanges)
			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.wantFindings)
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
