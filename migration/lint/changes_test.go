package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

type changeProjection struct {
	Kind   lint.SchemaChangeKind
	Object string
}

func projectChanges(changes []lint.SchemaChange) []changeProjection {
	out := make([]changeProjection, 0, len(changes))
	for _, ch := range changes {
		out = append(out, changeProjection{Kind: ch.Kind, Object: ch.Object})
	}
	return out
}

func fileByName(c *qt.C, analysis lint.Analysis, name string) lint.File {
	c.Helper()
	for _, f := range analysis.Files() {
		if f.Name == name {
			return f
		}
	}
	c.Fatalf("file %q not present in analysis", name)
	return lint.File{}
}

func analyzeSQLite(c *qt.C, files map[string]string) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(files), lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Dialect:   "sqlite",
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// TestAnalyzeFS_SchemaChangeCardinality proves the change count is decoupled
// from the statement and file count: a statement can yield zero, one, or many
// semantic schema changes.
func TestAnalyzeFS_SchemaChangeCardinality(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []changeProjection
	}{
		{
			name: "single create is one change",
			sql:  "CREATE TABLE users (id INTEGER);",
			want: []changeProjection{{lint.SchemaChangeAdd, "users"}},
		},
		{
			name: "single drop is one change",
			sql:  "DROP TABLE users;",
			want: []changeProjection{{lint.SchemaChangeDrop, "users"}},
		},
		{
			name: "one statement dropping several tables is several changes",
			sql:  "DROP TABLE a, b, c;",
			want: []changeProjection{
				{lint.SchemaChangeDrop, "a"},
				{lint.SchemaChangeDrop, "b"},
				{lint.SchemaChangeDrop, "c"},
			},
		},
		{
			name: "one multi-action alter is several changes",
			sql:  "ALTER TABLE t ADD COLUMN a INTEGER, ADD COLUMN b INTEGER;",
			want: []changeProjection{
				{lint.SchemaChangeAdd, "a"},
				{lint.SchemaChangeAdd, "b"},
			},
		},
		{
			name: "drop column is one change",
			sql:  "ALTER TABLE users DROP COLUMN legacy;",
			want: []changeProjection{{lint.SchemaChangeDrop, "legacy"}},
		},
		{
			name: "create index is one change",
			sql:  "CREATE INDEX idx_users_id ON users (id);",
			want: []changeProjection{{lint.SchemaChangeAdd, "idx_users_id"}},
		},
		{
			name: "operational insert is zero changes",
			sql:  "INSERT INTO users (id) VALUES (1);",
			want: []changeProjection{},
		},
		{
			name: "select is zero changes",
			sql:  "SELECT 1;",
			want: []changeProjection{},
		},
		{
			name: "comment-only file is zero changes",
			sql:  "-- nothing structural here\n",
			want: []changeProjection{},
		},
		{
			// GRANT used to sit here, as a statement the parser refused. It
			// parses now (issue #932) and counts as the one change it is, so
			// the "outside the grammar" case needs a statement that is still
			// outside it.
			name: "statement outside the DDL grammar is zero changes",
			sql:  "CLUSTER users USING idx_users_id;",
			want: []changeProjection{},
		},
		{
			name: "grant is one change",
			sql:  "GRANT SELECT ON users TO app;",
			want: []changeProjection{{lint.SchemaChangeAdd, "users"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeSQLite(c, map[string]string{"1_change.sql": tc.sql})
			file := fileByName(c, analysis, "1_change.sql")
			c.Assert(projectChanges(file.Changes), qt.DeepEquals, tc.want)
		})
	}
}

// TestAnalyzeFS_MixedDDLAndNonDDLFile shows a single file mixing DDL and
// operational statements: the change count reflects only the structural
// statements, and each change points back at its producing statement.
func TestAnalyzeFS_MixedDDLAndNonDDLFile(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeSQLite(c, map[string]string{
		"7_mixed.sql": "CREATE TABLE t (id INTEGER);\n" +
			"INSERT INTO t (id) VALUES (1);\n" +
			"ALTER TABLE t ADD COLUMN a INTEGER, ADD COLUMN b INTEGER;\n",
	})

	file := fileByName(c, analysis, "7_mixed.sql")

	c.Assert(file.Statements, qt.HasLen, 3)
	c.Assert(file.Changes, qt.HasLen, 3)
	c.Assert(projectChanges(file.Changes), qt.DeepEquals, []changeProjection{
		{lint.SchemaChangeAdd, "t"},
		{lint.SchemaChangeAdd, "a"},
		{lint.SchemaChangeAdd, "b"},
	})
	// The CREATE came from statement 0; the two ADD COLUMN changes both came
	// from statement 2 — the INSERT (statement 1) produced nothing.
	c.Assert(file.Changes[0].StatementIndex, qt.Equals, 0)
	c.Assert(file.Changes[1].StatementIndex, qt.Equals, 2)
	c.Assert(file.Changes[2].StatementIndex, qt.Equals, 2)
	c.Assert(file.Changes[0].Version, qt.Equals, int64(7))
	c.Assert(file.Changes[0].File, qt.Equals, "7_mixed.sql")
	c.Assert(file.Changes[0].Line, qt.Equals, 1)
	c.Assert(file.Changes[1].Line, qt.Equals, 3)
}

func TestAnalyzeFS_DownMigrationHasNoChanges(t *testing.T) {
	c := qt.New(t)
	analysis, err := lint.AnalyzeFS(fixture(map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE users (id INTEGER);",
		"0000000001_init.down.sql": "DROP TABLE users;",
	}), lint.Options{DirFormat: migrator.MigrationDirFormatPtah})
	c.Assert(err, qt.IsNil)

	up := fileByName(c, analysis, "0000000001_init.up.sql")
	down := fileByName(c, analysis, "0000000001_init.down.sql")

	c.Assert(projectChanges(up.Changes), qt.DeepEquals, []changeProjection{
		{lint.SchemaChangeAdd, "users"},
	})
	c.Assert(down.Changes, qt.HasLen, 0)
}

func TestAnalyzeFS_SchemaChangesAreDeterministic(t *testing.T) {
	c := qt.New(t)
	files := map[string]string{
		"1_a.sql": "CREATE TABLE a (id INTEGER);\nALTER TABLE a ADD COLUMN x INTEGER, ADD COLUMN y INTEGER;\n",
		"2_b.sql": "DROP TABLE a, b;",
	}

	first := fileByName(c, analyzeSQLite(c, files), "1_a.sql")
	second := fileByName(c, analyzeSQLite(c, files), "1_a.sql")

	c.Assert(second.Changes, qt.DeepEquals, first.Changes)
}

// TestAnalyzeFS_SchemaChangesRespectDialect confirms changes flow through the
// dialect-aware parser rather than a dialect-blind heuristic.
func TestAnalyzeFS_SchemaChangesRespectDialect(t *testing.T) {
	c := qt.New(t)
	sql := "CREATE TABLE t (id INTEGER);\nALTER TABLE t ADD COLUMN a INTEGER, ADD COLUMN b INTEGER;\n"

	postgres, err := lint.AnalyzeFS(fixture(map[string]string{"1_change.sql": sql}), lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Dialect:   "postgres",
	})
	c.Assert(err, qt.IsNil)

	file := fileByName(c, postgres, "1_change.sql")
	c.Assert(projectChanges(file.Changes), qt.DeepEquals, []changeProjection{
		{lint.SchemaChangeAdd, "t"},
		{lint.SchemaChangeAdd, "a"},
		{lint.SchemaChangeAdd, "b"},
	})
}
