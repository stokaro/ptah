package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
)

// TestYDBCoordinationNodeDropped_Reports pins YD112: Ptah's statement that
// drops a coordination node, which YDB runs even while a session holds a
// semaphore on the node, taking its persistent semaphores and rate limiter
// resources with it. A down half is read too, since it drops the node alike.
func TestYDBCoordinationNodeDropped_Reports(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{
			name: "a drop in an up half",
			files: map[string]string{
				"0001_nodes.up.sql": "CREATE COORDINATION NODE `app/locks`;\n",
				"0002_nodes.up.sql": "DROP COORDINATION NODE `app/locks`;\n",
			},
			want: []string{"0002_nodes.up.sql:1:YD112"},
		},
		{
			name: "a drop in a down half, under a prefix",
			files: map[string]string{
				"0001_nodes.up.sql":   "CREATE COORDINATION NODE locks WITH (read_consistency_mode = 'strict');\n",
				"0001_nodes.down.sql": "PRAGMA TablePathPrefix(\"/local/app\");\nDROP COORDINATION NODE locks;\n",
			},
			want: []string{"0001_nodes.down.sql:2:YD112"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSites(ydbLint(c, test.files, "")), qt.DeepEquals, test.want)
		})
	}
}

// TestYDBCoordinationNodeDropped_LeavesTheRest holds the controls: a
// creation, a change, text that only mentions the statement, and a drop read
// for another dialect, where Ptah's statement means nothing.
func TestYDBCoordinationNodeDropped_LeavesTheRest(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
	}{
		{name: "a creation and a change", files: map[string]string{
			"0001_nodes.up.sql": "CREATE COORDINATION NODE `app/locks`;\n" +
				"ALTER COORDINATION NODE `app/locks` SET (session_grace_period = Interval('PT20S'));\n"}},
		{name: "a string naming the statement", files: map[string]string{
			"0001_notes.up.sql": "CREATE TABLE notes (id Uint64 NOT NULL, body Utf8, PRIMARY KEY (id));\n" +
				"UPSERT INTO notes (id, body) VALUES (1, 'DROP COORDINATION NODE locks'u);\n"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSites(ydbLint(c, test.files, "")), qt.HasLen, 0)
		})
	}
}

// The control on the dialect: the same drop linted for PostgreSQL reports no
// YD112, so the rule answers for YDB only.
func TestYDBCoordinationNodeDropped_OnlyOnYDB(t *testing.T) {
	c := qt.New(t)
	target, err := lint.ResolveTarget("postgres", "")
	c.Assert(err, qt.IsNil)

	findings, err := lint.LintFS(fixture(map[string]string{
		"0001_nodes.up.sql": "DROP COORDINATION NODE `app/locks`;\n",
	}), lint.Options{Dialect: "postgres", Target: target})

	c.Assert(err, qt.IsNil)
	c.Assert(ydbSites(findingSites(findings)), qt.HasLen, 0)
}
