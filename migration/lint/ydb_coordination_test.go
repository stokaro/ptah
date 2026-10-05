package lint_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// dsSites keeps the sites of the DS family.
func dsSites(sites []string) []string {
	return slices.DeleteFunc(slices.Clone(sites), func(site string) bool {
		return !strings.Contains(site, ":DS")
	})
}

// TestYDBRules_DS107ReportsADroppedCoordinationNode pins DS107 on Ptah's
// statement that drops a coordination node, as it reports DROP TOPIC: YDB
// runs the drop even while a session holds a semaphore on the node, and the
// node's persistent semaphores and rate limiter resources go with it. The
// rule reads the up half, where a drop is a change to the schema.
func TestYDBRules_DS107ReportsADroppedCoordinationNode(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{
			name: "a drop",
			files: map[string]string{
				"0001_nodes.up.sql": "CREATE COORDINATION NODE `app/locks`;\n",
				"0002_nodes.up.sql": "DROP COORDINATION NODE `app/locks`;\n",
			},
			want: []string{"0002_nodes.up.sql:1:DS107"},
		},
		{
			name: "a drop under a prefix",
			files: map[string]string{
				"0001_nodes.up.sql": "PRAGMA TablePathPrefix(\"/local/app\");\nDROP COORDINATION NODE locks;\n",
			},
			want: []string{"0001_nodes.up.sql:2:DS107"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(dsSites(ydbLint(c, test.files, "")), qt.DeepEquals, test.want)
		})
	}
}

// The controls on DS107: a creation, a change and text that only names the
// statement drop nothing, and the same words with another object after
// COORDINATION are not Ptah's statement.
func TestYDBRules_DS107LeavesTheOtherCoordinationNodeStatements(t *testing.T) {
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
			c.Assert(dsSites(ydbLint(c, test.files, "")), qt.HasLen, 0)
		})
	}
}
