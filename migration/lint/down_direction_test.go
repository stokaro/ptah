package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
)

// lintPair lints one up/down pair and returns the rule code and file basename
// of every finding, so a row can say which direction a hazard was reported in.
func lintPair(tb testing.TB, upSQL, downSQL string) []string {
	c := qt.New(tb)
	c.Helper()
	fsys := fixture(map[string]string{
		"0000000001_x.up.sql":   upSQL,
		"0000000001_x.down.sql": downSQL,
	})
	findings, err := lint.LintFS(fsys, lint.Options{Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	located := make([]string, 0, len(findings))
	for _, finding := range findings {
		located = append(located, finding.Rule+" "+finding.File)
	}
	return located
}

// TestLintFS_DownDirectionHazards pins which rules read the rollback half.
//
// Statement rules were confined to up files, so the blocking DROP INDEX that a
// rollback is normally made of -- including the one Ptah generates itself
// whenever the forward statement was not a concurrent build -- was reported as
// clean (stokaro/ptah#997). PG106 now opts in, and the opt-in is what is under
// test: the second and third rows are the discriminating controls. An
// implementation that simply ran every statement rule on every file would
// report PG101 for the down file's CREATE INDEX and TX201 for its BEGIN, and
// both rows would fail.
func TestLintFS_DownDirectionHazards(t *testing.T) {
	tests := []struct {
		name string
		up   string
		down string
		want []string
	}{
		{
			name: "blocking drop is reported in either direction",
			up:   "CREATE INDEX CONCURRENTLY idx ON t (id);\n",
			down: "DROP INDEX idx;\n",
			want: []string{
				"PG106 0000000001_x.down.sql",
				"PG103 0000000001_x.up.sql",
			},
		},
		{
			name: "a rule that did not opt in stays silent on the down file",
			up:   "ALTER TABLE t ADD COLUMN c INT;\n",
			down: "CREATE INDEX idx ON t (id);\n",
			want: []string{},
		},
		{
			name: "a second rule that did not opt in stays silent on the down file",
			up:   "ALTER TABLE t ADD COLUMN c INT;\n",
			down: "BEGIN;\nALTER TABLE t DROP COLUMN c;\nCOMMIT;\n",
			want: []string{},
		},
		{
			name: "a concurrent rollback without the marker cannot execute",
			up:   "-- +ptah no_transaction\nCREATE INDEX CONCURRENTLY idx ON t (id);\n",
			down: "DROP INDEX CONCURRENTLY idx;\n",
			want: []string{"PG103 0000000001_x.down.sql"},
		},
		{
			name: "the marker exempts the rollback Ptah generates",
			up:   "-- +ptah no_transaction\nCREATE INDEX CONCURRENTLY idx ON t (id);\n",
			down: "-- +ptah no_transaction\nDROP INDEX CONCURRENTLY idx;\n",
			want: []string{},
		},
		{
			name: "a rollback mixing autocommit and transactional DDL is reported",
			up:   "ALTER TABLE t ADD COLUMN c INT;\n",
			down: "DROP INDEX CONCURRENTLY idx;\nALTER TABLE t DROP COLUMN c;\n",
			want: []string{
				"PG103 0000000001_x.down.sql",
				"TX101 0000000001_x.down.sql",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got := lintPair(c.TB, tt.up, tt.down)

			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}
