package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// TestLintFS_EmbeddedTransactionFollowsTheTransactionOwner drives TX201
// through the public lint path with the bundled runtime: the owner reads
// Options.TransactionOwner from the request a run sends it. A run that leaves
// the field out of the request reports TX201 on the second row too.
//
// The directory is in Atlas's layout, the one ptah-compat lints a converted
// golang-migrate directory in, and the TX101 row is the control that the field
// is not read as a file outside a transaction.
func TestLintFS_EmbeddedTransactionFollowsTheTransactionOwner(t *testing.T) {
	const block = "BEGIN;\nCREATE TABLE t (id int);\nINSERT INTO t VALUES (1);\nCOMMIT;\n"
	const mix = "CREATE TABLE t (id int);\nCREATE INDEX CONCURRENTLY idx ON t (id);\n"
	for _, test := range []struct {
		name  string
		owner lintext.TransactionOwner
		sql   string
		want  []string
	}{
		{"a block the migrator wraps", lintext.TransactionOwnerMigrator, block, []string{"TX201"}},
		{"a block in a file that opens its own", lintext.TransactionOwnerFile, block, make([]string, 0)},
		{"a mix in a file that opens its own", lintext.TransactionOwnerFile, mix, []string{"PG103", "TX101"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := fixture(map[string]string{"20240826120919_init.sql": test.sql})

			findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{
				Dialect:          "postgres",
				DirFormat:        migrationfile.DirFormatAtlas,
				TransactionOwner: test.owner,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(findings), qt.DeepEquals, test.want)
		})
	}
}
