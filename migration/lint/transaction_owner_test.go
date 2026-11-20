package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// TestLintFS_EmbeddedTransactionFollowsTheTransactionOwner drives transaction/explicit-transaction
// through the public lint path with the bundled runtime: the owner reads
// Options.TransactionOwner from the request a run sends it. A run that leaves
// the field out of the request reports transaction/explicit-transaction on the second row too.
//
// The directory is in Atlas's layout, the one ptah-compat lints a converted
// golang-migrate directory in, and the transaction/mixed-statements row is the control that the field
// is not read as a file outside a transaction. A file of one statement is the
// exception the field makes: the runner sends it alone and the migrator runs
// it without a transaction, so transaction/concurrent-index does not apply, unless the file names a
// transaction mode. A nested BEGIN is still transaction/explicit-transaction, a warning: PostgreSQL
// ignores it with one.
func TestLintFS_EmbeddedTransactionFollowsTheTransactionOwner(t *testing.T) {
	const block = "BEGIN;\nCREATE TABLE t (id int);\nINSERT INTO t VALUES (1);\nCOMMIT;\n"
	const mix = "CREATE TABLE t (id int);\nCREATE INDEX CONCURRENTLY idx ON t (id);\n"
	const build = "CREATE INDEX CONCURRENTLY idx ON t (id);\n"
	const nested = "BEGIN;\nCREATE TABLE t (id int);\nBEGIN;\nINSERT INTO t VALUES (1);\nCOMMIT;\n"
	for _, test := range []struct {
		name  string
		owner lintext.TransactionOwner
		sql   string
		want  []string
	}{
		{"a block the migrator wraps", lintext.TransactionOwnerMigrator, block, []string{"transaction/explicit-transaction"}},
		{"a block in a file that opens its own", lintext.TransactionOwnerFile, block, make([]string, 0)},
		{"a mix in a file that opens its own", lintext.TransactionOwnerFile, mix, []string{"transaction/concurrent-index", "transaction/mixed-statements"}},
		{"a lone concurrent build the migrator wraps", lintext.TransactionOwnerMigrator, build, []string{"transaction/concurrent-index"}},
		{"a lone concurrent build in a file that opens its own", lintext.TransactionOwnerFile, build, make([]string, 0)},
		{"a lone concurrent build in a file that asks for a transaction", lintext.TransactionOwnerFile,
			"-- atlas:txmode file\n\n" + build, []string{"transaction/concurrent-index"}},
		{"a nested block in a file that opens its own", lintext.TransactionOwnerFile, nested, []string{"transaction/explicit-transaction"}},
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

// TestLintFS_DownFileFollowsTheTransactionOwner is the rollback half of
// TestLintFS_EmbeddedTransactionFollowsTheTransactionOwner, in a golang-migrate
// directory: the runner sends a down file as it sends an up file, and the
// migrator rolls it back the same way, so a lone concurrent drop runs without
// a transaction and transaction/concurrent-index does not apply, unless the file names a transaction
// mode, and a block the file opens and closes is not transaction/explicit-transaction.
func TestLintFS_DownFileFollowsTheTransactionOwner(t *testing.T) {
	const up = "CREATE TABLE t (id int);\n"
	const drop = "DROP INDEX CONCURRENTLY idx;\n"
	const block = "BEGIN;\nDROP TABLE t;\nCOMMIT;\n"
	const down = "0000000001_x.down.sql"
	for _, test := range []struct {
		name  string
		owner lintext.TransactionOwner
		sql   string
		want  []string
	}{
		{"a lone concurrent drop the migrator wraps", lintext.TransactionOwnerMigrator, drop,
			[]string{down + ":1:transaction/concurrent-index"}},
		{"a lone concurrent drop in a file that opens its own", lintext.TransactionOwnerFile, drop, make([]string, 0)},
		{"a lone concurrent drop in a file that asks for a transaction", lintext.TransactionOwnerFile,
			"-- atlas:txmode file\n\n" + drop, []string{down + ":3:PG103"}},
		{"a block the migrator wraps", lintext.TransactionOwnerMigrator, block, []string{down + ":1:transaction/explicit-transaction"}},
		{"a block in a file that opens its own", lintext.TransactionOwnerFile, block, make([]string, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := fixture(map[string]string{"0000000001_x.up.sql": up, down: test.sql})

			findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{
				Dialect:          "postgres",
				TransactionOwner: test.owner,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(findingSites(findings), qt.DeepEquals, test.want)
		})
	}
}
