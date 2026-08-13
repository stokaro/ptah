package migrations_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/lint"
	"go.5x5.cz/ptah/cmd/migratebaseline"
	"go.5x5.cz/ptah/cmd/migratecheckpoint"
	"go.5x5.cz/ptah/cmd/migratedown"
	"go.5x5.cz/ptah/cmd/migraterepair"
	"go.5x5.cz/ptah/cmd/migrateup"
	"go.5x5.cz/ptah/cmd/migrationstest"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// This file is the CLASS test for the integrity gate.
//
// The defect on stokaro/ptah#928 item 4 was reported against `migrations down`,
// and repairing only `down` would have left `checkpoint`, `baseline` and `test`
// executing the same tampered directory at exit 0. The class is "every verb in
// this namespace that executes SQL from a migration directory", and it is
// enumerated here — beside the namespace that registers the verbs — rather than
// inside any one verb's package, so that a verb added to the namespace has a
// visible place where its verdict is missing.
//
// Each row runs a REAL command against a REAL SQLite database. The refusal row
// and the acceptance row use the same fixture builder and differ only in
// whether the down file was rewritten, so a gate that refused every hashed
// directory fails the acceptance half instead of passing both.

// classFixture is one hashed migration directory plus the scratch paths a verb
// needs to run against it.
type classFixture struct {
	dir      string
	testsDir string
	tmp      string
}

// newClassFixture writes a hashed one-migration ptah directory. The sum records
// the honest files; callers that want drift rewrite a file afterwards WITHOUT
// re-hashing.
func newClassFixture(c *qt.C) classFixture {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\n",
		"0000000001_init.down.sql": "DROP TABLE widgets;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatPtah)
	c.Assert(err, qt.IsNil)

	testsDir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(testsDir, "roundtrip.yaml"),
		[]byte("cases:\n  - name: up-then-down\n    steps:\n      - migrate_to: latest\n      - migrate_to: \"0\"\n"),
		0o600), qt.IsNil)

	return classFixture{dir: dir, testsDir: testsDir, tmp: c.TempDir()}
}

// tamper rewrites the down migration and leaves the sum stale.
func (f classFixture) tamper(c *qt.C) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(f.dir, "0000000001_init.down.sql"),
		[]byte("CREATE TABLE evil_down (id INTEGER PRIMARY KEY);\nDROP TABLE widgets;\n"), 0o600), qt.IsNil)
}

func (f classFixture) db(name string) string {
	return "sqlite://" + filepath.Join(f.tmp, name+".db")
}

func runClassCommand(cmd *cobra.Command, args ...string) (string, error) {
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// classVerb is one member of the class: a native verb that executes SQL taken
// from a migration directory.
//
// run carries the invocation as a func field because the repository's test
// style forbids branching inside a test body; the table is the branch.
type classVerb struct {
	name string
	run  func(f classFixture) (string, error)
}

// executingVerbs enumerates the class.
//
// Membership was derived by grepping every non-test call site of the executing
// Migrator methods (MigrateUp / MigrateUpWithOptions / MigrateDownTo /
// MigrateDownToWithPreflight / MigrateTo) and mapping each back to the verb
// that reaches it:
//
//	up          cmd/migrateup            MigrateUpWithOptions
//	down        cmd/migratedown          MigrateDownToWithPreflight
//	checkpoint  migration/generator      MigrateUp on the shadow database
//	baseline    migration/generator      MigrateUp on the shadow database
//	test        migration/dbtest         MigrateUp / MigrateDownTo / MigrateTo
//	lint        internal/migrationreplay Replay on the --dev-url database
//
// plus one that the method grep alone does NOT reach:
//
//	repair --resume-from   migrator.RepairMigration, which executes the
//	                       remaining statements of the body that failed
//
// It belongs to the class only in that spelling. A plain `repair` rewrites
// revision metadata and executes none of the directory's SQL, so it stays
// usable on a drifted directory — clearing a dirty row is a recovery step an
// operator may need BEFORE they can re-hash anything. See
// TestRepairResumeFrom_* below, which pins both halves.
//
// `status` and `set` read the directory but execute none of its SQL, and
// `hash`, `edit`, `rebase` and `rm` exist to REWRITE the integrity file, so
// verifying it first would refuse their purpose. Those are out of the class by
// the predicate, not by omission.
func executingVerbs() []classVerb {
	return []classVerb{
		{
			name: "up",
			run: func(f classFixture) (string, error) {
				return runClassCommand(migrateup.NewMigrateUpCommand(),
					"--db-url", f.db("up"), "--migrations-dir", f.dir)
			},
		},
		{
			name: "down",
			run: func(f classFixture) (string, error) {
				return runClassCommand(migratedown.NewMigrateDownCommand(),
					"--db-url", f.db("down"), "--migrations-dir", f.dir,
					"--target", "0", "--confirm")
			},
		},
		{
			name: "checkpoint",
			run: func(f classFixture) (string, error) {
				return runClassCommand(migratecheckpoint.NewMigrateCheckpointCommand(),
					"--migrations-dir", f.dir, "--shadow-db", f.db("checkpoint-shadow"),
					"--dir-format", "ptah", "--dry-run")
			},
		},
		{
			name: "baseline",
			run: func(f classFixture) (string, error) {
				return runClassCommand(migratebaseline.NewMigrateBaselineCommand(),
					"--db-url", f.db("baseline"), "--migrations-dir", f.dir,
					"--shadow-db", f.db("baseline-shadow"), "--version", "1")
			},
		},
		{
			name: "test",
			run: func(f classFixture) (string, error) {
				return runClassCommand(migrationstest.NewMigrationsTestCommand(),
					"--db-url", f.db("test"), "--migrations-dir", f.dir, "--dir", f.testsDir)
			},
		},
		{
			name: "lint",
			run: func(f classFixture) (string, error) {
				return runClassCommand(lint.NewLintCommand(),
					"--dir", f.dir, "--dev-url", f.db("lint-dev"))
			},
		},
		{
			name: "repair --resume-from",
			run: func(f classFixture) (string, error) {
				return runClassCommand(migraterepair.NewMigrateRepairCommand(),
					"--db-url", f.db("repair"), "--migrations-dir", f.dir,
					"--version", "1", "--resume-from", "1")
			},
		},
	}
}

// TestRepairResumeFrom_PlainRepairStaysUsableOnADriftedDirectory is the other
// half of the `repair` verdict, and the reason its gate keys on --resume-from
// rather than on the verb.
//
// A plain repair executes none of the directory's SQL — it rewrites a dirty
// revision row — so it is outside the class. Gating it anyway would block the
// recovery path at exactly the wrong moment: an operator whose migration died
// half-applied has a dirty row to clear BEFORE they can sensibly re-hash, and a
// gate here would tell them to fix the directory first while the directory is
// the thing they are trying to reason about.
//
// The assertion is deliberately about the checksum text and not about success:
// this fixture has no dirty row to repair, so the command fails for its own
// reason. What must not appear is a refusal from the integrity gate.
func TestRepairResumeFrom_PlainRepairStaysUsableOnADriftedDirectory(t *testing.T) {
	c := qt.New(t)
	f := newClassFixture(c)
	f.tamper(c)

	out, err := runClassCommand(migraterepair.NewMigrateRepairCommand(),
		"--db-url", f.db("repair-plain"), "--migrations-dir", f.dir, "--version", "1")

	c.Check(out, qt.Not(qt.Contains), "migration sum verification failed")
	c.Check(errorText(err), qt.Not(qt.Contains), "migration sum verification failed")
}

// TestExecutingVerbs_RefuseATamperedHashedDirectory is the class regression.
//
// Before the fix only `up` failed this; `down`, `checkpoint`, `baseline` and
// `test` all executed the rewritten file and exited 0.
func TestExecutingVerbs_RefuseATamperedHashedDirectory(t *testing.T) {
	for _, verb := range executingVerbs() {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			f := newClassFixture(c)
			f.tamper(c)

			out, err := verb.run(f)

			c.Assert(err, qt.IsNotNil, qt.Commentf("stdout+stderr:\n%s", out))
			c.Assert(err.Error(), qt.Contains, "migration sum verification failed")
			c.Assert(err.Error(), qt.Contains, "changed: 0000000001_init.down.sql")
		})
	}
}

// TestExecutingVerbs_AcceptACleanHashedDirectory is the non-interference
// control for every row above.
//
// It is what separates "the gate refuses drift" from "the gate refuses any
// hashed directory". Reverting the fix cannot redden this test, which is why it
// is stated as its own table rather than folded into the refusal rows: the two
// halves fail for opposite reasons and a reader needs to see which one moved.
//
// The rows assert only that no CHECKSUM refusal occurred. Some verbs
// legitimately fail a clean fixture for their own reasons — `baseline` refuses
// a database whose revision table it did not write — and asserting a nil error
// would pin those unrelated contracts here.
func TestExecutingVerbs_AcceptACleanHashedDirectory(t *testing.T) {
	for _, verb := range executingVerbs() {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			f := newClassFixture(c)

			out, err := verb.run(f)

			c.Check(out, qt.Not(qt.Contains), "migration sum verification failed")
			c.Check(errorText(err), qt.Not(qt.Contains), "migration sum verification failed")
		})
	}
}

// errorText renders an error for a Contains assertion without branching in a
// test body: a nil error contributes the empty string, which contains nothing.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
