package migrations_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migratedown"
	"go.5x5.cz/ptah/cmd/migratestatus"
	"go.5x5.cz/ptah/cmd/migrateup"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// This file is the CLASS test for --verify-sum, and it exists to answer one
// question a reader will reasonably ask after stokaro/ptah#1450: given that
// every verb which executes migration SQL now verifies a hashed directory
// automatically, what is the flag still for?
//
// It is for the case the always-on gate deliberately does not cover. That gate
// asks "does this directory match the sum it carries", and a directory carrying
// NO sum passes, because there is no recorded intent to compare against —
// refusing it would remove a capability from every project that never adopted
// `ptah migrations hash`. --verify-sum asks the different question: "is this
// directory covered by a sum at all". Nothing else on the native surface asks
// it for a registry source. `ptah migrations validate` asks it for a local
// path, and measured against a published artifact it cannot even resolve the
// reference — `migrations validate --dir oci://…` answers `stat oci://…: no
// such file or directory` — so for an oci:// directory the flag is the only
// spelling there is.
//
// The rows below are therefore stated as a table over the verbs that offer the
// flag, with BOTH directions on the same fixture: the unhashed directory each
// verb still accepts without the flag, and refuses with it. A gate that refused
// unhashed directories by default would fail the accepting half; a flag wired
// to nothing would fail the refusing half.

// unhashedFixture is one migration directory that was never hashed, plus the
// scratch paths a verb needs to run against it.
type unhashedFixture struct {
	dir string
	tmp string
}

// newUnhashedFixture writes a one-migration ptah directory with NO integrity
// file. That absence is the whole point of the fixture, so nothing here calls
// migratesum.
func newUnhashedFixture(c *qt.C) unhashedFixture {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\n",
		"0000000001_init.down.sql": "DROP TABLE widgets;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	// Guard the fixture's own premise: a stray sum file would make every
	// refusing row below pass for the wrong reason.
	_, err := os.Stat(filepath.Join(dir, migratesum.FileName))
	c.Assert(os.IsNotExist(err), qt.IsTrue)
	return unhashedFixture{dir: dir, tmp: c.TempDir()}
}

func (f unhashedFixture) db(name string) string {
	return "sqlite://" + filepath.Join(f.tmp, name+".db")
}

// verifySumClassVerb is one member of the class: a verb offering --verify-sum.
//
// run carries the invocation as a func field because the repository's test
// style forbids branching inside a test body; the table is the branch. extra
// is threaded through rather than baked in so the accepting and refusing
// halves share one wiring and cannot drift apart — a row wired one way for the
// refusal and another for the control would let the two disagree about which
// command they measured.
type verifySumClassVerb struct {
	name string
	run  func(f unhashedFixture, extra []string) (string, error)
}

// verifySumOn and verifySumOff are the two argument tails the tables pass.
// Naming them keeps the flag out of the row bodies and off a bool parameter.
var (
	verifySumOn  = []string{"--verify-sum"}
	verifySumOff []string
)

// verifySumClassVerbs enumerates the native verbs offering the flag.
//
// `migrations push` is out: it publishes a LOCAL directory and never resolves
// an oci:// source, so its half of the contract has no unhashed-artifact case
// to measure and is covered where the push surface is tested. The registration
// census that keeps this list honest against the built command tree lives in
// cmd/root/oci_flag_surface_test.go.
func verifySumClassVerbs() []verifySumClassVerb {
	return []verifySumClassVerb{
		{
			name: "up",
			run: func(f unhashedFixture, extra []string) (string, error) {
				return runClassCommand(migrateup.NewMigrateUpCommand(), append([]string{
					"--db-url", f.db("up"), "--migrations-dir", f.dir, "--skip-report",
				}, extra...)...)
			},
		},
		{
			name: "down",
			run: func(f unhashedFixture, extra []string) (string, error) {
				dbURL := f.db("down")
				f.applyOnceFor(dbURL)
				return runClassCommand(migratedown.NewMigrateDownCommand(), append([]string{
					"--db-url", dbURL, "--migrations-dir", f.dir, "--target", "0", "--confirm",
				}, extra...)...)
			},
		},
		{
			name: "status",
			run: func(f unhashedFixture, extra []string) (string, error) {
				return runClassCommand(migratestatus.NewMigrateStatusCommand(), append([]string{
					"--db-url", f.db("status"), "--migrations-dir", f.dir,
				}, extra...)...)
			},
		},
	}
}

// applyOnceFor is the error-swallowing form used inside a row's func field,
// where there is no *qt.C to assert on. A failure here surfaces as the row's
// own assertion failing, which names the verb.
func (f unhashedFixture) applyOnceFor(dbURL string) {
	_, _ = runClassCommand(migrateup.NewMigrateUpCommand(),
		"--db-url", dbURL, "--migrations-dir", f.dir, "--skip-report")
}

// TestVerifySum_RefusesADirectoryCarryingNoSum is the property the flag exists
// for, and the reason it is not redundant with the always-on gate.
func TestVerifySum_RefusesADirectoryCarryingNoSum(t *testing.T) {
	for _, verb := range verifySumClassVerbs() {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			f := newUnhashedFixture(c)

			out, err := verb.run(f, verifySumOn)

			c.Assert(err, qt.IsNotNil, qt.Commentf("stdout+stderr:\n%s", out))
			c.Assert(err.Error(), qt.Contains, "migration sum verification failed")
			c.Assert(err.Error(), qt.Contains, "ptah.sum not found")
		})
	}
}

// TestVerifySum_AbsentLeavesAnUnhashedDirectoryUngated is the non-interference
// control for every row above.
//
// It is what separates "the flag demands a sum" from "the build started
// demanding one everywhere", and it pins the boundary stokaro/ptah#955 set and
// stokaro/ptah#1450 kept: a directory nobody ever hashed runs. Reverting the
// flag's wiring cannot redden this test, which is why it is its own table — the
// two halves fail for opposite reasons and a reader needs to see which moved.
func TestVerifySum_AbsentLeavesAnUnhashedDirectoryUngated(t *testing.T) {
	for _, verb := range verifySumClassVerbs() {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			f := newUnhashedFixture(c)

			out, err := verb.run(f, verifySumOff)

			c.Check(out, qt.Not(qt.Contains), "migration sum verification failed")
			c.Check(errorText(err), qt.Not(qt.Contains), "migration sum verification failed")
		})
	}
}

// TestVerifySum_RefusesADriftedHashedDirectoryToo pins that the stricter
// contract is a superset of the default one rather than a replacement.
//
// Without this a build could satisfy every row above by checking only for the
// sum FILE and never comparing anything to it, which is a cheaper wrong
// implementation than the one it replaces.
func TestVerifySum_RefusesADriftedHashedDirectoryToo(t *testing.T) {
	for _, verb := range verifySumClassVerbs() {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			f := newUnhashedFixture(c)
			_, err := migratesum.WriteWithFormat(f.dir, migrator.MigrationDirFormatPtah)
			c.Assert(err, qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(f.dir, "0000000001_init.down.sql"),
				[]byte("CREATE TABLE evil_down (id INTEGER PRIMARY KEY);\nDROP TABLE widgets;\n"), 0o600), qt.IsNil)

			out, runErr := verb.run(f, verifySumOn)

			c.Assert(runErr, qt.IsNotNil, qt.Commentf("stdout+stderr:\n%s", out))
			c.Assert(runErr.Error(), qt.Contains, "migration sum verification failed")
			c.Assert(runErr.Error(), qt.Contains, "changed: 0000000001_init.down.sql")
		})
	}
}

// TestStatus_StaysUsableOnADriftedDirectoryWithoutTheFlag is the decision
// stokaro/ptah#1450 made, pinned so this change cannot quietly reverse it.
//
// status executes none of the directory's SQL, so it is outside the always-on
// class by that predicate. It is also the verb an operator reaches for while
// diagnosing a directory that has drifted, and a default gate would refuse to
// describe the thing being investigated. An intermediate revision of this
// change did gate it, turning a long-standing exit 0 into exit 2 for every
// project whose directory has drifted for a reason they already know about;
// this row is what caught that.
func TestStatus_StaysUsableOnADriftedDirectoryWithoutTheFlag(t *testing.T) {
	c := qt.New(t)
	f := newUnhashedFixture(c)
	_, err := migratesum.WriteWithFormat(f.dir, migrator.MigrationDirFormatPtah)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(f.dir, "0000000001_init.down.sql"),
		[]byte("CREATE TABLE evil_down (id INTEGER PRIMARY KEY);\nDROP TABLE widgets;\n"), 0o600), qt.IsNil)

	out, runErr := runClassCommand(migratestatus.NewMigrateStatusCommand(),
		"--db-url", f.db("status-drifted"), "--migrations-dir", f.dir)

	c.Check(out, qt.Not(qt.Contains), "migration sum verification failed")
	c.Check(errorText(runErr), qt.Not(qt.Contains), "migration sum verification failed")
}
