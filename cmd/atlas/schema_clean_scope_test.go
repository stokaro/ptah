package atlas_test

import (
	"bytes"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// runSchemaCleanScope runs `atlas schema clean` against dbPath with extra
// selector arguments and returns the combined output.
func runSchemaCleanScope(c *qt.C, dbPath string, extra ...string) (string, error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	args := append([]string{"schema", "clean", "--url", "sqlite://" + dbPath}, extra...)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// TestSchemaCleanSelectorsNarrowThePlan pins what the selectors print.
//
// Reverted, every row fails with `unknown flag: --include` (or --exclude): the
// flags do not exist on master.
func TestSchemaCleanSelectorsNarrowThePlan(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		planned []string
		absent  []string
	}{
		{
			name:    "no selector plans every table",
			args:    []string{"--dry-run"},
			planned: []string{`DROP TABLE IF EXISTS "users"`, `DROP TABLE IF EXISTS "audit_log"`},
		},
		{
			name:    "include keeps only the named table",
			args:    []string{"--dry-run", "--include", "users"},
			planned: []string{`DROP TABLE IF EXISTS "users"`},
			absent:  []string{`DROP TABLE IF EXISTS "audit_log"`},
		},
		{
			name:    "exclude subtracts the named table",
			args:    []string{"--dry-run", "--exclude", "audit_log"},
			planned: []string{`DROP TABLE IF EXISTS "users"`},
			absent:  []string{`DROP TABLE IF EXISTS "audit_log"`},
		},
		{
			name:    "include accepts the type selector spelling",
			args:    []string{"--dry-run", "--include", "users[type=table]"},
			planned: []string{`DROP TABLE IF EXISTS "users"`},
			absent:  []string{`DROP TABLE IF EXISTS "audit_log"`},
		},
		{
			name:    "include accepts a glob",
			args:    []string{"--dry-run", "--include", "a*"},
			planned: []string{`DROP TABLE IF EXISTS "audit_log"`},
			absent:  []string{`DROP TABLE IF EXISTS "users"`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(t.TempDir(), "clean-scope.db")
			createSQLiteSchemaCleanTable(c, dbPath, "users")
			createSQLiteSchemaCleanTable(c, dbPath, "audit_log")

			out, err := runSchemaCleanScope(c, dbPath, test.args...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			for _, planned := range test.planned {
				c.Assert(out, qt.Contains, planned)
			}
			for _, absent := range test.absent {
				c.Assert(out, qt.Not(qt.Contains), absent)
			}
		})
	}
}

// TestSchemaCleanSelectorsNarrowWhatIsDestroyed is the load-bearing one. A
// selector that only shapes the printed plan while the executed drop still goes
// through the whole-database path would pass the plan assertions above and
// still destroy the excluded table.
//
// Reverted, the run fails with `unknown flag: --include`. With the selector
// routed to the plan but the execution left on the whole-database drop, the
// remaining-table assertion fails instead.
func TestSchemaCleanSelectorsNarrowWhatIsDestroyed(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		dropped   string
		surviving string
	}{
		{
			name:      "include drops only the selected table",
			args:      []string{"--auto-approve", "--include", "users"},
			dropped:   "users",
			surviving: "audit_log",
		},
		{
			name:      "exclude spares the excluded table",
			args:      []string{"--auto-approve", "--exclude", "audit_log"},
			dropped:   "users",
			surviving: "audit_log",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(t.TempDir(), "clean-scope-apply.db")
			createSQLiteSchemaCleanTable(c, dbPath, "users")
			createSQLiteSchemaCleanTable(c, dbPath, "audit_log")

			out, err := runSchemaCleanScope(c, dbPath, test.args...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(sqliteTableCount(c, dbPath, test.dropped), qt.Equals, 0)
			c.Assert(sqliteTableCount(c, dbPath, test.surviving), qt.Equals, 1)
		})
	}
}

// TestSchemaCleanUnselectedRunStillDropsEverything pins the unflagged path, so
// routing the selectors cannot quietly turn a plain `schema clean` into a
// partial one.
func TestSchemaCleanUnselectedRunStillDropsEverything(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "clean-scope-full.db")
	createSQLiteSchemaCleanTable(c, dbPath, "users")
	createSQLiteSchemaCleanTable(c, dbPath, "audit_log")

	out, err := runSchemaCleanScope(c, dbPath, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(sqliteTableCount(c, dbPath, "users"), qt.Equals, 0)
	c.Assert(sqliteTableCount(c, dbPath, "audit_log"), qt.Equals, 0)
}

// TestSchemaCleanRejectsUnsupportedSelectorsBeforeConnecting checks that a
// selector form the engine refuses is refused before any database is touched.
func TestSchemaCleanRejectsUnsupportedSelectorsBeforeConnecting(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "include cannot name a child resource",
			args: []string{"--dry-run", "--include", "users[type=column]"},
			want: "cannot be included on their own",
		},
		{
			name: "include cannot name a schema",
			args: []string{"--dry-run", "--include", "main[type=schema]"},
			want: "use --schema to select schemas",
		},
		{
			name: "exclude rejects an unsupported field selector",
			args: []string{"--dry-run", "--exclude", "users[type=table].name"},
			want: "unsupported Atlas exclude field selector",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(t.TempDir(), "clean-scope-invalid.db")
			createSQLiteSchemaCleanTable(c, dbPath, "users")

			out, err := runSchemaCleanScope(c, dbPath, test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out, qt.Contains, test.want)
			c.Assert(sqliteTableCount(c, dbPath, "users"), qt.Equals, 1)
		})
	}
}

// A confirmation prompt is the price of a destructive act, and an empty
// database has no act to price. Measured on the pinned community binary v1.3.0
// with no --auto-approve and no terminal: it prints `Nothing to drop` and exits
// 0, where this binary asked for the confirmation phrase and exited 1 when it
// could not read one -- so a CI step cleaning an already-clean dev database
// worked under one binary and failed under the other (stokaro/ptah#1334).
//
// The second row is the control, and it is what keeps the first from being a
// blanket removal of the confirmation: with something to drop, an unconfirmed
// clean is still refused.
func TestSchemaCleanConfirmationFollowsThePlan(t *testing.T) {
	tests := []struct {
		name       string
		tables     []string
		wantErr    bool
		wantOutput string
	}{
		{
			name:       "an empty database needs no confirmation",
			wantOutput: "Nothing to drop.",
		},
		{
			name:    "a database with objects still does",
			tables:  []string{"users"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			dbPath := filepath.Join(t.TempDir(), "clean-confirm.db")
			for _, table := range tt.tables {
				createSQLiteSchemaCleanTable(c, dbPath, table)
			}

			out, err := runSchemaCleanScope(c, dbPath)

			c.Assert(err != nil, qt.Equals, tt.wantErr, qt.Commentf("output:\n%s", out))
			c.Assert(out, qt.Contains, tt.wantOutput)
		})
	}
}

// The clean still happens where there is nothing to clean: an empty database is
// left empty and the run reports success, rather than being skipped in a way
// that would also skip a later apply.
func TestSchemaCleanOnAnEmptyDatabaseWithAutoApprove(t *testing.T) {
	c := qt.New(t)

	dbPath := filepath.Join(t.TempDir(), "clean-empty-approve.db")

	out, err := runSchemaCleanScope(c, dbPath, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
}
