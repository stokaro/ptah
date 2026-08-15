package atlas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// The three versions the fixture below carries. They are named so a test can
// say which one it bounds the apply at without repeating the literal.
const (
	toVersionOne   = "20240101000001"
	toVersionTwo   = "20240101000002"
	toVersionThree = "20240101000003"
)

// writeToVersionFixture writes an Atlas-format directory of three independent
// migrations, hashed, so the apply integrity gate lets it through. Each
// migration creates its own table, which is what makes "exactly two ran"
// observable in the database rather than only in the revision table.
func writeToVersionFixture(tb testing.TB) (migrationsDir, dbPath string) {
	c := qt.New(tb)
	c.Helper()
	root := c.TempDir()
	migrationsDir = filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	files := map[string]string{
		toVersionOne + "_one.sql":     "CREATE TABLE tv_one (id INTEGER PRIMARY KEY);\n",
		toVersionTwo + "_two.sql":     "CREATE TABLE tv_two (id INTEGER PRIMARY KEY);\n",
		toVersionThree + "_three.sql": "CREATE TABLE tv_three (id INTEGER PRIMARY KEY);\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, name), []byte(body), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return migrationsDir, filepath.Join(root, "live.db")
}

// runCompatStreams runs one compat invocation with stdout and stderr captured
// separately, because some assertions below are about which stream a line
// landed on.
func runCompatStreams(tb testing.TB, args ...string) (stdout, stderr string, err error) {
	c := qt.New(tb)
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func compatTableNames(tb testing.TB, dbPath string) []string {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	schema, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		names = append(names, table.Name)
	}
	return names
}

// TestCompatCommand_MigrateApplyToVersionAppliesBoundedPrefix is the headline
// contract of stokaro/ptah#951's `migrate apply --to-version`: the bound
// selects a prefix of the pending migrations, the rest stays pending, and the
// verb that reports state agrees with the verb that changed it.
//
// Reverted, this test does not fail on a count — it fails with
// `unknown flag: --to-version`, because the compat surface refuses the spelling
// outright.
func TestCompatCommand_MigrateApplyToVersionAppliesBoundedPrefix(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := writeToVersionFixture(c.TB)

	stdout, _, err := runCompatStreams(c.TB,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
		"--to-version", toVersionTwo,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Migrating to version "+toVersionTwo+" from 2 pending migrations.")
	c.Assert(stdout, qt.Contains, "Migration complete. Current version: "+toVersionTwo)
	// Exactly two of the three ran: the third migration's table is absent.
	c.Assert(compatTableNames(c.TB, dbPath), qt.Contains, "tv_one")
	c.Assert(compatTableNames(c.TB, dbPath), qt.Contains, "tv_two")
	c.Assert(compatTableNames(c.TB, dbPath), qt.Not(qt.Contains), "tv_three")
	c.Assert(sqliteAtlasRevisionVersions(c.TB, dbPath), qt.DeepEquals, []string{toVersionOne, toVersionTwo})

	// The following status must agree that one migration is still pending.
	statusOut, _, statusErr := runCompatStreams(c.TB,
		"migrate", "status",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(statusErr, qt.IsNil)
	c.Assert(statusOut, qt.Contains, "-- Current Version: "+toVersionTwo)
	c.Assert(statusOut, qt.Contains, "-- Executed Files:  2")
	c.Assert(statusOut, qt.Contains, "-- Pending Files:   1")
	c.Assert(statusOut, qt.Contains, "Migration Status: PENDING")

	// A second bounded apply finishes the directory, so the bound is a bound
	// and not a permanent ceiling.
	finishOut, _, finishErr := runCompatStreams(c.TB,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
		"--to-version", toVersionThree,
	)
	c.Assert(finishErr, qt.IsNil)
	c.Assert(finishOut, qt.Contains, "Migration complete. Current version: "+toVersionThree)
	c.Assert(compatTableNames(c.TB, dbPath), qt.Contains, "tv_three")
}

// TestCompatCommand_MigrateApplyToVersionDryRunPreviewsTheSamePrefix pins the
// preview: a dry run must count the bounded prefix, not the whole pending set,
// and must leave the database untouched. A preview that ignored the bound would
// promise three migrations and then apply two.
func TestCompatCommand_MigrateApplyToVersionDryRunPreviewsTheSamePrefix(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := writeToVersionFixture(c.TB)

	stdout, _, err := runCompatStreams(c.TB,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
		"--to-version", toVersionTwo,
		"--dry-run",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Dry run mode: no changes will be made.")
	c.Assert(stdout, qt.Contains, "Would have applied 2 migrations.")
	c.Assert(compatTableNames(c.TB, dbPath), qt.Not(qt.Contains), "tv_one")
}

// TestCompatCommand_MigrateApplyToVersionRefusals covers the three ways the
// bound can be wrong. Each row states what the invocation asks for and the
// diagnostic it must get; none of them may apply anything.
func TestCompatCommand_MigrateApplyToVersionRefusals(t *testing.T) {
	tests := []struct {
		name      string
		extraArgs func(migrationsDir string) []string
		wantErr   string
	}{
		{
			name: "non-numeric value",
			extraArgs: func(string) []string {
				return []string{"--to-version", "not-a-version"}
			},
			wantErr: `--to-version "not-a-version" is not a valid migration version.*`,
		},
		{
			name: "zero is not a version",
			extraArgs: func(string) []string {
				return []string{"--to-version", "0"}
			},
			wantErr: `--to-version must be greater than zero`,
		},
		{
			name: "bound and amount together",
			extraArgs: func(string) []string {
				return []string{"--to-version", toVersionThree, "1"}
			},
			wantErr: `--to-version and the amount argument cannot both be set`,
		},
		{
			name: "version the directory does not carry",
			extraArgs: func(string) []string {
				return []string{"--to-version", "19990101000000"}
			},
			wantErr: `.*target version 19990101000000 was not found in the migration provider`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir, dbPath := writeToVersionFixture(c.TB)
			args := append([]string{
				"migrate", "apply",
				"--url", "sqlite://" + dbPath,
				"--dir", "file://" + migrationsDir,
			}, test.extraArgs(migrationsDir)...)

			_, _, err := runCompatStreams(c.TB, args...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(compatTableNames(c.TB, dbPath), qt.Not(qt.Contains), "tv_one")
		})
	}
}
