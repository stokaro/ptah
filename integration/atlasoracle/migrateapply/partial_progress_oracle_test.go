//go:build integration

package migrateapply_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

const (
	partialProgressVersion = "20260101000000"
	partialProgressHash    = "h1:JQRZq9FDd63WgeZNL/x+OzNrMIbecUSWvdB0MVOPWOg="
)

const partialProgressBody = `CREATE TABLE first_success (id INTEGER PRIMARY KEY);

CREATE TABLE second_success (id INTEGER PRIMARY KEY);
`

type partialProgressState struct {
	applied          int
	total            int
	failure          string
	failureStatement string
	partialHashes    string
	operatorVersion  string
}

// TestOraclePartialProgressInteroperatesBidirectionally closes the pinned
// black-box acceptance contour for stokaro/ptah#887. The two execution gates
// matter independently: a global none mode and an Atlas file directive reach
// different selection paths before the shared non-transactional executor.
//
// Each writer fails after statement one, and the other implementation resumes
// the same revision. A reader that starts from statement one cannot pass: the
// first_success table is already committed and CREATE TABLE would fail.
func TestOraclePartialProgressInteroperatesBidirectionally(t *testing.T) {
	c := qt.New(t)
	// The Ptah and Atlas children must see the same neutral environment. A
	// caller's feature toggle can otherwise change only the Ptah half and make
	// this look like a comparison of identical command surfaces.
	c.Assert(commandEnvironmentWithoutPtahVariables([]string{
		"PTAH_ATLAS_STRICT_COMPAT=1",
		"ptah_allow_external_schema=true",
		"PATH=/usr/bin",
		"ORACLE_NOTE=PTAH_ATLAS_STRICT_COMPAT=1",
	}), qt.DeepEquals, []string{
		"PATH=/usr/bin",
		"ORACLE_NOTE=PTAH_ATLAS_STRICT_COMPAT=1",
	})

	oracle := requireAtlasOracle(t)
	compat := buildCompatBinary(c)

	tests := []struct {
		name              string
		writer            string
		reader            string
		directive         string
		applyArgs         []string
		wantFinalOperator string
	}{
		{
			name:              "global none Ptah to Atlas",
			writer:            compat,
			reader:            oracle,
			applyArgs:         []string{"--tx-mode", "none"},
			wantFinalOperator: "Atlas CLI v1.3.0",
		},
		{
			name:              "global none Atlas to Ptah",
			writer:            oracle,
			reader:            compat,
			applyArgs:         []string{"--tx-mode", "none"},
			wantFinalOperator: "Ptah",
		},
		{
			name:              "file directive Ptah to Atlas",
			writer:            compat,
			reader:            oracle,
			directive:         "-- atlas:txmode none\n\n",
			wantFinalOperator: "Atlas CLI v1.3.0",
		},
		{
			name:              "file directive Atlas to Ptah",
			writer:            oracle,
			reader:            compat,
			directive:         "-- atlas:txmode none\n\n",
			wantFinalOperator: "Ptah",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeOracleMigrationDir(c, oracle, map[string]string{
				partialProgressVersion + "_two.sql": test.directive + partialProgressBody,
			})
			dbPath := filepath.Join(c.TempDir(), "partial-progress.db")
			executePartialProgressSQL(c, dbPath, "CREATE TABLE second_success (id INTEGER PRIMARY KEY)")

			failed := runPartialProgressApply(c, test.writer, dbPath, dir, test.applyArgs...)
			c.Assert(failed.code, qt.Equals, 1, qt.Commentf("writer output: %s", failed.output))
			assertPartialProgressFailure(c, dbPath)

			executePartialProgressSQL(c, dbPath, "DROP TABLE second_success")
			resumed := runPartialProgressApply(c, test.reader, dbPath, dir, test.applyArgs...)
			c.Assert(resumed.code, qt.Equals, 0, qt.Commentf("reader output: %s", resumed.output))
			assertPartialProgressCompletion(c, dbPath, test.wantFinalOperator)
			assertMigrationState(c, dbPath,
				[]string{"first_success", "second_success"},
				[]string{partialProgressVersion})
		})
	}
}

func runPartialProgressApply(
	c *qt.C,
	binary string,
	dbPath string,
	dir string,
	extraArgs ...string,
) commandResult {
	c.Helper()
	args := []string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + dir,
		"--allow-dirty",
	}
	args = append(args, extraArgs...)
	return runCommand(c, binary, args...)
}

func executePartialProgressSQL(c *qt.C, dbPath, statement string) {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.Exec(statement)
	c.Assert(err, qt.IsNil)
}

func readPartialProgressState(c *qt.C, dbPath string) partialProgressState {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	var state partialProgressState
	err = db.QueryRow(`
SELECT applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''),
       COALESCE(CAST(partial_hashes AS TEXT), ''), COALESCE(operator_version, '')
FROM atlas_schema_revisions
WHERE version = ?`, partialProgressVersion).Scan(
		&state.applied,
		&state.total,
		&state.failure,
		&state.failureStatement,
		&state.partialHashes,
		&state.operatorVersion,
	)
	c.Assert(err, qt.IsNil)
	return state
}

func assertPartialProgressFailure(c *qt.C, dbPath string) {
	c.Helper()
	state := readPartialProgressState(c, dbPath)
	c.Assert(state.applied, qt.Equals, 1)
	c.Assert(state.total, qt.Equals, 2)
	c.Assert(state.failure, qt.Not(qt.Equals), "")
	c.Assert(state.failureStatement, qt.Contains, "CREATE TABLE second_success")
	c.Assert(state.partialHashes, qt.Equals, `["`+partialProgressHash+`"]`)
}

func assertPartialProgressCompletion(c *qt.C, dbPath, wantOperator string) {
	c.Helper()
	state := readPartialProgressState(c, dbPath)
	c.Assert(state.applied, qt.Equals, 2)
	c.Assert(state.total, qt.Equals, 2)
	c.Assert(state.failure, qt.Equals, "")
	c.Assert(state.failureStatement, qt.Equals, "")
	c.Assert(state.partialHashes, qt.Equals, "null")
	c.Assert(state.operatorVersion, qt.Equals, wantOperator)
}
