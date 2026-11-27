//go:build integration

package statementreading_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
	"ptah.run/internal/clirun"
)

// TestMigrationsLintSQLServer_ControlFlow drives the default error threshold
// through the native command. Each spelling can drop the same table, so each
// must report the drop and fail the command.
func TestMigrationsLintSQLServer_ControlFlow(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		want []sqlServerLintFinding
	}{
		{
			name: "a standalone drop",
			sql:  "DROP TABLE dbo.users;",
			want: []sqlServerLintFinding{{Rule: "destructive/drop-table", Line: 1}, {Rule: "rollout/drop-table", Line: 1}},
		},
		{
			name: "a conditional block",
			sql:  "IF OBJECT_ID(N'dbo.users') IS NOT NULL\nBEGIN\n  DROP TABLE dbo.users;\nEND;",
			want: []sqlServerLintFinding{
				{Rule: "coverage/unparsed-statement", Line: 1},
				{Rule: "destructive/drop-table", Line: 3},
				{Rule: "rollout/drop-table", Line: 3},
			},
		},
		{
			name: "an unseparated pair",
			sql:  "CREATE TABLE dbo.a (id int) DROP TABLE dbo.users",
			want: []sqlServerLintFinding{{Rule: "destructive/drop-table", Line: 1}, {Rule: "rollout/drop-table", Line: 1}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := sqlServerMigrationLint(c, test.sql)
			c.Assert(run.ExitCode, qt.Equals, 1, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(sqlServerLintFindings(c, run.Stdout), qt.DeepEquals, test.want)
		})
	}
}

func TestMigrationsLintSQLServer_StatementSuppressions(t *testing.T) {
	for _, test := range []struct {
		name     string
		sql      string
		wantExit int
		want     []sqlServerLintFinding
	}{
		{
			name: "directly above the nested drop",
			sql:  "BEGIN\n-- ptah:nolint destructive/drop-table\nDROP TABLE dbo.users;\nEND;",
			want: []sqlServerLintFinding{{Rule: "rollout/drop-table", Line: 3}},
		},
		{
			name:     "a blank line detaches the directive",
			sql:      "BEGIN\n-- ptah:nolint destructive/drop-table\n\nDROP TABLE dbo.users;\nEND;",
			wantExit: 1,
			want:     []sqlServerLintFinding{{Rule: "destructive/drop-table", Line: 4}, {Rule: "rollout/drop-table", Line: 4}},
		},
		{
			name:     "a trailing directive belongs to the previous statement",
			sql:      "BEGIN\nSELECT 1; -- ptah:nolint destructive/drop-table\nDROP TABLE dbo.users;\nEND;",
			wantExit: 1,
			want:     []sqlServerLintFinding{{Rule: "destructive/drop-table", Line: 3}, {Rule: "rollout/drop-table", Line: 3}},
		},
		{
			name:     "a directive above a condition does not suppress its body",
			sql:      "-- ptah:nolint\nIF 1 = 1\nBEGIN\nDROP TABLE dbo.users;\nEND;",
			wantExit: 1,
			want:     []sqlServerLintFinding{{Rule: "destructive/drop-table", Line: 4}, {Rule: "rollout/drop-table", Line: 4}},
		},
		{
			name:     "a directive on GO belongs to no statement",
			sql:      "SELECT 1;\nGO 2 -- ptah:nolint destructive/drop-table\nDROP TABLE dbo.users;",
			wantExit: 1,
			want:     []sqlServerLintFinding{{Rule: "destructive/drop-table", Line: 3}, {Rule: "rollout/drop-table", Line: 3}},
		},
		{
			name: "a directive after GO belongs to the next statement",
			sql:  "SELECT 1;\nGO\n-- ptah:nolint destructive/drop-table\nDROP TABLE dbo.users;",
			want: []sqlServerLintFinding{{Rule: "rollout/drop-table", Line: 4}},
		},
		{
			name: "a directive after a terminated routine belongs to the next piece",
			sql:  "CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END;\n-- ptah:nolint destructive/drop-table\nDROP TABLE dbo.users;",
			want: []sqlServerLintFinding{{Rule: "coverage/routine-body", Line: 1}, {Rule: "rollout/drop-table", Line: 3}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := sqlServerMigrationLint(c, test.sql)
			c.Assert(run.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(sqlServerLintFindings(c, run.Stdout), qt.DeepEquals, test.want)
		})
	}
}

// A DROP inside the procedure piece Ptah sends is stored code. A semicolon
// after the body's END, or GO, makes the next DROP an executable piece.
func TestMigrationsLintSQLServer_ModuleBoundary(t *testing.T) {
	const procedure = "CREATE PROCEDURE dbo.p AS\nBEGIN\n  SELECT 1;\nEND;\n"
	for _, test := range []struct {
		name       string
		sql        string
		wantPieces int
		wantExit   int
		want       []sqlServerLintFinding
	}{
		{
			name:       "inside the compound body",
			sql:        "CREATE PROCEDURE dbo.p AS\nBEGIN\n  DROP TABLE dbo.stored;\nEND;",
			wantPieces: 1,
			want:       []sqlServerLintFinding{{Rule: "coverage/routine-body", Line: 1}},
		},
		{
			name:       "no terminator after END",
			sql:        "CREATE PROCEDURE dbo.p AS\nBEGIN\n  SELECT 1;\nEND\nDROP TABLE dbo.stored;",
			wantPieces: 1,
			want:       []sqlServerLintFinding{{Rule: "coverage/routine-body", Line: 1}},
		},
		{
			name:       "a new piece without GO",
			sql:        procedure + "DROP TABLE dbo.users;",
			wantPieces: 2,
			wantExit:   1,
			want: []sqlServerLintFinding{
				{Rule: "coverage/routine-body", Line: 1},
				{Rule: "destructive/drop-table", Line: 5},
				{Rule: "rollout/drop-table", Line: 5},
			},
		},
		{
			name:       "a new batch",
			sql:        procedure + "GO\nDROP TABLE dbo.users;",
			wantPieces: 2,
			wantExit:   1,
			want: []sqlServerLintFinding{
				{Rule: "coverage/routine-body", Line: 1},
				{Rule: "destructive/drop-table", Line: 6},
				{Rule: "rollout/drop-table", Line: 6},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlutil.SplitSourceStatements(test.sql, "sqlserver"), qt.HasLen, test.wantPieces)
			run := sqlServerMigrationLint(c, test.sql)
			c.Assert(run.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(sqlServerLintFindings(c, run.Stdout), qt.DeepEquals, test.want)
		})
	}
}

func sqlServerMigrationLint(c *qt.C, sql string) clirun.Result {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.up.sql"), []byte(sql), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "lint", "--dir", ".", "--dialect", "sqlserver", "--format", "json",
	)
}

type sqlServerLintFinding struct {
	Rule string `json:"rule"`
	Line int    `json:"line"`
}

func sqlServerLintFindings(c *qt.C, output string) []sqlServerLintFinding {
	c.Helper()
	var report struct {
		Findings []sqlServerLintFinding `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(output), &report), qt.IsNil, qt.Commentf("stdout: %s", output))
	return report.Findings
}
