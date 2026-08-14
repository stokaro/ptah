package atlas_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

const compatAtlasCloudIdentifierVersion = ".atlas_cloud_identifier"

func TestCompatMigrateApply_FlywayRecordsSourceIdentityWithoutChangingOrder(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		migrations []setFlywayMigration
		wantRows   []string
	}{
		{
			name: "plain",
			migrations: []setFlywayMigration{
				{name: "V1__plain.sql", body: "CREATE TABLE plain_identity (id INTEGER PRIMARY KEY);\n"},
			},
			wantRows: []string{"1"},
		},
		{
			name: "dotted",
			migrations: []setFlywayMigration{
				{name: "V1.5__dotted.sql", body: "CREATE TABLE dotted_identity (id INTEGER PRIMARY KEY);\n"},
			},
			wantRows: []string{"1.5"},
		},
		{
			name: "zero padded",
			migrations: []setFlywayMigration{
				{name: "V01__padded.sql", body: "CREATE TABLE padded_identity (id INTEGER PRIMARY KEY);\n"},
			},
			wantRows: []string{"01"},
		},
		{
			name: "non numeric",
			migrations: []setFlywayMigration{
				{name: "Vx__named.sql", body: "CREATE TABLE named_identity (id INTEGER PRIMARY KEY);\n"},
			},
			wantRows: []string{"x"},
		},
		{
			name: "baseline remains first by order key",
			migrations: []setFlywayMigration{
				{name: "B1.5__base.sql", body: "CREATE TABLE baseline_identity (id INTEGER PRIMARY KEY);\n"},
				{name: "V2__later.sql", body: "CREATE TABLE later_identity (id INTEGER PRIMARY KEY);\n"},
			},
			wantRows: []string{"1.5", "2"},
		},
		{
			name: "repeatable has exact empty identity",
			migrations: []setFlywayMigration{
				{name: "V1__table.sql", body: "CREATE TABLE repeatable_identity (id INTEGER PRIMARY KEY);\n"},
				{name: "R__view.sql", body: "CREATE VIEW repeatable_view AS SELECT id FROM repeatable_identity;\n"},
			},
			wantRows: []string{"", "1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := writeHashedFlywayDir(c, test.migrations)
			dbPath := filepath.Join(filepath.Dir(dir), "apply.db")

			stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, test.wantRows)
			for _, version := range test.wantRows {
				c.Assert(revisionOperatorVersion(c, dbPath, version), qt.Equals, "Ptah/source-identity")
			}
		})
	}
}

func TestCompatMigrateApply_FlywayRemovedOpaqueHistoryRemainsReadable(t *testing.T) {
	t.Parallel()
	assertCompatFlywayRemovedHistoryRemainsReadable(
		t,
		"Vfoo__retired.sql",
		"foo",
		"Va__older.sql",
		"retired_opaque_history",
	)
}

func TestCompatMigrateApply_FlywayRemovedDotHistoryRemainsReadable(t *testing.T) {
	t.Parallel()
	assertCompatFlywayRemovedHistoryRemainsReadable(
		t,
		"V.foo__retired.sql",
		".foo",
		"V.aaa__older.sql",
		"retired_dot_history",
	)
}

func TestCompatMigrateApply_FormatCurrentUsesRetiredExactIdentityOrder(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V1__remaining.sql", body: "CREATE TABLE remaining_current (id INTEGER PRIMARY KEY);\n"},
		{name: "V2__retired.sql", body: "CREATE TABLE retired_current (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "retired-current.db")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(os.Remove(filepath.Join(dir, "V2__retired.sql")), qt.IsNil)
	hashConvertedApplyDir(c, dir, "flyway")

	stdout, stderr, err = runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--format", "{{ .Current }}",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "2")
}

func TestCompatMigrateStatus_CorruptRetiredIdentityKeepsExactDiagnostic(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "Vretired__old.sql", body: "CREATE TABLE corrupt_retired (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "corrupt-retired.db")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(os.Remove(filepath.Join(dir, "Vretired__old.sql")), qt.IsNil)
	hashConvertedApplyDir(c, dir, "flyway")
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.Exec("UPDATE atlas_schema_revisions SET applied = -1 WHERE version = 'retired'")
	c.Assert(err, qt.IsNil)

	_, stderr, err = runCompatExit(
		"migrate", "status",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)
	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	c.Assert(message, qt.Contains, "migration retired cannot read revision metadata")
	c.Assert(message, qt.Not(qt.Contains), "migration 0 cannot")
}

func TestCompatMigrateStatus_FormatUsesRetiredDirtyRevisionDetails(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V1__remaining.sql", body: "CREATE TABLE remaining_clean (id INTEGER PRIMARY KEY);\n"},
		{name: "Vretired__old.sql", body: "CREATE TABLE retired_dirty (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "retired-dirty.db")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(os.Remove(filepath.Join(dir, "Vretired__old.sql")), qt.IsNil)
	hashConvertedApplyDir(c, dir, "flyway")
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.Exec(`UPDATE atlas_schema_revisions
SET applied = 1, total = 2, error = 'boom
detail', error_stmt = 'CREATE TABLE broken (
  id integer
);'
WHERE version = 'retired'`)
	c.Assert(err, qt.IsNil)

	stdout, stderr, err = runCompatExit(
		"migrate", "status",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--format", `{{ .Count }}|{{ .Total }}|{{ .Error }}|{{ .SQL }}`,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("status stderr: %s", stderr))
	c.Assert(stdout, qt.Equals,
		"1|2|boom detail|CREATE TABLE broken (   id integer );")
}

func TestCompatMigrateApply_AllowDirtyRefusesRetiredOpaqueIdentity(t *testing.T) {
	t.Parallel()
	assertCompatMigrateApplyAllowDirtyRefusesRetiredExactIdentity(
		t,
		"Vretired__old.sql",
		"retired",
		"migration retired is dirty",
	)
}

func TestCompatMigrateApply_AllowDirtyRefusesRetiredEmptyIdentity(t *testing.T) {
	t.Parallel()
	assertCompatMigrateApplyAllowDirtyRefusesRetiredExactIdentity(
		t,
		"V.sql",
		"",
		`migration "" is dirty`,
	)
}

func TestCompatMigrateApply_AllowDirtyRetriesOwnedEmptyIdentity(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{{
		name: "V.sql",
		body: `CREATE TABLE owned_empty_partial (id INTEGER PRIMARY KEY);
THIS IS A FAILING STATEMENT;
`,
	}})
	dbPath := filepath.Join(c.TempDir(), "owned-empty-dirty-apply.db")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath, "--tx-mode", "none")
	c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"owned_empty_partial"})

	writeAtlasApplyProjectMigration(c, dir, "V.sql", `CREATE TABLE owned_empty_partial (id INTEGER PRIMARY KEY);
CREATE TABLE owned_empty_retry (id INTEGER PRIMARY KEY);
`)
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err = compatApplyConverted(
		dir,
		"flyway",
		dbPath,
		"--tx-mode",
		"none",
		"--allow-dirty",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"owned_empty_partial", "owned_empty_retry"})
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{""})
}

func assertCompatMigrateApplyAllowDirtyRefusesRetiredExactIdentity(
	t *testing.T,
	retiredFile, retiredIdentity, wantDiagnostic string,
) {
	t.Helper()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{{
		name: retiredFile,
		body: `CREATE TABLE retired_partial (id INTEGER PRIMARY KEY);
THIS IS A FAILING STATEMENT;
`,
	}})
	dbPath := filepath.Join(c.TempDir(), "retired-dirty-apply.db")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath, "--tx-mode", "none")
	c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(errorText(err)+stderr, qt.Contains, "failed to apply migration")
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"retired_partial"})

	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	var beforeApplied, beforeTotal int
	var beforeError, beforeStatement string
	c.Assert(db.QueryRow(`SELECT applied, total, error, error_stmt
FROM atlas_schema_revisions WHERE version = ?`, retiredIdentity).Scan(
		&beforeApplied,
		&beforeTotal,
		&beforeError,
		&beforeStatement,
	), qt.IsNil)

	c.Assert(os.Remove(filepath.Join(dir, retiredFile)), qt.IsNil)
	writeAtlasApplyProjectMigration(c, dir, "Vsurviving__later.sql",
		"CREATE TABLE later_must_not_run (id INTEGER PRIMARY KEY);\n")
	hashConvertedApplyDir(c, dir, "flyway")

	stdout, stderr, err = compatApplyConverted(
		dir,
		"flyway",
		dbPath,
		"--tx-mode",
		"none",
		"--allow-dirty",
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	message := errorText(err) + stderr
	c.Assert(message, qt.Contains, wantDiagnostic)
	c.Assert(message, qt.Not(qt.Contains), "migration 0 is dirty")
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"retired_partial"})
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{retiredIdentity})
	var afterApplied, afterTotal int
	var afterError, afterStatement string
	c.Assert(db.QueryRow(`SELECT applied, total, error, error_stmt
FROM atlas_schema_revisions WHERE version = ?`, retiredIdentity).Scan(
		&afterApplied,
		&afterTotal,
		&afterError,
		&afterStatement,
	), qt.IsNil)
	c.Assert(afterApplied, qt.Equals, beforeApplied)
	c.Assert(afterTotal, qt.Equals, beforeTotal)
	c.Assert(afterError, qt.Equals, beforeError)
	c.Assert(afterStatement, qt.Equals, beforeStatement)
}

func assertCompatFlywayRemovedHistoryRemainsReadable(
	t *testing.T,
	retiredFile, retiredToken, olderFile, table string,
) {
	t.Helper()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{{
		name: retiredFile,
		body: "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY);\n",
	}})
	dbPath := filepath.Join(c.TempDir(), "retired-history.db")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	insertCompatAtlasCloudIdentifier(c, dbPath)
	c.Assert(os.Remove(filepath.Join(dir, retiredFile)), qt.IsNil)
	hashConvertedApplyDir(c, dir, "flyway")

	stdout, stderr, err = runCompatExit(
		"migrate", "status",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "  -- Current Version: "+retiredToken+"\n")

	stdout, stderr, err = compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "No migration files to execute")

	writeAtlasApplyProjectMigration(c, dir, olderFile,
		"CREATE TABLE retired_history_must_guard_order (id INTEGER PRIMARY KEY);\n")
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err = compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(errorText(err)+stderr, qt.Contains, "out-of-order pending migrations")
	c.Assert(errorText(err)+stderr, qt.Contains, `current version "`+retiredToken+`"`)
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{table})
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{compatAtlasCloudIdentifierVersion, retiredToken})
}

func insertCompatAtlasCloudIdentifier(c *qt.C, dbPath string) {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.Exec(`INSERT INTO atlas_schema_revisions
(version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES (?, '472fecf4-5a9c-431f-8ff1-8e1facd1d50b', 2, 0, 0, '2026-08-01 12:04:21.291103+02:00', 0, NULL, NULL, '', NULL, 'Atlas')`,
		compatAtlasCloudIdentifierVersion,
	)
	c.Assert(err, qt.IsNil)
}

func TestCompatMigrateApply_FlywayBaselineResolvesExactToken(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "B1.5__base.sql", body: "CREATE TABLE exact_baseline (id INTEGER PRIMARY KEY);\n"},
		{name: "V2__later.sql", body: "CREATE TABLE after_exact_baseline (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "exact-baseline.db")

	stdout, stderr, err := runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--baseline", "1.5",
		"--dry-run",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "Would baseline migrations at version 1.5.\n")
	c.Assert(stdout, qt.Contains, "Migrating to version 2 from 1 pending migrations.\n")
	c.Assert(stdout, qt.Not(qt.Contains), "461168")
	c.Assert(userTables(c, dbPath), qt.HasLen, 0)
}

func TestCompatMigrateApply_FlywayBaselineTreatsExactEmptyTokenAsPresent(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V.sql", body: "CREATE TABLE empty_baseline_must_not_run (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "empty-baseline.db")

	stdout, stderr, err := runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--baseline", "",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(userTables(c, dbPath), qt.HasLen, 0)
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{""})
}

func TestCompatMigrateApply_FlywayBaselineSettlesSameTokenSquash(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V2__base.sql", body: "CREATE TABLE squashed_version_must_not_run (id INTEGER PRIMARY KEY);\n"},
		{name: "B2__base.sql", body: "CREATE TABLE baseline_body_must_not_run (id INTEGER PRIMARY KEY);\n"},
		{name: "V3__later.sql", body: "CREATE TABLE after_same_token_baseline (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "same-token-baseline.db")

	stdout, stderr, err := runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--baseline", "2",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"after_same_token_baseline"})
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{"2", "3"})
	c.Assert(revisionType(c, dbPath, "2"), qt.Equals, 1)
	c.Assert(revisionOperatorVersion(c, dbPath, "2"), qt.Equals, "Ptah/source-baseline")

	stdout, stderr, err = compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "No migration files to execute")
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"after_same_token_baseline"})
}

func TestCompatMigrateApply_FlywayBaselineDoesNotSettleALaterSameTokenBaseline(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V2__base.sql", body: "CREATE TABLE versioned_body_must_not_run (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "versioned-baseline.db")

	stdout, stderr, err := runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--baseline", "2",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(revisionType(c, dbPath, "2"), qt.Equals, 1)
	c.Assert(revisionOperatorVersion(c, dbPath, "2"), qt.Equals, "Ptah/source-identity")

	writeAtlasApplyProjectMigration(
		c,
		dir,
		"B2__base.sql",
		"CREATE TABLE baseline_body_must_not_be_skipped (id INTEGER PRIMARY KEY);\n",
	)
	hashConvertedApplyDir(c, dir, "flyway")

	stdout, stderr, err = compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(errorText(err)+stderr, qt.Contains, "B2__base.sql")
	c.Assert(userTables(c, dbPath), qt.HasLen, 0)
	c.Assert(revisionOperatorVersion(c, dbPath, "2"), qt.Equals, "Ptah/source-identity")
}

func revisionOperatorVersion(c *qt.C, dbPath, version string) string {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	var operatorVersion string
	c.Assert(db.QueryRow(
		"SELECT operator_version FROM atlas_schema_revisions WHERE version = ?",
		version,
	).Scan(&operatorVersion), qt.IsNil)
	return operatorVersion
}

func TestCompatMigrateApply_FlywayBaselinePreparationErrorUsesExactToken(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "B1.5__base.sql", body: "CREATE TABLE exact_baseline_error (id INTEGER PRIMARY KEY);\n"},
		{name: "V2__later.sql", body: "CREATE TABLE after_exact_baseline_error (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "exact-baseline-error.db")

	stdout, stderr, err := runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--baseline", "1.5",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	markAtlasRevisionDirty(c, dbPath, "1.5", 3)

	_, stderr, err = runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--baseline", "1.5",
	)

	c.Assert(err, qt.IsNotNil)
	message := errorText(err) + stderr
	c.Assert(message, qt.Contains, "migration 1.5 is dirty")
	c.Assert(message, qt.Not(qt.Contains), "84032")
}

func TestCompatMigrateApply_FlywayToVersionResolvesExactToken(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V1__one.sql", body: "CREATE TABLE exact_bound_one (id INTEGER PRIMARY KEY);\n"},
		{name: "V1.5__half.sql", body: "CREATE TABLE exact_bound_half (id INTEGER PRIMARY KEY);\n"},
		{name: "V2__two.sql", body: "CREATE TABLE exact_bound_two (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "exact-bound.db")

	stdout, stderr, err := runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--to-version", "1.5",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "Migrating to version 1.5 from 2 pending migrations.\n")
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{"1", "1.5"})
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"exact_bound_half", "exact_bound_one"})
}

func TestCompatMigrateApply_FlywayToVersionTreatsExactEmptyTokenAsPresent(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V.sql", body: "CREATE TABLE empty_target (id INTEGER PRIMARY KEY);\n"},
		{name: "V1__later.sql", body: "CREATE TABLE after_empty_target (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "empty-target.db")

	stdout, stderr, err := runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--to-version", "",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"empty_target"})
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{""})
}

func TestCompatMigrateApply_FlywayVersionFlagsRefuseUnknownExactToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		flag    string
		wantErr string
	}{
		{name: "baseline", flag: "--baseline", wantErr: `baseline version "missing" not found`},
		{name: "to version", flag: "--to-version", wantErr: `target version "missing" was not found in the migration provider`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := writeHashedFlywayDir(c, []setFlywayMigration{
				{name: "V1__one.sql", body: "CREATE TABLE unknown_exact_token (id INTEGER PRIMARY KEY);\n"},
			})
			dbPath := filepath.Join(c.TempDir(), "unknown-token.db")

			_, _, err := runCompatExit(
				"migrate", "apply",
				"--dir", "file://"+dir+"?format=flyway",
				"--url", "sqlite://"+dbPath,
				test.flag, "missing",
			)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(userTables(c, dbPath), qt.HasLen, 0)
		})
	}
}

func TestCompatMigrateApply_FlywayOnlyRepeatableNeverLeaksOrderKey(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	const repeatableOrderKey = "9223372036854775807"
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "R__only.sql", body: "CREATE TABLE repeatable_only (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(filepath.Dir(dir), "only-repeatable.db")
	formattedDBPath := filepath.Join(filepath.Dir(dir), "only-repeatable-formatted.db")
	formattedStdout, formattedStderr, formattedErr := runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+formattedDBPath,
		"--format", `{{ json . }}`,
	)
	c.Assert(formattedErr, qt.IsNil, qt.Commentf("formatted apply stderr: %s", formattedStderr))
	c.Assert(formattedStdout, qt.Contains,
		`"Pending":[{"Name":"9223372036854775807_only.sql","Version":""`)
	c.Assert(formattedStdout, qt.Contains,
		`"Applied":[{"Name":"9223372036854775807_only.sql","Version":""`)

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "Migrating to version  from 1 pending migrations.\n")
	c.Assert(stdout, qt.Contains, "Migration complete. Current version: \"\"\n")
	c.Assert(stdout, qt.Not(qt.Contains), "Migration complete. Current version: \n")
	c.Assert(stdout, qt.Not(qt.Contains), repeatableOrderKey)

	stdout, stderr, err = runCompatExit(
		"migrate", "status",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("status stderr: %s", stderr))
	c.Assert(stdout, qt.Contains, "  -- Current Version: \n")
	c.Assert(stdout, qt.Not(qt.Contains), repeatableOrderKey)

	stdout, stderr, err = runCompatExit(
		"migrate", "status",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--format", `{{ json . }}`,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("formatted status stderr: %s", stderr))
	c.Assert(stdout, qt.Contains, `"Current":""`)
	c.Assert(stdout, qt.Contains, `"Applied":[{"Version":""`)

	stdout, stderr, err = runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--format", `{{ json . }}`,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("formatted apply stderr: %s", stderr))
	c.Assert(stdout, qt.Contains, `"Current":"","Target":""`)
	c.Assert(stdout, qt.Not(qt.Contains), repeatableOrderKey)
}

func TestCompatMigrateStatus_FlywayMetadataErrorNamesExactToken(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V1.5__invalid_metadata.sql", body: "CREATE TABLE invalid_metadata (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "invalid-metadata.db")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.Exec(`UPDATE atlas_schema_revisions SET applied = 2, total = 1 WHERE version = '1.5'`)
	c.Assert(err, qt.IsNil)

	stdout, stderr, err = runCompatExit(
		"migrate", "status",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)

	c.Assert(err, qt.IsNotNil)
	message := stdout + stderr + errorText(err)
	c.Assert(message, qt.Contains, "migration 1.5 cannot read revision metadata")
	c.Assert(message, qt.Not(qt.Contains), "4611686018427471935")
	c.Assert(message, qt.Not(qt.Contains), "461168")
}

func TestCompatMigrateApply_FlywayRepeatableBodyChangeRemainsSettled(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "R__only.sql", body: "CREATE TABLE repeatable_settled (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "repeatable-settled.db")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	writeAtlasApplyProjectMigration(c, dir, "R__only.sql",
		"CREATE TABLE repeatable_settled (id INTEGER PRIMARY KEY);\n"+
			"CREATE TABLE repeatable_changed (id INTEGER PRIMARY KEY);\n")
	hashConvertedApplyDir(c, dir, "flyway")

	stdout, stderr, err = compatApplyConverted(dir, "flyway", dbPath)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "No migration files to execute")
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"repeatable_settled"})
}

func TestCompatMigrateApply_FlywayDotPrefixedIdentityRemainsVisible(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V.foo.sql", body: "CREATE TABLE reserved_identity (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(filepath.Dir(dir), "reserved.db")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(revisionRows(c, dbPath), qt.DeepEquals,
		[]setFlywayRow{{Version: ".foo", Description: ""}})

	stdout, stderr, err = compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "No migration files to execute")
}

func TestCompatMigrateApply_FlywayRolledBackFailureLeavesNoExactDirtyRow(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V1.5__retry.sql", body: "CREATE TABLE rolled_back_exact (id INTEGER PRIMARY KEY);\nINVALID SQL;\n"},
	})
	dbPath := filepath.Join(filepath.Dir(dir), "retry.db")

	_, _, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNotNil)
	c.Assert(revisionVersions(c, dbPath), qt.HasLen, 0)

	writeAtlasApplyProjectMigration(c, dir, "V1.5__retry.sql",
		"CREATE TABLE rolled_back_exact (id INTEGER PRIMARY KEY);\n")
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{"1.5"})
}

func TestCompatMigrateApply_FlywayTokenEndingRIsNotAtlasRepeatable(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V1R__ordinary.sql", body: "CREATE TABLE token_ending_r (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(filepath.Dir(dir), "token-ending-r.db")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(revisionRows(c, dbPath), qt.DeepEquals,
		[]setFlywayRow{{Version: "1R", Description: "ordinary"}})

	writeAtlasApplyProjectMigration(c, dir, "V1R__ordinary.sql",
		"CREATE TABLE token_ending_r_changed (id INTEGER PRIMARY KEY);\n")
	hashConvertedApplyDir(c, dir, "flyway")
	stdout, stderr, err = compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stdout, qt.Not(qt.Contains), "461168")
	c.Assert(stderr, qt.Contains, "checksum mismatch")
	c.Assert(stderr, qt.Contains, "1R")
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"token_ending_r"})
}

func TestCompatMigrateApply_FlywayExecutionFailureNamesExactToken(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V1.5__broken.sql", body: "INVALID SQL;\n"},
	})
	dbPath := filepath.Join(filepath.Dir(dir), "exact-error.db")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Contains, "Migrating to version 1.5")
	c.Assert(stdout, qt.Not(qt.Contains), "4611686018427471935")
	c.Assert(stdout, qt.Not(qt.Contains), "461168")
	c.Assert(stderr, qt.Contains, "1.5")
	c.Assert(stderr, qt.Not(qt.Contains), "4611686018427471935")
	c.Assert(stderr, qt.Not(qt.Contains), "461168")
}

func TestCompatMigrateApply_FlywayFormatFailureNamesExactToken(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V1.5__broken.sql", body: "INVALID SQL;\n"},
	})

	stdout, stderr, err := runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+filepath.Join(c.TempDir(), "exact-format-error.db"),
		"--format", `{{ .Error }}`,
	)
	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Contains, "failed to apply migration 1.5")
	c.Assert(stdout, qt.Not(qt.Contains), "4611686018427471935")
	c.Assert(stdout, qt.Not(qt.Contains), "461168")
	c.Assert(stderr, qt.Equals, "")

	stdout, stderr, err = runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+filepath.Join(c.TempDir(), "exact-json-error.db"),
		"--format", `{{ json . }}`,
	)
	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Contains, `"Version":"1.5"`)
	c.Assert(stdout, qt.Contains, `"Error":"failed to apply migration 1.5`)
	c.Assert(stdout, qt.Not(qt.Contains), `"Error":"failed to apply migration 4611686018427471935`)
	c.Assert(stderr, qt.Equals, "")
}

func TestCompatMigrateApply_ExactIdentityMappingPreservesNumericSQLObject(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	const runtime = "4611686018427471935"
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V1.5__broken.sql", body: `INSERT INTO "` + runtime + `" VALUES (1);`},
	})
	dbPath := filepath.Join(c.TempDir(), "numeric-object-error.db")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout+stderr+errorText(err), qt.Contains, "failed to apply migration 1.5")
	c.Assert(stdout+stderr+errorText(err), qt.Contains, runtime)
}

// TestCompatMigrateApply_ExactFlywayIdentityDoesNotNarrowOtherFormats pins the
// other side of the dual-identity rule. Goose has no distinct source-token
// identity: a zero-padded revision row and the migration's numeric prefix still
// identify the same applied migration. Requiring Flyway's exact match here
// would replay a non-idempotent CREATE TABLE on the second invocation.
func TestCompatMigrateApply_ExactFlywayIdentityDoesNotNarrowOtherFormats(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	dbPath := filepath.Join(c.TempDir(), "goose.db")
	writeAtlasApplyProjectMigration(c, dir, "1_init.sql",
		"-- +goose Up\nCREATE TABLE goose_identity (id INTEGER PRIMARY KEY);\n")
	hashConvertedApplyDir(c, dir, "goose")

	stdout, stderr, err := compatApplyConverted(dir, "goose", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	rewriteRevisionVersion(c, dbPath, "1", "00001")

	stdout, stderr, err = compatApplyConverted(dir, "goose", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "No migration files to execute")
	c.Assert(revisionVersions(c, dbPath), qt.DeepEquals, []string{"00001"})
}
