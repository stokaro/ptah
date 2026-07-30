package atlas_test

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"

	"github.com/stokaro/ptah/cmd/atlas"
)

func TestMigrateApply_RecordsAppliedAtlasRevisionMetadata(t *testing.T) {
	c := qt.New(t)
	fixture := newAtlasRevisionFixture(c)

	runAtlasCommand(c,
		"migrate", "apply",
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)

	got := readAtlasRevision(c, fixture.dbPath, fixture.version)
	c.Assert(got, qt.DeepEquals, atlasRevisionRow{
		Version:         fixture.version,
		Description:     "create_users",
		Type:            2,
		Applied:         1,
		Total:           1,
		Hash:            fixture.hash,
		PartialHashes:   "null",
		OperatorVersion: "Ptah",
	})
	c.Assert(
		readAtlasRevisionStorageTypes(c, fixture.dbPath, fixture.version),
		qt.DeepEquals,
		atlasRevisionStorageTypes{
			Error:          "text",
			ErrorStatement: "text",
			PartialHashes:  "blob",
		},
	)
	_, err := time.Parse(time.RFC3339Nano, readAtlasRevisionExecutedAt(c, fixture.dbPath, fixture.version))
	c.Assert(err, qt.IsNil)
	c.Assert(readAtlasRevisionVersions(c, fixture.dbPath), qt.DeepEquals, []string{
		"20260719000000",
		fixture.version,
	})
	c.Assert(renderAtlasRevisionStatus(c, fixture), qt.Equals, "applied|0")
}

func TestMigrateSet_RecordsManuallySetAtlasRevisionMetadata(t *testing.T) {
	c := qt.New(t)
	fixture := newAtlasRevisionFixture(c)

	output := runAtlasCommand(c,
		"migrate", "set", fixture.version,
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)

	c.Assert(output, qt.Equals,
		"Current version is 20260719010000 (2 set):\n\n"+
			"  + 20260719000000 (create_accounts)\n"+
			"  + 20260719010000 (create_users)",
	)
	got := readAtlasRevision(c, fixture.dbPath, fixture.version)
	c.Assert(got, qt.DeepEquals, atlasRevisionRow{
		Version:         fixture.version,
		Description:     "create_users",
		Type:            4,
		Applied:         0,
		Total:           0,
		Hash:            fixture.hash,
		PartialHashes:   "null",
		OperatorVersion: "Ptah",
	})
	c.Assert(readAtlasRevisionVersions(c, fixture.dbPath), qt.DeepEquals, []string{
		"20260719000000",
		fixture.version,
	})
	c.Assert(renderAtlasRevisionStatus(c, fixture), qt.Equals, "manually set|0")
}

func TestMigrateSet_PreservesExistingAtlasRevisions(t *testing.T) {
	c := qt.New(t)
	fixture := newAtlasRevisionFixture(c)

	runAtlasCommand(c,
		"migrate", "apply", "1",
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)
	runAtlasCommand(c,
		"migrate", "set", fixture.version,
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)

	first := readAtlasRevision(c, fixture.dbPath, fixture.previousVersion)
	c.Assert(first, qt.DeepEquals, atlasRevisionRow{
		Version:         fixture.previousVersion,
		Description:     "create_accounts",
		Type:            2,
		Applied:         1,
		Total:           1,
		Hash:            fixture.previousHash,
		PartialHashes:   "null",
		OperatorVersion: "Ptah",
	})
	second := readAtlasRevision(c, fixture.dbPath, fixture.version)
	c.Assert(second.Type, qt.Equals, 4)
	c.Assert(second.Applied, qt.Equals, 0)
	c.Assert(second.Total, qt.Equals, 0)
}

func TestMigrateSet_RemovesAtlasRevisionsAboveTarget(t *testing.T) {
	c := qt.New(t)
	fixture := newAtlasRevisionFixture(c)

	runAtlasCommand(c,
		"migrate", "apply",
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)
	runAtlasCommand(c,
		"migrate", "set", fixture.previousVersion,
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)

	got := readAtlasRevision(c, fixture.dbPath, fixture.previousVersion)
	c.Assert(got.Type, qt.Equals, 2)
	c.Assert(got.Applied, qt.Equals, 1)
	c.Assert(got.Total, qt.Equals, 1)
	c.Assert(readAtlasRevisionVersions(c, fixture.dbPath), qt.DeepEquals, []string{fixture.previousVersion})
	c.Assert(renderAtlasRevisionStatus(c, fixture), qt.Equals, "applied|1")
}

func TestMigrateSet_PreservesDirtyAtlasRevisionAndCombinesType(t *testing.T) {
	c := qt.New(t)
	fixture := newAtlasRevisionFixture(c)

	runAtlasCommand(c,
		"migrate", "apply",
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)
	markAtlasRevisionDirty(c, fixture.dbPath, fixture.version, 2)
	output := runAtlasCommand(c,
		"migrate", "set", fixture.version,
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)

	c.Assert(output, qt.Equals,
		"Current version is 20260719010000 (1 set):\n\n"+
			"  + 20260719010000 (create_users)",
	)
	got := readAtlasRevision(c, fixture.dbPath, fixture.version)
	c.Assert(got.Type, qt.Equals, 6)
	c.Assert(got.Applied, qt.Equals, 0)
	c.Assert(got.Total, qt.Equals, 1)
	c.Assert(got.Error, qt.Equals, "broken")
	status := runAtlasCommand(c,
		"migrate", "status",
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
		"--format", "{{ (index .Applied 1).Type }}|{{ (index .Applied 1).Error }}|{{ .Current }}|{{ .Next }}|{{ .Status }}",
	)
	c.Assert(status, qt.Equals, "applied + manually set|broken|20260719010000|20260719010000|PENDING")
}

func TestMigrateSet_ReplacesDirtyBaselineType(t *testing.T) {
	c := qt.New(t)
	fixture := newAtlasRevisionFixture(c)

	runAtlasCommand(c,
		"migrate", "apply",
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
		"--baseline", fixture.version,
	)
	markAtlasRevisionDirty(c, fixture.dbPath, fixture.version, 1)
	runAtlasCommand(c,
		"migrate", "set", fixture.version,
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)

	got := readAtlasRevision(c, fixture.dbPath, fixture.version)
	c.Assert(got.Type, qt.Equals, 6)
	c.Assert(got.Error, qt.Equals, "broken")
}

func TestMigrateSet_ReplacesDirtyUnknownType(t *testing.T) {
	c := qt.New(t)
	fixture := newAtlasRevisionFixture(c)

	runAtlasCommand(c,
		"migrate", "set", fixture.version,
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)
	markAtlasRevisionDirty(c, fixture.dbPath, fixture.version, 8)
	runAtlasCommand(c,
		"migrate", "set", fixture.version,
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)

	got := readAtlasRevision(c, fixture.dbPath, fixture.version)
	c.Assert(got.Type, qt.Equals, 6)
	c.Assert(got.Error, qt.Equals, "broken")
}

func TestMigrateSet_ReportsSetAndRemovedRevisions(t *testing.T) {
	c := qt.New(t)
	fixture := newAtlasRevisionFixture(c)

	runAtlasCommand(c,
		"migrate", "apply",
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)
	markAtlasRevisionDirty(c, fixture.dbPath, fixture.previousVersion, 1)
	output := runAtlasCommand(c,
		"migrate", "set", fixture.previousVersion,
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)

	c.Assert(output, qt.Equals,
		"Current version is 20260719000000 (1 set, 1 removed):\n\n"+
			"  + 20260719000000 (create_accounts)\n"+
			"  - 20260719010000 (create_users)",
	)
}

func TestMigrateSet_NoChangesProducesNoOutput(t *testing.T) {
	c := qt.New(t)
	fixture := newAtlasRevisionFixture(c)

	runAtlasCommand(c,
		"migrate", "set", fixture.version,
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)
	output := runAtlasCommand(c,
		"migrate", "set", fixture.version,
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)

	c.Assert(output, qt.Equals, "")
}

func TestMigrateSet_VersionOnlyFileOmitsEmptyDescription(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	migrationsDir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(
		os.WriteFile(filepath.Join(migrationsDir, "1.sql"), []byte("SELECT 1;\n"), 0o600),
		qt.IsNil,
	)
	runAtlasCommand(c, "migrate", "hash", "--dir", "file://"+migrationsDir)

	output := runAtlasCommand(c,
		"migrate", "set", "1",
		"--url", "sqlite://"+filepath.Join(root, "state.db"),
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(output, qt.Equals, "Current version is 1 (1 set):\n\n  + 1")
}

func TestMigrateSet_OutputMatchesAtlasBytes(t *testing.T) {
	c := qt.New(t)
	fixture := newAtlasRevisionFixture(c)

	output := executeAtlasCommand(c,
		"migrate", "set", fixture.version,
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
	)

	c.Assert(output, qt.Equals,
		"Current version is 20260719010000 (2 set):\n\n"+
			"  + 20260719000000 (create_accounts)\n"+
			"  + 20260719010000 (create_users)\n\n",
	)
}

func TestMigrateSet_OutputFailureIsReported(t *testing.T) {
	c := qt.New(t)
	fixture := newAtlasRevisionFixture(c)
	cmd := atlas.NewCompatCommand("atlas")
	cmd.SetOut(closedWriter{})
	var errOut bytes.Buffer
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"migrate", "set", fixture.version,
		"--url", "sqlite://" + fixture.dbPath,
		"--dir", "file://" + fixture.migrationsDir,
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "write migrate set summary: io: read/write on closed pipe")
}

func TestMigrateApply_BaselineRecordsAtlasRevisionMetadata(t *testing.T) {
	c := qt.New(t)
	fixture := newAtlasRevisionFixture(c)

	runAtlasCommand(c,
		"migrate", "apply",
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
		"--baseline", fixture.version,
	)

	got := readAtlasRevision(c, fixture.dbPath, fixture.version)
	c.Assert(got, qt.DeepEquals, atlasRevisionRow{
		Version:         fixture.version,
		Description:     "create_users",
		Type:            1,
		Applied:         0,
		Total:           0,
		Hash:            "",
		PartialHashes:   "null",
		OperatorVersion: "Ptah",
	})
	c.Assert(readAtlasRevisionVersions(c, fixture.dbPath), qt.DeepEquals, []string{fixture.version})
	c.Assert(renderAtlasRevisionStatus(c, fixture), qt.Equals, "baseline|0")
}

func TestMigrateApply_BaselineRejectsMissingAtlasVersion(t *testing.T) {
	c := qt.New(t)
	fixture := newAtlasRevisionFixture(c)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + fixture.dbPath,
		"--dir", "file://" + fixture.migrationsDir,
		"--baseline", "20260719020000",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `.*baseline version "20260719020000" not found`)
}

type atlasRevisionFixture struct {
	dbPath          string
	migrationsDir   string
	previousVersion string
	previousHash    string
	version         string
	hash            string
}

func newAtlasRevisionFixture(c *qt.C) atlasRevisionFixture {
	c.Helper()
	root := c.TempDir()
	migrationsDir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)

	const (
		previousFilename = "20260719000000_create_accounts.sql"
		previousSQLBody  = "CREATE TABLE accounts (id INTEGER PRIMARY KEY);\n"
		version          = "20260719010000"
		filename         = version + "_create_users.sql"
		sqlBody          = "CREATE TABLE users (id INTEGER PRIMARY KEY);\n"
	)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, previousFilename), []byte(previousSQLBody), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, filename), []byte(sqlBody), 0o600), qt.IsNil)
	runAtlasCommand(c, "migrate", "hash", "--dir", "file://"+migrationsDir)

	previousHashInput := append([]byte(previousFilename), []byte(previousSQLBody)...)
	previousHashSum := sha256.Sum256(previousHashInput)
	hashInput := append([]byte(nil), previousHashInput...)
	hashInput = append(hashInput, []byte(filename)...)
	hashInput = append(hashInput, []byte(sqlBody)...)
	hashSum := sha256.Sum256(hashInput)

	return atlasRevisionFixture{
		dbPath:          filepath.Join(root, "state.db"),
		migrationsDir:   migrationsDir,
		previousVersion: "20260719000000",
		previousHash:    base64.StdEncoding.EncodeToString(previousHashSum[:]),
		version:         version,
		hash:            base64.StdEncoding.EncodeToString(hashSum[:]),
	}
}

type atlasRevisionRow struct {
	Version         string
	Description     string
	Type            int
	Applied         int
	Total           int
	Error           string
	Hash            string
	PartialHashes   string
	OperatorVersion string
}

type atlasRevisionStorageTypes struct {
	Error          string
	ErrorStatement string
	PartialHashes  string
}

func readAtlasRevision(c *qt.C, dbPath, version string) atlasRevisionRow {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(db.Close(), qt.IsNil)
	})

	var got atlasRevisionRow
	err = db.QueryRow(`
SELECT version, description, type, applied, total, COALESCE(error, ''), hash,
       COALESCE(CAST(partial_hashes AS TEXT), 'sql-null'), operator_version
FROM atlas_schema_revisions
WHERE version = ?
`, version).Scan(
		&got.Version,
		&got.Description,
		&got.Type,
		&got.Applied,
		&got.Total,
		&got.Error,
		&got.Hash,
		&got.PartialHashes,
		&got.OperatorVersion,
	)
	c.Assert(err, qt.IsNil)
	return got
}

func readAtlasRevisionExecutedAt(c *qt.C, dbPath, version string) string {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(db.Close(), qt.IsNil)
	})

	var executedAt string
	err = db.QueryRow(
		`SELECT CAST(executed_at AS TEXT) FROM atlas_schema_revisions WHERE version = ?`,
		version,
	).Scan(&executedAt)
	c.Assert(err, qt.IsNil)
	return executedAt
}

func readAtlasRevisionStorageTypes(c *qt.C, dbPath, version string) atlasRevisionStorageTypes {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(db.Close(), qt.IsNil)
	})

	var got atlasRevisionStorageTypes
	err = db.QueryRow(
		`SELECT typeof(error), typeof(error_stmt), typeof(partial_hashes)
FROM atlas_schema_revisions
WHERE version = ?`,
		version,
	).Scan(&got.Error, &got.ErrorStatement, &got.PartialHashes)
	c.Assert(err, qt.IsNil)
	return got
}

func markAtlasRevisionDirty(c *qt.C, dbPath, version string, revisionType int) {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(db.Close(), qt.IsNil)
	})

	_, err = db.Exec(
		`UPDATE atlas_schema_revisions SET type = ?, applied = 0, total = 1, error = 'broken' WHERE version = ?`,
		revisionType,
		version,
	)
	c.Assert(err, qt.IsNil)
}

func readAtlasRevisionVersions(c *qt.C, dbPath string) []string {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(db.Close(), qt.IsNil)
	})

	var versions string
	err = db.QueryRow(`
SELECT group_concat(version, ',')
FROM (
	SELECT version
	FROM atlas_schema_revisions
	ORDER BY CAST(version AS INTEGER)
)
`).Scan(&versions)
	c.Assert(err, qt.IsNil)
	return strings.Split(versions, ",")
}

func renderAtlasRevisionStatus(c *qt.C, fixture atlasRevisionFixture) string {
	c.Helper()
	return runAtlasCommand(c,
		"migrate", "status",
		"--url", "sqlite://"+fixture.dbPath,
		"--dir", "file://"+fixture.migrationsDir,
		"--format", "{{ (index .Applied 0).Type }}|{{ len .Pending }}",
	)
}

func runAtlasCommand(c *qt.C, args ...string) string {
	c.Helper()
	return strings.TrimSpace(executeAtlasCommand(c, args...))
}

func executeAtlasCommand(c *qt.C, args ...string) string {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out.String()))
	return out.String()
}

type closedWriter struct{}

func (closedWriter) Write(_ []byte) (int, error) {
	return 0, io.ErrClosedPipe
}
