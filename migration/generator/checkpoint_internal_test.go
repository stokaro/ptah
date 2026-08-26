package generator

// White-box testing required: the offline checkpoint renderers and the
// fixed-option checkpoint writers exercised here (generateCheckpoint,
// generateCheckpointWithDatabaseInfo, writeCheckpointFiles, and
// writeAtlasCheckpointFile) have no exported name — embedders render
// checkpoints through GenerateCheckpointFromShadow and write them through the
// *WithOptions entry points, which are covered black-box next door. These
// tests pin the internal surface those entry points delegate to: rendering
// order and determinism, identifier resolution from caller-supplied metadata,
// and the writers' refuse-and-withdraw behavior.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/migrationfile"
)

func TestWriteCheckpointFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	// Seed an ordinary migration so ptah.sum starts with prior content.
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE t;\n"), 0o600), qt.IsNil)

	upPath, downPath, err := writeCheckpointFiles(dir, 2, "snapshot",
		"CREATE TABLE t (id INT, name TEXT);\n", "DROP TABLE t;\n")
	c.Assert(err, qt.IsNil)
	c.Assert(filepath.Base(upPath), qt.Equals, "0000000002_snapshot.checkpoint.up.sql")
	c.Assert(filepath.Base(downPath), qt.Equals, "0000000002_snapshot.checkpoint.down.sql")

	upContent, err := os.ReadFile(upPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upContent), qt.Contains, "name TEXT")

	parsed, err := migrationfile.ParseFileName(filepath.Base(upPath))
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.IsCheckpoint, qt.IsTrue)

	// ptah.sum was rewritten and now covers the checkpoint pair.
	sum, err := os.ReadFile(filepath.Join(dir, "ptah.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, "0000000002_snapshot.checkpoint.up.sql")
	c.Assert(string(sum), qt.Contains, "0000000002_snapshot.checkpoint.down.sql")

	// Writing the same version again refuses rather than overwriting.
	_, _, err = writeCheckpointFiles(dir, 2, "snapshot", "x", "y")
	c.Assert(err, qt.ErrorMatches, `checkpoint files for version 2 already exist`)
}

func TestWriteAtlasCheckpointFile_WritesFileAndSum(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "20250801000001_init.sql"),
		[]byte("CREATE TABLE users (id integer);\n"), 0o600), qt.IsNil)

	path, err := writeAtlasCheckpointFile(dir, 20250801000003, "snapshot", "CREATE TABLE users (id integer);")
	c.Assert(err, qt.IsNil)
	c.Assert(filepath.Base(path), qt.Equals, "20250801000003_snapshot.sql")

	// atlas.sum must cover BOTH the pre-existing migration and the checkpoint:
	// a sum over the checkpoint alone would verify against itself and still be
	// rejected by any reader of the whole directory.
	sum, err := os.ReadFile(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, "20250801000001_init.sql")
	c.Assert(string(sum), qt.Contains, "20250801000003_snapshot.sql")

	_, err = os.Stat(filepath.Join(dir, "ptah.sum"))
	c.Assert(os.IsNotExist(err), qt.IsTrue)
}

func TestWriteAtlasCheckpointFile_RollsBackWhenTheSumCannotBeWritten(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "20250801000001_init.sql"),
		[]byte("CREATE TABLE users (id integer);\n"), 0o600), qt.IsNil)
	// atlas.sum as a directory: the checkpoint file writes fine, then the
	// atomic sum replace fails. This is the only way to reach the rollback.
	c.Assert(os.Mkdir(filepath.Join(dir, "atlas.sum"), 0o755), qt.IsNil)

	_, err := writeAtlasCheckpointFile(dir, 20250801000003, "snapshot", "CREATE TABLE users (id integer);")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "checksum")

	// The checkpoint must NOT survive: a checkpoint no integrity file covers
	// makes the whole directory fail verification, and it would be applied on
	// the next run. Asserting the file is gone is the point — the error alone
	// says nothing about what was left behind.
	leftovers, globErr := filepath.Glob(filepath.Join(dir, "*_snapshot.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(leftovers, qt.HasLen, 0)
}

func TestWriteAtlasCheckpointFile_RefusesToOverwrite(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	existing := filepath.Join(dir, "20250801000003_snapshot.sql")
	c.Assert(os.WriteFile(existing, []byte("-- original\n"), 0o600), qt.IsNil)

	_, err := writeAtlasCheckpointFile(dir, 20250801000003, "snapshot", "CREATE TABLE users (id integer);")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "already exists")

	// Assert the protected state, not the message: the original file must still
	// hold its own bytes.
	body, readErr := os.ReadFile(existing)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(body), qt.Equals, "-- original\n")
	// A refused write must not leave a sum behind either.
	_, statErr := os.Stat(filepath.Join(dir, "atlas.sum"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestWriteAtlasCheckpointFile_RejectsNonPositiveVersion(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	_, err := writeAtlasCheckpointFile(dir, 0, "snapshot", "SELECT 1;")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "must be greater than zero")

	entries, readErr := os.ReadDir(dir)
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

func checkpointSampleSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Post", Name: "posts"},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Post", Name: "user_id", Type: "INTEGER", Foreign: "users(id)"},
		},
	}
}

func TestGenerateCheckpoint_UpCreatesAndDownDropsInDependencyOrder(t *testing.T) {
	c := qt.New(t)

	up, down, err := generateCheckpoint(checkpointSampleSchema(), "postgres")
	c.Assert(err, qt.IsNil)

	// Up creates every table; the referenced table (users) comes before the
	// referencing one (posts), and the foreign key is added afterward.
	c.Assert(up, qt.Contains, `CREATE TABLE "users"`)
	c.Assert(up, qt.Contains, `CREATE TABLE "posts"`)
	c.Assert(strings.Index(up, `CREATE TABLE "users"`) < strings.Index(up, `CREATE TABLE "posts"`), qt.IsTrue)
	c.Assert(up, qt.Contains, `FOREIGN KEY ("user_id") REFERENCES "users"`)

	// Down drops in reverse dependency order: posts before users.
	c.Assert(down, qt.Contains, `DROP TABLE IF EXISTS "posts"`)
	c.Assert(down, qt.Contains, `DROP TABLE IF EXISTS "users"`)
	c.Assert(strings.Index(down, `DROP TABLE IF EXISTS "posts"`) < strings.Index(down, `DROP TABLE IF EXISTS "users"`), qt.IsTrue)
}

func TestGenerateCheckpoint_DeterministicSchemaContent(t *testing.T) {
	c := qt.New(t)

	up1, down1, err := generateCheckpoint(checkpointSampleSchema(), "postgres")
	c.Assert(err, qt.IsNil)
	up2, down2, err := generateCheckpoint(checkpointSampleSchema(), "postgres")
	c.Assert(err, qt.IsNil)

	// The generated DDL is deterministic; only the generated-on timestamp
	// comment varies, so compare with that line stripped.
	c.Assert(stripGeneratedOn(up1), qt.Equals, stripGeneratedOn(up2))
	c.Assert(stripGeneratedOn(down1), qt.Equals, stripGeneratedOn(down2))
}

func TestGenerateCheckpoint_NilAndEmpty(t *testing.T) {
	c := qt.New(t)

	_, _, err := generateCheckpoint(nil, "postgres")
	c.Assert(err, qt.ErrorMatches, `checkpoint schema is required`)

	up, down, err := generateCheckpoint(&schemamodel.Database{}, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Equals, "")
	c.Assert(down, qt.Equals, "")
}

func TestGenerateCheckpointWithDatabaseInfo_SQLServerCaseSensitiveVariants(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CS_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
			{Name: "users", Key: "users"},
			{Name: "email", Key: "email"},
			{Name: "status", Key: "status"},
			{Name: "idx_email", Key: "idx_email"},
			{Name: "IDX_Email", Key: "IDX_Email"},
		})
	schema := sqlServerCaseVariantIndexSchema()

	up, down, err := generateCheckpointWithDatabaseInfo(schema, catalog.ServerInfo{
		Dialect:             "sqlserver",
		Capabilities:        capability.SQLServer2022(),
		IdentifierSemantics: semantics,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, "CREATE INDEX [idx_email]")
	c.Assert(up, qt.Contains, "CREATE INDEX [IDX_Email]")
	c.Assert(down, qt.Contains, "DROP INDEX IF EXISTS [idx_email]")
	c.Assert(down, qt.Contains, "DROP INDEX IF EXISTS [IDX_Email]")
}

func TestGenerateCheckpoint_SQLServerUnknownRejectsCaseVariants(t *testing.T) {
	c := qt.New(t)

	_, _, err := generateCheckpoint(
		sqlServerCaseVariantIndexSchema(),
		"sqlserver",
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
}

func TestGenerateCheckpointWithDatabaseInfo_SQLServerTableCollision_FailurePath(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "Users", Key: "Users"},
			{Name: "dbo", Key: "dbo"},
			{Name: "users", Key: "Users"},
		})
	schema := &schemamodel.Database{Tables: []schemamodel.Table{
		{StructName: "UpperUser", Schema: "dbo", Name: "Users"},
		{StructName: "LowerUser", Schema: "dbo", Name: "users"},
	}}

	up, down, err := generateCheckpointWithDatabaseInfo(schema, catalog.ServerInfo{
		Dialect:             "sqlserver",
		Capabilities:        capability.SQLServer2022(),
		IdentifierSemantics: semantics,
	})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*target tables dbo\.Users and dbo\.users may have the same catalog identity.*`)
	c.Assert(up, qt.Equals, "")
	c.Assert(down, qt.Equals, "")
}

func TestGenerateCheckpointWithDatabaseInfo_SQLServerIncompleteSnapshot_FailurePath(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
		})
	schema := &schemamodel.Database{Tables: []schemamodel.Table{
		{StructName: "User", Schema: "dbo", Name: "users"},
	}}

	up, down, err := generateCheckpointWithDatabaseInfo(schema, catalog.ServerInfo{
		Dialect:             "sqlserver",
		Capabilities:        capability.SQLServer2022(),
		IdentifierSemantics: semantics,
	})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*snapshot does not resolve "users".*`)
	c.Assert(up, qt.Equals, "")
	c.Assert(down, qt.Equals, "")
}

func sqlServerCaseVariantIndexSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Schema: "dbo", Name: "users"},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "email", Type: "NVARCHAR(320)"},
			{StructName: "User", Name: "status", Type: "INT"},
		},
		Indexes: []schemamodel.Index{
			{StructName: "User", Name: "idx_email", Fields: []string{"email"}},
			{StructName: "User", Name: "IDX_Email", Fields: []string{"status"}},
		},
	}
}

func stripGeneratedOn(sql string) string {
	var b strings.Builder
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "-- Generated on:") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}
