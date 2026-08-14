package generator_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestGenerateCheckpointFromShadow_ReplaysHistoryIntoCumulativeSnapshot(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	writeMigration := func(name, up, down string) {
		c.Assert(os.WriteFile(filepath.Join(dir, name+".up.sql"), []byte(up), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, name+".down.sql"), []byte(down), 0o600), qt.IsNil)
	}
	writeMigration("0000000001_init",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\n", "DROP TABLE users;\n")
	writeMigration("0000000002_add_email",
		"ALTER TABLE users ADD COLUMN email TEXT;\n", "ALTER TABLE users DROP COLUMN email;\n")

	shadowURL := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	up, down, err := generator.GenerateCheckpointFromShadow(context.Background(), generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL: shadowURL,
		MigrationsDir:     dir,
		Dialect:           "sqlite",
	})
	c.Assert(err, qt.IsNil)

	// The checkpoint is the cumulative schema: one CREATE TABLE users carrying
	// both the original id column and the column added by the second migration.
	c.Assert(up, qt.Contains, "CREATE TABLE")
	c.Assert(up, qt.Contains, "users")
	c.Assert(up, qt.Contains, "email")
	c.Assert(down, qt.Contains, "DROP TABLE")
	c.Assert(down, qt.Contains, "users")
}

func TestGenerateCheckpointFromShadow_UsesProvidedSnapshotInsteadOfPath(t *testing.T) {
	c := qt.New(t)
	reopenedDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(reopenedDir, "0000000001_changed.up.sql"),
		[]byte("CREATE TABLE changed_after_verification (id INTEGER PRIMARY KEY);"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(reopenedDir, "0000000001_changed.down.sql"),
		[]byte("DROP TABLE changed_after_verification;"),
		0o600,
	), qt.IsNil)
	authorized := fstest.MapFS{
		"0000000001_authorized.up.sql": {Data: []byte(
			"CREATE TABLE authorized_snapshot (id INTEGER PRIMARY KEY);",
		)},
		"0000000001_authorized.down.sql": {Data: []byte(
			"DROP TABLE authorized_snapshot;",
		)},
	}

	up, _, err := generator.GenerateCheckpointFromShadow(t.Context(), generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL: "sqlite://" + filepath.Join(t.TempDir(), "shadow.db"),
		MigrationsDir:     reopenedDir,
		MigrationsFS:      authorized,
		Dialect:           "sqlite",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, "authorized_snapshot")
	c.Assert(up, qt.Not(qt.Contains), "changed_after_verification")
}

func TestGenerateCheckpointFromShadow_EmptyDirectoryErrors(t *testing.T) {
	c := qt.New(t)

	shadowURL := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")
	_, _, err := generator.GenerateCheckpointFromShadow(context.Background(), generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL: shadowURL,
		MigrationsDir:     t.TempDir(),
		Dialect:           "sqlite",
	})
	c.Assert(err, qt.ErrorMatches, `.*no migrations found.*`)
}

func TestWriteCheckpointFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	// Seed an ordinary migration so ptah.sum starts with prior content.
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE t;\n"), 0o600), qt.IsNil)

	upPath, downPath, err := generator.WriteCheckpointFiles(dir, 2, "snapshot",
		"CREATE TABLE t (id INT, name TEXT);\n", "DROP TABLE t;\n")
	c.Assert(err, qt.IsNil)
	c.Assert(filepath.Base(upPath), qt.Equals, "0000000002_snapshot.checkpoint.up.sql")
	c.Assert(filepath.Base(downPath), qt.Equals, "0000000002_snapshot.checkpoint.down.sql")

	upContent, err := os.ReadFile(upPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upContent), qt.Contains, "name TEXT")

	parsed, err := migrator.ParseMigrationFileName(filepath.Base(upPath))
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.IsCheckpoint, qt.IsTrue)

	// ptah.sum was rewritten and now covers the checkpoint pair.
	sum, err := os.ReadFile(filepath.Join(dir, "ptah.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, "0000000002_snapshot.checkpoint.up.sql")
	c.Assert(string(sum), qt.Contains, "0000000002_snapshot.checkpoint.down.sql")

	// Writing the same version again refuses rather than overwriting.
	_, _, err = generator.WriteCheckpointFiles(dir, 2, "snapshot", "x", "y")
	c.Assert(err, qt.ErrorMatches, `checkpoint files for version 2 already exist`)
}

func TestWriteCheckpointFilesWithOptions_RefusesChangedAuthorizedHistory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	prior := filepath.Join(dir, "0000000001_init.up.sql")
	c.Assert(os.WriteFile(prior, []byte("CREATE TABLE original (id INT);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE original;\n"),
		0o600,
	), qt.IsNil)
	authorized, err := migrationsnapshot.CaptureDirectory(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(prior, []byte("CREATE TABLE tampered (id INT);\n"), 0o600), qt.IsNil)

	_, _, err = generator.WriteCheckpointFilesWithOptions(
		dir,
		2,
		"snapshot",
		"CREATE TABLE original (id INT);\n",
		"DROP TABLE original;\n",
		generator.CheckpointWriteOptions{AuthorizedMigrationsFS: authorized},
	)

	c.Assert(err, qt.ErrorIs, generator.ErrMigrationDirectoryChanged)
	matches, globErr := filepath.Glob(filepath.Join(dir, "*.checkpoint.*.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 0)
	_, statErr := os.Stat(filepath.Join(dir, "ptah.sum"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func checkpointSampleSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Post", Name: "posts"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Post", Name: "user_id", Type: "INTEGER", Foreign: "users(id)"},
		},
	}
}

func TestGenerateCheckpoint_UpCreatesAndDownDropsInDependencyOrder(t *testing.T) {
	c := qt.New(t)

	up, down, err := generator.GenerateCheckpoint(checkpointSampleSchema(), "postgres")
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

	up1, down1, err := generator.GenerateCheckpoint(checkpointSampleSchema(), "postgres")
	c.Assert(err, qt.IsNil)
	up2, down2, err := generator.GenerateCheckpoint(checkpointSampleSchema(), "postgres")
	c.Assert(err, qt.IsNil)

	// The generated DDL is deterministic; only the generated-on timestamp
	// comment varies, so compare with that line stripped.
	c.Assert(stripGeneratedOn(up1), qt.Equals, stripGeneratedOn(up2))
	c.Assert(stripGeneratedOn(down1), qt.Equals, stripGeneratedOn(down2))
}

func TestGenerateCheckpoint_NilAndEmpty(t *testing.T) {
	c := qt.New(t)

	_, _, err := generator.GenerateCheckpoint(nil, "postgres")
	c.Assert(err, qt.ErrorMatches, `checkpoint schema is required`)

	up, down, err := generator.GenerateCheckpoint(&goschema.Database{}, "postgres")
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

	up, down, err := generator.GenerateCheckpointWithDatabaseInfo(schema, dbschematypes.DBInfo{
		Dialect:             "sqlserver",
		Capabilities:        capability.SQLServer2022(),
		IdentifierSemantics: semantics,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, "CREATE INDEX [idx_email]")
	c.Assert(up, qt.Contains, "CREATE INDEX [IDX_Email]")
	c.Assert(down, qt.Contains, "DROP INDEX [idx_email]")
	c.Assert(down, qt.Contains, "DROP INDEX [IDX_Email]")
}

func TestGenerateCheckpoint_SQLServerUnknownRejectsCaseVariants(t *testing.T) {
	c := qt.New(t)

	_, _, err := generator.GenerateCheckpoint(
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
	schema := &goschema.Database{Tables: []goschema.Table{
		{StructName: "UpperUser", Schema: "dbo", Name: "Users"},
		{StructName: "LowerUser", Schema: "dbo", Name: "users"},
	}}

	up, down, err := generator.GenerateCheckpointWithDatabaseInfo(schema, dbschematypes.DBInfo{
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
	schema := &goschema.Database{Tables: []goschema.Table{
		{StructName: "User", Schema: "dbo", Name: "users"},
	}}

	up, down, err := generator.GenerateCheckpointWithDatabaseInfo(schema, dbschematypes.DBInfo{
		Dialect:             "sqlserver",
		Capabilities:        capability.SQLServer2022(),
		IdentifierSemantics: semantics,
	})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*snapshot does not resolve "users".*`)
	c.Assert(up, qt.Equals, "")
	c.Assert(down, qt.Equals, "")
}

func sqlServerCaseVariantIndexSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "dbo", Name: "users"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "email", Type: "NVARCHAR(320)"},
			{StructName: "User", Name: "status", Type: "INT"},
		},
		Indexes: []goschema.Index{
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
