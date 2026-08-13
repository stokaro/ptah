//go:build integration

package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestGenerateMigration_ConcurrentIndexArtifactsPassPtahLintWithRealPostgres
// runs Ptah's own linter over the concurrent-index artifacts Ptah itself
// generates, which is acceptance line 7 of stokaro/ptah#997.
//
// Both halves of that line are here on purpose. The shadow round trip is
// requested through ShadowDatabaseURL, so the generator drops the shadow,
// replays the directory onto it, applies the candidate and re-introspects
// before a byte of the migration is published; the lint runs afterwards over
// what was published.
//
// The table is created by a first cycle rather than by hand, because a shadow
// database starts empty and can say nothing about a migration that only adds
// an index to a table the directory never created -- it fails there with
// `relation "members" does not exist` whatever the index looks like. The rows
// arrive between the cycles, which is also what makes the second cycle choose
// the concurrent build.
//
// Linting the generated pair is not a formality. Statement rules used to be
// confined to up files, so the rollback half -- the half that is a DROP INDEX
// by construction -- was never read, and a green lint said nothing about it.
// PG106 and PG103 now read down files, which means Ptah's linter can finally
// contradict Ptah's generator, and this is the test that would notice: PG106
// fires on a blocking DROP INDEX in either direction, and PG103 fires on a
// concurrent index statement in a file that does not carry the
// no_transaction marker it needs in order to execute at all.
//
// The findings are compared as an exact list rather than by length, so a rule
// that starts firing here names itself in the failure.
func TestGenerateMigration_ConcurrentIndexArtifactsPassPtahLintWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := requireGeneratorPostgresURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	c.Assert(platform.NormalizeDialect(admin.Info().Dialect), qt.Equals, platform.Postgres)

	targetURL, targetDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_generator_lint_target")
	defer dropGeneratorTestPostgres(c, admin, targetDatabase)
	shadowURL, shadowDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_generator_lint_shadow")
	defer dropGeneratorTestPostgres(c, admin, shadowDatabase)

	target, err := dbschema.ConnectToDatabase(ctx, targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)

	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)

	// Cycle 1 puts the table into the directory's history, verified on the
	// shadow like every other cycle here.
	baseFiles, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesDir:     writeGeneratorEntities(c, filepath.Join(dir, "base"), membersEntitiesWithoutIndex),
		DatabaseURL:       targetURL,
		ShadowDatabaseURL: shadowURL,
		MigrationName:     "create_members",
		OutputDir:         migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(baseFiles, qt.IsNotNil)
	c.Assert(baseFiles.Files, qt.HasLen, 1)
	c.Assert(baseFiles.Files[0].NoTransaction, qt.IsFalse)

	baseProvider, err := migrator.NewFSMigrationProvider(os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	c.Assert(migrator.NewMigrator(target, baseProvider).MigrateUp(ctx), qt.IsNil)

	// A populated, analyzed table is what makes the next cycle choose the
	// concurrent build, so this is the artifact the acceptance line is about
	// rather than a plain one that would lint clean trivially.
	_, err = target.ExecContext(ctx, `
		INSERT INTO members SELECT g, 'user-' || g || '@example.com' FROM generate_series(1, 5000) AS g;
		ANALYZE members;
	`)
	c.Assert(err, qt.IsNil)

	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesDir:     writeGeneratorEntities(c, filepath.Join(dir, "indexed"), membersEntitiesWithIndex),
		DatabaseURL:       targetURL,
		ShadowDatabaseURL: shadowURL,
		MigrationName:     "add_members_email_index",
		OutputDir:         migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(files.Files[0].NoTransaction, qt.IsTrue)

	// The lint verdict comes first deliberately. Behind a text assertion it
	// could never be the finding that names a regression, and a rule that only
	// ever runs after the bytes were already pinned by hand is decoration.
	c.Assert(lintGeneratedMigrations(c, migrationsDir), qt.DeepEquals, []string{})

	upSQL, err := os.ReadFile(files.Files[0].UpFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upSQL), qt.Contains, "-- +ptah no_transaction")
	c.Assert(string(upSQL), qt.Contains, `CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_members_email" ON "members" ("email");`)
	downSQL, err := os.ReadFile(files.Files[0].DownFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(downSQL), qt.Contains, "-- +ptah no_transaction")
	c.Assert(string(downSQL), qt.Contains, `DROP INDEX CONCURRENTLY IF EXISTS "idx_members_email";`)

	// The linted bytes are the executed bytes: the directory is applied,
	// rolled back and applied again, and pg_index is what says so.
	provider, err := migrator.NewFSMigrationProvider(os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	migrations := migrator.NewMigrator(target, provider)
	c.Assert(migrations.MigrateUp(ctx), qt.IsNil)
	exists, valid := readGeneratorPostgresIndexState(c, target, "idx_members_email")
	c.Assert(exists, qt.IsTrue)
	c.Assert(valid, qt.IsTrue)

	c.Assert(migrations.MigrateDown(ctx), qt.IsNil)
	exists, _ = readGeneratorPostgresIndexState(c, target, "idx_members_email")
	c.Assert(exists, qt.IsFalse)

	c.Assert(migrations.MigrateUp(ctx), qt.IsNil)
	exists, valid = readGeneratorPostgresIndexState(c, target, "idx_members_email")
	c.Assert(exists, qt.IsTrue)
	c.Assert(valid, qt.IsTrue)
}

// TestLintFS_BlockingRollbackOfAConcurrentBuildIsReported is the discriminating
// control for the run above.
//
// A clean lint over generated artifacts only means something if this linter,
// on this directory layout, reports the hazards those artifacts avoid. The two
// rows are the two ways the rollback half of a concurrent build goes wrong --
// blocking, and concurrent without the marker that lets it execute -- written
// into the same native up/down file layout the generator publishes.
func TestLintFS_BlockingRollbackOfAConcurrentBuildIsReported(t *testing.T) {
	tests := []struct {
		name string
		down string
		want []string
	}{
		{
			name: "a blocking rollback is reported",
			down: "DROP INDEX IF EXISTS \"idx_members_email\";\n",
			want: []string{"PG106"},
		},
		{
			name: "a concurrent rollback without the marker is reported",
			down: "DROP INDEX CONCURRENTLY IF EXISTS \"idx_members_email\";\n",
			want: []string{"PG103"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			c.Assert(os.WriteFile(
				filepath.Join(dir, "0000000001_add_members_email_index.up.sql"),
				[]byte("-- +ptah no_transaction\nCREATE INDEX CONCURRENTLY IF NOT EXISTS \"idx_members_email\" ON \"members\" (\"email\");\n"),
				0o600,
			), qt.IsNil)
			c.Assert(os.WriteFile(
				filepath.Join(dir, "0000000001_add_members_email_index.down.sql"),
				[]byte(test.down),
				0o600,
			), qt.IsNil)

			c.Assert(lintGeneratedMigrations(c, dir), qt.DeepEquals, test.want)
		})
	}
}

// The two cycles differ only by the index annotation, so the second cycle's
// plan is exactly one added index and nothing else.
const (
	membersEntitiesWithoutIndex = `package entities

//ptah:schema:table name="members"
type Member struct {
	//ptah:schema:field name="id" type="BIGINT" not_null="true"
	ID int64

	//ptah:schema:field name="email" type="TEXT" not_null="true"
	Email string
}
`

	membersEntitiesWithIndex = `package entities

//ptah:schema:table name="members"
type Member struct {
	//ptah:schema:field name="id" type="BIGINT" not_null="true"
	ID int64

	//ptah:schema:field name="email" type="TEXT" not_null="true"
	//ptah:schema:index name="idx_members_email" fields="email"
	Email string
}
`
)

// lintGeneratedMigrations runs Ptah's linter over a published migration
// directory and returns the rule code of every finding, in report order.
func lintGeneratedMigrations(c *qt.C, migrationsDir string) []string {
	c.Helper()
	findings, err := lint.LintFS(os.DirFS(migrationsDir), lint.Options{Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)
	rules := make([]string, 0, len(findings))
	for _, finding := range findings {
		rules = append(rules, finding.Rule)
	}
	return rules
}
