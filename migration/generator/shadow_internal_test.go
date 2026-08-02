package generator

// White-box testing required: these tests exercise shadow replay stages,
// deterministic mismatch collection, and migration-version helpers that are
// not observable independently through the exported generation API. Public
// propagation is covered separately in shadow_external_test.go.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestDescribeShadowDiffMissingColumn(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{
			{
				TableName:    "users",
				ColumnsAdded: []string{"email", "name"},
			},
		},
	}

	c.Assert(describeShadowDiff(diff), qt.Equals, "missing column users.email")
	mismatches := collectShadowMismatches(diff)
	c.Assert(mismatches, qt.DeepEquals, []ShadowMismatch{
		{
			Kind:    "missing_column",
			Object:  "users.email",
			Table:   "users",
			Column:  "email",
			Message: "missing column users.email",
		},
		{
			Kind:    "missing_column",
			Object:  "users.name",
			Table:   "users",
			Column:  "name",
			Message: "missing column users.name",
		},
	})
	c.Assert((&ShadowVerificationError{Result: ShadowVerificationResult{
		Stage:      "schema-match",
		Mismatches: mismatches,
	}}).Error(), qt.Equals, "shadow check failed: missing column users.email")
}

func TestDescribeChangesIsDeterministic(t *testing.T) {
	c := qt.New(t)

	got := describeChanges(map[string]string{
		"nullable": "true -> false",
		"type":     "text -> varchar",
	})

	c.Assert(got, qt.Equals, "nullable true -> false, type text -> varchar")
}

func TestCollectShadowMismatchesCoversEverySchemaDiffCategory(t *testing.T) {
	c := qt.New(t)
	changes := map[string]string{"definition": "old -> new"}
	diff := &types.SchemaDiff{
		TablesAdded:   []string{"missing_table"},
		TablesRemoved: []string{"extra_table"},
		TablesModified: []types.TableDiff{{
			TableName:          "changed_table",
			ColumnsAdded:       []string{"missing_column"},
			ColumnsRemoved:     []string{"extra_column"},
			ColumnsModified:    []types.ColumnDiff{{ColumnName: "changed_column", Changes: changes}},
			ConstraintsAdded:   []string{"missing_table_constraint"},
			ConstraintsRemoved: []string{"extra_table_constraint"},
		}},
		EnumsAdded:   []string{"missing_enum"},
		EnumsRemoved: []string{"extra_enum"},
		EnumsModified: []types.EnumDiff{{
			EnumName:      "changed_enum",
			ValuesAdded:   []string{"missing_value"},
			ValuesRemoved: []string{"extra_value"},
		}},
		IndexesAdded:              []types.IndexRef{{TableName: "users", Name: "missing_index"}},
		IndexesRemoved:            []types.IndexRef{{TableName: "users", Name: "extra_index"}},
		ExtensionsAdded:           []string{"missing_extension"},
		ExtensionsRemoved:         []string{"extra_extension"},
		FunctionsAdded:            []string{"missing_function"},
		FunctionsRemoved:          []string{"extra_function"},
		FunctionsModified:         []types.FunctionDiff{{FunctionName: "changed_function", Changes: changes}},
		SequencesAdded:            []string{"missing_sequence"},
		SequencesRemoved:          []string{"extra_sequence"},
		SequencesModified:         []types.SequenceDiff{{SequenceName: "changed_sequence", Changes: changes}},
		DomainsAdded:              []string{"missing_domain"},
		DomainsRemoved:            []string{"extra_domain"},
		DomainsModified:           []types.DomainDiff{{DomainName: "changed_domain", Changes: changes}},
		CompositeTypesAdded:       []string{"missing_composite"},
		CompositeTypesRemoved:     []string{"extra_composite"},
		CompositeTypesModified:    []types.CompositeTypeDiff{{TypeName: "changed_composite", Changes: changes}},
		RangesAdded:               []string{"missing_range"},
		RangesRemoved:             []string{"extra_range"},
		ViewsAdded:                []string{"missing_view"},
		ViewsRemoved:              []string{"extra_view"},
		ViewsModified:             []types.ViewDiff{{ViewName: "changed_view", Changes: changes}},
		MaterializedViewsAdded:    []string{"missing_materialized_view"},
		MaterializedViewsRemoved:  []string{"extra_materialized_view"},
		MaterializedViewsModified: []types.MaterializedViewDiff{{ViewName: "changed_materialized_view", Changes: changes}},
		TriggersAdded:             []types.TriggerRef{{TableName: "users", TriggerName: "missing_trigger"}},
		TriggersRemoved:           []types.TriggerRef{{TableName: "users", TriggerName: "extra_trigger"}},
		TriggersModified:          []types.TriggerDiff{{TableName: "users", TriggerName: "changed_trigger", Changes: changes}},
		RLSPoliciesAdded:          []string{"missing_policy"},
		RLSPoliciesRemoved:        []types.RLSPolicyRef{{TableName: "users", PolicyName: "extra_policy"}},
		RLSPoliciesModified:       []types.RLSPolicyDiff{{TableName: "users", PolicyName: "changed_policy", Changes: changes}},
		RLSEnabledTablesAdded:     []string{"missing_rls_table"},
		RLSEnabledTablesRemoved:   []string{"extra_rls_table"},
		RolesAdded:                []string{"missing_role"},
		RolesRemoved:              []string{"extra_role"},
		RolesModified:             []types.RoleDiff{{RoleName: "changed_role", Changes: changes}},
		GrantsAdded:               []types.GrantRef{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users"}},
		GrantsRemoved:             []types.GrantRef{{Role: "app", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "users"}},
		GrantOptionsAdded:         []types.GrantRef{{Role: "app", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "users"}},
		GrantOptionsRevoked:       []types.GrantRef{{Role: "app", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "users"}},
		ConstraintsAdded:          []string{"missing_global_constraint"},
		ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
			Name:      "missing_global_constraint",
			TableName: "accounts",
		}},
		ConstraintsRemoved: []string{"extra_global_constraint"},
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{{
			Name:      "extra_global_constraint",
			TableName: "accounts",
		}},
	}

	mismatches := collectShadowMismatches(diff)
	c.Assert(mismatchKinds(mismatches), qt.DeepEquals, []string{
		"missing_table",
		"extra_table",
		"missing_column",
		"missing_constraint",
		"column_mismatch",
		"extra_column",
		"extra_constraint",
		"missing_enum",
		"extra_enum",
		"missing_enum_value",
		"extra_enum_value",
		"missing_index",
		"extra_index",
		"missing_extension",
		"extra_extension",
		"missing_function",
		"extra_function",
		"function_mismatch",
		"missing_sequence",
		"extra_sequence",
		"sequence_mismatch",
		"missing_domain",
		"extra_domain",
		"domain_mismatch",
		"missing_composite_type",
		"extra_composite_type",
		"composite_type_mismatch",
		"missing_range",
		"extra_range",
		"missing_view",
		"extra_view",
		"view_mismatch",
		"missing_materialized_view",
		"extra_materialized_view",
		"materialized_view_mismatch",
		"missing_trigger",
		"extra_trigger",
		"trigger_mismatch",
		"missing_rls_policy",
		"extra_rls_policy",
		"rls_policy_mismatch",
		"missing_rls_enablement",
		"extra_rls_enablement",
		"missing_role",
		"extra_role",
		"role_mismatch",
		"missing_grant",
		"extra_grant",
		"missing_grant_option",
		"extra_grant_option",
		"missing_constraint",
		"extra_constraint",
	})
	c.Assert(mismatches[len(mismatches)-2].Object, qt.Equals, "accounts.missing_global_constraint")
	c.Assert(mismatches[len(mismatches)-2].Table, qt.Equals, "accounts")
	c.Assert(mismatches[len(mismatches)-1].Object, qt.Equals, "accounts.extra_global_constraint")
	c.Assert(mismatches[len(mismatches)-1].Table, qt.Equals, "accounts")
}

func mismatchKinds(mismatches []ShadowMismatch) []string {
	kinds := make([]string, len(mismatches))
	for index, mismatch := range mismatches {
		kinds[index] = mismatch.Kind
	}
	return kinds
}

func TestNextAvailableMigrationVersionChecksUpAndDownFiles(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, migrator.GenerateMigrationFileName(100, "add_email", "down")), []byte("SELECT 1;"), 0600)
	c.Assert(err, qt.IsNil)
	err = os.WriteFile(filepath.Join(dir, migrator.GenerateMigrationFileName(105, "future", "up")), []byte("SELECT 1;"), 0600)
	c.Assert(err, qt.IsNil)

	c.Assert(nextAvailableMigrationVersion(dir, 100, "add_email"), qt.Equals, int64(106))
}

func TestLoadPriorMigrationsMissingDir(t *testing.T) {
	c := qt.New(t)

	migrations, err := loadPriorMigrations(filepath.Join(t.TempDir(), "missing"))

	c.Assert(err, qt.IsNil)
	c.Assert(migrations, qt.HasLen, 0)
}

func TestVerifyShadowMigrationConnectErrorIsStructured(t *testing.T) {
	c := qt.New(t)

	err := verifyShadowMigration(t.Context(), shadowMigrationOptions{
		DatabaseURL: "not-a-dsn",
		Dialect:     "postgres",
	})

	var shadowErr *ShadowVerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result.Stage, qt.Equals, "connect")
	c.Assert(shadowErr.Result.Mismatches, qt.HasLen, 1)
	c.Assert(shadowErr.Result.Mismatches[0].Kind, qt.Equals, "connect_error")
	c.Assert(shadowErr.Err, qt.IsNotNil)
	c.Assert(err, qt.ErrorMatches, `shadow check failed: connect to shadow database: invalid database URL: missing scheme`)
}

func TestGenerateMigrationShadowVerificationWithRealDB(t *testing.T) {
	dbURL := shadowTestDatabaseURL()
	if dbURL == "" {
		t.Skip("PostgreSQL test database URL is not set")
	}

	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	if err != nil {
		t.Skipf("test database is not available: %v", err)
	}
	defer dbschema.CloseAndWarn(conn)
	if platform.NormalizeDialect(conn.Info().Dialect) != platform.Postgres {
		t.Skipf("shadow acceptance test requires PostgreSQL, got %s", conn.Info().Dialect)
	}
	releaseLock := acquireShadowTestLock(c, ctx, conn)
	defer releaseLock()
	defer func() {
		c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	}()

	c.Run("broken prior migration aborts with missing column", func(c *qt.C) {
		dir := c.TempDir()
		entitiesDir := writeShadowEntities(c, dir)
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
		writePriorMigration(c, migrationsDir, "CREATE TABLE users (id SERIAL PRIMARY KEY);\n")

		prepareShadowTargetDB(c, ctx, conn)

		files, err := GenerateMigration(ctx, GenerateMigrationOptions{
			GoEntitiesDir:     entitiesDir,
			DatabaseURL:       dbURL,
			MigrationName:     "add_email",
			OutputDir:         migrationsDir,
			ShadowDatabaseURL: dbURL,
		})

		c.Assert(files, qt.IsNil)
		c.Assert(err.Error(), qt.Contains, "shadow check failed: missing column users.name: ")
		var shadowErr *ShadowVerificationError
		c.Assert(err, qt.ErrorAs, &shadowErr)
		c.Assert(shadowErr.Result.Stage, qt.Equals, "replay")
		c.Assert(shadowErr.Result.Mismatches, qt.HasLen, 1)
		c.Assert(shadowErr.Result.Mismatches[0].Kind, qt.Equals, "replay_error")
		c.Assert(shadowErr.Err, qt.IsNotNil)
		matches, globErr := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 2)
	})

	c.Run("correct prior migration passes and writes files", func(c *qt.C) {
		dir := c.TempDir()
		entitiesDir := writeShadowEntities(c, dir)
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
		writePriorMigration(c, migrationsDir, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);\n")

		prepareShadowTargetDB(c, ctx, conn)

		files, err := GenerateMigration(ctx, GenerateMigrationOptions{
			GoEntitiesDir:     entitiesDir,
			DatabaseURL:       dbURL,
			MigrationName:     "add_email",
			OutputDir:         migrationsDir,
			ShadowDatabaseURL: dbURL,
		})

		c.Assert(err, qt.IsNil)
		c.Assert(files, qt.IsNotNil)
		c.Assert(files.UpFile, qt.Not(qt.Equals), "")
		c.Assert(files.DownFile, qt.Not(qt.Equals), "")
		upSQL, readErr := os.ReadFile(files.UpFile)
		c.Assert(readErr, qt.IsNil)
		c.Assert(string(upSQL), qt.Contains, "email")
	})
}

func TestGenerateMigrationConcurrentIndexOnPopulatedPostgresTableWithRealDB(t *testing.T) {
	dbURL := shadowTestDatabaseURL()
	if dbURL == "" {
		t.Skip("PostgreSQL test database URL is not set")
	}

	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	if err != nil {
		t.Skipf("test database is not available: %v", err)
	}
	defer dbschema.CloseAndWarn(conn)
	if platform.NormalizeDialect(conn.Info().Dialect) != platform.Postgres {
		t.Skipf("concurrent index acceptance test requires PostgreSQL, got %s", conn.Info().Dialect)
	}
	releaseLock := acquireShadowTestLock(c, ctx, conn)
	defer releaseLock()
	defer func() {
		c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	}()

	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		);
		INSERT INTO users (name, email) VALUES ('Ada', 'ada@example.com');
		ANALYZE users;
	`)
	c.Assert(err, qt.IsNil)

	dir := t.TempDir()
	entitiesDir := writeConcurrentIndexEntities(c, dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)

	files, err := GenerateMigration(ctx, GenerateMigrationOptions{
		GoEntitiesDir: entitiesDir,
		DatabaseURL:   dbURL,
		MigrationName: "add_users_email_index",
		OutputDir:     migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	c.Assert(files.Files, qt.HasLen, 2)
	c.Assert(files.Files[0].NoTransaction, qt.IsFalse)
	c.Assert(files.Files[1].NoTransaction, qt.IsTrue)

	upSQL, err := os.ReadFile(files.Files[1].UpFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upSQL), qt.Contains, "-- +ptah no_transaction")
	c.Assert(string(upSQL), qt.Contains, `CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_users_email" ON "users" ("email");`)

	provider, err := migrator.NewFSMigrationProvider(os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	mig := migrator.NewMigrator(conn, provider)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
}

func shadowTestDatabaseURL() string {
	for _, name := range []string{"TEST_DATABASE_URL", "TEST_DB_URL", "POSTGRES_TEST_DSN", "POSTGRES_URL"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}

func writeConcurrentIndexEntities(c *qt.C, dir string) string {
	entitiesDir := filepath.Join(dir, "entities")
	c.Assert(os.MkdirAll(entitiesDir, 0755), qt.IsNil)

	content := `package entities

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="TEXT"
	Name string

	//ptah:schema:field name="email" type="TEXT"
	//ptah:schema:index name="idx_users_email" fields="email"
	Email string
}
`
	c.Assert(os.WriteFile(filepath.Join(entitiesDir, "schema.go"), []byte(content), 0600), qt.IsNil)
	return entitiesDir
}

func acquireShadowTestLock(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) func() {
	_, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock(156156156)")
	c.Assert(err, qt.IsNil)

	return func() {
		_, unlockErr := conn.ExecContext(ctx, "SELECT pg_advisory_unlock(156156156)")
		c.Assert(unlockErr, qt.IsNil)
	}
}

func writeShadowEntities(c *qt.C, dir string) string {
	entitiesDir := filepath.Join(dir, "entities")
	c.Assert(os.MkdirAll(entitiesDir, 0755), qt.IsNil)

	content := `package entities

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="TEXT"
	Name string

	//ptah:schema:field name="email" type="TEXT"
	Email string
}
`
	c.Assert(os.WriteFile(filepath.Join(entitiesDir, "schema.go"), []byte(content), 0600), qt.IsNil)
	return entitiesDir
}

func writePriorMigration(c *qt.C, dir, upSQL string) {
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(upSQL), 0600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE IF EXISTS users;\n"), 0600), qt.IsNil)
}

func prepareShadowTargetDB(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) {
	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	_, err := conn.ExecContext(ctx, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
	c.Assert(err, qt.IsNil)
}
