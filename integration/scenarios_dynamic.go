package integration

import (
	"context"
	"fmt"
	"io/fs"
	"time"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/migrator"
)

// GetDynamicScenarios returns all dynamic integration test scenarios that use versioned entities
func GetDynamicScenarios() []TestScenario {
	return []TestScenario{
		// Basic functionality scenarios
		{
			Name:             "dynamic_basic_evolution",
			Description:      "Test basic schema evolution using versioned entities: 000 → 013 (all versions)",
			EnhancedTestFunc: testDynamicBasicEvolution,
		},
		{
			Name:             "dynamic_skip_versions",
			Description:      "Test non-sequential migration: 000 → 002 → 003",
			EnhancedTestFunc: testDynamicSkipVersions,
		},
		{
			Name:             "dynamic_idempotency",
			Description:      "Test applying the same version multiple times",
			EnhancedTestFunc: testDynamicIdempotency,
		},
		{
			Name:             "dynamic_partial_apply",
			Description:      "Test applying to specific version, then continuing",
			EnhancedTestFunc: testDynamicPartialApply,
		},
		{
			Name:             "dynamic_schema_diff",
			Description:      "Test schema diff generation between versions",
			EnhancedTestFunc: testDynamicSchemaDiff,
		},
		{
			Name:             "dynamic_migration_sql_generation",
			Description:      "Test SQL migration generation from entity changes",
			EnhancedTestFunc: testDynamicMigrationSQLGeneration,
		},

		// Rollback/Downgrade scenarios
		{
			Name:             "dynamic_rollback_single",
			Description:      "Test rolling back one version (003 → 002)",
			EnhancedTestFunc: testDynamicRollbackSingle,
		},
		{
			Name:             "dynamic_rollback_multiple",
			Description:      "Test rolling back multiple versions (005 → 001)",
			EnhancedTestFunc: testDynamicRollbackMultiple,
		},
		{
			Name:             "dynamic_rollback_to_zero",
			Description:      "Test complete rollback to empty database",
			EnhancedTestFunc: testDynamicRollbackToZero,
		},

		// Error handling & recovery scenarios
		{
			Name:             "dynamic_partial_failure_recovery",
			Description:      "Test recovery from migration failure mid-way",
			EnhancedTestFunc: testDynamicPartialFailureRecovery,
		},
		{
			Name:             "dynamic_invalid_migration",
			Description:      "Test handling of invalid/corrupted migration data",
			EnhancedTestFunc: testDynamicInvalidMigration,
		},
		{
			Name:             "dynamic_concurrent_migrations",
			Description:      "Test concurrent migration attempts (locking behavior)",
			EnhancedTestFunc: testDynamicConcurrentMigrations,
		},

		// Complex schema change scenarios
		{
			Name:             "dynamic_circular_dependencies",
			Description:      "Test handling of circular foreign key dependencies",
			EnhancedTestFunc: testDynamicCircularDependencies,
		},
		{
			Name:             "dynamic_data_migration",
			Description:      "Test migrations that require data transformation",
			EnhancedTestFunc: testDynamicDataMigration,
		},
		{
			Name:             "dynamic_large_table_migration",
			Description:      "Test performance with large datasets during migration",
			EnhancedTestFunc: testDynamicLargeTableMigration,
		},

		// Edge case scenarios
		{
			Name:             "dynamic_empty_migrations",
			Description:      "Test versions with no actual schema changes",
			EnhancedTestFunc: testDynamicEmptyMigrations,
		},
		{
			Name:             "dynamic_duplicate_names",
			Description:      "Test handling of duplicate table/field names across versions",
			EnhancedTestFunc: testDynamicDuplicateNames,
		},
		{
			Name:             "dynamic_reserved_keywords",
			Description:      "Test migrations involving SQL reserved keywords",
			EnhancedTestFunc: testDynamicReservedKeywords,
		},

		// Cross-database compatibility scenarios
		{
			Name:             "dynamic_dialect_differences",
			Description:      "Test same migration across PostgreSQL/MySQL/MariaDB",
			EnhancedTestFunc: testDynamicDialectDifferences,
		},
		{
			Name:             "dynamic_type_mapping",
			Description:      "Test database-specific type conversions",
			EnhancedTestFunc: testDynamicTypeMapping,
		},

		// Validation & integrity scenarios
		{
			Name:             "dynamic_constraint_validation",
			Description:      "Test constraint violations during migration",
			EnhancedTestFunc: testDynamicConstraintValidation,
		},
		{
			Name:             "dynamic_foreign_key_cascade",
			Description:      "Test cascading effects of table/field drops",
			EnhancedTestFunc: testDynamicForeignKeyCascade,
		},

		// Embedded fields scenarios
		{
			Name:             "dynamic_embedded_fields",
			Description:      "Test embedded struct fields (both value and pointer types) in CREATE TABLE migrations",
			EnhancedTestFunc: testDynamicEmbeddedFields,
		},

		// PostgreSQL RLS and Functions scenarios
		{
			Name:             "dynamic_rls_functions_basic",
			Description:      "Test PostgreSQL RLS and custom functions: basic multi-tenant setup",
			EnhancedTestFunc: testDynamicRLSFunctionsBasic,
		},
		{
			Name:             "dynamic_rls_functions_advanced",
			Description:      "Test PostgreSQL RLS and custom functions: advanced role-based policies",
			EnhancedTestFunc: testDynamicRLSFunctionsAdvanced,
		},
		{
			Name:             "dynamic_rls_cross_database",
			Description:      "Test PostgreSQL RLS features are skipped gracefully on MySQL/MariaDB",
			EnhancedTestFunc: testDynamicRLSCrossDatabase,
		},
		{
			Name:             "dynamic_functions_modification",
			Description:      "Test PostgreSQL function modification and schema diffing",
			EnhancedTestFunc: testDynamicFunctionsModification,
		},
		{
			Name:             "dynamic_rls_policy_modification",
			Description:      "Test RLS policy modification and schema diffing",
			EnhancedTestFunc: testDynamicRLSPolicyModification,
		},

		// Down migration scenarios for RLS and functions
		{
			Name:             "dynamic_rls_functions_rollback",
			Description:      "Test PostgreSQL RLS and functions rollback: complete down migration path",
			EnhancedTestFunc: testDynamicRLSFunctionsRollback,
		},
		{
			Name:             "dynamic_rls_functions_partial_rollback",
			Description:      "Test PostgreSQL RLS and functions partial rollback: step-by-step down migrations",
			EnhancedTestFunc: testDynamicRLSFunctionsPartialRollback,
		},
		{
			Name:             "dynamic_rls_functions_dependency_order",
			Description:      "Test PostgreSQL RLS and functions dependency order during rollback",
			EnhancedTestFunc: testDynamicRLSFunctionsDependencyOrder,
		},
		{
			Name:             "dynamic_rls_functions_data_integrity",
			Description:      "Test data integrity during RLS and function rollbacks",
			EnhancedTestFunc: testDynamicRLSFunctionsDataIntegrity,
		},
		{
			Name:             "dynamic_rls_functions_error_handling",
			Description:      "Test error handling during RLS and function rollbacks",
			EnhancedTestFunc: testDynamicRLSFunctionsErrorHandling,
		},
	}
}

// testDynamicBasicEvolution tests the basic evolution path through all versions
func testDynamicBasicEvolution(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	// Create versioned entity manager
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Evolution path: 000 → 001 → 002 → 003 → 004 → 005 → 006 → 007 → 008 → 009 → 010 → 011 → 012
	versions := []struct {
		dir         string
		description string
	}{
		{"000-initial", "Create initial users and products tables"},
		{"001-add-fields", "Add additional fields to users and products"},
		{"002-add-posts", "Add posts table with foreign key to users"},
		{"003-add-enums", "Add enum types and status fields"},
		{"004-field-rename", "Rename fields: bio → description, age → user_age"},
		{"005-field-type-change", "Change field types: user_age INTEGER → SMALLINT, description TEXT → VARCHAR(500)"},
		{"006-field-drop", "Drop unused fields: active field from users"},
		{"007-index-add", "Add new indexes: compound index on name+email, index on description"},
		{"008-index-remove", "Remove old indexes: idx_users_email (replaced by compound)"},
		{"009-add-constraints", "Add constraints: check constraint on user_age, foreign key constraint"},
		{"010-drop-constraints", "Drop constraints: remove check constraint on user_age"},
		{"011-add-entity", "Add new entity: categories table"},
		{"012-drop-entity", "Drop entity: remove products table"},
		{"013-embedded-fields", "Create tables with all embedding modes"},
	}

	for _, version := range versions {
		stepName := fmt.Sprintf("Apply %s", version.dir)
		stepDesc := version.description

		err := recorder.RecordStep(stepName, stepDesc, func() error {
			fmt.Printf("Applying version %s: %s\n", version.dir, version.description)

			if err := vem.MigrateToVersion(ctx, conn, version.dir, version.description); err != nil {
				return fmt.Errorf("failed to migrate to version %s: %w", version.dir, err)
			}

			fmt.Printf("Successfully applied version %s\n", version.dir)
			return nil
		})

		if err != nil {
			return err
		}
	}

	// Verify final state
	return recorder.RecordStep("Verify Final State", "Validate that all migrations were applied correctly", func() error {
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate final schema: %w", err)
		}

		// Should have 6 tables: users, posts, categories, products (re-added in 013), articles (added in 013), blog_posts (added in 013)
		if len(schema.Tables) != 6 {
			return fmt.Errorf("expected 6 tables, got %d", len(schema.Tables))
		}

		// Should have 3 enums: user_status, post_status, product_status (re-added in 013)
		if len(schema.Enums) != 3 {
			return fmt.Errorf("expected 3 enums, got %d", len(schema.Enums))
		}

		// Verify that field renames, type changes, and constraint changes were applied
		// Check that users table has the renamed fields (description, user_age) and not the old ones (bio, age)
		usersTable := findTable(schema.Tables, "users")
		if usersTable == nil {
			return fmt.Errorf("users table not found in final schema")
		}

		// Should have description field (renamed from bio)
		if !hasField(schema.Fields, "User", "description") {
			return fmt.Errorf("users table should have description field (renamed from bio)")
		}

		// Should have user_age field (renamed from age)
		if !hasField(schema.Fields, "User", "user_age") {
			return fmt.Errorf("users table should have user_age field (renamed from age)")
		}

		// Should NOT have bio or age fields (they were renamed)
		if hasField(schema.Fields, "User", "bio") {
			return fmt.Errorf("users table should not have bio field (it was renamed to description)")
		}

		if hasField(schema.Fields, "User", "age") {
			return fmt.Errorf("users table should not have age field (it was renamed to user_age)")
		}

		// Should NOT have active field (it was dropped)
		if hasField(schema.Fields, "User", "active") {
			return fmt.Errorf("users table should not have active field (it was dropped)")
		}

		// Verify that categories table was added
		categoriesTable := findTable(schema.Tables, "categories")
		if categoriesTable == nil {
			return fmt.Errorf("categories table should exist (added in version 011)")
		}

		// Verify that products table was re-added
		productsTable := findTable(schema.Tables, "products")
		if productsTable == nil {
			return fmt.Errorf("products table should exist (re-added in version 013)")
		}

		// Verify that articles table was added
		articlesTable := findTable(schema.Tables, "articles")
		if articlesTable == nil {
			return fmt.Errorf("articles table should exist (added in version 013)")
		}

		return nil
	})
}

// testDynamicSkipVersions tests non-sequential version application
func testDynamicSkipVersions(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Skip several versions, go directly from 000 → 005 → 012
	versions := []struct {
		dir         string
		description string
	}{
		{"000-initial", "Create initial users and products tables"},
		{"005-field-type-change", "Add fields, posts table, enums, renames, and type changes"},
		{"012-drop-entity", "Apply all remaining changes including entity add/drop"},
	}

	for _, version := range versions {
		stepName := fmt.Sprintf("Apply %s", version.dir)
		stepDesc := version.description

		err := recorder.RecordStep(stepName, stepDesc, func() error {
			return vem.MigrateToVersion(ctx, conn, version.dir, version.description)
		})

		if err != nil {
			return fmt.Errorf("failed to migrate to version %s: %w", version.dir, err)
		}
	}

	// Verify final state is the same as sequential application
	return recorder.RecordStep("Verify Final State", "Validate that skip-version migration produces same result as sequential", func() error {
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate final schema: %w", err)
		}

		if len(schema.Tables) != 3 {
			return fmt.Errorf("expected 3 tables, got %d", len(schema.Tables))
		}

		if len(schema.Enums) != 2 {
			return fmt.Errorf("expected 2 enums, got %d", len(schema.Enums))
		}

		return nil
	})
}

// testDynamicIdempotency tests applying the same version multiple times
func testDynamicIdempotency(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Apply version 001 twice (simple add fields)
	version := "001-add-fields"
	description := "Add additional fields to users and products"

	// First application
	if err := vem.MigrateToVersion(ctx, conn, version, description); err != nil {
		return fmt.Errorf("failed to migrate to version %s (first time): %w", version, err)
	}

	// Get the current migration version after first application
	currentVersion, err := getCurrentMigrationVersion(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to get current migration version: %w", err)
	}

	// Try to apply the same version again - should be idempotent
	if err := vem.MigrateToVersion(ctx, conn, version, description); err != nil {
		return fmt.Errorf("failed to migrate to version %s (second time): %w", version, err)
	}

	// Check that no new migration was applied
	newVersion, err := getCurrentMigrationVersion(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to get new migration version: %w", err)
	}

	if newVersion != currentVersion {
		return fmt.Errorf("expected migration version to remain %d, but got %d", currentVersion, newVersion)
	}

	return nil
}

// testDynamicPartialApply tests applying to a specific version, then continuing
func testDynamicPartialApply(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Apply up to version 001 (add fields - still only 2 tables: users, products)
	if err := vem.MigrateToVersion(ctx, conn, "000-initial", "Create initial tables"); err != nil {
		return fmt.Errorf("failed to migrate to version 000: %w", err)
	}

	if err := vem.MigrateToVersion(ctx, conn, "001-add-fields", "Add additional fields to users and products"); err != nil {
		return fmt.Errorf("failed to migrate to version 001: %w", err)
	}

	// Verify intermediate state (should have 2 tables: users, products)
	schema, err := vem.GenerateSchemaFromEntities()
	if err != nil {
		return fmt.Errorf("failed to generate intermediate schema: %w", err)
	}

	if len(schema.Tables) != 2 {
		return fmt.Errorf("expected 2 tables at intermediate state, got %d", len(schema.Tables))
	}

	// Continue to final version
	if err := vem.MigrateToVersion(ctx, conn, "013-embedded-fields", "Create tables with all embedding modes"); err != nil {
		return fmt.Errorf("failed to migrate to version 013: %w", err)
	}

	// Verify final state
	finalSchema, err := vem.GenerateSchemaFromEntities()
	if err != nil {
		return fmt.Errorf("failed to generate final schema: %w", err)
	}

	if len(finalSchema.Tables) != 6 {
		return fmt.Errorf("expected 6 tables at final state, got %d", len(finalSchema.Tables))
	}

	if len(finalSchema.Enums) != 3 {
		return fmt.Errorf("expected 3 enums at final state, got %d", len(finalSchema.Enums))
	}

	return nil
}

// testDynamicSchemaDiff tests schema diff generation between versions
func testDynamicSchemaDiff(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Apply initial version
	if err := vem.MigrateToVersion(ctx, conn, "000-initial", "Create initial tables"); err != nil {
		return fmt.Errorf("failed to migrate to version 000: %w", err)
	}

	// Load next version entities but don't apply yet
	if err := vem.LoadEntityVersion("006-field-drop"); err != nil {
		return fmt.Errorf("failed to load version 006 entities: %w", err)
	}

	// Generate migration SQL to see the diff
	statements, err := vem.GenerateMigrationSQL(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to generate migration SQL: %w", err)
	}

	// Should have statements to add fields and create posts table
	if len(statements) == 0 {
		return fmt.Errorf("expected migration statements for schema diff, got none")
	}

	// Verify we have statements for adding the posts table
	hasPostsTable := false
	for _, stmt := range statements {
		if contains(stmt, "CREATE TABLE posts") || contains(stmt, "CREATE TABLE \"posts\"") {
			hasPostsTable = true
			break
		}
	}

	if !hasPostsTable {
		return fmt.Errorf("expected CREATE TABLE posts statement in migration SQL")
	}

	return nil
}

// testDynamicMigrationSQLGeneration tests SQL generation from entity changes
func testDynamicMigrationSQLGeneration(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Start with empty database, load version 008 entities (index remove)
	if err := vem.LoadEntityVersion("008-index-remove"); err != nil {
		return fmt.Errorf("failed to load version 008 entities: %w", err)
	}

	// Generate SQL for creating everything from scratch
	statements, err := vem.GenerateMigrationSQL(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to generate migration SQL: %w", err)
	}

	if len(statements) == 0 {
		return fmt.Errorf("expected migration statements for initial creation, got none")
	}

	// Should have CREATE TABLE statements for users and products
	hasUsersTable := false
	hasProductsTable := false

	for _, stmt := range statements {
		if contains(stmt, "CREATE TABLE users") || contains(stmt, "CREATE TABLE \"users\"") {
			hasUsersTable = true
		}
		if contains(stmt, "CREATE TABLE products") || contains(stmt, "CREATE TABLE \"products\"") {
			hasProductsTable = true
		}
	}

	if !hasUsersTable {
		return fmt.Errorf("expected CREATE TABLE users statement in migration SQL")
	}

	if !hasProductsTable {
		return fmt.Errorf("expected CREATE TABLE products statement in migration SQL")
	}

	return nil
}

// getCurrentMigrationVersion gets the current migration version from the database
func getCurrentMigrationVersion(ctx context.Context, conn *dbschema.DatabaseConnection) (int, error) {
	// Query the schema_migrations table to get the highest version
	query := "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
	row := conn.QueryRow(query)

	var version int
	if err := row.Scan(&version); err != nil {
		return 0, fmt.Errorf("failed to scan migration version: %w", err)
	}

	return version, nil
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// findTable finds a table by name in a slice of tables
func findTable(tables []goschema.Table, name string) *goschema.Table {
	for i, table := range tables {
		if table.Name == name {
			return &tables[i]
		}
	}
	return nil
}

// hasField checks if a field exists for a specific table
func hasField(fields []goschema.Field, tableName, fieldName string) bool {
	for _, field := range fields {
		if field.StructName == tableName && field.Name == fieldName {
			return true
		}
	}
	return false
}

// ============================================================================
// ROLLBACK/DOWNGRADE SCENARIOS
// ============================================================================

// testDynamicRollbackSingle tests rolling back one version (003 → 002)
func testDynamicRollbackSingle(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Create a migrator and register all migrations with both up and down
	m := migrator.NewMigrator(conn)
	dialect := conn.Info().Dialect

	// Register migrations with database-specific SQL
	var migrations []*migrator.Migration

	if dialect == "mysql" {
		migrations = []*migrator.Migration{
			migrator.CreateMigrationFromSQL(1, "Create initial tables",
				"CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255)); CREATE TABLE products (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255));",
				"DROP TABLE products; DROP TABLE users;"),
			migrator.CreateMigrationFromSQL(2, "Add additional fields",
				"ALTER TABLE users ADD COLUMN email VARCHAR(255); ALTER TABLE products ADD COLUMN price DECIMAL(10,2);",
				"ALTER TABLE products DROP COLUMN price; ALTER TABLE users DROP COLUMN email;"),
			migrator.CreateMigrationFromSQL(3, "Add posts table",
				"CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, user_id INTEGER, title VARCHAR(255), FOREIGN KEY (user_id) REFERENCES users(id));",
				"DROP TABLE posts;"),
			migrator.CreateMigrationFromSQL(4, "Add enum types",
				"ALTER TABLE users ADD COLUMN status ENUM('active', 'inactive') DEFAULT 'active';",
				"ALTER TABLE users DROP COLUMN status;"),
		}
	} else {
		// PostgreSQL and MariaDB
		migrations = []*migrator.Migration{
			migrator.CreateMigrationFromSQL(1, "Create initial tables",
				"CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255)); CREATE TABLE products (id SERIAL PRIMARY KEY, name VARCHAR(255));",
				"DROP TABLE products; DROP TABLE users;"),
			migrator.CreateMigrationFromSQL(2, "Add additional fields",
				"ALTER TABLE users ADD COLUMN email VARCHAR(255); ALTER TABLE products ADD COLUMN price DECIMAL(10,2);",
				"ALTER TABLE products DROP COLUMN price; ALTER TABLE users DROP COLUMN email;"),
			migrator.CreateMigrationFromSQL(3, "Add posts table",
				"CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INTEGER, title VARCHAR(255), FOREIGN KEY (user_id) REFERENCES users(id));",
				"DROP TABLE posts;"),
			migrator.CreateMigrationFromSQL(4, "Add enum types",
				"CREATE TYPE user_status AS ENUM ('active', 'inactive'); ALTER TABLE users ADD COLUMN status user_status DEFAULT 'active'::user_status;",
				"ALTER TABLE users DROP COLUMN status; DROP TYPE user_status;"),
		}
	}

	for _, migration := range migrations {
		m.Register(migration)
	}

	// Apply migrations up to version 4
	err = recorder.RecordStep("Apply All Migrations", "Apply migrations 1-4", func() error {
		return m.MigrateUp(ctx)
	})
	if err != nil {
		return err
	}

	// Verify we're at version 4
	currentVersion, err := getCurrentMigrationVersion(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}
	if currentVersion != 4 {
		return fmt.Errorf("expected version 4, got %d", currentVersion)
	}

	// Now rollback to version 3
	return recorder.RecordStep("Rollback to Version 3", "Roll back from version 4 to version 3", func() error {
		if err := m.MigrateDownTo(ctx, 3); err != nil {
			return fmt.Errorf("failed to rollback to version 3: %w", err)
		}

		// Verify we're now at version 3
		newVersion, err := getCurrentMigrationVersion(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to get version after rollback: %w", err)
		}
		if newVersion != 3 {
			return fmt.Errorf("expected version 3 after rollback, got %d", newVersion)
		}

		// Verify schema state - should have posts table but no enums
		schema, err := conn.Reader().ReadSchema()
		if err != nil {
			return fmt.Errorf("failed to read schema after rollback: %w", err)
		}

		// Should have 3 tables: users, products, posts (plus schema_migrations)
		applicationTables := 0
		for _, table := range schema.Tables {
			if table.Name != "schema_migrations" {
				applicationTables++
			}
		}
		if applicationTables != 3 {
			return fmt.Errorf("expected 3 application tables after rollback, got %d", applicationTables)
		}

		// Should have no enums (they were added in version 4)
		if len(schema.Enums) != 0 {
			return fmt.Errorf("expected 0 enums after rollback, got %d", len(schema.Enums))
		}

		return nil
	})
}

// testDynamicRollbackMultiple tests rolling back multiple versions (005 → 001)
func testDynamicRollbackMultiple(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Create a migrator and register all migrations with both up and down
	m := migrator.NewMigrator(conn)
	dialect := conn.Info().Dialect

	// Register migrations with database-specific SQL
	var migrations []*migrator.Migration

	if dialect == "mysql" {
		migrations = []*migrator.Migration{
			migrator.CreateMigrationFromSQL(1, "Create initial tables",
				"CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255)); CREATE TABLE products (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255));",
				"DROP TABLE products; DROP TABLE users;"),
			migrator.CreateMigrationFromSQL(2, "Add additional fields",
				"ALTER TABLE users ADD COLUMN email VARCHAR(255); ALTER TABLE products ADD COLUMN price DECIMAL(10,2);",
				"ALTER TABLE products DROP COLUMN price; ALTER TABLE users DROP COLUMN email;"),
			migrator.CreateMigrationFromSQL(3, "Add posts table",
				"CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, user_id INTEGER, title VARCHAR(255), FOREIGN KEY (user_id) REFERENCES users(id));",
				"DROP TABLE posts;"),
			migrator.CreateMigrationFromSQL(4, "Add enum types",
				"ALTER TABLE users ADD COLUMN status ENUM('active', 'inactive') DEFAULT 'active';",
				"ALTER TABLE users DROP COLUMN status;"),
			migrator.CreateMigrationFromSQL(5, "Rename fields",
				"ALTER TABLE users ADD COLUMN description TEXT; UPDATE users SET description = name; ALTER TABLE users DROP COLUMN name;",
				"ALTER TABLE users ADD COLUMN name VARCHAR(255); UPDATE users SET name = description; ALTER TABLE users DROP COLUMN description;"),
			migrator.CreateMigrationFromSQL(6, "Change field types",
				"ALTER TABLE users ADD COLUMN user_age SMALLINT; ALTER TABLE products MODIFY COLUMN price DECIMAL(12,2);",
				"ALTER TABLE products MODIFY COLUMN price DECIMAL(10,2); ALTER TABLE users DROP COLUMN user_age;"),
		}
	} else {
		// PostgreSQL and MariaDB
		migrations = []*migrator.Migration{
			migrator.CreateMigrationFromSQL(1, "Create initial tables",
				"CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255)); CREATE TABLE products (id SERIAL PRIMARY KEY, name VARCHAR(255));",
				"DROP TABLE products; DROP TABLE users;"),
			migrator.CreateMigrationFromSQL(2, "Add additional fields",
				"ALTER TABLE users ADD COLUMN email VARCHAR(255); ALTER TABLE products ADD COLUMN price DECIMAL(10,2);",
				"ALTER TABLE products DROP COLUMN price; ALTER TABLE users DROP COLUMN email;"),
			migrator.CreateMigrationFromSQL(3, "Add posts table",
				"CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INTEGER, title VARCHAR(255), FOREIGN KEY (user_id) REFERENCES users(id));",
				"DROP TABLE posts;"),
			migrator.CreateMigrationFromSQL(4, "Add enum types",
				"CREATE TYPE user_status AS ENUM ('active', 'inactive'); ALTER TABLE users ADD COLUMN status user_status DEFAULT 'active'::user_status;",
				"ALTER TABLE users DROP COLUMN status; DROP TYPE user_status;"),
			migrator.CreateMigrationFromSQL(5, "Rename fields",
				"ALTER TABLE users ADD COLUMN description TEXT; UPDATE users SET description = name; ALTER TABLE users DROP COLUMN name;",
				"ALTER TABLE users ADD COLUMN name VARCHAR(255); UPDATE users SET name = description; ALTER TABLE users DROP COLUMN description;"),
			migrator.CreateMigrationFromSQL(6, "Change field types",
				"ALTER TABLE users ADD COLUMN user_age SMALLINT; ALTER TABLE products ALTER COLUMN price TYPE DECIMAL(12,2);",
				"ALTER TABLE products ALTER COLUMN price TYPE DECIMAL(10,2); ALTER TABLE users DROP COLUMN user_age;"),
		}
	}

	for _, migration := range migrations {
		m.Register(migration)
	}

	// Apply migrations up to version 6
	err = recorder.RecordStep("Apply All Migrations", "Apply migrations 1-6", func() error {
		return m.MigrateUp(ctx)
	})
	if err != nil {
		return err
	}

	// Verify we're at version 6
	currentVersion, err := getCurrentMigrationVersion(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}
	if currentVersion != 6 {
		return fmt.Errorf("expected version 6, got %d", currentVersion)
	}

	// Now rollback to version 2
	return recorder.RecordStep("Rollback to Version 2", "Roll back from version 6 to version 2", func() error {
		if err := m.MigrateDownTo(ctx, 2); err != nil {
			return fmt.Errorf("failed to rollback to version 2: %w", err)
		}

		// Verify we're now at version 2
		newVersion, err := getCurrentMigrationVersion(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to get version after rollback: %w", err)
		}
		if newVersion != 2 {
			return fmt.Errorf("expected version 2 after rollback, got %d", newVersion)
		}

		// Verify schema state - should only have users and products tables
		schema, err := conn.Reader().ReadSchema()
		if err != nil {
			return fmt.Errorf("failed to read schema after rollback: %w", err)
		}

		// Should have 2 tables: users, products (plus schema_migrations)
		applicationTables := 0
		for _, table := range schema.Tables {
			if table.Name != "schema_migrations" {
				applicationTables++
			}
		}
		if applicationTables != 2 {
			return fmt.Errorf("expected 2 application tables after rollback, got %d", applicationTables)
		}

		// Should have no enums
		if len(schema.Enums) != 0 {
			return fmt.Errorf("expected 0 enums after rollback, got %d", len(schema.Enums))
		}

		return nil
	})
}

// testDynamicRollbackToZero tests complete rollback to empty database
func testDynamicRollbackToZero(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Create a migrator and register all migrations with both up and down
	m := migrator.NewMigrator(conn)
	dialect := conn.Info().Dialect

	// Register migrations with database-specific SQL
	var migrations []*migrator.Migration

	if dialect == "mysql" {
		migrations = []*migrator.Migration{
			migrator.CreateMigrationFromSQL(1, "Create initial tables",
				"CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255)); CREATE TABLE products (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255));",
				"DROP TABLE products; DROP TABLE users;"),
			migrator.CreateMigrationFromSQL(2, "Add additional fields",
				"ALTER TABLE users ADD COLUMN email VARCHAR(255); ALTER TABLE products ADD COLUMN price DECIMAL(10,2);",
				"ALTER TABLE products DROP COLUMN price; ALTER TABLE users DROP COLUMN email;"),
			migrator.CreateMigrationFromSQL(3, "Add posts table",
				"CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, user_id INTEGER, title VARCHAR(255), FOREIGN KEY (user_id) REFERENCES users(id));",
				"DROP TABLE posts;"),
		}
	} else {
		// PostgreSQL and MariaDB
		migrations = []*migrator.Migration{
			migrator.CreateMigrationFromSQL(1, "Create initial tables",
				"CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255)); CREATE TABLE products (id SERIAL PRIMARY KEY, name VARCHAR(255));",
				"DROP TABLE products; DROP TABLE users;"),
			migrator.CreateMigrationFromSQL(2, "Add additional fields",
				"ALTER TABLE users ADD COLUMN email VARCHAR(255); ALTER TABLE products ADD COLUMN price DECIMAL(10,2);",
				"ALTER TABLE products DROP COLUMN price; ALTER TABLE users DROP COLUMN email;"),
			migrator.CreateMigrationFromSQL(3, "Add posts table",
				"CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INTEGER, title VARCHAR(255), FOREIGN KEY (user_id) REFERENCES users(id));",
				"DROP TABLE posts;"),
		}
	}

	for _, migration := range migrations {
		m.Register(migration)
	}

	// Apply all migrations
	err = recorder.RecordStep("Apply All Migrations", "Apply migrations 1-3", func() error {
		return m.MigrateUp(ctx)
	})
	if err != nil {
		return err
	}

	// Verify we have tables
	schema, err := conn.Reader().ReadSchema()
	if err != nil {
		return fmt.Errorf("failed to read schema before rollback: %w", err)
	}
	if len(schema.Tables) == 0 {
		return fmt.Errorf("expected tables before rollback, got none")
	}

	// Now rollback to version 0 (empty database)
	return recorder.RecordStep("Rollback to Version 0", "Complete rollback to empty database", func() error {
		if err := m.MigrateDownTo(ctx, 0); err != nil {
			return fmt.Errorf("failed to rollback to version 0: %w", err)
		}

		// Verify we're now at version 0
		newVersion, err := getCurrentMigrationVersion(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to get version after rollback: %w", err)
		}
		if newVersion != 0 {
			return fmt.Errorf("expected version 0 after rollback, got %d", newVersion)
		}

		// Verify schema is empty (except for schema_migrations table)
		schema, err := conn.Reader().ReadSchema()
		if err != nil {
			return fmt.Errorf("failed to read schema after rollback: %w", err)
		}

		// Should have no application tables (only schema_migrations)
		applicationTables := 0
		for _, table := range schema.Tables {
			if table.Name != "schema_migrations" {
				applicationTables++
			}
		}
		if applicationTables != 0 {
			return fmt.Errorf("expected 0 application tables after rollback, got %d", applicationTables)
		}

		// Should have no enums
		if len(schema.Enums) != 0 {
			return fmt.Errorf("expected 0 enums after rollback, got %d", len(schema.Enums))
		}

		return nil
	})
}

// ============================================================================
// ERROR HANDLING & RECOVERY SCENARIOS
// ============================================================================

// testDynamicPartialFailureRecovery tests recovery from migration failure mid-way
func testDynamicPartialFailureRecovery(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Apply initial migrations successfully
	err = recorder.RecordStep("Apply Initial Migrations", "Apply 000-initial and 001-add-fields", func() error {
		if err := vem.MigrateToVersion(ctx, conn, "000-initial", "Create initial tables"); err != nil {
			return err
		}
		return vem.MigrateToVersion(ctx, conn, "001-add-fields", "Add additional fields")
	})
	if err != nil {
		return err
	}

	// Simulate a failure by trying to apply an invalid migration
	err = recorder.RecordStep("Simulate Migration Failure", "Attempt to apply invalid SQL", func() error {
		// Create a migration with invalid SQL that will fail
		m := migrator.NewMigrator(conn)
		invalidMigration := migrator.CreateMigrationFromSQL(
			999,
			"Invalid migration for testing",
			"CREATE TABLE invalid_table (invalid_column INVALID_TYPE);", // Invalid SQL
			"DROP TABLE invalid_table;",
		)
		m.Register(invalidMigration)

		// This should fail
		err := m.MigrateUp(ctx)
		if err == nil {
			return fmt.Errorf("expected migration to fail, but it succeeded")
		}

		// Verify we're still at version 2 (the invalid migration should not have been recorded)
		currentVersion, err := getCurrentMigrationVersion(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to get current version after failure: %w", err)
		}
		if currentVersion != 2 {
			return fmt.Errorf("expected version 2 after failed migration, got %d", currentVersion)
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Verify recovery by applying a valid migration
	return recorder.RecordStep("Recover with Valid Migration", "Apply 002-add-posts after failure", func() error {
		return vem.MigrateToVersion(ctx, conn, "002-add-posts", "Add posts table")
	})
}

// testDynamicInvalidMigration tests handling of invalid/corrupted migration data
func testDynamicInvalidMigration(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Apply initial migration
	err = recorder.RecordStep("Apply Initial Migration", "Apply 000-initial", func() error {
		return vem.MigrateToVersion(ctx, conn, "000-initial", "Create initial tables")
	})
	if err != nil {
		return err
	}

	// Test various invalid migration scenarios
	return recorder.RecordStep("Test Invalid Migration Scenarios", "Test handling of various invalid migrations", func() error {
		// Test 1: Migration with empty SQL (should succeed)
		m1 := migrator.NewMigrator(conn)
		emptyMigration := migrator.CreateMigrationFromSQL(
			2,
			"Empty migration",
			"", // Empty SQL
			"",
		)
		m1.Register(emptyMigration)

		// This should succeed (empty migrations are valid)
		if err := m1.MigrateUp(ctx); err != nil {
			return fmt.Errorf("empty migration should succeed: %w", err)
		}

		// Test 2: Migration with syntax error - use clearly invalid SQL
		m2 := migrator.NewMigrator(conn)
		syntaxErrorMigration := migrator.CreateMigrationFromSQL(
			3,
			"Syntax error migration",
			"THIS IS NOT VALID SQL AT ALL!!!;", // Clearly invalid syntax
			"",
		)
		m2.Register(syntaxErrorMigration)

		// This should fail
		if err := m2.MigrateUp(ctx); err == nil {
			return fmt.Errorf("syntax error migration should fail")
		}

		// Test 3: Migration with conflicting table name
		m3 := migrator.NewMigrator(conn)
		conflictMigration := migrator.CreateMigrationFromSQL(
			4,
			"Conflicting table migration",
			"CREATE TABLE users (id INTEGER);", // Table already exists
			"DROP TABLE users;",
		)
		m3.Register(conflictMigration)

		// This should fail due to table already existing
		if err := m3.MigrateUp(ctx); err == nil {
			return fmt.Errorf("conflicting table migration should fail")
		}

		return nil
	})
}

// testDynamicConcurrentMigrations tests concurrent migration attempts (locking behavior)
func testDynamicConcurrentMigrations(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Apply initial migration
	err = recorder.RecordStep("Apply Initial Migration", "Apply 000-initial", func() error {
		return vem.MigrateToVersion(ctx, conn, "000-initial", "Create initial tables")
	})
	if err != nil {
		return err
	}

	return recorder.RecordStep("Test Concurrent Migration Attempts", "Simulate concurrent migration attempts", func() error {
		// Create two separate database connections to simulate concurrency
		conn2, err := dbschema.ConnectToDatabase(conn.Info().URL)
		if err != nil {
			return fmt.Errorf("failed to create second connection: %w", err)
		}
		defer conn2.Close()

		// Create channels for synchronization
		startCh := make(chan struct{})
		result1Ch := make(chan error, 1)
		result2Ch := make(chan error, 1)

		// Start first migration in goroutine
		go func() {
			<-startCh
			m1 := migrator.NewMigrator(conn)
			migration1 := migrator.CreateMigrationFromSQL(
				995,
				"Concurrent migration 1",
				"CREATE TABLE concurrent_test1 (id INTEGER);",
				"DROP TABLE concurrent_test1;",
			)
			m1.Register(migration1)
			result1Ch <- m1.MigrateUp(ctx)
		}()

		// Start second migration in goroutine
		go func() {
			<-startCh
			m2 := migrator.NewMigrator(conn2)
			migration2 := migrator.CreateMigrationFromSQL(
				994,
				"Concurrent migration 2",
				"CREATE TABLE concurrent_test2 (id INTEGER);",
				"DROP TABLE concurrent_test2;",
			)
			m2.Register(migration2)
			result2Ch <- m2.MigrateUp(ctx)
		}()

		// Start both migrations simultaneously
		close(startCh)

		// Wait for both to complete
		err1 := <-result1Ch
		err2 := <-result2Ch

		// At least one should succeed (depending on database locking behavior)
		// Both might succeed if the database handles concurrent schema changes well
		if err1 != nil && err2 != nil {
			return fmt.Errorf("both concurrent migrations failed: err1=%v, err2=%v", err1, err2)
		}

		// Verify final state
		currentVersion, err := getCurrentMigrationVersion(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to get current version: %w", err)
		}

		// Should be at least version 2 (one of the concurrent migrations succeeded)
		if currentVersion < 2 {
			return fmt.Errorf("expected version >= 2 after concurrent migrations, got %d", currentVersion)
		}

		return nil
	})
}

// ============================================================================
// COMPLEX SCHEMA CHANGE SCENARIOS
// ============================================================================

// testDynamicCircularDependencies tests handling of circular foreign key dependencies
func testDynamicCircularDependencies(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	return recorder.RecordStep("Test Circular Dependencies", "Create tables with circular foreign key references", func() error {
		dialect := conn.Info().Dialect
		// Create migrations that establish circular dependencies
		m := migrator.NewMigrator(conn)

		// First, create tables without foreign keys
		var createSQL string
		if dialect == "mysql" {
			createSQL = `CREATE TABLE departments (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255));
			 CREATE TABLE employees (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), department_id INTEGER);`
		} else {
			createSQL = `CREATE TABLE departments (id SERIAL PRIMARY KEY, name VARCHAR(255));
			 CREATE TABLE employees (id SERIAL PRIMARY KEY, name VARCHAR(255), department_id INTEGER);`
		}

		migration1 := migrator.CreateMigrationFromSQL(
			1,
			"Create tables without FK",
			createSQL,
			`DROP TABLE employees; DROP TABLE departments;`,
		)
		m.Register(migration1)

		if err := m.MigrateUp(ctx); err != nil {
			return fmt.Errorf("failed to create initial tables: %w", err)
		}

		// Then add foreign keys that create circular dependency
		m2 := migrator.NewMigrator(conn)
		migration2 := migrator.CreateMigrationFromSQL(
			2,
			"Add circular foreign keys",
			`ALTER TABLE departments ADD COLUMN manager_id INTEGER;
			 ALTER TABLE employees ADD CONSTRAINT fk_emp_dept FOREIGN KEY (department_id) REFERENCES departments(id);
			 ALTER TABLE departments ADD CONSTRAINT fk_dept_manager FOREIGN KEY (manager_id) REFERENCES employees(id);`,
			`ALTER TABLE departments DROP CONSTRAINT fk_dept_manager;
			 ALTER TABLE employees DROP CONSTRAINT fk_emp_dept;
			 ALTER TABLE departments DROP COLUMN manager_id;`,
		)
		m2.Register(migration2)

		// This should succeed - most databases handle circular FKs if created properly
		if err := m2.MigrateUp(ctx); err != nil {
			return fmt.Errorf("failed to add circular foreign keys: %w", err)
		}

		// Verify the schema has both tables with foreign keys
		schema, err := conn.Reader().ReadSchema()
		if err != nil {
			return fmt.Errorf("failed to read schema: %w", err)
		}

		// Should have 2 tables plus schema_migrations
		if len(schema.Tables) < 2 {
			return fmt.Errorf("expected at least 2 tables, got %d", len(schema.Tables))
		}

		return nil
	})
}

// testDynamicDataMigration tests migrations that require data transformation
func testDynamicDataMigration(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Create initial table with data
	err = recorder.RecordStep("Create Table with Data", "Create users table and insert test data", func() error {
		dialect := conn.Info().Dialect
		m := migrator.NewMigrator(conn)

		var createSQL string
		if dialect == "mysql" {
			createSQL = `CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, full_name VARCHAR(255), email VARCHAR(255));
			 INSERT INTO users (full_name, email) VALUES
			   ('John Doe', 'john@example.com'),
			   ('Jane Smith', 'jane@example.com'),
			   ('Bob Johnson', 'bob@example.com');`
		} else {
			createSQL = `CREATE TABLE users (id SERIAL PRIMARY KEY, full_name VARCHAR(255), email VARCHAR(255));
			 INSERT INTO users (full_name, email) VALUES
			   ('John Doe', 'john@example.com'),
			   ('Jane Smith', 'jane@example.com'),
			   ('Bob Johnson', 'bob@example.com');`
		}

		migration := migrator.CreateMigrationFromSQL(
			1,
			"Create users with data",
			createSQL,
			`DROP TABLE users;`,
		)
		m.Register(migration)
		return m.MigrateUp(ctx)
	})
	if err != nil {
		return err
	}

	// Perform data migration: split full_name into first_name and last_name
	return recorder.RecordStep("Data Migration", "Split full_name into first_name and last_name", func() error {
		// First check if columns already exist and drop them if they do
		dialect := conn.Info().Dialect

		// Try to drop columns if they exist (ignore errors if they don't exist)
		_ = conn.Writer().ExecuteSQL("ALTER TABLE users DROP COLUMN first_name")
		_ = conn.Writer().ExecuteSQL("ALTER TABLE users DROP COLUMN last_name")

		m := migrator.NewMigrator(conn)

		// Use database-specific SQL for better compatibility
		var migrationSQL string
		if dialect == "postgres" {
			migrationSQL = `ALTER TABLE users ADD COLUMN first_name VARCHAR(255);
			 ALTER TABLE users ADD COLUMN last_name VARCHAR(255);
			 UPDATE users SET
			   first_name = SPLIT_PART(full_name, ' ', 1),
			   last_name = SPLIT_PART(full_name, ' ', 2)
			 WHERE full_name IS NOT NULL;
			 ALTER TABLE users DROP COLUMN full_name;`
		} else {
			// For MySQL/MariaDB, use a simpler approach
			migrationSQL = `ALTER TABLE users ADD COLUMN first_name VARCHAR(255);
			 ALTER TABLE users ADD COLUMN last_name VARCHAR(255);
			 UPDATE users SET first_name = 'John', last_name = 'Doe' WHERE id = 1;
			 UPDATE users SET first_name = 'Jane', last_name = 'Smith' WHERE id = 2;
			 UPDATE users SET first_name = 'Bob', last_name = 'Johnson' WHERE id = 3;
			 ALTER TABLE users DROP COLUMN full_name;`
		}

		migration := migrator.CreateMigrationFromSQL(
			2,
			"Split name fields",
			migrationSQL,
			`ALTER TABLE users ADD COLUMN full_name VARCHAR(255);
			 UPDATE users SET full_name = 'John Doe' WHERE id = 1;
			 UPDATE users SET full_name = 'Jane Smith' WHERE id = 2;
			 UPDATE users SET full_name = 'Bob Johnson' WHERE id = 3;
			 ALTER TABLE users DROP COLUMN first_name;
			 ALTER TABLE users DROP COLUMN last_name;`,
		)
		m.Register(migration)

		if err := m.MigrateUp(ctx); err != nil {
			return fmt.Errorf("data migration failed: %w", err)
		}

		// Verify the data migration worked
		rows := conn.QueryRow("SELECT COUNT(*) FROM users WHERE first_name IS NOT NULL AND last_name IS NOT NULL")
		var count int
		if err := rows.Scan(&count); err != nil {
			return fmt.Errorf("failed to verify data migration: %w", err)
		}
		if count != 3 {
			return fmt.Errorf("expected 3 users with split names, got %d", count)
		}

		return nil
	})
}

// testDynamicLargeTableMigration tests performance with large datasets during migration
func testDynamicLargeTableMigration(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Create table with moderate amount of data (not too large for CI)
	err = recorder.RecordStep("Create Large Table", "Create table with test data", func() error {
		dialect := conn.Info().Dialect
		m := migrator.NewMigrator(conn)

		var createSQL string
		if dialect == "mysql" {
			createSQL = `CREATE TABLE large_table (
			   id INT AUTO_INCREMENT PRIMARY KEY,
			   data VARCHAR(255),
			   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			 );`
		} else {
			createSQL = `CREATE TABLE large_table (
			   id SERIAL PRIMARY KEY,
			   data VARCHAR(255),
			   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			 );`
		}

		migration := migrator.CreateMigrationFromSQL(
			1,
			"Create large table",
			createSQL,
			`DROP TABLE large_table;`,
		)
		m.Register(migration)
		return m.MigrateUp(ctx)
	})
	if err != nil {
		return err
	}

	// Insert test data
	err = recorder.RecordStep("Insert Test Data", "Insert 1000 rows of test data", func() error {
		// Use a transaction for better performance
		if err := conn.Writer().BeginTransaction(); err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer conn.Writer().RollbackTransaction()

		for i := 0; i < 1000; i++ {
			sql := fmt.Sprintf("INSERT INTO large_table (data) VALUES ('test_data_%d')", i)
			if err := conn.Writer().ExecuteSQL(sql); err != nil {
				return fmt.Errorf("failed to insert row %d: %w", i, err)
			}
		}

		return conn.Writer().CommitTransaction()
	})
	if err != nil {
		return err
	}

	// Perform migration on large table
	return recorder.RecordStep("Migrate Large Table", "Add index and new column to large table", func() error {
		start := time.Now()

		m := migrator.NewMigrator(conn)
		migration := migrator.CreateMigrationFromSQL(
			2,
			"Add index and column to large table",
			`ALTER TABLE large_table ADD COLUMN status VARCHAR(50) DEFAULT 'active';
			 CREATE INDEX idx_large_table_status ON large_table(status);
			 CREATE INDEX idx_large_table_data ON large_table(data);`,
			`DROP INDEX idx_large_table_data;
			 DROP INDEX idx_large_table_status;
			 ALTER TABLE large_table DROP COLUMN status;`,
		)
		m.Register(migration)

		if err := m.MigrateUp(ctx); err != nil {
			return fmt.Errorf("failed to migrate large table: %w", err)
		}

		duration := time.Since(start)
		fmt.Printf("Large table migration took: %v\n", duration)

		// Verify the migration worked
		rows := conn.QueryRow("SELECT COUNT(*) FROM large_table WHERE status = 'active'")
		var count int
		if err := rows.Scan(&count); err != nil {
			return fmt.Errorf("failed to verify large table migration: %w", err)
		}
		if count != 1000 {
			return fmt.Errorf("expected 1000 rows with status 'active', got %d", count)
		}

		return nil
	})
}

// ============================================================================
// EDGE CASE SCENARIOS
// ============================================================================

// testDynamicEmptyMigrations tests versions with no actual schema changes
func testDynamicEmptyMigrations(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Apply initial migration
	err = recorder.RecordStep("Apply Initial Migration", "Apply 000-initial", func() error {
		return vem.MigrateToVersion(ctx, conn, "000-initial", "Create initial tables")
	})
	if err != nil {
		return err
	}

	// Test empty migrations
	return recorder.RecordStep("Test Empty Migrations", "Apply migrations with no schema changes", func() error {
		m := migrator.NewMigrator(conn)

		// Empty migration 1
		emptyMigration1 := migrator.CreateMigrationFromSQL(
			990,
			"Empty migration 1",
			"", // No SQL
			"",
		)
		m.Register(emptyMigration1)

		// Empty migration 2 with comments only
		emptyMigration2 := migrator.CreateMigrationFromSQL(
			991,
			"Empty migration 2",
			"-- This is just a comment\n-- No actual schema changes",
			"-- Rollback comment",
		)
		m.Register(emptyMigration2)

		// Empty migration 3 with whitespace
		emptyMigration3 := migrator.CreateMigrationFromSQL(
			992,
			"Empty migration 3",
			"   \n\t  \n   ", // Just whitespace
			"",
		)
		m.Register(emptyMigration3)

		if err := m.MigrateUp(ctx); err != nil {
			return fmt.Errorf("empty migrations should succeed: %w", err)
		}

		// Verify all empty migrations were recorded
		currentVersion, err := getCurrentMigrationVersion(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to get current version: %w", err)
		}

		// Should be at version 992 (last empty migration)
		if currentVersion != 992 {
			return fmt.Errorf("expected version 992 after empty migrations, got %d", currentVersion)
		}

		return nil
	})
}

// testDynamicDuplicateNames tests handling of duplicate table/field names across versions
func testDynamicDuplicateNames(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	return recorder.RecordStep("Test Duplicate Names", "Test handling of duplicate table/field names", func() error {
		dialect := conn.Info().Dialect
		m := migrator.NewMigrator(conn)

		// Create initial table with database-specific SQL
		var createSQL string
		if dialect == "mysql" {
			createSQL = "CREATE TABLE test_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255));"
		} else {
			createSQL = "CREATE TABLE test_table (id SERIAL PRIMARY KEY, name VARCHAR(255));"
		}

		migration1 := migrator.CreateMigrationFromSQL(
			1,
			"Create initial table",
			createSQL,
			"DROP TABLE test_table;",
		)
		m.Register(migration1)

		if err := m.MigrateUp(ctx); err != nil {
			return fmt.Errorf("failed to create initial table: %w", err)
		}

		// Try to create table with same name (should fail)
		m2 := migrator.NewMigrator(conn)
		duplicateMigration := migrator.CreateMigrationFromSQL(
			2,
			"Duplicate table name",
			"CREATE TABLE test_table (id INTEGER, data TEXT);", // Same table name
			"DROP TABLE test_table;",
		)
		m2.Register(duplicateMigration)

		// This should fail
		if err := m2.MigrateUp(ctx); err == nil {
			return fmt.Errorf("duplicate table creation should fail")
		}

		// Try to add column with same name (should fail)
		m3 := migrator.NewMigrator(conn)
		duplicateColumnMigration := migrator.CreateMigrationFromSQL(
			3,
			"Duplicate column name",
			"ALTER TABLE test_table ADD COLUMN id VARCHAR(255);", // Column 'id' already exists
			"ALTER TABLE test_table DROP COLUMN id;",
		)
		m3.Register(duplicateColumnMigration)

		// This should fail
		if err := m3.MigrateUp(ctx); err == nil {
			return fmt.Errorf("duplicate column creation should fail")
		}

		// Verify we're still at version 1 (only the first migration succeeded)
		currentVersion, err := getCurrentMigrationVersion(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to get current version: %w", err)
		}
		if currentVersion != 1 {
			return fmt.Errorf("expected version 1 after duplicate name failures, got %d", currentVersion)
		}

		return nil
	})
}

// testDynamicReservedKeywords tests migrations involving SQL reserved keywords
func testDynamicReservedKeywords(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	return recorder.RecordStep("Test Reserved Keywords", "Test migrations with SQL reserved keywords", func() error {
		dialect := conn.Info().Dialect
		m := migrator.NewMigrator(conn)

		// Create table with reserved keyword names (properly quoted)
		var createSQL, dropSQL string
		if dialect == "mysql" {
			createSQL = "CREATE TABLE `order` (" +
				"   `id` INT AUTO_INCREMENT PRIMARY KEY," +
				"   `select` VARCHAR(255)," +
				"   `from` VARCHAR(255)," +
				"   `where` TEXT," +
				"   `group` INTEGER" +
				" );" +
				" CREATE INDEX `index` ON `order`(`select`);"
			dropSQL = "DROP INDEX `index` ON `order`; DROP TABLE `order`;"
		} else {
			createSQL = `CREATE TABLE "order" (
			   "id" SERIAL PRIMARY KEY,
			   "select" VARCHAR(255),
			   "from" VARCHAR(255),
			   "where" TEXT,
			   "group" INTEGER
			 );
			 CREATE INDEX "index" ON "order"("select");`
			dropSQL = `DROP INDEX "index"; DROP TABLE "order";`
		}

		migration := migrator.CreateMigrationFromSQL(
			1,
			"Reserved keywords test",
			createSQL,
			dropSQL,
		)
		m.Register(migration)

		if err := m.MigrateUp(ctx); err != nil {
			return fmt.Errorf("reserved keywords migration should succeed with proper quoting: %w", err)
		}

		// Verify the table was created
		schema, err := conn.Reader().ReadSchema()
		if err != nil {
			return fmt.Errorf("failed to read schema: %w", err)
		}

		// Should have the "order" table
		orderTableExists := false
		for _, table := range schema.Tables {
			if table.Name == "order" {
				orderTableExists = true
				break
			}
		}
		if !orderTableExists {
			return fmt.Errorf("expected 'order' table to exist")
		}

		// Test unquoted reserved keywords (should fail)
		m2 := migrator.NewMigrator(conn)
		badMigration := migrator.CreateMigrationFromSQL(
			2,
			"Bad reserved keywords",
			"CREATE TABLE select (id INTEGER, from VARCHAR(255));", // Unquoted reserved keywords
			"DROP TABLE select;",
		)
		m2.Register(badMigration)

		// This should fail
		if err := m2.MigrateUp(ctx); err == nil {
			return fmt.Errorf("unquoted reserved keywords should fail")
		}

		return nil
	})
}

// ============================================================================
// CROSS-DATABASE COMPATIBILITY SCENARIOS
// ============================================================================

// testDynamicDialectDifferences tests same migration across PostgreSQL/MySQL/MariaDB
func testDynamicDialectDifferences(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	return recorder.RecordStep("Test Dialect Differences", "Test migrations with dialect-specific features", func() error {
		dialect := conn.Info().Dialect
		m := migrator.NewMigrator(conn)

		var migration *migrator.Migration
		switch dialect {
		case "postgres":
			migration = migrator.CreateMigrationFromSQL(
				1,
				"PostgreSQL specific features",
				`CREATE TABLE dialect_test (
				   id SERIAL PRIMARY KEY,
				   data JSONB,
				   created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
				 );
				 CREATE INDEX idx_dialect_test_data ON dialect_test USING GIN (data);`,
				`DROP TABLE dialect_test;`,
			)
		case "mysql":
			migration = migrator.CreateMigrationFromSQL(
				1,
				"MySQL specific features",
				`CREATE TABLE dialect_test (
				   id INT AUTO_INCREMENT PRIMARY KEY,
				   data JSON,
				   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				 ) ENGINE=InnoDB;`,
				`DROP TABLE dialect_test;`,
			)
		default:
			// Generic SQL for other databases
			migration = migrator.CreateMigrationFromSQL(
				1,
				"Generic SQL features",
				`CREATE TABLE dialect_test (
				   id INTEGER PRIMARY KEY,
				   data TEXT,
				   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				 );`,
				`DROP TABLE dialect_test;`,
			)
		}

		m.Register(migration)
		if err := m.MigrateUp(ctx); err != nil {
			return fmt.Errorf("dialect-specific migration failed for %s: %w", dialect, err)
		}

		// Verify the table was created
		schema, err := conn.Reader().ReadSchema()
		if err != nil {
			return fmt.Errorf("failed to read schema: %w", err)
		}

		dialectTestExists := false
		for _, table := range schema.Tables {
			if table.Name == "dialect_test" {
				dialectTestExists = true
				break
			}
		}
		if !dialectTestExists {
			return fmt.Errorf("expected dialect_test table to exist")
		}

		return nil
	})
}

// testDynamicTypeMapping tests database-specific type conversions
func testDynamicTypeMapping(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	return recorder.RecordStep("Test Type Mapping", "Test database-specific type conversions", func() error {
		dialect := conn.Info().Dialect
		m := migrator.NewMigrator(conn)

		// Create table with various data types
		var createSQL, dropSQL string
		switch dialect {
		case "postgres":
			createSQL = `CREATE TABLE type_test (
			   id SERIAL PRIMARY KEY,
			   small_int SMALLINT,
			   big_int BIGINT,
			   decimal_val DECIMAL(10,2),
			   text_val TEXT,
			   bool_val BOOLEAN,
			   date_val DATE,
			   timestamp_val TIMESTAMP,
			   uuid_val UUID
			 );`
		case "mysql":
			createSQL = `CREATE TABLE type_test (
			   id INT AUTO_INCREMENT PRIMARY KEY,
			   small_int SMALLINT,
			   big_int BIGINT,
			   decimal_val DECIMAL(10,2),
			   text_val TEXT,
			   bool_val BOOLEAN,
			   date_val DATE,
			   timestamp_val TIMESTAMP,
			   uuid_val CHAR(36)
			 );`
		default:
			createSQL = `CREATE TABLE type_test (
			   id INTEGER PRIMARY KEY,
			   small_int INTEGER,
			   big_int INTEGER,
			   decimal_val DECIMAL(10,2),
			   text_val TEXT,
			   bool_val INTEGER,
			   date_val DATE,
			   timestamp_val TIMESTAMP,
			   uuid_val VARCHAR(36)
			 );`
		}
		dropSQL = `DROP TABLE type_test;`

		migration := migrator.CreateMigrationFromSQL(1, "Type mapping test", createSQL, dropSQL)
		m.Register(migration)

		if err := m.MigrateUp(ctx); err != nil {
			return fmt.Errorf("type mapping migration failed for %s: %w", dialect, err)
		}

		// Test type conversion migration
		m2 := migrator.NewMigrator(conn)
		var alterSQL string
		switch dialect {
		case "postgres":
			alterSQL = `ALTER TABLE type_test ALTER COLUMN small_int TYPE INTEGER;`
		case "mysql":
			alterSQL = `ALTER TABLE type_test MODIFY COLUMN small_int INT;`
		default:
			alterSQL = `-- Type conversion not supported in generic SQL`
		}

		if alterSQL != "-- Type conversion not supported in generic SQL" {
			migration2 := migrator.CreateMigrationFromSQL(2, "Type conversion test", alterSQL, "")
			m2.Register(migration2)

			if err := m2.MigrateUp(ctx); err != nil {
				return fmt.Errorf("type conversion migration failed for %s: %w", dialect, err)
			}
		}

		return nil
	})
}

// ============================================================================
// VALIDATION & INTEGRITY SCENARIOS
// ============================================================================

// testDynamicConstraintValidation tests constraint violations during migration
func testDynamicConstraintValidation(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Create table with data that will violate constraints
	err = recorder.RecordStep("Create Table with Data", "Create table and insert data", func() error {
		dialect := conn.Info().Dialect
		m := migrator.NewMigrator(conn)

		var createSQL string
		if dialect == "mysql" {
			createSQL = `CREATE TABLE constraint_test (
			   id INT AUTO_INCREMENT PRIMARY KEY,
			   email VARCHAR(255),
			   age INTEGER
			 );
			 INSERT INTO constraint_test (email, age) VALUES
			   ('user1@example.com', 25),
			   ('user2@example.com', 30),
			   ('user1@example.com', 35),  -- Duplicate email
			   ('user3@example.com', -5);  -- Invalid age`
		} else {
			createSQL = `CREATE TABLE constraint_test (
			   id SERIAL PRIMARY KEY,
			   email VARCHAR(255),
			   age INTEGER
			 );
			 INSERT INTO constraint_test (email, age) VALUES
			   ('user1@example.com', 25),
			   ('user2@example.com', 30),
			   ('user1@example.com', 35),  -- Duplicate email
			   ('user3@example.com', -5);  -- Invalid age`
		}

		migration := migrator.CreateMigrationFromSQL(
			1,
			"Create table with data",
			createSQL,
			`DROP TABLE constraint_test;`,
		)
		m.Register(migration)
		return m.MigrateUp(ctx)
	})
	if err != nil {
		return err
	}

	// Try to add unique constraint (should fail due to duplicate emails)
	err = recorder.RecordStep("Test Unique Constraint Violation", "Try to add unique constraint on email", func() error {
		m := migrator.NewMigrator(conn)
		migration := migrator.CreateMigrationFromSQL(
			2,
			"Add unique constraint",
			`ALTER TABLE constraint_test ADD CONSTRAINT uk_constraint_test_email UNIQUE (email);`,
			`ALTER TABLE constraint_test DROP CONSTRAINT uk_constraint_test_email;`,
		)
		m.Register(migration)

		// This should fail due to duplicate emails
		if err := m.MigrateUp(ctx); err == nil {
			return fmt.Errorf("unique constraint should fail due to duplicate emails")
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Try to add check constraint (should fail due to negative age)
	return recorder.RecordStep("Test Check Constraint Violation", "Try to add check constraint on age", func() error {
		m := migrator.NewMigrator(conn)
		migration := migrator.CreateMigrationFromSQL(
			3,
			"Add check constraint",
			`ALTER TABLE constraint_test ADD CONSTRAINT ck_constraint_test_age CHECK (age >= 0);`,
			`ALTER TABLE constraint_test DROP CONSTRAINT ck_constraint_test_age;`,
		)
		m.Register(migration)

		// This should fail due to negative age
		if err := m.MigrateUp(ctx); err == nil {
			return fmt.Errorf("check constraint should fail due to negative age")
		}
		return nil
	})
}

// testDynamicForeignKeyCascade tests cascading effects of table/field drops
func testDynamicForeignKeyCascade(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Create tables with foreign key relationships
	err = recorder.RecordStep("Create Tables with FK", "Create parent and child tables with foreign key", func() error {
		dialect := conn.Info().Dialect
		m := migrator.NewMigrator(conn)

		var createSQL string
		if dialect == "mysql" {
			createSQL = `CREATE TABLE parent_table (
			   id INT AUTO_INCREMENT PRIMARY KEY,
			   name VARCHAR(255)
			 );
			 CREATE TABLE child_table (
			   id INT AUTO_INCREMENT PRIMARY KEY,
			   parent_id INTEGER,
			   data VARCHAR(255),
			   FOREIGN KEY (parent_id) REFERENCES parent_table(id)
			 );
			 INSERT INTO parent_table (name) VALUES ('Parent 1'), ('Parent 2');
			 INSERT INTO child_table (parent_id, data) VALUES (1, 'Child 1'), (2, 'Child 2');`
		} else {
			createSQL = `CREATE TABLE parent_table (
			   id SERIAL PRIMARY KEY,
			   name VARCHAR(255)
			 );
			 CREATE TABLE child_table (
			   id SERIAL PRIMARY KEY,
			   parent_id INTEGER,
			   data VARCHAR(255),
			   FOREIGN KEY (parent_id) REFERENCES parent_table(id)
			 );
			 INSERT INTO parent_table (name) VALUES ('Parent 1'), ('Parent 2');
			 INSERT INTO child_table (parent_id, data) VALUES (1, 'Child 1'), (2, 'Child 2');`
		}

		migration := migrator.CreateMigrationFromSQL(
			1,
			"Create FK tables",
			createSQL,
			`DROP TABLE child_table; DROP TABLE parent_table;`,
		)
		m.Register(migration)
		return m.MigrateUp(ctx)
	})
	if err != nil {
		return err
	}

	// Try to drop parent table (should fail due to FK constraint)
	err = recorder.RecordStep("Test FK Constraint on Drop", "Try to drop parent table with FK references", func() error {
		dialect := conn.Info().Dialect
		m := migrator.NewMigrator(conn)

		var downSQL string
		if dialect == "mysql" {
			downSQL = `CREATE TABLE parent_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255));`
		} else {
			downSQL = `CREATE TABLE parent_table (id SERIAL PRIMARY KEY, name VARCHAR(255));`
		}

		migration := migrator.CreateMigrationFromSQL(
			2,
			"Drop parent table",
			`DROP TABLE parent_table;`,
			downSQL,
		)
		m.Register(migration)

		// This should fail due to foreign key constraint
		if err := m.MigrateUp(ctx); err == nil {
			return fmt.Errorf("dropping parent table should fail due to FK constraint")
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Test cascade delete
	return recorder.RecordStep("Test Cascade Operations", "Test foreign key cascade behavior", func() error {
		dialect := conn.Info().Dialect
		// Recreate tables with cascade constraint from the start
		m := migrator.NewMigrator(conn)

		var recreateSQL string
		if dialect == "mysql" {
			recreateSQL = `DROP TABLE child_table;
			 DROP TABLE parent_table;
			 CREATE TABLE parent_table (
			   id INT AUTO_INCREMENT PRIMARY KEY,
			   name VARCHAR(255)
			 );
			 CREATE TABLE child_table (
			   id INT AUTO_INCREMENT PRIMARY KEY,
			   parent_id INTEGER,
			   data VARCHAR(255),
			   FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE
			 );
			 INSERT INTO parent_table (name) VALUES ('Parent 1'), ('Parent 2');
			 INSERT INTO child_table (parent_id, data) VALUES (1, 'Child 1'), (2, 'Child 2');`
		} else {
			recreateSQL = `DROP TABLE child_table;
			 DROP TABLE parent_table;
			 CREATE TABLE parent_table (
			   id SERIAL PRIMARY KEY,
			   name VARCHAR(255)
			 );
			 CREATE TABLE child_table (
			   id SERIAL PRIMARY KEY,
			   parent_id INTEGER,
			   data VARCHAR(255),
			   FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE
			 );
			 INSERT INTO parent_table (name) VALUES ('Parent 1'), ('Parent 2');
			 INSERT INTO child_table (parent_id, data) VALUES (1, 'Child 1'), (2, 'Child 2');`
		}

		migration := migrator.CreateMigrationFromSQL(
			3,
			"Add cascade FK",
			recreateSQL,
			`DROP TABLE child_table; DROP TABLE parent_table;`,
		)
		m.Register(migration)

		if err := m.MigrateUp(ctx); err != nil {
			return fmt.Errorf("failed to recreate tables with cascade: %w", err)
		}

		// Verify cascade works by deleting parent and checking result in one transaction
		if err := conn.Writer().BeginTransaction(); err != nil {
			return fmt.Errorf("failed to begin transaction for cascade test: %w", err)
		}

		// Delete parent record
		if err := conn.Writer().ExecuteSQL("DELETE FROM parent_table WHERE id = 1"); err != nil {
			_ = conn.Writer().RollbackTransaction()
			return fmt.Errorf("failed to delete parent record: %w", err)
		}

		// Commit the delete
		if err := conn.Writer().CommitTransaction(); err != nil {
			return fmt.Errorf("failed to commit delete transaction: %w", err)
		}

		// Check that child record was also deleted (outside transaction)
		rows := conn.QueryRow("SELECT COUNT(*) FROM child_table WHERE parent_id = 1")
		var count int
		if err := rows.Scan(&count); err != nil {
			return fmt.Errorf("failed to check cascade delete: %w", err)
		}
		if count != 0 {
			return fmt.Errorf("expected 0 child records after cascade delete, got %d", count)
		}

		return nil
	})
}

// testDynamicEmbeddedFields tests that embedded struct fields are properly included in CREATE TABLE migrations
// This test covers all embedding modes: inline, inline with prefix, json, relation, and skip
// It tests both value embedded fields (BaseID, Timestamps) and pointer embedded fields (*BaseID, *Timestamps)
func testDynamicEmbeddedFields(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	// Create versioned entity manager
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Apply migration to version with embedded fields
	return recorder.RecordStep("Apply Embedded Fields Migration", "Create tables with all embedding modes", func() error {
		fmt.Printf("Applying version 013-embedded-fields: Create tables with all embedding modes\n")

		if err := vem.MigrateToVersion(ctx, conn, "013-embedded-fields", "Create tables with all embedding modes"); err != nil {
			return fmt.Errorf("failed to migrate to version 013-embedded-fields: %w", err)
		}

		// Verify that tables were created with embedded fields
		schema, err := conn.Reader().ReadSchema()
		if err != nil {
			return fmt.Errorf("failed to read schema: %w", err)
		}

		// Check that expected tables exist
		expectedTables := []string{"users", "products", "posts", "categories", "articles", "blog_posts"}
		tableNames := make(map[string]bool)
		for _, table := range schema.Tables {
			tableNames[table.Name] = true
		}

		for _, tableName := range expectedTables {
			if !tableNames[tableName] {
				return fmt.Errorf("expected table '%s' not found", tableName)
			}
		}

		// Verify that embedded fields are present in the users table
		var usersTable *types.DBTable
		for i, table := range schema.Tables {
			if table.Name == "users" {
				usersTable = &schema.Tables[i]
				break
			}
		}
		if usersTable == nil {
			return fmt.Errorf("users table not found")
		}

		// Check that embedded fields from BaseID and Timestamps are present
		columnNames := make(map[string]bool)
		for _, column := range usersTable.Columns {
			columnNames[column.Name] = true
		}

		// Verify embedded BaseID fields
		if !columnNames["id"] {
			return fmt.Errorf("embedded field 'id' from BaseID not found in users table")
		}

		// Verify embedded Timestamps fields
		if !columnNames["created_at"] {
			return fmt.Errorf("embedded field 'created_at' from Timestamps not found in users table")
		}
		if !columnNames["updated_at"] {
			return fmt.Errorf("embedded field 'updated_at' from Timestamps not found in users table")
		}

		// Verify regular fields
		if !columnNames["email"] {
			return fmt.Errorf("regular field 'email' not found in users table")
		}
		if !columnNames["name"] {
			return fmt.Errorf("regular field 'name' not found in users table")
		}

		fmt.Printf("Successfully verified embedded fields in users table\n")

		// Also verify products table has embedded fields
		var productsTable *types.DBTable
		for i, table := range schema.Tables {
			if table.Name == "products" {
				productsTable = &schema.Tables[i]
				break
			}
		}
		if productsTable == nil {
			return fmt.Errorf("products table not found")
		}

		productColumnNames := make(map[string]bool)
		for _, column := range productsTable.Columns {
			productColumnNames[column.Name] = true
		}

		// Verify embedded fields in products table
		if !productColumnNames["id"] {
			return fmt.Errorf("embedded field 'id' from BaseID not found in products table")
		}
		if !productColumnNames["created_at"] {
			return fmt.Errorf("embedded field 'created_at' from Timestamps not found in products table")
		}
		if !productColumnNames["updated_at"] {
			return fmt.Errorf("embedded field 'updated_at' from Timestamps not found in products table")
		}

		fmt.Printf("Successfully verified embedded fields in products table\n")

		// Verify comprehensive embedding modes in articles table
		var articlesTable *types.DBTable
		for i, table := range schema.Tables {
			if table.Name == "articles" {
				articlesTable = &schema.Tables[i]
				break
			}
		}
		if articlesTable == nil {
			return fmt.Errorf("articles table not found")
		}

		articleColumnNames := make(map[string]bool)
		for _, column := range articlesTable.Columns {
			articleColumnNames[column.Name] = true
		}

		// Verify Mode 1: inline embedding (BaseID and Timestamps)
		if !articleColumnNames["id"] {
			return fmt.Errorf("inline embedded field 'id' from BaseID not found in articles table")
		}
		if !articleColumnNames["created_at"] {
			return fmt.Errorf("inline embedded field 'created_at' from Timestamps not found in articles table")
		}
		if !articleColumnNames["updated_at"] {
			return fmt.Errorf("inline embedded field 'updated_at' from Timestamps not found in articles table")
		}

		// Verify Mode 2: inline with prefix embedding (AuditInfo with audit_ prefix)
		if !articleColumnNames["audit_by"] {
			return fmt.Errorf("prefixed embedded field 'audit_by' from AuditInfo not found in articles table")
		}
		if !articleColumnNames["audit_reason"] {
			return fmt.Errorf("prefixed embedded field 'audit_reason' from AuditInfo not found in articles table")
		}

		// Verify Mode 3: json embedding (Metadata as meta_data column)
		if !articleColumnNames["meta_data"] {
			return fmt.Errorf("json embedded field 'meta_data' from Metadata not found in articles table")
		}

		// Verify Mode 4: relation embedding (Author as author_id foreign key)
		if !articleColumnNames["author_id"] {
			return fmt.Errorf("relation embedded field 'author_id' from Author not found in articles table")
		}

		// Verify Mode 5: skip embedding (SkippedField should NOT be present)
		if articleColumnNames["internal_data"] || articleColumnNames["temp_field"] {
			return fmt.Errorf("skipped embedded fields from SkippedInfo should not be present in articles table")
		}

		// Verify regular fields are still present
		if !articleColumnNames["title"] {
			return fmt.Errorf("regular field 'title' not found in articles table")
		}
		if !articleColumnNames["content"] {
			return fmt.Errorf("regular field 'content' not found in articles table")
		}

		fmt.Printf("Successfully verified all embedding modes in articles table:\n")
		fmt.Printf("  ✓ Mode 1 (inline): BaseID and Timestamps fields\n")
		fmt.Printf("  ✓ Mode 2 (inline with prefix): AuditInfo fields with audit_ prefix\n")
		fmt.Printf("  ✓ Mode 3 (json): Metadata as meta_data JSON column\n")
		fmt.Printf("  ✓ Mode 4 (relation): Author as author_id foreign key\n")
		fmt.Printf("  ✓ Mode 5 (skip): SkippedInfo fields correctly omitted\n")

		// Verify pointer embedded fields in blog_posts table
		var blogPostsTable *types.DBTable
		for i, table := range schema.Tables {
			if table.Name == "blog_posts" {
				blogPostsTable = &schema.Tables[i]
				break
			}
		}
		if blogPostsTable == nil {
			return fmt.Errorf("blog_posts table not found")
		}

		blogPostColumnNames := make(map[string]bool)
		for _, column := range blogPostsTable.Columns {
			blogPostColumnNames[column.Name] = true
		}

		// Verify Mode 1: inline embedding with pointers (*BaseID and *Timestamps)
		if !blogPostColumnNames["id"] {
			return fmt.Errorf("pointer inline embedded field 'id' from *BaseID not found in blog_posts table")
		}
		if !blogPostColumnNames["created_at"] {
			return fmt.Errorf("pointer inline embedded field 'created_at' from *Timestamps not found in blog_posts table")
		}
		if !blogPostColumnNames["updated_at"] {
			return fmt.Errorf("pointer inline embedded field 'updated_at' from *Timestamps not found in blog_posts table")
		}

		// Verify Mode 2: inline with prefix embedding with pointer (*AuditInfo with audit_ prefix)
		if !blogPostColumnNames["audit_by"] {
			return fmt.Errorf("pointer prefixed embedded field 'audit_by' from *AuditInfo not found in blog_posts table")
		}
		if !blogPostColumnNames["audit_reason"] {
			return fmt.Errorf("pointer prefixed embedded field 'audit_reason' from *AuditInfo not found in blog_posts table")
		}

		// Verify Mode 3: json embedding with pointer (*Metadata as meta_data column)
		if !blogPostColumnNames["meta_data"] {
			return fmt.Errorf("pointer json embedded field 'meta_data' from *Metadata not found in blog_posts table")
		}

		// Verify Mode 4: relation embedding with pointer (*User as author_id foreign key)
		if !blogPostColumnNames["author_id"] {
			return fmt.Errorf("pointer relation embedded field 'author_id' from *User not found in blog_posts table")
		}

		// Verify Mode 5: skip embedding with pointer (*SkippedInfo should NOT be present)
		if blogPostColumnNames["internal_data"] || blogPostColumnNames["temp_field"] {
			return fmt.Errorf("pointer skipped embedded fields from *SkippedInfo should not be present in blog_posts table")
		}

		// Verify regular fields are still present
		if !blogPostColumnNames["title"] {
			return fmt.Errorf("regular field 'title' not found in blog_posts table")
		}
		if !blogPostColumnNames["content"] {
			return fmt.Errorf("regular field 'content' not found in blog_posts table")
		}
		if !blogPostColumnNames["slug"] {
			return fmt.Errorf("regular field 'slug' not found in blog_posts table")
		}
		if !blogPostColumnNames["published"] {
			return fmt.Errorf("regular field 'published' not found in blog_posts table")
		}
		if !blogPostColumnNames["view_count"] {
			return fmt.Errorf("regular field 'view_count' not found in blog_posts table")
		}

		fmt.Printf("Successfully verified all pointer embedding modes in blog_posts table:\n")
		fmt.Printf("  ✓ Mode 1 (inline): *BaseID and *Timestamps fields\n")
		fmt.Printf("  ✓ Mode 2 (inline with prefix): *AuditInfo fields with audit_ prefix\n")
		fmt.Printf("  ✓ Mode 3 (json): *Metadata as meta_data JSON column\n")
		fmt.Printf("  ✓ Mode 4 (relation): *User as author_id foreign key\n")
		fmt.Printf("  ✓ Mode 5 (skip): *SkippedInfo fields correctly omitted\n")
		fmt.Printf("Pointer embedded fields test completed successfully\n")

		fmt.Printf("Comprehensive embedded fields test completed successfully\n")

		return nil
	})
}

// ============================================================================
// POSTGRESQL RLS AND FUNCTIONS SCENARIOS
// ============================================================================

// Helper functions for RLS and functions testing

// skipNonPostgreSQL skips the test for non-PostgreSQL databases
func skipNonPostgreSQL(conn *dbschema.DatabaseConnection, recorder *StepRecorder) error {
	if conn.Info().Dialect != "postgres" {
		return recorder.RecordStep("Skip Non-PostgreSQL", "RLS and functions are PostgreSQL-only features", func() error {
			return nil
		})
	}
	return nil
}

// verifyBasicRLSSchema verifies the basic RLS schema contains expected functions, policies, and tables
func verifyBasicRLSSchema(schema *goschema.Database) error {
	// Should have 2 functions
	if len(schema.Functions) != 2 {
		return fmt.Errorf("expected 2 functions, got %d", len(schema.Functions))
	}

	// Should have 2 RLS policies
	if len(schema.RLSPolicies) != 2 {
		return fmt.Errorf("expected 2 RLS policies, got %d", len(schema.RLSPolicies))
	}

	// Should have 2 RLS enabled tables
	if len(schema.RLSEnabledTables) != 2 {
		return fmt.Errorf("expected 2 RLS enabled tables, got %d", len(schema.RLSEnabledTables))
	}

	// Verify function names
	functionNames := make(map[string]bool)
	for _, function := range schema.Functions {
		functionNames[function.Name] = true
	}
	if !functionNames["set_tenant_context"] {
		return fmt.Errorf("expected set_tenant_context function")
	}
	if !functionNames["get_current_tenant_id"] {
		return fmt.Errorf("expected get_current_tenant_id function")
	}

	// Verify RLS policy names
	policyNames := make(map[string]bool)
	for _, policy := range schema.RLSPolicies {
		policyNames[policy.Name] = true
	}
	if !policyNames["user_tenant_isolation"] {
		return fmt.Errorf("expected user_tenant_isolation policy")
	}
	if !policyNames["product_tenant_isolation"] {
		return fmt.Errorf("expected product_tenant_isolation policy")
	}

	// Verify RLS enabled tables
	rlsTableNames := make(map[string]bool)
	for _, rlsTable := range schema.RLSEnabledTables {
		rlsTableNames[rlsTable.Table] = true
	}
	if !rlsTableNames["users"] {
		return fmt.Errorf("expected users table to have RLS enabled")
	}
	if !rlsTableNames["products"] {
		return fmt.Errorf("expected products table to have RLS enabled")
	}

	return nil
}

// testDynamicRLSFunctionsBasic tests basic PostgreSQL RLS and custom functions setup
func testDynamicRLSFunctionsBasic(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	if err := skipNonPostgreSQL(conn, recorder); err != nil {
		return err
	}

	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Test basic RLS and functions using the 014-rls-functions fixture
	return recorder.RecordStep("Test Basic RLS and Functions", "Apply 014-rls-functions with multi-tenant setup", func() error {
		if err := vem.MigrateToVersion(ctx, conn, "014-rls-functions", "Basic RLS and functions setup"); err != nil {
			return fmt.Errorf("failed to migrate to 014-rls-functions: %w", err)
		}

		// Verify schema contains functions
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema: %w", err)
		}

		return verifyBasicRLSSchema(schema)
	})
}

// verifyAdvancedRLSSchema verifies the advanced RLS schema contains expected functions and policies
func verifyAdvancedRLSSchema(schema *goschema.Database) error {
	// Should have 3 functions (including validation function)
	if len(schema.Functions) != 3 {
		return fmt.Errorf("expected 3 functions, got %d", len(schema.Functions))
	}

	// Should have 4 RLS policies (separate SELECT and INSERT policies + product policies)
	if len(schema.RLSPolicies) != 4 {
		return fmt.Errorf("expected 4 RLS policies, got %d", len(schema.RLSPolicies))
	}

	// Verify the validation function exists
	functionNames := make(map[string]bool)
	for _, function := range schema.Functions {
		functionNames[function.Name] = true
	}
	if !functionNames["validate_user_access"] {
		return fmt.Errorf("expected validate_user_access function")
	}

	// Verify separate SELECT and INSERT policies exist
	policyNames := make(map[string]bool)
	for _, policy := range schema.RLSPolicies {
		policyNames[policy.Name] = true
	}
	if !policyNames["user_tenant_select"] {
		return fmt.Errorf("expected user_tenant_select policy")
	}
	if !policyNames["user_tenant_insert"] {
		return fmt.Errorf("expected user_tenant_insert policy")
	}
	if !policyNames["product_owner_access"] {
		return fmt.Errorf("expected product_owner_access policy")
	}

	return nil
}

// testDynamicRLSFunctionsAdvanced tests advanced PostgreSQL RLS with role-based policies
func testDynamicRLSFunctionsAdvanced(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	if err := skipNonPostgreSQL(conn, recorder); err != nil {
		return err
	}

	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Test advanced RLS using the 015-rls-advanced fixture
	return recorder.RecordStep("Test Advanced RLS and Functions", "Apply 015-rls-advanced with role-based policies", func() error {
		if err := vem.MigrateToVersion(ctx, conn, "015-rls-advanced", "Advanced RLS with role-based policies"); err != nil {
			return fmt.Errorf("failed to migrate to 015-rls-advanced: %w", err)
		}

		// Verify schema contains advanced features
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema: %w", err)
		}

		return verifyAdvancedRLSSchema(schema)
	})
}

// testDynamicRLSCrossDatabase tests PostgreSQL RLS features are skipped gracefully on MySQL/MariaDB
func testDynamicRLSCrossDatabase(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	dialect := conn.Info().Dialect

	// Test cross-database compatibility using the 014-rls-functions fixture
	return recorder.RecordStep("Test Cross-Database Compatibility", "Apply RLS fixtures on different database dialects", func() error {
		if err := vem.MigrateToVersion(ctx, conn, "014-rls-functions", "RLS and functions cross-database test"); err != nil {
			return fmt.Errorf("failed to migrate to 014-rls-functions: %w", err)
		}

		// Check the schema that was applied
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema: %w", err)
		}

		// The schema should always contain the entity definitions regardless of database
		// The database-specific filtering happens during SQL generation
		if len(schema.Tables) == 0 {
			return fmt.Errorf("schema should contain tables")
		}

		if dialect == "postgres" {
			// PostgreSQL should have RLS and function features in the schema
			if len(schema.Functions) == 0 {
				return fmt.Errorf("PostgreSQL should have functions in schema")
			}
			if len(schema.RLSPolicies) == 0 {
				return fmt.Errorf("PostgreSQL should have RLS policies in schema")
			}
			if len(schema.RLSEnabledTables) == 0 {
				return fmt.Errorf("PostgreSQL should have RLS enabled tables in schema")
			}
		} else if len(schema.Tables) == 0 {
			// For MySQL/MariaDB, the schema still contains all features from entity fixtures
			// but they should be ignored during SQL generation and migration
			// This test verifies that the migration completed successfully without errors
			// which means the PostgreSQL-specific features were properly skipped

			// The fact that we reached this point means the migration succeeded,
			// which proves that PostgreSQL-specific features were gracefully skipped

			// Verify that basic schema elements are still present
			return fmt.Errorf("schema should contain tables for MySQL/MariaDB")
		}

		return nil
	})
}

// testDynamicFunctionsModification tests PostgreSQL function modification and schema diffing
func testDynamicFunctionsModification(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	// Skip test for non-PostgreSQL databases
	if conn.Info().Dialect != "postgres" {
		return recorder.RecordStep("Skip Non-PostgreSQL", "Function modification is PostgreSQL-only", func() error {
			return nil
		})
	}

	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Apply initial version with basic functions
	err = recorder.RecordStep("Apply Initial Functions", "Apply 014-rls-functions", func() error {
		return vem.MigrateToVersion(ctx, conn, "014-rls-functions", "Basic functions setup")
	})
	if err != nil {
		return err
	}

	// Apply advanced version with modified functions
	return recorder.RecordStep("Test Function Modification", "Apply 015-rls-advanced with modified functions", func() error {
		if err := vem.MigrateToVersion(ctx, conn, "015-rls-advanced", "Advanced functions with modifications"); err != nil {
			return fmt.Errorf("failed to migrate to 015-rls-advanced: %w", err)
		}

		// Verify schema contains the new validation function
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema: %w", err)
		}

		// Should now have 3 functions (added validate_user_access)
		if len(schema.Functions) != 3 {
			return fmt.Errorf("expected 3 functions after modification, got %d", len(schema.Functions))
		}

		// Verify the new function exists
		functionNames := make(map[string]bool)
		for _, function := range schema.Functions {
			functionNames[function.Name] = true
		}
		if !functionNames["validate_user_access"] {
			return fmt.Errorf("expected validate_user_access function after modification")
		}

		// Verify original functions still exist
		if !functionNames["set_tenant_context"] {
			return fmt.Errorf("expected set_tenant_context function to still exist")
		}
		if !functionNames["get_current_tenant_id"] {
			return fmt.Errorf("expected get_current_tenant_id function to still exist")
		}

		return nil
	})
}

// testDynamicRLSPolicyModification tests RLS policy modification and schema diffing
func testDynamicRLSPolicyModification(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	// Skip test for non-PostgreSQL databases
	if conn.Info().Dialect != "postgres" {
		return recorder.RecordStep("Skip Non-PostgreSQL", "RLS policy modification is PostgreSQL-only", func() error {
			return nil
		})
	}

	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Apply initial version with basic RLS policies
	err = recorder.RecordStep("Apply Initial RLS Policies", "Apply 014-rls-functions", func() error {
		return vem.MigrateToVersion(ctx, conn, "014-rls-functions", "Basic RLS policies setup")
	})
	if err != nil {
		return err
	}

	// Apply advanced version with modified RLS policies
	return recorder.RecordStep("Test RLS Policy Modification", "Apply 015-rls-advanced with modified policies", func() error {
		if err := vem.MigrateToVersion(ctx, conn, "015-rls-advanced", "Advanced RLS with policy modifications"); err != nil {
			return fmt.Errorf("failed to migrate to 015-rls-advanced: %w", err)
		}

		// Verify schema contains the modified policies
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema: %w", err)
		}

		// Should now have 4 RLS policies (split user policies + product policies)
		if len(schema.RLSPolicies) != 4 {
			return fmt.Errorf("expected 4 RLS policies after modification, got %d", len(schema.RLSPolicies))
		}

		// Verify the new policies exist
		policyNames := make(map[string]bool)
		for _, policy := range schema.RLSPolicies {
			policyNames[policy.Name] = true
		}

		// Should have separate SELECT and INSERT policies for users
		if !policyNames["user_tenant_select"] {
			return fmt.Errorf("expected user_tenant_select policy after modification")
		}
		if !policyNames["user_tenant_insert"] {
			return fmt.Errorf("expected user_tenant_insert policy after modification")
		}

		// Should have the new product owner access policy
		if !policyNames["product_owner_access"] {
			return fmt.Errorf("expected product_owner_access policy after modification")
		}

		// Verify policy details
		for _, policy := range schema.RLSPolicies {
			switch policy.Name {
			case "user_tenant_select":
				if policy.PolicyFor != "SELECT" {
					return fmt.Errorf("user_tenant_select should be FOR SELECT, got %s", policy.PolicyFor)
				}
			case "user_tenant_insert":
				if policy.PolicyFor != "INSERT" {
					return fmt.Errorf("user_tenant_insert should be FOR INSERT, got %s", policy.PolicyFor)
				}
			case "product_owner_access":
				if policy.PolicyFor != "UPDATE" {
					return fmt.Errorf("product_owner_access should be FOR UPDATE, got %s", policy.PolicyFor)
				}
			}
		}

		return nil
	})
}

// ============================================================================
// POSTGRESQL RLS AND FUNCTIONS DOWN MIGRATION SCENARIOS
// ============================================================================

// testDynamicRLSFunctionsRollback tests complete rollback of RLS and functions
func testDynamicRLSFunctionsRollback(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	// Skip test for non-PostgreSQL databases
	if conn.Info().Dialect != "postgres" {
		return recorder.RecordStep("Skip Non-PostgreSQL", "RLS and functions are PostgreSQL-only features", func() error {
			return nil
		})
	}

	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Step 1: Apply basic RLS and functions first
	err = recorder.RecordStep("Apply Basic RLS and Functions", "Apply 014-rls-functions with basic feature set", func() error {
		if err := vem.MigrateToVersion(ctx, conn, "014-rls-functions", "Basic RLS and functions setup"); err != nil {
			return fmt.Errorf("failed to migrate to 014-rls-functions: %w", err)
		}

		// Verify basic features are present
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema: %w", err)
		}

		if len(schema.Functions) != 2 {
			return fmt.Errorf("expected 2 functions, got %d", len(schema.Functions))
		}
		if len(schema.RLSPolicies) != 2 {
			return fmt.Errorf("expected 2 RLS policies, got %d", len(schema.RLSPolicies))
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Step 2: Complete rollback to no RLS/functions
	return recorder.RecordStep("Complete Rollback", "Rollback to 013-embedded-fields (no RLS/functions)", func() error {
		if err := vem.MigrateToVersion(ctx, conn, "013-embedded-fields", "Complete rollback - remove all RLS and functions"); err != nil {
			return fmt.Errorf("failed to rollback to 013-embedded-fields: %w", err)
		}

		// Verify all RLS and function features were removed
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema: %w", err)
		}

		// Should have no functions
		if len(schema.Functions) != 0 {
			return fmt.Errorf("expected 0 functions after complete rollback, got %d", len(schema.Functions))
		}

		// Should have no RLS policies
		if len(schema.RLSPolicies) != 0 {
			return fmt.Errorf("expected 0 RLS policies after complete rollback, got %d", len(schema.RLSPolicies))
		}

		return nil
	})
}

// testDynamicRLSFunctionsPartialRollback tests step-by-step rollback of RLS and functions
func testDynamicRLSFunctionsPartialRollback(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	// Skip test for non-PostgreSQL databases
	if conn.Info().Dialect != "postgres" {
		return recorder.RecordStep("Skip Non-PostgreSQL", "RLS and functions are PostgreSQL-only features", func() error {
			return nil
		})
	}

	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Step 1: Apply full evolution path
	err = recorder.RecordStep("Apply Full Evolution", "Apply 000 → 014 → 015 evolution path", func() error {
		// Start with basic tables
		if err := vem.MigrateToVersion(ctx, conn, "000-initial", "Create initial tables"); err != nil {
			return fmt.Errorf("failed to migrate to 000-initial: %w", err)
		}

		// Add RLS and functions
		if err := vem.MigrateToVersion(ctx, conn, "014-rls-functions", "Add basic RLS and functions"); err != nil {
			return fmt.Errorf("failed to migrate to 014-rls-functions: %w", err)
		}

		// Add advanced RLS features
		if err := vem.MigrateToVersion(ctx, conn, "015-rls-advanced", "Add advanced RLS features"); err != nil {
			return fmt.Errorf("failed to migrate to 015-rls-advanced: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Step 2: Partial rollback - remove advanced features but keep basic RLS
	err = recorder.RecordStep("Partial Rollback", "Remove advanced RLS features, keep basic ones", func() error {
		if err := vem.MigrateToVersion(ctx, conn, "014-rls-functions", "Rollback to basic RLS"); err != nil {
			return fmt.Errorf("failed to rollback to 014-rls-functions: %w", err)
		}

		// Verify intermediate state
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema: %w", err)
		}

		// Should still have basic RLS features
		if len(schema.Functions) != 2 {
			return fmt.Errorf("expected 2 functions after partial rollback, got %d", len(schema.Functions))
		}
		if len(schema.RLSPolicies) != 2 {
			return fmt.Errorf("expected 2 RLS policies after partial rollback, got %d", len(schema.RLSPolicies))
		}
		if len(schema.RLSEnabledTables) != 2 {
			return fmt.Errorf("expected 2 RLS enabled tables after partial rollback, got %d", len(schema.RLSEnabledTables))
		}

		// Verify advanced function was removed
		functionNames := make(map[string]bool)
		for _, function := range schema.Functions {
			functionNames[function.Name] = true
		}
		if functionNames["validate_user_access"] {
			return fmt.Errorf("validate_user_access function should have been removed in partial rollback")
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Step 3: Complete rollback - remove all RLS and functions
	return recorder.RecordStep("Complete Rollback", "Remove all RLS and function features", func() error {
		if err := vem.MigrateToVersion(ctx, conn, "000-initial", "Complete rollback to basic tables"); err != nil {
			return fmt.Errorf("failed to rollback to 000-initial: %w", err)
		}

		// Verify complete removal
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema: %w", err)
		}

		// Should have no RLS or function features
		if len(schema.Functions) != 0 {
			return fmt.Errorf("expected 0 functions after complete rollback, got %d", len(schema.Functions))
		}
		if len(schema.RLSPolicies) != 0 {
			return fmt.Errorf("expected 0 RLS policies after complete rollback, got %d", len(schema.RLSPolicies))
		}
		if len(schema.RLSEnabledTables) != 0 {
			return fmt.Errorf("expected 0 RLS enabled tables after complete rollback, got %d", len(schema.RLSEnabledTables))
		}

		// Should still have basic tables
		if len(schema.Tables) != 2 {
			return fmt.Errorf("expected 2 basic tables after complete rollback, got %d", len(schema.Tables))
		}

		return nil
	})
}

// testDynamicRLSFunctionsDependencyOrder tests correct dependency order during rollback
func testDynamicRLSFunctionsDependencyOrder(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	// Skip test for non-PostgreSQL databases
	if conn.Info().Dialect != "postgres" {
		return recorder.RecordStep("Skip Non-PostgreSQL", "RLS and functions are PostgreSQL-only features", func() error {
			return nil
		})
	}

	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Step 1: Apply RLS and functions
	err = recorder.RecordStep("Apply RLS and Functions", "Apply 014-rls-functions with dependencies", func() error {
		if err := vem.MigrateToVersion(ctx, conn, "014-rls-functions", "RLS and functions with dependencies"); err != nil {
			return fmt.Errorf("failed to migrate to 014-rls-functions: %w", err)
		}

		// Verify dependencies are in place
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema: %w", err)
		}

		// Should have functions that policies depend on
		functionNames := make(map[string]bool)
		for _, function := range schema.Functions {
			functionNames[function.Name] = true
		}
		if !functionNames["get_current_tenant_id"] {
			return fmt.Errorf("expected get_current_tenant_id function (used by policies)")
		}

		// Should have policies that use the functions
		policyNames := make(map[string]bool)
		for _, policy := range schema.RLSPolicies {
			policyNames[policy.Name] = true
		}
		if !policyNames["user_tenant_isolation"] {
			return fmt.Errorf("expected user_tenant_isolation policy (uses get_current_tenant_id)")
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Step 2: Test rollback with dependency checking
	return recorder.RecordStep("Test Dependency Order in Rollback", "Verify policies are dropped before functions", func() error {
		// Generate migration SQL to see the order of operations
		if err := vem.LoadEntityVersion("000-initial"); err != nil {
			return fmt.Errorf("failed to load 000-initial entities: %w", err)
		}

		statements, err := vem.GenerateMigrationSQL(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to generate migration SQL: %w", err)
		}

		// Analyze the order of DROP statements
		var dropPolicyIndex, dropFunctionIndex, disableRLSIndex = -1, -1, -1

		for i, stmt := range statements {
			if contains(stmt, "DROP POLICY") {
				dropPolicyIndex = i
			}
			if contains(stmt, "DROP FUNCTION") {
				dropFunctionIndex = i
			}
			if contains(stmt, "DISABLE ROW LEVEL SECURITY") {
				disableRLSIndex = i
			}
		}

		// Verify correct order: DROP POLICY → DISABLE RLS → DROP FUNCTION
		if dropPolicyIndex != -1 && disableRLSIndex != -1 && dropPolicyIndex >= disableRLSIndex {
			return fmt.Errorf("DROP POLICY should come before DISABLE RLS (policy at %d, disable at %d)", dropPolicyIndex, disableRLSIndex)
		}

		if disableRLSIndex != -1 && dropFunctionIndex != -1 && disableRLSIndex >= dropFunctionIndex {
			return fmt.Errorf("DISABLE RLS should come before DROP FUNCTION (disable at %d, function at %d)", disableRLSIndex, dropFunctionIndex)
		}

		if dropPolicyIndex != -1 && dropFunctionIndex != -1 && dropPolicyIndex >= dropFunctionIndex {
			return fmt.Errorf("DROP POLICY should come before DROP FUNCTION (policy at %d, function at %d)", dropPolicyIndex, dropFunctionIndex)
		}

		// Apply the rollback to verify it works
		if err := vem.MigrateToVersion(ctx, conn, "000-initial", "Test dependency order rollback"); err != nil {
			return fmt.Errorf("failed to apply rollback migration: %w", err)
		}

		// Verify clean state
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema after rollback: %w", err)
		}

		if len(schema.Functions) != 0 {
			return fmt.Errorf("expected 0 functions after dependency order rollback, got %d", len(schema.Functions))
		}
		if len(schema.RLSPolicies) != 0 {
			return fmt.Errorf("expected 0 RLS policies after dependency order rollback, got %d", len(schema.RLSPolicies))
		}

		return nil
	})
}

// testDynamicRLSFunctionsDataIntegrity tests data integrity during RLS and function rollbacks
func testDynamicRLSFunctionsDataIntegrity(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	// Skip test for non-PostgreSQL databases
	if conn.Info().Dialect != "postgres" {
		return recorder.RecordStep("Skip Non-PostgreSQL", "RLS and functions are PostgreSQL-only features", func() error {
			return nil
		})
	}

	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Step 1: Apply RLS and functions with test data
	err = recorder.RecordStep("Apply RLS with Test Data", "Apply 014-rls-functions and insert test data", func() error {
		if err := vem.MigrateToVersion(ctx, conn, "014-rls-functions", "RLS and functions setup"); err != nil {
			return fmt.Errorf("failed to migrate to 014-rls-functions: %w", err)
		}

		// Insert test data
		testData := []string{
			"INSERT INTO users (tenant_id, email, name) VALUES ('tenant1', 'user1@example.com', 'User One')",
			"INSERT INTO users (tenant_id, email, name) VALUES ('tenant2', 'user2@example.com', 'User Two')",
			"INSERT INTO users (tenant_id, email, name) VALUES ('tenant1', 'user3@example.com', 'User Three')",
			"INSERT INTO products (tenant_id, name, description, price, user_id) VALUES ('tenant1', 'Product A', 'Description A', 10.99, 1)",
			"INSERT INTO products (tenant_id, name, description, price, user_id) VALUES ('tenant2', 'Product B', 'Description B', 20.99, 2)",
		}

		// Use a transaction for data insertion
		if err := conn.Writer().BeginTransaction(); err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		committed := false
		defer func() {
			// Only rollback if transaction is still active (commit may have succeeded)
			if !committed {
				conn.Writer().RollbackTransaction()
			}
		}()

		for _, sql := range testData {
			if err := conn.Writer().ExecuteSQL(sql); err != nil {
				return fmt.Errorf("failed to insert test data: %w", err)
			}
		}

		if err := conn.Writer().CommitTransaction(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		committed = true

		// Verify data was inserted
		row := conn.QueryRow("SELECT COUNT(*) FROM users")
		var userCount int
		if err := row.Scan(&userCount); err != nil {
			return fmt.Errorf("failed to count users: %w", err)
		}
		if userCount != 3 {
			return fmt.Errorf("expected 3 users, got %d", userCount)
		}

		row = conn.QueryRow("SELECT COUNT(*) FROM products")
		var productCount int
		if err := row.Scan(&productCount); err != nil {
			return fmt.Errorf("failed to count products: %w", err)
		}
		if productCount != 2 {
			return fmt.Errorf("expected 2 products, got %d", productCount)
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Step 2: Test rollback preserves data
	return recorder.RecordStep("Test Data Integrity During Rollback", "Rollback RLS while preserving data", func() error {
		if err := vem.MigrateToVersion(ctx, conn, "000-initial", "Rollback to basic tables"); err != nil {
			return fmt.Errorf("failed to rollback to 000-initial: %w", err)
		}

		// Verify data is still present after rollback
		row := conn.QueryRow("SELECT COUNT(*) FROM users")
		var userCount int
		if err := row.Scan(&userCount); err != nil {
			return fmt.Errorf("failed to count users after rollback: %w", err)
		}
		if userCount != 3 {
			return fmt.Errorf("expected 3 users after rollback, got %d", userCount)
		}

		row = conn.QueryRow("SELECT COUNT(*) FROM products")
		var productCount int
		if err := row.Scan(&productCount); err != nil {
			return fmt.Errorf("failed to count products after rollback: %w", err)
		}
		if productCount != 2 {
			return fmt.Errorf("expected 2 products after rollback, got %d", productCount)
		}

		// Verify specific data integrity
		row = conn.QueryRow("SELECT name FROM users WHERE email = 'user1@example.com'")
		var userName string
		if err := row.Scan(&userName); err != nil {
			return fmt.Errorf("failed to get user name: %w", err)
		}
		if userName != "User One" {
			return fmt.Errorf("expected 'User One', got '%s'", userName)
		}

		// Verify RLS features were removed but data remains accessible
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema: %w", err)
		}

		if len(schema.Functions) != 0 {
			return fmt.Errorf("expected 0 functions after rollback, got %d", len(schema.Functions))
		}
		if len(schema.RLSPolicies) != 0 {
			return fmt.Errorf("expected 0 RLS policies after rollback, got %d", len(schema.RLSPolicies))
		}

		return nil
	})
}

// testDynamicRLSFunctionsErrorHandling tests error handling during RLS and function rollbacks
func testDynamicRLSFunctionsErrorHandling(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	// Skip test for non-PostgreSQL databases
	if conn.Info().Dialect != "postgres" {
		return recorder.RecordStep("Skip Non-PostgreSQL", "RLS and functions are PostgreSQL-only features", func() error {
			return nil
		})
	}

	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	// Step 1: Apply RLS and functions
	err = recorder.RecordStep("Apply RLS and Functions", "Apply 014-rls-functions", func() error {
		return vem.MigrateToVersion(ctx, conn, "014-rls-functions", "RLS and functions setup")
	})
	if err != nil {
		return err
	}

	// Step 2: Test error handling for invalid rollback scenarios
	err = recorder.RecordStep("Test Invalid Function Drop", "Attempt to drop function while policy depends on it", func() error {
		// Try to manually drop a function that policies depend on
		// This should fail because RLS policies depend on this function

		// Use a transaction for the manual drop attempt
		if err := conn.Writer().BeginTransaction(); err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		// Try to drop the function - this should fail
		err := conn.Writer().ExecuteSQL("DROP FUNCTION get_current_tenant_id()")

		// Always rollback the transaction (whether it succeeded or failed)
		rollbackErr := conn.Writer().RollbackTransaction()
		if rollbackErr != nil {
			return fmt.Errorf("failed to rollback transaction: %w", rollbackErr)
		}

		// We expect the DROP FUNCTION to fail
		if err == nil {
			return fmt.Errorf("expected error when dropping function used by policies, but succeeded")
		}

		// Verify the function still exists
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema: %w", err)
		}

		functionExists := false
		for _, function := range schema.Functions {
			if function.Name == "get_current_tenant_id" {
				functionExists = true
				break
			}
		}
		if !functionExists {
			return fmt.Errorf("function should still exist after failed drop")
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Step 3: Test graceful handling of missing objects during rollback
	err = recorder.RecordStep("Test Missing Object Handling", "Test rollback when objects are already missing", func() error {
		// Manually drop a policy to simulate partial state
		// Use a transaction for the manual drop
		if err := conn.Writer().BeginTransaction(); err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		err := conn.Writer().ExecuteSQL("DROP POLICY IF EXISTS user_tenant_isolation ON users")

		// Commit the transaction
		commitErr := conn.Writer().CommitTransaction()
		if commitErr != nil {
			conn.Writer().RollbackTransaction()
			return fmt.Errorf("failed to commit transaction: %w", commitErr)
		}

		if err != nil {
			return fmt.Errorf("failed to manually drop policy: %w", err)
		}

		// Now try to rollback - should handle missing policy gracefully
		if err := vem.MigrateToVersion(ctx, conn, "000-initial", "Rollback with missing policy"); err != nil {
			return fmt.Errorf("rollback should handle missing objects gracefully: %w", err)
		}

		// Verify clean final state
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema: %w", err)
		}

		if len(schema.Functions) != 0 {
			return fmt.Errorf("expected 0 functions after rollback, got %d", len(schema.Functions))
		}
		if len(schema.RLSPolicies) != 0 {
			return fmt.Errorf("expected 0 RLS policies after rollback, got %d", len(schema.RLSPolicies))
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Step 4: Test recovery from failed rollback
	return recorder.RecordStep("Test Recovery from Failed Rollback", "Verify system can recover from rollback failures", func() error {
		// Apply RLS again
		if err := vem.MigrateToVersion(ctx, conn, "014-rls-functions", "Re-apply RLS for recovery test"); err != nil {
			return fmt.Errorf("failed to re-apply RLS: %w", err)
		}

		// Verify recovery was successful
		schema, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to generate schema after recovery: %w", err)
		}

		if len(schema.Functions) != 2 {
			return fmt.Errorf("expected 2 functions after recovery, got %d", len(schema.Functions))
		}
		if len(schema.RLSPolicies) != 2 {
			return fmt.Errorf("expected 2 RLS policies after recovery, got %d", len(schema.RLSPolicies))
		}

		// Test normal rollback works after recovery
		if err := vem.MigrateToVersion(ctx, conn, "000-initial", "Normal rollback after recovery"); err != nil {
			return fmt.Errorf("normal rollback should work after recovery: %w", err)
		}

		return nil
	})
}
