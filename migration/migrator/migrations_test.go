package migrator

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
)

func TestMigration_Basic(t *testing.T) {
	c := qt.New(t)

	// Test creating a new migration
	migration := &Migration{
		Version:     1,
		Description: "Test migration",
		Up:          NoopMigrationFunc,
		Down:        NoopMigrationFunc,
	}

	c.Assert(migration.Version, qt.Equals, 1)
	c.Assert(migration.Description, qt.Equals, "Test migration")
	c.Assert(migration.Up, qt.IsNotNil)
	c.Assert(migration.Down, qt.IsNotNil)
}

func TestNoopMigrationFunc(t *testing.T) {
	c := qt.New(t)

	// Test that noop migration function doesn't error
	err := NoopMigrationFunc(context.Background(), nil)
	c.Assert(err, qt.IsNil)
}

func TestCreateMigrationFromSQL(t *testing.T) {
	c := qt.New(t)

	upSQL := "CREATE TABLE test (id SERIAL PRIMARY KEY)"
	downSQL := "DROP TABLE test"

	migration := CreateMigrationFromSQL(1, "Create test table", upSQL, downSQL)

	c.Assert(migration.Version, qt.Equals, 1)
	c.Assert(migration.Description, qt.Equals, "Create test table")
	c.Assert(migration.Up, qt.IsNotNil)
	c.Assert(migration.Down, qt.IsNotNil)

	// Test that the functions don't panic (we can't test execution without a real DB)
	c.Assert(migration.Up, qt.IsNotNil)
	c.Assert(migration.Down, qt.IsNotNil)
}

func TestMigrationStatus(t *testing.T) {
	c := qt.New(t)

	status := &MigrationStatus{
		CurrentVersion:    5,
		PendingMigrations: []int{6, 7, 8},
		TotalMigrations:   8,
		HasPendingChanges: true,
	}

	c.Assert(status.CurrentVersion, qt.Equals, 5)
	c.Assert(status.PendingMigrations, qt.HasLen, 3)
	c.Assert(status.TotalMigrations, qt.Equals, 8)
	c.Assert(status.HasPendingChanges, qt.IsTrue)
}

func TestMigrationStatus_NoPending(t *testing.T) {
	c := qt.New(t)

	status := &MigrationStatus{
		CurrentVersion:    5,
		PendingMigrations: []int{},
		TotalMigrations:   5,
		HasPendingChanges: false,
	}

	c.Assert(status.CurrentVersion, qt.Equals, 5)
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
	c.Assert(status.TotalMigrations, qt.Equals, 5)
	c.Assert(status.HasPendingChanges, qt.IsFalse)
}

func TestSplitSQLStatements(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name: "single statement",
			sql:  "CREATE TABLE users (id SERIAL PRIMARY KEY);",
			expected: []string{
				"CREATE TABLE users (id SERIAL PRIMARY KEY)",
			},
		},
		{
			name: "multiple statements",
			sql:  "CREATE TABLE users (id SERIAL PRIMARY KEY); CREATE INDEX idx_users_id ON users(id);",
			expected: []string{
				"CREATE TABLE users (id SERIAL PRIMARY KEY)",
				"CREATE INDEX idx_users_id ON users(id)",
			},
		},
		{
			name: "statements with comments",
			sql:  "-- Create users table\nCREATE TABLE users (id SERIAL PRIMARY KEY);\n-- Create index\nCREATE INDEX idx_users_id ON users(id);",
			expected: []string{
				"CREATE TABLE users (id SERIAL PRIMARY KEY)",
				"CREATE INDEX idx_users_id ON users(id)",
			},
		},
		{
			name:     "empty SQL",
			sql:      "",
			expected: []string{},
		},
		{
			name:     "only comments",
			sql:      "-- This is a comment\n/* Another comment */",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := SplitSQLStatements(tt.sql)
			c.Assert(result, qt.DeepEquals, tt.expected)
		})
	}
}

func TestMigrationFuncFromSQLFilename_Success(t *testing.T) {
	c := qt.New(t)

	// Create a test filesystem with SQL content
	fsys := fstest.MapFS{
		"test.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE test (id SERIAL PRIMARY KEY);"),
		},
	}

	migrationFunc := MigrationFuncFromSQLFilename("test.sql", fsys)
	c.Assert(migrationFunc, qt.IsNotNil)

	// We can't easily test execution without a real database connection,
	// but we can test that the function was created successfully
}

func TestMigrationFuncFromSQLFilename_FileNotFound(t *testing.T) {
	c := qt.New(t)

	// Create an empty filesystem
	fsys := fstest.MapFS{}

	migrationFunc := MigrationFuncFromSQLFilename("nonexistent.sql", fsys)
	c.Assert(migrationFunc, qt.IsNotNil)

	// Test that the function returns an error when executed
	err := migrationFunc(context.Background(), nil)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "failed to read migration file")
}
