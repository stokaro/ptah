package migrator

import (
	"context"
	"testing"
	"testing/fstest"
	"time"

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

	c.Assert(migration.Version, qt.Equals, int64(1))
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

	c.Assert(migration.Version, qt.Equals, int64(1))
	c.Assert(migration.Description, qt.Equals, "Create test table")
	c.Assert(migration.Up, qt.IsNotNil)
	c.Assert(migration.Down, qt.IsNotNil)
	c.Assert(migration.NoTransaction, qt.IsFalse)

	// Test that the functions don't panic (we can't test execution without a real DB)
	c.Assert(migration.Up, qt.IsNotNil)
	c.Assert(migration.Down, qt.IsNotNil)
}

func TestCreateMigrationFromSQL_NoTransactionDirective(t *testing.T) {
	c := qt.New(t)

	migration := CreateMigrationFromSQL(1, "Add enum value",
		"-- +ptah no_transaction\nALTER TYPE mood ADD VALUE 'ok';",
		"-- manual down migration required",
	)

	c.Assert(migration.NoTransaction, qt.IsTrue)
}

func TestCreateMigrationFromSQL_InvalidNoTransactionDirective(t *testing.T) {
	c := qt.New(t)

	migration := CreateMigrationFromSQL(1, "Invalid directive",
		"-- +ptah no_transaction=maybe\nSELECT 1;",
		"",
	)

	err := migration.Up(context.Background(), nil)
	c.Assert(err, qt.ErrorMatches, `invalid up migration directives: invalid \+ptah no_transaction value "maybe": expected true or false`)
}

func TestMigrationStatus(t *testing.T) {
	c := qt.New(t)

	status := &MigrationStatus{
		CurrentVersion:    5,
		PendingMigrations: []int64{6, 7, 8},
		TotalMigrations:   8,
		HasPendingChanges: true,
	}

	c.Assert(status.CurrentVersion, qt.Equals, int64(5))
	c.Assert(status.PendingMigrations, qt.HasLen, 3)
	c.Assert(status.TotalMigrations, qt.Equals, 8)
	c.Assert(status.HasPendingChanges, qt.IsTrue)
}

func TestMigrationStatus_NoPending(t *testing.T) {
	c := qt.New(t)

	status := &MigrationStatus{
		CurrentVersion:    5,
		PendingMigrations: []int64{},
		TotalMigrations:   5,
		HasPendingChanges: false,
	}

	c.Assert(status.CurrentVersion, qt.Equals, int64(5))
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

func TestParseAtlasTxtarSQL(t *testing.T) {
	c := qt.New(t)

	parsed, ok, err := parseAtlasTxtarSQL("20240305171146_seed.sql", `-- atlas:txtar

-- migration.sql --
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- down.sql --
DELETE FROM users WHERE id = 1;
`)
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(parsed.hasDown, qt.IsTrue)
	c.Assert(parsed.migrationSQL, qt.Contains, "INSERT INTO users")
	c.Assert(parsed.downSQL, qt.Contains, "DELETE FROM users")
}

func TestParseAtlasTxtarSQLWithoutDown(t *testing.T) {
	c := qt.New(t)

	parsed, ok, err := parseAtlasTxtarSQL("20240305171146_seed.sql", `-- atlas:txtar

-- migration.sql --
INSERT INTO users (id, name) VALUES (1, 'Alice');
`)
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(parsed.hasDown, qt.IsFalse)
	c.Assert(parsed.migrationSQL, qt.Contains, "INSERT INTO users")
	c.Assert(parsed.downSQL, qt.Equals, "")
}

func TestParseAtlasTxtarSQLRequiresMigrationSection(t *testing.T) {
	c := qt.New(t)

	_, ok, err := parseAtlasTxtarSQL("20240305171146_seed.sql", `-- atlas:txtar

-- down.sql --
DELETE FROM users WHERE id = 1;
`)
	c.Assert(ok, qt.IsTrue)
	c.Assert(err, qt.ErrorMatches, `invalid Atlas txtar migration 20240305171146_seed.sql: missing migration.sql section`)
}

func TestParseAtlasTxtarSQLRejectsSQLBeforeSection(t *testing.T) {
	c := qt.New(t)

	_, ok, err := parseAtlasTxtarSQL("20240305171146_seed.sql", `-- atlas:txtar
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- migration.sql --
SELECT 1;
`)
	c.Assert(ok, qt.IsTrue)
	c.Assert(err, qt.ErrorMatches, `invalid Atlas txtar migration 20240305171146_seed.sql: SQL appears before the first txtar section`)
}

func TestParseAtlasTxtarSQLIgnoresUnknownCommentMarkers(t *testing.T) {
	c := qt.New(t)

	parsed, ok, err := parseAtlasTxtarSQL("20240305171146_seed.sql", `-- atlas:txtar

-- migration.sql --
-- keep this comment --
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- down.sql --
DELETE FROM users WHERE id = 1;
`)
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(parsed.migrationSQL, qt.Contains, "-- keep this comment --")
	c.Assert(parsed.migrationSQL, qt.Contains, "INSERT INTO users")
	c.Assert(parsed.hasDown, qt.IsTrue)
}

func TestParseAtlasTxtarSQLIgnoresUnknownFileSections(t *testing.T) {
	c := qt.New(t)

	parsed, ok, err := parseAtlasTxtarSQL("20240305171146_seed.sql", `-- atlas:txtar

-- migration.sql --
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- schema.sql --
THIS IS NOT MIGRATION SQL;

-- down.sql --
DELETE FROM users WHERE id = 1;
`)
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(parsed.migrationSQL, qt.Contains, "INSERT INTO users")
	c.Assert(parsed.migrationSQL, qt.Not(qt.Contains), "THIS IS NOT MIGRATION SQL")
	c.Assert(parsed.downSQL, qt.Contains, "DELETE FROM users")
}

func TestParseMigrationTimeoutDirectives(t *testing.T) {
	tests := []struct {
		name                    string
		sql                     string
		wantLockTimeout         time.Duration
		wantStatementTimeout    time.Duration
		wantHasLockTimeout      bool
		wantHasStatementTimeout bool
		wantErr                 string
	}{
		{
			name: "directives at top of file",
			sql: `-- Migration header
-- +ptah lock_timeout=3s
-- +ptah statement_timeout=30s

ALTER TABLE users ADD COLUMN email TEXT;`,
			wantLockTimeout:         3 * time.Second,
			wantStatementTimeout:    30 * time.Second,
			wantHasLockTimeout:      true,
			wantHasStatementTimeout: true,
		},
		{
			name: "multiple directives on one line",
			sql: `-- +ptah lock_timeout=500ms statement_timeout=2m
ALTER TABLE users ADD COLUMN email TEXT;`,
			wantLockTimeout:         500 * time.Millisecond,
			wantStatementTimeout:    2 * time.Minute,
			wantHasLockTimeout:      true,
			wantHasStatementTimeout: true,
		},
		{
			name: "directive after SQL is ignored",
			sql: `ALTER TABLE users ADD COLUMN email TEXT;
-- +ptah lock_timeout=3s`,
		},
		{
			name: "other ptah directive is ignored",
			sql:  "-- +ptah unknown_timeout=3s\nALTER TABLE users ADD COLUMN email TEXT;",
		},
		{
			name: "online ddl directive is ignored by timeout parser",
			sql:  "-- +ptah online_ddl_tool=ghost\nALTER TABLE users ADD COLUMN email TEXT;",
		},
		{
			name: "no transaction directive is ignored by timeout parser",
			sql:  "-- +ptah no_transaction\nALTER TABLE users ADD COLUMN email TEXT;",
		},
		{
			name:    "invalid duration fails",
			sql:     "-- +ptah lock_timeout=soon\nALTER TABLE users ADD COLUMN email TEXT;",
			wantErr: "invalid +ptah lock_timeout value",
		},
		{
			name:    "zero duration fails",
			sql:     "-- +ptah statement_timeout=0s\nALTER TABLE users ADD COLUMN email TEXT;",
			wantErr: "must be greater than zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := parseMigrationTimeoutDirectives(tt.sql)
			if tt.wantErr != "" {
				c.Assert(err, qt.IsNotNil)
				c.Assert(err.Error(), qt.Contains, tt.wantErr)
				return
			}

			c.Assert(err, qt.IsNil)
			c.Assert(got.HasLockTimeout, qt.Equals, tt.wantHasLockTimeout)
			c.Assert(got.HasStatementTimeout, qt.Equals, tt.wantHasStatementTimeout)
			c.Assert(got.LockTimeout, qt.Equals, tt.wantLockTimeout)
			c.Assert(got.StatementTimeout, qt.Equals, tt.wantStatementTimeout)
		})
	}
}

func TestMigrationFuncFromSQLFilenameWithTimeouts(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"test.sql": &fstest.MapFile{
			Data: []byte("-- +ptah lock_timeout=3s\nCREATE TABLE test (id SERIAL PRIMARY KEY);"),
		},
	}

	migrationFunc, timeouts, err := MigrationFuncFromSQLFilenameWithTimeouts("test.sql", fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFunc, qt.IsNotNil)
	c.Assert(timeouts.HasLockTimeout, qt.IsTrue)
	c.Assert(timeouts.LockTimeout, qt.Equals, 3*time.Second)
}
