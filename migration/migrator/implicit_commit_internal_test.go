package migrator

// White-box testing required: the error-only rollback fallback is unexported.
// Live MySQL and MariaDB tests cover the transactional progress witness that
// replaces this fallback when implicit commits are possible.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestMySQLStorageEngineSelection_Selected(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{name: "create table", statement: "CREATE TABLE jobs (id BIGINT) ENGINE=MyISAM", want: "MyISAM"},
		{name: "alter table", statement: "ALTER TABLE jobs ENGINE = InnoDB", want: "InnoDB"},
		{name: "alter table without equals", statement: "ALTER TABLE jobs ENGINE MyISAM", want: "MyISAM"},
		{name: "qualified create table", statement: "CREATE TABLE archive.jobs (id BIGINT) ENGINE=MyISAM", want: "MyISAM"},
		{name: "MariaDB replace table", statement: "CREATE OR REPLACE TABLE jobs (id BIGINT) ENGINE=MyISAM", want: "MyISAM"},
		{name: "session default", statement: "SET SESSION default_storage_engine = 'MyISAM'", want: "MyISAM"},
		{name: "qualified session default", statement: "SET @@SESSION.default_storage_engine = MyISAM", want: "MyISAM"},
		{name: "legacy default", statement: "SET storage_engine=DEFAULT", want: "DEFAULT"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, selected := mysqlStorageEngineSelection(test.statement, "mysql")
			c.Assert(selected, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestMySQLUnwitnessedStateChange_Present(t *testing.T) {
	c := qt.New(t)

	statements := []string{
		"RESET PERSIST",
		"RESET CONNECTION",
		"SET GLOBAL sql_mode = 'ANSI_QUOTES'",
		"SET @@GLOBAL.sql_mode = 'ANSI_QUOTES'",
		"SET PERSIST max_connections = 200",
		"SET PASSWORD = 'secret'",
		"SET DEFAULT ROLE ALL TO app",
		"SET RESOURCE GROUP batch",
		"SET STATEMENT max_statement_time=1 FOR SELECT 1",
	}

	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			got := mysqlUnwitnessedStateChange(significantSQLTokens(statement, "mysql"))
			c.Assert(got, qt.IsTrue)
		})
	}
}

func TestMySQLUnwitnessedStateChange_Absent(t *testing.T) {
	c := qt.New(t)

	statements := []string{
		"SET SESSION sql_mode = 'ANSI_QUOTES'",
		"SET @@SESSION.sql_mode = 'ANSI_QUOTES'",
		"SET @migration_tenant = 7",
		"SET NAMES utf8mb4",
		"SET NAMES DEFAULT",
		"SET CHARACTER SET utf8mb4",
		"SET CHARACTER SET DEFAULT",
		"SET SESSION sql_mode = 'GLOBAL'",
		"SET ROLE app_writer",
		"INSERT INTO jobs (id) VALUES (1)",
	}

	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			got := mysqlUnwitnessedStateChange(significantSQLTokens(statement, "mysql"))
			c.Assert(got, qt.IsFalse)
		})
	}
}

func TestMySQLCreateTableLike_Present(t *testing.T) {
	c := qt.New(t)

	statements := []string{
		"CREATE TABLE jobs_archive LIKE archive.jobs",
		"CREATE TEMPORARY TABLE jobs_copy LIKE jobs",
		"CREATE OR REPLACE TABLE jobs_copy LIKE jobs",
	}

	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			got := mysqlCreateTableLike(significantSQLTokens(statement, "mysql"))
			c.Assert(got, qt.IsTrue)
		})
	}
}

func TestMySQLCreateTableLike_Absent(t *testing.T) {
	c := qt.New(t)

	statements := []string{
		"CREATE TABLE jobs (id BIGINT) ENGINE=InnoDB",
		"CREATE TABLE jobs (note VARCHAR(32) CHECK (note LIKE 'ok%'))",
		"CREATE VIEW jobs_like AS SELECT * FROM jobs",
	}

	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			got := mysqlCreateTableLike(significantSQLTokens(statement, "mysql"))
			c.Assert(got, qt.IsFalse)
		})
	}
}

func TestMySQLStorageEngineSelection_Absent(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		statement string
	}{
		{name: "column name", statement: "CREATE TABLE jobs (engine VARCHAR(32), id BIGINT)"},
		{name: "alter column name", statement: "ALTER TABLE jobs ADD COLUMN engine VARCHAR(32)"},
		{name: "select alias", statement: "CREATE TABLE jobs AS SELECT engine FROM source_jobs"},
		{name: "setting value", statement: "SET SESSION note = default_storage_engine"},
		{name: "unrelated variable name", statement: "SET SESSION ptah_storage_engine_note = 'MyISAM'"},
		{name: "unrelated setting", statement: "SET SESSION sql_mode = 'ANSI_QUOTES'"},
		{name: "insert", statement: "INSERT INTO jobs (id) VALUES (1)"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, selected := mysqlStorageEngineSelection(test.statement, "mysql")
			c.Assert(selected, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestRolledBackApplied pins the error-only fallback. Transactional failures
// report zero without database evidence; the MySQL-family integration path
// replaces that value with the committed revision-row witness.
func TestRolledBackApplied(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		dialect  string
		txMode   MigrationTxMode
		executed int
		want     int
	}{
		{
			name:     "MySQL file mode drops a rolled-back DML prefix",
			dialect:  "mysql",
			txMode:   MigrationTxModeFile,
			executed: 1,
			want:     0,
		},
		{
			name:     "MariaDB file mode drops a rolled-back DML prefix",
			dialect:  "mariadb",
			txMode:   MigrationTxModeFile,
			executed: 1,
			want:     0,
		},
		{
			name:     "MySQL file mode does not infer a DDL prefix",
			dialect:  "mysql",
			txMode:   MigrationTxModeFile,
			executed: 2,
			want:     0,
		},
		{
			name:     "MySQL all mode drops a rolled-back DML prefix",
			dialect:  "mysql",
			txMode:   MigrationTxModeAll,
			executed: 2,
			want:     0,
		},
		{
			name:     "MySQL no-transaction mode keeps every committed statement",
			dialect:  "mysql",
			txMode:   MigrationTxModeNone,
			executed: 2,
			want:     2,
		},
		{
			name:     "PostgreSQL file mode still drops the whole body",
			dialect:  "postgres",
			txMode:   MigrationTxModeFile,
			executed: 2,
			want:     0,
		},
		{
			name:     "SQLite file mode still drops the whole body",
			dialect:  "sqlite",
			txMode:   MigrationTxModeFile,
			executed: 2,
			want:     0,
		},
		{
			name:     "PostgreSQL no-transaction mode keeps every committed statement",
			dialect:  "postgres",
			txMode:   MigrationTxModeNone,
			executed: 2,
			want:     2,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got := rolledBackApplied(tt.dialect, tt.txMode, tt.executed)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

// TestMigrationExecutionProgress_RolledBackMySQLBody walks the same correction
// through the error the executor actually raises, so the wiring between the
// failure and the recorded counter is covered and not only the helper.
func TestMigrationExecutionProgress_RolledBackMySQLBody(t *testing.T) {
	c := qt.New(t)

	failure := &MigrationExecutionError{
		Statement:      "INSERT INTO ledger (id) SELECT 2 FROM blocker",
		StatementIndex: 2,
		Total:          3,
	}

	progress := migrationExecutionProgress(failure, "mysql", MigrationTxModeFile)

	c.Assert(progress.Applied, qt.Equals, 0)
	c.Assert(progress.Total, qt.Equals, 3)
	c.Assert(progress.FailedIndex, qt.Equals, 2)
}
