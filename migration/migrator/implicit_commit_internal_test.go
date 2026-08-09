package migrator

// White-box testing required: this file verifies unexported MySQL-family safety
// classifiers, catalog-reference scans, and rollback-progress invariants that
// cannot be observed independently through the exported migration API.

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
		{name: "ANSI quoted create table", statement: `CREATE TABLE "jobs" (id BIGINT) ENGINE=MyISAM`, want: "MyISAM"},
		{name: "ANSI quoted alter table", statement: `ALTER TABLE "jobs" ENGINE=MyISAM`, want: "MyISAM"},
		{
			name:      "ANSI quoted qualified create table",
			statement: `CREATE TABLE "archive"."jobs" (id BIGINT) ENGINE=MyISAM`,
			want:      "MyISAM",
		},
		{
			name:      "ANSI quoted escaped table name",
			statement: `CREATE TABLE "jobs""audit" (id BIGINT) ENGINE=MyISAM`,
			want:      "MyISAM",
		},
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
		"CREATE DATABASE archive",
		"CREATE OR REPLACE SCHEMA archive",
		"ALTER DATABASE archive READ ONLY = 1",
		"DROP SCHEMA IF EXISTS archive",
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

func TestMySQLUnwitnessedFilesystemWrite_Present(t *testing.T) {
	c := qt.New(t)

	statements := []string{
		"SELECT id INTO OUTFILE '/tmp/jobs.csv' FROM jobs",
		"SELECT payload INTO DUMPFILE '/tmp/job.bin' FROM jobs LIMIT 1",
		"TABLE jobs INTO OUTFILE '/tmp/jobs.csv'",
	}

	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			got := mysqlUnwitnessedFilesystemWrite(significantSQLTokens(statement, "mysql"))
			c.Assert(got, qt.IsTrue)
		})
	}
}

func TestMySQLUnwitnessedFilesystemWrite_Absent(t *testing.T) {
	c := qt.New(t)

	statements := []string{
		"INSERT INTO outfile VALUES (1)",
		"SELECT 'INTO OUTFILE /tmp/jobs.csv'",
		"SELECT id INTO @job_id FROM jobs",
	}

	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			got := mysqlUnwitnessedFilesystemWrite(significantSQLTokens(statement, "mysql"))
			c.Assert(got, qt.IsFalse)
		})
	}
}

func TestMySQLExecutableComment_Present(t *testing.T) {
	c := qt.New(t)

	statements := []string{
		"/*! SET autocommit = 0 */",
		"/*!50699 SET autocommit = 0 */",
		"/*M! SET autocommit = 0 */",
		"SELECT 1 /*! + 1 */",
	}

	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			got := mysqlExecutableComment(significantSQLTokens(statement, "mariadb"))
			c.Assert(got, qt.IsTrue)
		})
	}
}

func TestMySQLExecutableComment_Absent(t *testing.T) {
	c := qt.New(t)

	statements := []string{
		"/* ordinary comment */ SELECT 1",
		"SELECT '/*! SET autocommit = 0 */'",
		"SELECT 1 /*+ MAX_EXECUTION_TIME(10) */",
	}

	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			got := mysqlExecutableComment(significantSQLTokens(statement, "mysql"))
			c.Assert(got, qt.IsFalse)
		})
	}
}

func TestMySQLOpaqueExecution_Present(t *testing.T) {
	c := qt.New(t)

	statements := []string{
		"CALL apply_changes()",
		"PREPARE stmt FROM @sql",
		"EXECUTE stmt",
		"EXECUTE IMMEDIATE @sql",
		"DEALLOCATE PREPARE stmt",
		"LOCK TABLES jobs WRITE",
		"UNLOCK TABLES",
	}

	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			got := mysqlOpaqueExecution(significantSQLTokens(statement, "mysql"))
			c.Assert(got, qt.IsTrue)
		})
	}
}

func TestMySQLOpaqueExecution_Absent(t *testing.T) {
	c := qt.New(t)

	statements := []string{
		"INSERT INTO jobs (id) VALUES (1)",
		"SET SESSION sql_mode = 'ANSI_QUOTES'",
		"CREATE TABLE jobs (id BIGINT) ENGINE=InnoDB",
	}

	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			got := mysqlOpaqueExecution(significantSQLTokens(statement, "mysql"))
			c.Assert(got, qt.IsFalse)
		})
	}
}

func TestMySQLDefinesIndirectWriter_Present(t *testing.T) {
	c := qt.New(t)

	statements := []string{
		"CREATE VIEW active_jobs AS SELECT * FROM jobs",
		"CREATE TRIGGER jobs_audit AFTER INSERT ON jobs FOR EACH ROW INSERT INTO audit VALUES (NEW.id)",
		"CREATE DEFINER = app PROCEDURE apply_jobs() INSERT INTO jobs VALUES (1)",
		"ALTER FUNCTION next_job_id COMMENT 'changed'",
		"CREATE EVENT cleanup_jobs DO DELETE FROM jobs",
	}

	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			got := mysqlDefinesIndirectWriter(significantSQLTokens(statement, "mysql"))
			c.Assert(got, qt.IsTrue)
		})
	}
}

func TestMySQLDefinesIndirectWriter_Absent(t *testing.T) {
	c := qt.New(t)

	statements := []string{
		"CREATE TABLE jobs (function_name VARCHAR(32)) ENGINE=InnoDB",
		"ALTER TABLE jobs ADD COLUMN event_id BIGINT",
		"DROP VIEW active_jobs",
	}

	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			got := mysqlDefinesIndirectWriter(significantSQLTokens(statement, "mysql"))
			c.Assert(got, qt.IsFalse)
		})
	}
}

func TestMySQLReferencedExternalSchema(t *testing.T) {
	c := qt.New(t)

	qualified, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("INSERT INTO `archive`.jobs VALUES (1)", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(qualified, qt.Equals, "archive")

	routine, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("INSERT INTO jobs VALUES (archive.next_job_id())", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(routine, qt.Equals, "archive")

	createIndex, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("CREATE INDEX jobs_created_at ON archive.jobs (created_at)", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(createIndex, qt.Equals, "archive")

	dropIndex, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("DROP INDEX jobs_created_at ON archive.jobs", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(dropIndex, qt.Equals, "archive")

	renameTarget, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("RENAME TABLE ptahtest.jobs TO archive.jobs", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(renameTarget, qt.Equals, "archive")

	renameListSource, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens(
			"RENAME TABLE ptahtest.jobs TO ptahtest.old_jobs, archive.pending_jobs TO ptahtest.jobs",
			"mysql",
		),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(renameListSource, qt.Equals, "archive")

	alterRenameTarget, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("ALTER TABLE ptahtest.jobs RENAME TO archive.jobs", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(alterRenameTarget, qt.Equals, "archive")

	dropView, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("DROP VIEW ptahtest.active_jobs, archive.active_jobs", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(dropView, qt.Equals, "archive")

	dropFunction, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("DROP FUNCTION IF EXISTS archive.next_job_id", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(dropFunction, qt.Equals, "archive")

	foreignKey, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens(
			"CREATE TABLE child (parent_id BIGINT, FOREIGN KEY (parent_id) REFERENCES archive.parent (id))",
			"mysql",
		),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(foreignKey, qt.Equals, "archive")

	grantTable, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("GRANT SELECT ON archive.jobs TO app", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(grantTable, qt.Equals, "archive")

	grantDatabase, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("GRANT TRIGGER ON archive.* TO app", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(grantDatabase, qt.Equals, "archive")

	quoted, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens(`INSERT INTO "archive"."jobs" VALUES (1)`, "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(quoted, qt.Equals, "archive")

	createIfNotExists, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("CREATE TABLE IF NOT EXISTS archive.jobs (id BIGINT)", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(createIfNotExists, qt.Equals, "archive")

	dropIfExists, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("DROP TABLE IF EXISTS archive.jobs", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(dropIfExists, qt.Equals, "archive")

	truncate, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("TRUNCATE archive.jobs", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(truncate, qt.Equals, "archive")

	grantFunction, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("GRANT EXECUTE ON FUNCTION archive.next_job_id TO app", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(grantFunction, qt.Equals, "archive")

	grantProcedure, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("GRANT EXECUTE ON PROCEDURE archive.apply_jobs TO app", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(grantProcedure, qt.Equals, "archive")

	selected, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("INSERT INTO ptahtest.jobs VALUES (1)", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsFalse)
	c.Assert(selected, qt.Equals, "")

	selectedQuoted, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens(`INSERT INTO "ptahtest"."jobs" VALUES (1)`, "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsFalse)
	c.Assert(selectedQuoted, qt.Equals, "")

	selectedForeignKey, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens(
			"CREATE TABLE child (parent_id BIGINT, FOREIGN KEY (parent_id) REFERENCES ptahtest.parent (id))",
			"mysql",
		),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsFalse)
	c.Assert(selectedForeignKey, qt.Equals, "")

	selectedGrant, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("GRANT TRIGGER ON ptahtest.* TO app", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsFalse)
	c.Assert(selectedGrant, qt.Equals, "")

	caseVariant, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("INSERT INTO PTAHTEST.jobs VALUES (1)", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsTrue)
	c.Assert(caseVariant, qt.Equals, "PTAHTEST")

	unqualified, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("SELECT archive FROM jobs", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsFalse)
	c.Assert(unqualified, qt.Equals, "")

	alias, referenced := mysqlReferencedExternalSchema(
		significantSQLTokens("SELECT archive.id FROM jobs AS archive", "mysql"),
		"ptahtest",
	)
	c.Assert(referenced, qt.IsFalse)
	c.Assert(alias, qt.Equals, "")
}

func TestMySQLGrantsProvideTriggerCatalogVisibility_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		grants []string
		schema string
	}{
		{
			name:   "database trigger",
			grants: []string{"GRANT SELECT, INSERT, TRIGGER ON `ptahtest`.* TO `ptah`@`%`"},
			schema: "ptahtest",
		},
		{
			name:   "escaped wildcard in database name",
			grants: []string{"GRANT ALL PRIVILEGES ON `ptah\\\\_test`.* TO `ptah`@`%`"},
			schema: "ptah_test",
		},
		{
			name:   "global all privileges",
			grants: []string{"GRANT ALL PRIVILEGES ON *.* TO `ptah`@`%`"},
			schema: "ptahtest",
		},
		{
			name:   "active role expanded by server",
			grants: []string{"GRANT TRIGGER ON `ptahtest`.* TO `ptah_trigger_role`"},
			schema: "ptahtest",
		},
		{
			name:   "ANSI quotes session",
			grants: []string{`GRANT ALL PRIVILEGES ON "ptahtest".* TO "ptah"@"%"`},
			schema: "ptahtest",
		},
		{
			name:   "ANSI quotes escaped wildcard in database name",
			grants: []string{`GRANT ALL PRIVILEGES ON "ptah\\_test".* TO "ptah"@"%"`},
			schema: "ptah_test",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			visible := mysqlGrantsProvideTriggerCatalogVisibility(test.grants, test.schema, "mysql")
			c.Assert(visible, qt.IsTrue)
		})
	}
}

func TestMySQLGrantsProvideTriggerCatalogVisibility_FailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		grants []string
	}{
		{
			name:   "no trigger privilege",
			grants: []string{"GRANT SELECT, INSERT ON `ptahtest`.* TO `ptah`@`%`"},
		},
		{
			name:   "different database",
			grants: []string{"GRANT TRIGGER ON `archive`.* TO `ptah`@`%`"},
		},
		{
			name:   "table scope is incomplete",
			grants: []string{"GRANT TRIGGER ON `ptahtest`.`jobs` TO `ptah`@`%`"},
		},
		{
			name: "partial revoke",
			grants: []string{
				"GRANT ALL PRIVILEGES ON *.* TO `ptah`@`%`",
				"REVOKE TRIGGER ON `ptahtest`.* FROM `ptah`@`%`",
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			visible := mysqlGrantsProvideTriggerCatalogVisibility(test.grants, "ptahtest", "mysql")
			c.Assert(visible, qt.IsFalse)
		})
	}

	c.Run("unknown grant escape fails closed", func(c *qt.C) {
		visible := mysqlGrantsProvideTriggerCatalogVisibility(
			[]string{"GRANT TRIGGER ON `ptah\\qtest`.* TO `ptah`@`%`"},
			"ptahqtest",
			"mysql",
		)
		c.Assert(visible, qt.IsFalse)
	})

	c.Run("database wildcard grant fails closed", func(c *qt.C) {
		visible := mysqlGrantsProvideTriggerCatalogVisibility(
			[]string{"GRANT TRIGGER ON `ptah%`.* TO `ptah`@`%`"},
			"ptahtest",
			"mysql",
		)
		c.Assert(visible, qt.IsFalse)
	})

	c.Run("escaped database revoke overrides global grant", func(c *qt.C) {
		visible := mysqlGrantsProvideTriggerCatalogVisibility(
			[]string{
				"GRANT ALL PRIVILEGES ON *.* TO `ptah`@`%`",
				"REVOKE TRIGGER ON `ptah\\\\_test`.* FROM `ptah`@`%`",
			},
			"ptah_test",
			"mysql",
		)
		c.Assert(visible, qt.IsFalse)
	})
}

// TestMySQLGrantsProvideTriggerCatalogVisibility_SchemaPattern pins the
// database of a schema-level privilege as the pattern the server stores rather
// than as a name. Every schema here contains an underscore, which the cases
// above never do, and that is the whole point: `SHOW GRANTS` prints the
// escaped `ptah\_test` for the privilege the official MySQL and MariaDB images
// create for MYSQL_USER on `ptah_test`, and a name comparison both refuses a
// user who holds TRIGGER and misses the REVOKE that took it away.
func TestMySQLGrantsProvideTriggerCatalogVisibility_SchemaPattern(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		schema string
		grants []string
		want   bool
	}{
		{
			name:   "escaped underscore covers the database",
			schema: "ptah_test",
			grants: []string{"GRANT ALL PRIVILEGES ON `ptah\\_test`.* TO `ptah`@`%`"},
			want:   true,
		},
		{
			name:   "literal underscore covers the database",
			schema: "ptah_test",
			grants: []string{"GRANT ALL PRIVILEGES ON `ptah_test`.* TO `ptah`@`%`"},
			want:   true,
		},
		{
			name:   "trailing wildcard covers the database",
			schema: "ptah_test",
			grants: []string{"GRANT TRIGGER ON `ptah%`.* TO `ptah`@`%`"},
			want:   true,
		},
		{
			name:   "unescaped underscore matches any single character",
			schema: "ptahXtest",
			grants: []string{"GRANT TRIGGER ON `ptah_test`.* TO `ptah`@`%`"},
			want:   true,
		},
		{
			name:   "escaped percent is not a wildcard",
			schema: "ptah_test",
			grants: []string{"GRANT TRIGGER ON `ptah\\%test`.* TO `ptah`@`%`"},
			want:   false,
		},
		{
			name:   "escaped underscore does not reach another database",
			schema: "ptah_test",
			grants: []string{"GRANT TRIGGER ON `archive\\_test`.* TO `ptah`@`%`"},
			want:   false,
		},
		{
			name:   "escaped revoke takes the privilege away",
			schema: "ptah_test",
			grants: []string{
				"GRANT ALL PRIVILEGES ON *.* TO `ptah`@`%`",
				"REVOKE TRIGGER ON `ptah\\_test`.* FROM `ptah`@`%`",
			},
			want: false,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			visible := mysqlGrantsProvideTriggerCatalogVisibility(test.grants, test.schema, "mysql")
			c.Assert(visible, qt.Equals, test.want)
		})
	}
}

func TestMySQLReferencedCatalogName(t *testing.T) {
	c := qt.New(t)
	names := map[string]struct{}{"active_jobs": {}}

	statements := []string{
		"INSERT INTO active_jobs VALUES (1)",
		`INSERT INTO "active_jobs" VALUES (1)`,
		"INSERT LOW_PRIORITY active_jobs VALUES (1)",
		"UPDATE IGNORE active_jobs SET id = 1",
		"WITH pending AS (SELECT 1) UPDATE IGNORE active_jobs SET id = 1",
		"SELECT * FROM active_jobs",
		"SELECT * FROM jobs JOIN active_jobs ON active_jobs.id = jobs.id",
		"SELECT * FROM jobs, active_jobs",
		"SELECT * FROM ptahtest.active_jobs",
	}
	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			relation, referenced := mysqlReferencedCatalogName(significantSQLTokens(statement, "mysql"), names)
			c.Assert(referenced, qt.IsTrue)
			c.Assert(relation, qt.Equals, "active_jobs")
		})
	}
}

func TestMySQLReferencedCatalogName_IgnoresNonRelationIdentifiers(t *testing.T) {
	c := qt.New(t)
	names := map[string]struct{}{"active_jobs": {}}

	statements := []string{
		"SELECT active_jobs FROM jobs",
		"INSERT INTO jobs (active_jobs) VALUES (1)",
		"UPDATE jobs SET active_jobs = 1",
		"CREATE TABLE jobs (active_jobs INT)",
	}
	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			relation, referenced := mysqlReferencedCatalogName(significantSQLTokens(statement, "mysql"), names)
			c.Assert(referenced, qt.IsFalse)
			c.Assert(relation, qt.Equals, "")
		})
	}
}

func TestMySQLInvokedRoutine(t *testing.T) {
	c := qt.New(t)
	routines := map[string]struct{}{"next_job_id": {}}

	invoked, found := mysqlInvokedRoutine(
		significantSQLTokens("INSERT INTO jobs VALUES (next_job_id())", "mysql"),
		routines,
	)
	c.Assert(found, qt.IsTrue)
	c.Assert(invoked, qt.Equals, "next_job_id")

	quoted, found := mysqlInvokedRoutine(
		significantSQLTokens(`INSERT INTO jobs VALUES ("next_job_id"())`, "mysql"),
		routines,
	)
	c.Assert(found, qt.IsTrue)
	c.Assert(quoted, qt.Equals, "next_job_id")

	identifier, found := mysqlInvokedRoutine(
		significantSQLTokens("SELECT next_job_id FROM jobs", "mysql"),
		routines,
	)
	c.Assert(found, qt.IsFalse)
	c.Assert(identifier, qt.Equals, "")
}

func TestMigrationHasSQLExecutor(t *testing.T) {
	c := qt.New(t)
	sqlMigration := CreateMigrationFromSQL(1, "sql", "SELECT 1", "SELECT 1")
	opaqueMigration := &Migration{Up: NoopMigrationFunc, Down: NoopMigrationFunc}

	c.Assert(sqlMigration.hasSQLExecutor(MigrationDirectionUp), qt.IsTrue)
	c.Assert(sqlMigration.hasSQLExecutor(MigrationDirectionDown), qt.IsTrue)
	c.Assert(opaqueMigration.hasSQLExecutor(MigrationDirectionUp), qt.IsFalse)
	c.Assert(opaqueMigration.hasSQLExecutor(MigrationDirectionDown), qt.IsFalse)
}

func TestMigrationHasStatementInterceptor(t *testing.T) {
	c := qt.New(t)
	migration := &Migration{upHasStatementInterceptor: true}

	c.Assert(migration.hasStatementInterceptor(MigrationDirectionUp), qt.IsTrue)
	c.Assert(migration.hasStatementInterceptor(MigrationDirectionDown), qt.IsFalse)
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

func TestPreservesProgressWitnessUnknownOutcome(t *testing.T) {
	c := qt.New(t)

	c.Assert(
		preservesProgressWitnessUnknownOutcome(
			&MigrationExecutionError{Statement: "CALL apply_changes()"},
			&MigrationRevision{Error: unknownStatementOutcomeError},
			"mysql",
		),
		qt.IsTrue,
	)
	c.Assert(
		preservesProgressWitnessUnknownOutcome(
			&MigrationExecutionError{Statement: "INSERT INTO jobs VALUES (1)"},
			&MigrationRevision{Error: unknownStatementOutcomeError},
			"mysql",
		),
		qt.IsFalse,
	)
	c.Assert(
		preservesProgressWitnessUnknownOutcome(
			&MigrationExecutionError{Statement: "INSERT INTO jobs VALUES (1)"},
			&MigrationRevision{Error: "statement failed"},
			"mysql",
		),
		qt.IsFalse,
	)
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
