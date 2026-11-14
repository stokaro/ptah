package txcontrol_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/txcontrol"
)

func TestControls_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		dialect   string
	}{
		{name: "PostgreSQL begin", statement: "BEGIN", dialect: "postgres"},
		{name: "PostgreSQL begin transaction", statement: "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE", dialect: "postgres"},
		{name: "PostgreSQL start transaction", statement: "START TRANSACTION", dialect: "postgres"},
		{name: "PostgreSQL commit", statement: "COMMIT", dialect: "postgres"},
		{name: "PostgreSQL rollback", statement: "ROLLBACK", dialect: "postgres"},
		{name: "PostgreSQL abort", statement: "ABORT", dialect: "postgres"},
		{name: "PostgreSQL end", statement: "END", dialect: "postgres"},
		{name: "PostgreSQL savepoint", statement: "SAVEPOINT ptah_save", dialect: "postgres"},
		{name: "PostgreSQL release savepoint", statement: "RELEASE SAVEPOINT ptah_save", dialect: "postgres"},
		{name: "PostgreSQL rollback to savepoint", statement: "ROLLBACK TO SAVEPOINT ptah_save", dialect: "postgres"},
		{name: "PostgreSQL set transaction", statement: "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", dialect: "postgres"},
		{name: "PostgreSQL prepare transaction", statement: "PREPARE TRANSACTION 'ptah_tx'", dialect: "postgres"},
		{name: "lowercase after a comment", statement: "-- open\n  begin", dialect: "postgres"},
		{name: "SQLite begin", statement: "BEGIN", dialect: "sqlite"},
		{name: "SQLite begin immediate", statement: "BEGIN IMMEDIATE TRANSACTION", dialect: "sqlite"},
		{name: "SQLite commit", statement: "COMMIT", dialect: "sqlite"},
		{name: "SQLite end", statement: "END TRANSACTION", dialect: "sqlite"},
		{name: "MySQL start transaction", statement: "START TRANSACTION", dialect: "mysql"},
		{name: "MySQL begin", statement: "BEGIN", dialect: "mysql"},
		{name: "MySQL commit and chain", statement: "COMMIT AND CHAIN", dialect: "mysql"},
		{name: "MySQL rollback and chain", statement: "ROLLBACK AND CHAIN", dialect: "mysql"},
		{name: "MySQL commit and no chain release", statement: "COMMIT AND NO CHAIN RELEASE", dialect: "mysql"},
		{name: "MySQL savepoint", statement: "SAVEPOINT ptah_save", dialect: "mysql"},
		{name: "MySQL release savepoint", statement: "RELEASE SAVEPOINT ptah_save", dialect: "mysql"},
		{name: "MySQL rollback to savepoint", statement: "ROLLBACK TO SAVEPOINT ptah_save", dialect: "mysql"},
		{name: "MySQL autocommit zero", statement: "SET autocommit = 0", dialect: "mysql"},
		{name: "MySQL XA start", statement: "XA START 'ptah_tx'", dialect: "mysql"},
		{name: "MariaDB session autocommit", statement: "SET @@session.autocommit = 1", dialect: "mariadb"},
		{name: "MariaDB begin", statement: "BEGIN", dialect: "mariadb"},
		{name: "MariaDB rollback and chain", statement: "ROLLBACK AND CHAIN", dialect: "mariadb"},
		{name: "SQL Server begin tran", statement: "BEGIN TRAN", dialect: "sqlserver"},
		{name: "SQL Server begin transaction", statement: "BEGIN TRANSACTION", dialect: "sqlserver"},
		{name: "SQL Server begin distributed", statement: "BEGIN DISTRIBUTED TRANSACTION", dialect: "sqlserver"},
		{name: "SQL Server save transaction", statement: "SAVE TRANSACTION ptah_save", dialect: "sqlserver"},
		{name: "SQL Server commit", statement: "COMMIT TRAN", dialect: "sqlserver"},
		{name: "SQL Server implicit transactions", statement: "SET IMPLICIT_TRANSACTIONS ON", dialect: "sqlserver"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(txcontrol.Controls(test.statement, test.dialect), qt.IsTrue)
		})
	}
}

func TestControls_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		dialect   string
	}{
		{name: "empty", statement: "", dialect: "postgres"},
		{name: "comment only", statement: "-- BEGIN", dialect: "sqlite"},
		{name: "create table", statement: "CREATE TABLE users (id BIGINT)", dialect: "postgres"},
		{name: "insert naming the word", statement: "INSERT INTO t VALUES ('BEGIN')", dialect: "sqlite"},
		{name: "table named begin", statement: "SELECT * FROM begin", dialect: "sqlite"},
		{name: "PostgreSQL search path", statement: "SET search_path = app, public", dialect: "postgres"},
		{name: "PostgreSQL do block", statement: "DO $$ BEGIN NULL; END $$", dialect: "postgres"},
		{name: "MySQL set names", statement: "SET NAMES utf8mb4", dialect: "mysql"},
		{name: "PostgreSQL autocommit is not a switch", statement: "SET autocommit = 0", dialect: "postgres"},
		{name: "SQL Server begin try", statement: "BEGIN TRY", dialect: "sqlserver"},
		{name: "SQL Server end try", statement: "END TRY", dialect: "sqlserver"},
		{name: "SQL Server end of a block", statement: "END", dialect: "sqlserver"},
		{name: "SQL Server block begin", statement: "BEGIN", dialect: "sqlserver"},
		{name: "SQL Server procedure body", statement: "CREATE PROCEDURE p AS BEGIN SELECT 1; END", dialect: "sqlserver"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(txcontrol.Controls(test.statement, test.dialect), qt.IsFalse)
		})
	}
}
