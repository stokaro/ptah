package mysqlreplay_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

const remoteAccountTables = " that can forward writes to account tables"

// A table's connection can name an account table even when its own name is
// in the dev database. Trust and ownership do not make that destination safe.
func TestGuard_RemoteTable_FailurePath(t *testing.T) {
	tests := []struct {
		name, statement, operation string
	}{
		{name: "federated", statement: "CREATE TABLE t (id INT) ENGINE=FEDERATED", operation: "FEDERATED storage engine"},
		{name: "spider", statement: "CREATE TABLE t (id INT) ENGINE=SPIDER", operation: "SPIDER storage engine"},
		{name: "connect", statement: "CREATE TABLE t (id INT) ENGINE=CONNECT TABLE_TYPE=MYSQL", operation: "CONNECT storage engine"},
		{name: "alter", statement: "ALTER TABLE t ENGINE=FEDERATED", operation: "FEDERATED storage engine"},
		{name: "alter after column", statement: "ALTER TABLE t ADD COLUMN n INT, ENGINE=SPIDER", operation: "SPIDER storage engine"},
		{name: "qualified name", statement: "CREATE TABLE ptah_dev.t (id INT) ENGINE=CONNECT", operation: "CONNECT storage engine"},
		{name: "replace", statement: "CREATE OR REPLACE TABLE t (id INT) ENGINE=SPIDER", operation: "SPIDER storage engine"},
		{name: "if absent", statement: "CREATE TABLE IF NOT EXISTS t (id INT) ENGINE=FEDERATED", operation: "FEDERATED storage engine"},
		{name: "mixed case", statement: "create table t (id int) engine=FeDeRaTeD", operation: "FEDERATED storage engine"},
		{name: "without equals", statement: "CREATE TABLE t (id INT) ENGINE CONNECT", operation: "CONNECT storage engine"},
		{name: "quoted string", statement: "CREATE TABLE t (id INT) ENGINE='federated'", operation: "FEDERATED storage engine"},
		{name: "quoted identifier", statement: "CREATE TABLE t (id INT) ENGINE=`spider`", operation: "SPIDER storage engine"},
		{name: "double quoted", statement: `CREATE TABLE t (id INT) ENGINE="connect"`, operation: "CONNECT storage engine"},
		{name: "escaped engine", statement: `CREATE TABLE t (id INT) ENGINE='fed\erated'`, operation: "FEDERATED storage engine"},
		{name: "repeated option", statement: "CREATE TABLE t (id INT) ENGINE=InnoDB ENGINE=FEDERATED", operation: "FEDERATED storage engine"},
		{name: "comment between words", statement: "CREATE TABLE t (id INT) ENGINE /* local name */ = /* remote rows */ SPIDER", operation: "SPIDER storage engine"},
		{name: "connection", statement: "CREATE TABLE t (id INT) CONNECTION='mysql://dev@localhost/mysql/user'", operation: "CONNECTION table option"},
		{name: "connection without equals", statement: "ALTER TABLE t CONNECTION 'server/t'", operation: "CONNECTION table option"},
		{name: "empty connection", statement: "ALTER TABLE t CONNECTION=''", operation: "CONNECTION table option"},
		{name: "local engine with connection", statement: "CREATE TABLE t (id INT) ENGINE=InnoDB CONNECTION='server/t'", operation: "CONNECTION table option"},
		{name: "wrapped", statement: "SET STATEMENT max_statement_time=1 FOR CREATE TABLE t (id INT) ENGINE=FEDERATED", operation: "FEDERATED storage engine"},
		{name: "procedure", statement: "CREATE PROCEDURE p() CREATE TABLE t (id INT) ENGINE=SPIDER", operation: "SPIDER storage engine"},
		{name: "compound procedure", statement: "CREATE PROCEDURE p() BEGIN SELECT 1; ALTER TABLE t CONNECTION='server/t'; END", operation: "CONNECTION table option"},
		{name: "anonymous compound", statement: "BEGIN NOT ATOMIC CREATE TABLE t (id INT) ENGINE=SPIDER; END", operation: "SPIDER storage engine"},
		{name: "conditional compound", statement: "IF 1 THEN ALTER TABLE t CONNECTION='server/t'; END IF", operation: "CONNECTION table option"},
		{name: "partition engine", statement: "CREATE TABLE t (id INT) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (10) ENGINE=SPIDER)", operation: "SPIDER storage engine"},
		{name: "added partition", statement: "ALTER TABLE t ADD PARTITION (PARTITION p1 VALUES LESS THAN (20) ENGINE=SPIDER)", operation: "SPIDER storage engine"},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			for _, scope := range accountScopes {
				t.Run(dialect+"/"+test.name+"/"+scope.name, func(t *testing.T) {
					c := qt.New(t)
					guard := boundGuard(c, dialect, utf8Session(devAccount, devRoles))

					c.Assert(guard.Validate(test.statement, scope.scope), qt.ErrorMatches,
						refusal(dialect, test.operation+remoteAccountTables+confined))
				})
			}
		}
	}
}

// Names, comments, strings and a SELECT's expressions are not table options.
func TestGuard_RemoteTable_HappyPath(t *testing.T) {
	tests := []struct {
		name, statement string
	}{
		{name: "local engine", statement: "CREATE TABLE t (id INT) ENGINE=InnoDB"},
		{name: "local engine changed", statement: "ALTER TABLE t ENGINE=InnoDB"},
		{name: "table named engine", statement: "CREATE TABLE engine (id INT) ENGINE=InnoDB"},
		{name: "table named connection", statement: "CREATE TABLE connection (id INT)"},
		{name: "qualified table named engine", statement: "CREATE TABLE ptah_dev.engine (id INT)"},
		{name: "column names", statement: "CREATE TABLE t (engine INT, connection VARCHAR(20))"},
		{name: "column default", statement: "CREATE TABLE t (id INT, value VARCHAR(30) DEFAULT 'ENGINE=FEDERATED')"},
		{name: "table comment", statement: "CREATE TABLE t (id INT) COMMENT='ENGINE=FEDERATED CONNECTION=x'"},
		{name: "ordinary comment", statement: "CREATE TABLE t (id INT) /* ENGINE=FEDERATED CONNECTION=x */ ENGINE=InnoDB"},
		{name: "column added", statement: "ALTER TABLE t ADD COLUMN connection VARCHAR(30) DEFAULT 'remote'"},
		{name: "column renamed", statement: "ALTER TABLE t CHANGE COLUMN engine federated INT"},
		{name: "renamed without column keyword", statement: "ALTER TABLE t CHANGE engine federated INT, ENGINE=InnoDB"},
		{name: "select expression", statement: "CREATE TABLE t AS SELECT engine = federated AS connected FROM source"},
		{name: "partition expression", statement: "CREATE TABLE t (engine INT, federated INT) PARTITION BY HASH(engine = federated) PARTITIONS 2"},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			for _, scope := range accountScopes {
				t.Run(dialect+"/"+test.name+"/"+scope.name, func(t *testing.T) {
					c := qt.New(t)
					guard := boundGuard(c, dialect, utf8Session(devAccount, devRoles))

					c.Assert(guard.Validate(test.statement, scope.scope), qt.IsNil)
				})
			}
		}
	}
}
