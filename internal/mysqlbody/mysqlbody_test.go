package mysqlbody_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlbody"
)

func TestStatementKind_NamesAnExecutableBody(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{name: "a trigger", statement: "CREATE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'", want: "TRIGGER"},
		{name: "a trigger with a definer", statement: "CREATE DEFINER=`app`@`%` TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'", want: "TRIGGER"},
		{name: "a trigger with the current user as definer", statement: "CREATE DEFINER = CURRENT_USER() TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'", want: "TRIGGER"},
		{name: "a replaced trigger", statement: "CREATE OR REPLACE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'", want: "TRIGGER"},
		{name: "a procedure", statement: "CREATE PROCEDURE p() BEGIN SELECT 1; END", want: "PROCEDURE"},
		{name: "a function", statement: "CREATE FUNCTION f() RETURNS INT DETERMINISTIC RETURN 1", want: "FUNCTION"},
		{name: "an aggregate function", statement: "CREATE AGGREGATE FUNCTION f(x INT) RETURNS INT BEGIN RETURN 1; END", want: "FUNCTION"},
		{name: "an event", statement: "CREATE EVENT e ON SCHEDULE EVERY 1 DAY DO SELECT 1", want: "EVENT"},
		{name: "an altered event", statement: "ALTER EVENT e DO SELECT 2", want: "EVENT"},
		{name: "an altered procedure", statement: "ALTER PROCEDURE p COMMENT 'x'", want: "PROCEDURE"},
		{name: "lower case", statement: "create trigger t_bi before insert on t for each row set new.code = 'x'", want: "TRIGGER"},
		{name: "a comment in front", statement: "/* trigger */ -- body\nCREATE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'", want: "TRIGGER"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mysqlbody.StatementKind(test.statement), qt.Equals, test.want)
		})
	}
}

func TestStatementKind_NamesNoKind(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "a table", statement: "CREATE TABLE t (id INT PRIMARY KEY)"},
		{name: "an altered table", statement: "ALTER TABLE t MODIFY code VARCHAR(5)"},
		{name: "a dropped trigger", statement: "DROP TRIGGER t_bi"},
		{name: "a view", statement: "CREATE VIEW v AS SELECT 1"},
		{name: "an insert naming a trigger", statement: "INSERT INTO t VALUES (1, 'CREATE TRIGGER')"},
		{name: "an executable comment", statement: "/*!50003 CREATE*/ /*!50003 TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x' */"},
		{name: "an empty statement", statement: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mysqlbody.StatementKind(test.statement), qt.Equals, "")
		})
	}
}
