package mysqlreplay_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// bodyStatements each write a body that holds a statement the rule refuses,
// and the refusal the statement that writes the body is given: a server the
// run owns lifts a body, and the body would run the statement unread.
var bodyStatements = []struct {
	name, statement, operation string
}{
	{name: "procedure of one statement", statement: "CREATE PROCEDURE p() DROP USER CURRENT_USER", operation: "DROP USER" + reaches},
	{name: "procedure with characteristics", statement: "CREATE PROCEDURE p() COMMENT 'x' MODIFIES SQL DATA SQL SECURITY INVOKER DROP USER 'dev'", operation: "DROP USER" + reaches},
	{name: "procedure with parameters", statement: "CREATE PROCEDURE p(IN n DECIMAL(10, 2)) NOT DETERMINISTIC REVOKE ALL ON *.* FROM dev", operation: "REVOKE" + reaches},
	{name: "procedure if new", statement: "CREATE PROCEDURE IF NOT EXISTS ptah_dev.p() DROP ROLE reader", operation: "DROP ROLE" + reaches},
	{name: "compound procedure", statement: "CREATE PROCEDURE p() BEGIN SELECT 1; REVOKE ALL ON *.* FROM 'dev'@'%'; END", operation: "REVOKE" + reaches},
	{name: "procedure of the mysql schema", statement: "CREATE PROCEDURE mysql.p() DELETE FROM user", operation: `write to account table "mysql.user"` + confined},
	{name: "IF and ELSE", statement: "CREATE PROCEDURE p() BEGIN IF 1 THEN SELECT 1; ELSE SET PASSWORD = 'x'; END IF; END", operation: "SET PASSWORD" + confined},
	{name: "CASE", statement: "CREATE PROCEDURE p() CASE 1 WHEN 1 THEN DROP USER CURRENT_USER; END CASE", operation: "DROP USER" + reaches},
	{name: "labeled LOOP", statement: "CREATE PROCEDURE p() l: LOOP ALTER USER CURRENT_USER ACCOUNT LOCK; END LOOP", operation: "ALTER USER" + confined},
	{name: "WHILE", statement: "CREATE PROCEDURE p() WHILE 1 DO DROP ROLE nested; END WHILE", operation: "DROP ROLE" + reaches},
	{name: "REPEAT", statement: "CREATE PROCEDURE p() REPEAT DROP USER dev; UNTIL 1 END REPEAT", operation: "DROP USER" + reaches},
	{name: "BEGIN NOT ATOMIC", statement: "CREATE PROCEDURE p() BEGIN NOT ATOMIC RENAME USER CURRENT_USER TO 'moved'@'%'; END", operation: "RENAME USER" + confined},
	{name: "handler", statement: "CREATE PROCEDURE p() BEGIN DECLARE EXIT HANDLER FOR SQLSTATE VALUE '42000', NOT FOUND, 1062 DROP USER CURRENT_USER; SELECT 1; END", operation: "DROP USER" + reaches},
	{name: "SET STATEMENT", statement: "CREATE PROCEDURE p() BEGIN SET STATEMENT max_statement_time = 1 FOR DROP USER CURRENT_USER; END", operation: "DROP USER" + reaches},
	{name: "function", statement: "CREATE FUNCTION f() RETURNS INT BEGIN DELETE FROM mysql.user; RETURN 1; END", operation: `write to account table "mysql.user"` + confined},
	{name: "trigger", statement: "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW UPDATE mysql.user SET account_locked = 'Y'", operation: `write to account table "mysql.user"` + confined},
	{name: "trigger after another", statement: "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW FOLLOWS other DELETE FROM mysql.db", operation: `write to account table "mysql.db"` + confined},
	{name: "event", statement: "CREATE EVENT e ON SCHEDULE EVERY 1 DAY DO DROP USER CURRENT_USER", operation: "DROP USER" + reaches},
	{name: "event body replaced", statement: "ALTER EVENT e DO BEGIN REVOKE ALL ON *.* FROM CURRENT_USER; END", operation: "REVOKE" + reaches},
}

// TestGuard_Body_FailurePath pins that the rule reads the statements of a
// static body, in every realm and under trust.
func TestGuard_Body_FailurePath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range bodyStatements {
			for _, scope := range accountScopes {
				t.Run(dialect+"/"+test.name+"/"+scope.name, func(t *testing.T) {
					c := qt.New(t)
					guard := boundGuard(c, dialect, utf8Session(devAccount, devRoles))

					c.Assert(guard.Validate(test.statement, scope.scope), qt.ErrorMatches, refusal(dialect, test.operation))
				})
			}
		}
	}
}

// bodyControls write a body whose statements reach no account the session
// runs as, and words the rule reads as a statement elsewhere: a SET in an
// UPDATE or a trigger's assignment, and THEN and ELSE in a CASE expression.
// A server the run owns and trust lift each, as they do without the rule.
var bodyControls = []struct {
	name, statement string
}{
	{name: "procedure dropping another account", statement: "CREATE PROCEDURE p() DROP USER 'other'@'%'"},
	{name: "procedure updating a password column", statement: "CREATE PROCEDURE p() BEGIN UPDATE t SET password = 'x'; END"},
	{name: "trigger setting a password column", statement: "CREATE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW SET NEW.password = 'x'"},
	{name: "function with a CASE expression", statement: "CREATE FUNCTION f() RETURNS INT RETURN (SELECT CASE WHEN 1 THEN 1 ELSE 2 END)"},
	{name: "procedure writing a table of the dev database", statement: "CREATE PROCEDURE p() DELETE FROM user"},
	{name: "procedure granting to the account", statement: "CREATE PROCEDURE p() GRANT SELECT ON ptah_dev.* TO CURRENT_USER"},
}

func TestGuard_Body_HappyPath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range bodyControls {
			for _, scope := range liftingScopes {
				t.Run(dialect+"/"+test.name+"/"+scope.name, func(t *testing.T) {
					c := qt.New(t)
					guard := boundGuard(c, dialect, utf8Session(devAccount, devRoles))

					c.Assert(guard.Validate(test.statement, scope.scope), qt.IsNil)
				})
			}
		}
	}
}
