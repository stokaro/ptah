//go:build integration

package grantoptions_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
	"ptah.run/internal/dbtarget"
)

// mysqlOptionDatabase holds table and column privileges beside a grant on
// another table. The role and database belong only to this test.
func mysqlOptionDatabase(c *qt.C, engine dbtarget.Engine, held []string) database {
	c.Helper()
	db := scratch(c, engine, "DROP DATABASE {name}")
	db.role = uniqueName(c)
	db.exec(c, []string{"CREATE ROLE " + db.role})
	c.Cleanup(func() {
		_, err := db.conn.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+db.role)
		c.Check(err, qt.IsNil)
	})
	db.exec(c, []string{
		"CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50))",
		"CREATE TABLE t2 (id INT PRIMARY KEY)",
		"GRANT SELECT ON t2 TO " + db.role + " WITH GRANT OPTION",
	})
	for _, statement := range held {
		db.exec(c, []string{strings.ReplaceAll(statement, "{role}", db.role)})
	}
	return db
}

// mysqlOptionRows reads base privileges and their options at both scopes.
// The complete role account distinguishes MySQL's '%' host from MariaDB's
// empty role host. A grant-option-only USAGE row is not a base privilege.
func mysqlOptionRows(c *qt.C, db database, host string) []string {
	c.Helper()
	return grantRows(c, db.conn, `
		SELECT CONCAT('table ', TABLE_NAME, ' ', PRIVILEGE_TYPE, ' ', IS_GRANTABLE) AS privilege
		FROM information_schema.TABLE_PRIVILEGES
		WHERE TABLE_SCHEMA = DATABASE() AND GRANTEE = CONCAT('''', ?, '''@''', ?, '''')
		  AND PRIVILEGE_TYPE <> 'USAGE'
		UNION ALL
		SELECT CONCAT('column ', TABLE_NAME, '.', COLUMN_NAME, ' ', PRIVILEGE_TYPE, ' ', IS_GRANTABLE)
		FROM information_schema.COLUMN_PRIVILEGES
		WHERE TABLE_SCHEMA = DATABASE() AND GRANTEE = CONCAT('''', ?, '''@''', ?, '''')
		ORDER BY privilege`, db.role, host, db.role, host)
}

// The native renderer takes one object-wide option only after all grants on
// that object. MySQL rejects a second revoke; both engines must keep every
// declared base privilege and leave another table's option untouched.
func TestSchemaRenderMySQLFamilyTakesOneObjectGrantOptionE2E(t *testing.T) {
	for _, engine := range []struct {
		dialect, host string
		admin         dbtarget.Engine
	}{
		{dialect: "mysql", host: "%", admin: dbtarget.MySQLAdmin},
		{dialect: "mariadb", host: "", admin: dbtarget.MariaDBAdmin},
	} {
		for _, test := range []struct {
			name, document string
			held           []string
			before, after  []string
			statements     int
		}{
			{
				name: "two table privileges", statements: 3,
				held: []string{"GRANT SELECT, INSERT ON t1 TO {role} WITH GRANT OPTION"},
				document: "GRANT SELECT, INSERT ON t1 TO {role} WITH GRANT OPTION;\n" +
					"REVOKE GRANT OPTION FOR SELECT, INSERT ON t1 FROM {role};\n",
				before: []string{"table t1 INSERT YES", "table t1 SELECT YES", "table t2 SELECT YES"},
				after:  []string{"table t1 INSERT NO", "table t1 SELECT NO", "table t2 SELECT YES"},
			},
			{
				name: "table and column privileges", statements: 3,
				held: []string{"GRANT SELECT ON t1 TO {role} WITH GRANT OPTION", "GRANT UPDATE (name) ON t1 TO {role}"},
				document: "GRANT SELECT ON t1 TO {role}; REVOKE GRANT OPTION FOR SELECT ON t1 FROM {role};\n" +
					"GRANT UPDATE (name) ON t1 TO {role}; REVOKE GRANT OPTION FOR UPDATE (name) ON t1 FROM {role};\n",
				before: []string{"column t1.name UPDATE YES", "table t1 SELECT YES", "table t2 SELECT YES"},
				after:  []string{"column t1.name UPDATE NO", "table t1 SELECT NO", "table t2 SELECT YES"},
			},
			{
				name: "only column privileges", statements: 3,
				held: []string{"GRANT SELECT (id) ON t1 TO {role} WITH GRANT OPTION", "GRANT UPDATE (name) ON t1 TO {role}"},
				document: "GRANT SELECT (id) ON t1 TO {role}; REVOKE GRANT OPTION FOR SELECT (id) ON t1 FROM {role};\n" +
					"GRANT UPDATE (name) ON t1 TO {role}; REVOKE GRANT OPTION FOR UPDATE (name) ON t1 FROM {role};\n",
				before: []string{"column t1.id SELECT YES", "column t1.name UPDATE YES", "table t2 SELECT YES"},
				after:  []string{"column t1.id SELECT NO", "column t1.name UPDATE NO", "table t2 SELECT YES"},
			},
			{
				name: "a later ordinary grant", statements: 3,
				held: []string{"GRANT SELECT, INSERT ON t1 TO {role} WITH GRANT OPTION"},
				document: "GRANT SELECT ON t1 TO {role}; REVOKE GRANT OPTION FOR SELECT ON t1 FROM {role};\n" +
					"GRANT INSERT ON t1 TO {role};\n",
				before: []string{"table t1 INSERT YES", "table t1 SELECT YES", "table t2 SELECT YES"},
				after:  []string{"table t1 INSERT NO", "table t1 SELECT NO", "table t2 SELECT YES"},
			},
		} {
			t.Run(engine.dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				dev := mysqlOptionDatabase(c, engine.admin, test.held)
				c.Assert(mysqlOptionRows(c, dev, engine.host), qt.DeepEquals, test.before)

				rendered := renderOptionDocument(c, engine.dialect, strings.ReplaceAll(test.document, "{role}", dev.role))
				statements := sqlutil.SplitSQLStatementsForDialect(rendered, engine.dialect)
				c.Assert(statements, qt.HasLen, test.statements)
				c.Assert(strings.Count(rendered, "REVOKE GRANT OPTION ON"), qt.Equals, 1)
				dev.exec(c, statements)

				c.Assert(mysqlOptionRows(c, dev, engine.host), qt.DeepEquals, test.after)
			})
		}
	}
}
