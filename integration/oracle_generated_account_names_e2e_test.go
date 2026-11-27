//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// accountNameMigration is a table, a function, a procedure and a view over the
// table that calls the function, created in the account that runs it.
const accountNameMigration = `CREATE TABLE t (id NUMBER(10) PRIMARY KEY, n NUMBER(10));
CREATE OR REPLACE FUNCTION rt(x NUMBER) RETURN NUMBER IS
BEGIN
  RETURN x * 2;
END;
CREATE OR REPLACE PROCEDURE rp IS
BEGIN
  NULL;
END;
CREATE VIEW vv AS SELECT id, rt(id) AS d FROM t;
`

// oracleScratchAccount creates an account of its own on the server the admin
// connection is on, dropped when the test ends, and returns its stored name and
// the URL Ptah connects to it with.
func oracleScratchAccount(c *qt.C, ctx context.Context, admin *dbschema.DatabaseConnection, prefix string) (name, dbURL string) {
	c.Helper()
	account := fmt.Sprintf("ptah_%s_%d_%d", prefix, os.Getpid(), time.Now().UnixNano()%1_000_000)
	c.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err := admin.ExecContext(dropCtx, "DROP USER "+account+" CASCADE")
		c.Check(err, qt.IsNil)
	})
	runAsAccount(c, ctx, admin, []string{
		"CREATE USER " + account + " IDENTIFIED BY ptah_password",
		"ALTER USER " + account + " QUOTA UNLIMITED ON USERS",
		"GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE PROCEDURE, CREATE SEQUENCE, " +
			"CREATE MATERIALIZED VIEW TO " + account,
	})
	parsed, err := url.Parse(dbtarget.URL(c, dbtarget.OracleAdmin))
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(account, "ptah_password")
	return strings.ToUpper(account), parsed.String()
}

// accountRoutinesAndViews answers the functions, procedures and views an
// account owns, as type and name, and whether each is valid.
func accountRoutinesAndViews(c *qt.C, ctx context.Context, admin *dbschema.DatabaseConnection, account string) []string {
	c.Helper()
	return accountNames(c, ctx, admin,
		"SELECT object_type || ' ' || object_name || ' ' || status FROM all_objects "+
			"WHERE owner = '"+account+"' AND object_type IN ('FUNCTION', 'PROCEDURE', 'VIEW') "+
			"ORDER BY object_type, object_name")
}

// TestMigrationsGenerateNamesNoOracleAccountE2E generates, from a read of one
// account, the migration that removes its function, its procedure and its
// view, and applies it, and then its rollback, as a second account that holds
// the same objects. The files name no account, so each statement runs in the
// account that applies it, and the account that was read keeps its objects:
// `DROP VIEW <account>.VV` run as the second account answers ORA-01031 or
// drops the first account's view (stokaro/ptah#6710).
func TestMigrationsGenerateNamesNoOracleAccountE2E(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.OracleAdmin)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	read, readURL := oracleScratchAccount(c, ctx, admin, "gen_read")
	applied, appliedURL := oracleScratchAccount(c, ctx, admin, "gen_apply")
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_base.up.sql"), []byte(accountNameMigration), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_base.down.sql"),
		[]byte("DROP VIEW vv;\nDROP PROCEDURE rp;\nDROP FUNCTION rt;\nDROP TABLE t PURGE;\n"), 0o600), qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t (id NUMBER(10) PRIMARY KEY, n NUMBER(10));\n"), 0o600), qt.IsNil)
	options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute, Env: environmentWithoutPtahVariables()}
	for _, account := range []string{readURL, appliedURL} {
		up := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", account, "--migrations-dir", dir)
		c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	}
	all := []string{"FUNCTION RT VALID", "PROCEDURE RP VALID", "VIEW VV VALID"}

	generated := clirun.Run(c, clirun.Ptah, options, "migrations", "generate", "--db-url", readURL,
		"--migrations-dir", dir, "--schema-file", schema, "--name", "drop_objects")

	c.Assert(generated.ExitCode, qt.Equals, 0,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	files := generatedFiles(c, dir, "drop_objects")
	c.Assert(files, qt.Contains, "DROP VIEW VV")
	c.Assert(files, qt.Contains, "DROP FUNCTION RT")
	c.Assert(files, qt.Contains, "DROP PROCEDURE RP")
	c.Assert(strings.ToUpper(files), qt.Not(qt.Contains), read)

	up := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", appliedURL, "--migrations-dir", dir,
		"--allow-destructive")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(accountRoutinesAndViews(c, ctx, admin, applied), qt.HasLen, 0)
	c.Assert(accountRoutinesAndViews(c, ctx, admin, read), qt.DeepEquals, all)

	down := clirun.Run(c, clirun.Ptah, options, "migrations", "down", "--db-url", appliedURL, "--migrations-dir", dir,
		"--target", "1", "--confirm")
	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(accountRoutinesAndViews(c, ctx, admin, applied), qt.DeepEquals, all)
	c.Assert(accountRoutinesAndViews(c, ctx, admin, read), qt.DeepEquals, all)
}

// accountObjectsMigration is two tables joined by a foreign key, a foreign
// key into the table ACCOUNTS of the account crm, an index, a sequence, a
// table whose column defaults to a second sequence, a materialized view, a
// function, a table whose virtual column calls a second function, a
// procedure and a view that calls the function, created in the account that
// runs it. Oracle stores the default as "<ACCOUNT>"."SK"."NEXTVAL" and the
// virtual column as "<ACCOUNT>"."DBL"("N"), with the account that runs it.
func accountObjectsMigration(crm string) string {
	return `CREATE TABLE t1 (id NUMBER(10) PRIMARY KEY, n NUMBER(10));
CREATE TABLE t2 (id NUMBER(10) PRIMARY KEY, t1_id NUMBER(10), crm_id NUMBER(10), note VARCHAR2(20),
  CONSTRAINT t2_t1_fk FOREIGN KEY (t1_id) REFERENCES t1 (id),
  CONSTRAINT t2_crm_fk FOREIGN KEY (crm_id) REFERENCES ` + crm + `.accounts (id));
CREATE INDEX t2_note ON t2 (note);
CREATE SEQUENCE s1 START WITH 5;
CREATE SEQUENCE sk;
CREATE TABLE t3 (id NUMBER(10) PRIMARY KEY, k NUMBER DEFAULT sk.NEXTVAL);
CREATE MATERIALIZED VIEW mv1 AS SELECT id FROM t1;
CREATE OR REPLACE FUNCTION dbl(x NUMBER) RETURN NUMBER DETERMINISTIC IS
BEGIN
  RETURN x * 2;
END;
CREATE TABLE t4 (id NUMBER(10) PRIMARY KEY, n NUMBER(10), v NUMBER GENERATED ALWAYS AS (dbl(n)) VIRTUAL);
CREATE OR REPLACE FUNCTION rt(x NUMBER) RETURN NUMBER IS
BEGIN
  RETURN x * 2;
END;
CREATE OR REPLACE PROCEDURE rp IS
BEGIN
  NULL;
END;
CREATE VIEW vv AS SELECT id, rt(id) AS d FROM t2;
`
}

// accountObjectsRollback drops what accountObjectsMigration creates.
const accountObjectsRollback = `DROP VIEW vv;
DROP PROCEDURE rp;
DROP FUNCTION rt;
DROP TABLE t4 PURGE;
DROP FUNCTION dbl;
DROP MATERIALIZED VIEW mv1;
DROP TABLE t3 PURGE;
DROP SEQUENCE sk;
DROP SEQUENCE s1;
DROP TABLE t2 PURGE;
DROP TABLE t1 PURGE;
`

// accountObjectStatuses answers the tables, indexes, sequences, materialized
// views, views, functions and procedures an account owns, as type, name and
// status, leaving out the names Oracle generates, Ptah's revision tables and
// the table its migration lock keeps its rows in.
func accountObjectStatuses(c *qt.C, ctx context.Context, admin *dbschema.DatabaseConnection, account string) []string {
	c.Helper()
	return accountNames(c, ctx, admin,
		"SELECT object_type || ' ' || object_name || ' ' || status FROM all_objects "+
			"WHERE owner = '"+account+"' AND object_type IN "+
			"('TABLE', 'INDEX', 'SEQUENCE', 'MATERIALIZED VIEW', 'VIEW', 'FUNCTION', 'PROCEDURE') "+
			"AND object_name NOT LIKE 'SYS!_%' ESCAPE '!' AND object_name NOT LIKE 'schema!_migrations%' ESCAPE '!' "+
			"AND object_name <> 'PTAH_LOCKS' "+
			"ORDER BY object_type, object_name")
}

// accountDraws inserts a row into T3 and T4 of account and answers, as
// k:v:SK:V, the value the default drew, the value the virtual column
// computed, LAST_NUMBER of the account's own SK, and the expression Oracle
// stores for T4's virtual column, which names the account of the function
// it calls. Oracle records no dependency of a table on that function.
func accountDraws(c *qt.C, ctx context.Context, admin *dbschema.DatabaseConnection, account string) string {
	c.Helper()
	runAsAccount(c, ctx, admin, []string{
		"INSERT INTO " + account + ".t3 (id) VALUES (1)",
		"INSERT INTO " + account + ".t4 (id, n) VALUES (1, 21)",
	})
	drawn := accountNames(c, ctx, admin, "SELECT t3.k || ':' || t4.v || ':' || s.last_number "+
		"FROM "+account+".t3 t3, "+account+".t4 t4, all_sequences s "+
		"WHERE s.sequence_owner = '"+account+"' AND s.sequence_name = 'SK'")
	virtual := accountNames(c, ctx, admin, "SELECT data_default FROM all_tab_cols "+
		"WHERE owner = '"+account+"' AND table_name = 'T4' AND column_name = 'V'")
	return strings.Join(append(drawn, virtual...), ":")
}

// TestMigrationsGenerateFromOneOracleAccountAppliesToAnotherE2E generates, from
// a read of one account, the migration that removes every object it holds,
// and applies the migration and then its rollback as a second account on the
// same server that holds the same objects. No statement names the account
// that was read, so the whole of each file runs in the account that applies
// it, and the account that was read keeps every object: run as the second
// account, `ALTER TABLE <account>.T2 DROP CONSTRAINT T2_T1_FK` answers
// ORA-00942 after the statements before it have run. A foreign key into a
// table of a third account names that account in both files, and `schema
// diff` of the two accounts plans nothing (stokaro/ptah#6710).
func TestMigrationsGenerateFromOneOracleAccountAppliesToAnotherE2E(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.OracleAdmin)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	crm, _ := oracleScratchAccount(c, ctx, admin, "gen_crm")
	read, readURL := oracleScratchAccount(c, ctx, admin, "gen_read")
	applied, appliedURL := oracleScratchAccount(c, ctx, admin, "gen_apply")
	runAsAccount(c, ctx, admin, []string{
		"CREATE TABLE " + crm + ".accounts (id NUMBER(10) PRIMARY KEY)",
		"GRANT REFERENCES ON " + crm + ".accounts TO " + read,
		"GRANT REFERENCES ON " + crm + ".accounts TO " + applied,
	})
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_base.up.sql"), []byte(accountObjectsMigration(crm)), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_base.down.sql"), []byte(accountObjectsRollback), 0o600), qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n"), 0o600), qt.IsNil)
	options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute, Env: environmentWithoutPtahVariables()}
	for _, account := range []string{readURL, appliedURL} {
		up := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", account, "--migrations-dir", dir)
		c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	}
	all := []string{
		"FUNCTION DBL VALID", "FUNCTION RT VALID", "INDEX T2_NOTE VALID", "MATERIALIZED VIEW MV1 VALID",
		"PROCEDURE RP VALID", "SEQUENCE S1 VALID", "SEQUENCE SK VALID", "TABLE MV1 VALID", "TABLE T1 VALID",
		"TABLE T2 VALID", "TABLE T3 VALID", "TABLE T4 VALID", "VIEW VV VALID",
	}
	c.Assert(accountObjectStatuses(c, ctx, admin, applied), qt.DeepEquals, all)
	// Read without their accounts, the two accounts hold one schema.
	diff := clirun.Run(c, clirun.Ptah, options, "schema", "diff", "--from", readURL, "--to", appliedURL)
	c.Assert(diff.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diff.Stdout, diff.Stderr))
	c.Assert(diff.Stdout, qt.Matches, `No changes found in the visible scope\.(?s:.*)`)

	generated := clirun.Run(c, clirun.Ptah, options, "migrations", "generate", "--db-url", readURL,
		"--migrations-dir", dir, "--schema-file", schema, "--name", "drop_objects")

	c.Assert(generated.ExitCode, qt.Equals, 0,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	files := generatedFiles(c, dir, "drop_objects")
	c.Assert(strings.ToUpper(files), qt.Not(qt.Contains), read)
	c.Assert(files, qt.Contains, "DROP TABLE T2 PURGE;")
	c.Assert(files, qt.Contains, "CREATE INDEX T2_NOTE ON T2 (NOTE);")
	c.Assert(files, qt.Contains, "ALTER TABLE T2 ADD CONSTRAINT T2_T1_FK FOREIGN KEY (T1_ID) REFERENCES T1 (ID);")
	c.Assert(files, qt.Contains,
		"ALTER TABLE T2 ADD CONSTRAINT T2_CRM_FK FOREIGN KEY (CRM_ID) REFERENCES "+crm+".ACCOUNTS (ID);")
	c.Assert(files, qt.Contains, `K NUMBER DEFAULT "SK"."NEXTVAL"`)
	c.Assert(files, qt.Contains, `V NUMBER GENERATED ALWAYS AS ("DBL"("N")) VIRTUAL`)

	up := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", appliedURL, "--migrations-dir", dir,
		"--allow-destructive")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(accountObjectStatuses(c, ctx, admin, applied), qt.DeepEquals, []string{"TABLE KEEP_ME VALID"})
	c.Assert(accountObjectStatuses(c, ctx, admin, read), qt.DeepEquals, all)

	down := clirun.Run(c, clirun.Ptah, options, "migrations", "down", "--db-url", appliedURL, "--migrations-dir", dir,
		"--target", "1", "--confirm")
	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(accountObjectStatuses(c, ctx, admin, applied), qt.DeepEquals, all)
	c.Assert(accountObjectStatuses(c, ctx, admin, read), qt.DeepEquals, all)
	// Each account's insert draws from its own sequence and calls its own
	// function: the first value drawn from SK caches 20, so LAST_NUMBER of
	// the sequence the row drew 1 from reads 21, and an unused one reads 1.
	c.Assert(accountDraws(c, ctx, admin, applied), qt.Equals, `1:42:21:"`+applied+`"."DBL"("N")`)
	c.Assert(accountDraws(c, ctx, admin, read), qt.Equals, `1:42:21:"`+read+`"."DBL"("N")`)
}
