package sqlserverbatch_test

import (
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlserverbatch"
)

var kindNames = map[sqlserverbatch.Kind]string{
	sqlserverbatch.Simple: "simple",
	sqlserverbatch.Block:  "block",
	sqlserverbatch.If:     "if",
	sqlserverbatch.While:  "while",
	sqlserverbatch.Module: "module",
	sqlserverbatch.Call:   "call",
	sqlserverbatch.Label:  "label",
}

// render returns one line per statement, its kind and its SQL, with the
// statements it runs indented below it.
func render(statements []sqlserverbatch.Statement, indent string) []string {
	lines := make([]string, 0, len(statements))
	for _, statement := range statements {
		lines = append(lines, indent+kindNames[statement.Kind]+": "+statement.SQL)
		lines = append(lines, render(statement.Statements, indent+"  ")...)
	}
	return lines
}

// TestRead_SplitsWhereTheServerDoes pins the statements Read finds. Each row
// was run on SQL Server 2022 and 2025 through go-mssqldb, against a table
// dbo.log that the INSERTs write to; the number in a statement is the value it
// logged, so a row that splits two statements is one where both ran, and a row
// that keeps text in one statement is one where the server read it as one or
// refused the whole batch.
func TestRead_SplitsWhereTheServerDoes(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []string
	}{
		{
			name: "a semicolon ends a statement",
			text: "SELECT 1; SELECT 2",
			want: []string{
				"simple: SELECT 1;",
				"simple: SELECT 2",
			},
		},
		{
			name: "ALTER TABLE, then DROP DATABASE with no separator",
			text: "ALTER TABLE dbo.t ADD DEFAULT 1 FOR c DROP DATABASE other",
			want: []string{
				"simple: ALTER TABLE dbo.t ADD DEFAULT 1 FOR c",
				"simple: DROP DATABASE other",
			},
		},
		{
			name: "DROP DATABASE, then ALTER TABLE with no separator",
			text: "DROP DATABASE other ALTER TABLE dbo.t ADD c int",
			want: []string{
				"simple: DROP DATABASE other",
				"simple: ALTER TABLE dbo.t ADD c int",
			},
		},
		{
			name: "CREATE TABLE, then EXEC with no separator",
			text: "CREATE TABLE dbo.t (id int) EXEC sp_executesql N'SELECT 1'",
			want: []string{
				"simple: CREATE TABLE dbo.t (id int)",
				"simple: EXEC sp_executesql N'SELECT 1'",
			},
		},
		{
			name: "EXEC, then CREATE TABLE with no separator",
			text: "EXEC sp_executesql N'SELECT 1' CREATE TABLE dbo.t (id int)",
			want: []string{
				"simple: EXEC sp_executesql N'SELECT 1'",
				"simple: CREATE TABLE dbo.t (id int)",
			},
		},
		{
			name: "a SELECT after the rows of VALUES",
			text: "INSERT dbo.log VALUES (54) SELECT 55 AS v INTO #x55",
			want: []string{
				"simple: INSERT dbo.log VALUES (54)",
				"simple: SELECT 55 AS v INTO #x55",
			},
		},
		{
			name: "a second SET after the SET clause of UPDATE",
			text: "UPDATE dbo.log SET v = v SET NOCOUNT ON",
			want: []string{
				"simple: UPDATE dbo.log SET v = v",
				"simple: SET NOCOUNT ON",
			},
		},
		{
			name: "a statement after the principal of GRANT",
			text: "GRANT SELECT ON dbo.log TO public EXEC dbo.p",
			want: []string{
				"simple: GRANT SELECT ON dbo.log TO public",
				"simple: EXEC dbo.p",
			},
		},
		{
			name: "each of GRANT, DENY and REVOKE",
			text: "GRANT EXECUTE ON dbo.p TO u\nDENY SELECT ON dbo.t TO v\nREVOKE EXEC ON dbo.p FROM w",
			want: []string{
				"simple: GRANT EXECUTE ON dbo.p TO u",
				"simple: DENY SELECT ON dbo.t TO v",
				"simple: REVOKE EXEC ON dbo.p FROM w",
			},
		},
		{
			name: "a DROP after the column ALTER TABLE drops",
			text: "ALTER TABLE dbo.t DROP COLUMN c58 DROP TABLE IF EXISTS dbo.nothing",
			want: []string{
				"simple: ALTER TABLE dbo.t DROP COLUMN c58",
				"simple: DROP TABLE IF EXISTS dbo.nothing",
			},
		},
		{
			name: "DROP as the action of ALTER COLUMN",
			text: "ALTER TABLE dbo.t ALTER COLUMN c DROP MASKED",
			want: []string{
				"simple: ALTER TABLE dbo.t ALTER COLUMN c DROP MASKED",
			},
		},
		{
			name: "a statement after ENABLE TRIGGER ALL",
			text: "ALTER TABLE dbo.t ENABLE TRIGGER ALL INSERT dbo.log VALUES (90)",
			want: []string{
				"simple: ALTER TABLE dbo.t ENABLE TRIGGER ALL",
				"simple: INSERT dbo.log VALUES (90)",
			},
		},
		{
			name: "a statement after CHECK CONSTRAINT ALL",
			text: "ALTER TABLE dbo.t WITH CHECK CHECK CONSTRAINT ALL INSERT dbo.log VALUES (89)",
			want: []string{
				"simple: ALTER TABLE dbo.t WITH CHECK CHECK CONSTRAINT ALL",
				"simple: INSERT dbo.log VALUES (89)",
			},
		},
		{
			name: "the actions of a foreign key",
			text: "ALTER TABLE dbo.t ADD CONSTRAINT fk163 FOREIGN KEY (id) REFERENCES dbo.u (id) ON DELETE NO ACTION ON UPDATE SET NULL INSERT dbo.log VALUES (163)",
			want: []string{
				"simple: ALTER TABLE dbo.t ADD CONSTRAINT fk163 FOREIGN KEY (id) REFERENCES dbo.u (id) ON DELETE NO ACTION ON UPDATE SET NULL",
				"simple: INSERT dbo.log VALUES (163)",
			},
		},
		{
			name: "a parenthesized query after WITH VALUES",
			text: "ALTER TABLE dbo.t ADD c200 int DEFAULT 0 WITH VALUES (SELECT 200 AS v INTO #x200)",
			want: []string{
				"simple: ALTER TABLE dbo.t ADD c200 int DEFAULT 0 WITH VALUES",
				"simple: (SELECT 200 AS v INTO #x200)",
			},
		},
		{
			name: "a statement after FOR UPDATE",
			text: "DECLARE c16 CURSOR FOR SELECT v FROM dbo.log FOR UPDATE INSERT dbo.log VALUES (16)",
			want: []string{
				"simple: DECLARE c16 CURSOR FOR SELECT v FROM dbo.log FOR UPDATE",
				"simple: INSERT dbo.log VALUES (16)",
			},
		},
		{
			name: "a statement after an alias that is not reserved",
			text: "SELECT 1 AS enable INSERT dbo.log VALUES (17)",
			want: []string{
				"simple: SELECT 1 AS enable",
				"simple: INSERT dbo.log VALUES (17)",
			},
		},
		{
			name: "a statement after WITH RECOMPILE",
			text: "EXEC dbo.p2 @a = 47 WITH RECOMPILE INSERT dbo.log VALUES (48)",
			want: []string{
				"simple: EXEC dbo.p2 @a = 47 WITH RECOMPILE",
				"simple: INSERT dbo.log VALUES (48)",
			},
		},
		{
			name: "a statement after EXECUTE AS",
			text: "EXECUTE AS USER = 'dbo' INSERT dbo.log VALUES (107) REVERT",
			want: []string{
				"simple: EXECUTE AS USER = 'dbo'",
				"simple: INSERT dbo.log VALUES (107)",
				"simple: REVERT",
			},
		},
		{
			name: "a statement after BEGIN TRAN WITH MARK",
			text: "BEGIN TRAN t162 WITH MARK 'x' INSERT dbo.log VALUES (162) COMMIT",
			want: []string{
				"simple: BEGIN TRAN t162 WITH MARK 'x'",
				"simple: INSERT dbo.log VALUES (162)",
				"simple: COMMIT",
			},
		},
		{
			name: "a statement after UPDATE STATISTICS",
			text: "UPDATE STATISTICS dbo.log INSERT dbo.log VALUES (94)",
			want: []string{
				"simple: UPDATE STATISTICS dbo.log",
				"simple: INSERT dbo.log VALUES (94)",
			},
		},
		{
			name: "FETCH of OFFSET, then a statement",
			text: "SELECT 103 AS v INTO #x103 ORDER BY 1 OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY INSERT dbo.log SELECT v FROM #x103",
			want: []string{
				"simple: SELECT 103 AS v INTO #x103 ORDER BY 1 OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY",
				"simple: INSERT dbo.log SELECT v FROM #x103",
			},
		},
		{
			name: "a cursor FETCH after OFFSET",
			text: "DECLARE c2 CURSOR FOR SELECT 1 AS a ORDER BY 1 OFFSET 0 ROWS OPEN c2 FETCH NEXT FROM c2 CLOSE c2 DEALLOCATE c2",
			want: []string{
				"simple: DECLARE c2 CURSOR FOR SELECT 1 AS a ORDER BY 1 OFFSET 0 ROWS",
				"simple: OPEN c2",
				"simple: FETCH NEXT FROM c2",
				"simple: CLOSE c2",
				"simple: DEALLOCATE c2",
			},
		},
		{
			name: "a statement after DROP IF EXISTS",
			text: "DROP PROCEDURE IF EXISTS dbo.nothing INSERT dbo.log VALUES (164)",
			want: []string{
				"simple: DROP PROCEDURE IF EXISTS dbo.nothing",
				"simple: INSERT dbo.log VALUES (164)",
			},
		},
		{
			name: "a statement after a table variable",
			text: "DECLARE @t TABLE (v int) INSERT @t VALUES (165)",
			want: []string{
				"simple: DECLARE @t TABLE (v int)",
				"simple: INSERT @t VALUES (165)",
			},
		},
		{
			name: "INSERT DEFAULT VALUES",
			text: "INSERT dbo.log DEFAULT VALUES",
			want: []string{
				"simple: INSERT dbo.log DEFAULT VALUES",
			},
		},
		{
			name: "each element of CREATE SCHEMA",
			text: "CREATE SCHEMA s20 CREATE TABLE t20 (id int) GRANT SELECT ON t20 TO public",
			want: []string{
				"simple: CREATE SCHEMA s20",
				"simple: CREATE TABLE t20 (id int)",
				"simple: GRANT SELECT ON t20 TO public",
			},
		},
		{
			name: "a parenthesized query after SET",
			text: "SET NOCOUNT ON (SELECT 119 AS v INTO #x119)",
			want: []string{
				"simple: SET NOCOUNT ON",
				"simple: (SELECT 119 AS v INTO #x119)",
			},
		},
		{
			name: "a parenthesized query after an alias",
			text: "SELECT 1 x (SELECT 123 AS v INTO #x123)",
			want: []string{
				"simple: SELECT 1 x",
				"simple: (SELECT 123 AS v INTO #x123)",
			},
		},
		{
			name: "a parenthesized query after the rows of VALUES",
			text: "INSERT dbo.log VALUES (13) (SELECT 113 AS v INTO #x113)",
			want: []string{
				"simple: INSERT dbo.log VALUES (13)",
				"simple: (SELECT 113 AS v INTO #x113)",
			},
		},
		{
			name: "a query in two parentheses",
			text: "SELECT 1 AS a ((SELECT 142 AS v INTO #x142))",
			want: []string{
				"simple: SELECT 1 AS a",
				"simple: ((SELECT 142 AS v INTO #x142))",
			},
		},
		{
			name: "a parenthesized query after EXEC",
			text: "EXEC dbo.p2 121 (SELECT 122 AS v INTO #x122)",
			want: []string{
				"simple: EXEC dbo.p2 121",
				"simple: (SELECT 122 AS v INTO #x122)",
			},
		},
		{
			name: "a parenthesized query after an alias named offset",
			text: "SELECT 1 AS offset (SELECT 176 AS v INTO #x176)",
			want: []string{
				"simple: SELECT 1 AS offset",
				"simple: (SELECT 176 AS v INTO #x176)",
			},
		},
		{
			name: "a parenthesized query after OFFSET ROWS",
			text: "SELECT 1 AS a ORDER BY 1 OFFSET 0 ROWS (SELECT 254 AS v INTO #x254)",
			want: []string{
				"simple: SELECT 1 AS a ORDER BY 1 OFFSET 0 ROWS",
				"simple: (SELECT 254 AS v INTO #x254)",
			},
		},
		{
			name: "a parenthesized query after UNION",
			text: "SELECT 1 AS a UNION (SELECT 143)",
			want: []string{
				"simple: SELECT 1 AS a UNION (SELECT 143)",
			},
		},
		{
			name: "a parenthesized query after ORDER BY",
			text: "SELECT 1 AS a ORDER BY (SELECT NULL)",
			want: []string{
				"simple: SELECT 1 AS a ORDER BY (SELECT NULL)",
			},
		},
		{
			name: "a parenthesized query after TOP",
			text: "SELECT TOP 1 (SELECT 170 AS v) AS v INTO #x170",
			want: []string{
				"simple: SELECT TOP 1 (SELECT 170 AS v) AS v INTO #x170",
			},
		},
		{
			name: "a parenthesized query after TOP in parentheses",
			text: "SELECT TOP (1) (SELECT 171 AS v) AS v INTO #x171",
			want: []string{
				"simple: SELECT TOP (1) (SELECT 171 AS v) AS v INTO #x171",
			},
		},
		{
			name: "a parenthesized query after TOP PERCENT",
			text: "SELECT TOP 1 PERCENT (SELECT 172 AS v) AS v INTO #x172",
			want: []string{
				"simple: SELECT TOP 1 PERCENT (SELECT 172 AS v) AS v INTO #x172",
			},
		},
		{
			name: "a parenthesized query after TOP WITH TIES",
			text: "SELECT TOP 1 WITH TIES (SELECT 173 AS v) AS v INTO #x173 ORDER BY 1",
			want: []string{
				"simple: SELECT TOP 1 WITH TIES (SELECT 173 AS v) AS v INTO #x173 ORDER BY 1",
			},
		},
		{
			name: "a parenthesized query as the rows of INSERT",
			text: "INSERT INTO dbo.log (v) (SELECT 174)",
			want: []string{
				"simple: INSERT INTO dbo.log (v) (SELECT 174)",
			},
		},
		{
			name: "a parenthesized query multiplied",
			text: "SELECT 2 * (SELECT 248) AS v INTO #x248",
			want: []string{
				"simple: SELECT 2 * (SELECT 248) AS v INTO #x248",
			},
		},
		{
			name: "parenthesized queries OFFSET and FETCH take",
			text: "SELECT 1 AS a ORDER BY 1 OFFSET (SELECT 0) ROWS FETCH NEXT (SELECT 1) ROWS ONLY INSERT dbo.log VALUES (255)",
			want: []string{
				"simple: SELECT 1 AS a ORDER BY 1 OFFSET (SELECT 0) ROWS FETCH NEXT (SELECT 1) ROWS ONLY",
				"simple: INSERT dbo.log VALUES (255)",
			},
		},
		{
			name: "a parenthesized query RETURN takes",
			text: "RETURN (SELECT 1) INSERT dbo.log VALUES (1)",
			want: []string{
				"simple: RETURN (SELECT 1)",
				"simple: INSERT dbo.log VALUES (1)",
			},
		},
		{
			name: "a SELECT after RETURN",
			text: "IF 1=0 RETURN SELECT 32 AS v INTO #x32",
			want: []string{
				"if: IF 1=0 RETURN",
				"  simple: RETURN",
				"simple: SELECT 32 AS v INTO #x32",
			},
		},
		{
			name: "a label after a statement",
			text: "SELECT 1 lbl2: INSERT dbo.log VALUES (14)",
			want: []string{
				"simple: SELECT 1",
				"label: lbl2:",
				"simple: INSERT dbo.log VALUES (14)",
			},
		},
		{
			name: "GOTO and its label",
			text: "GOTO l1 INSERT dbo.log VALUES (92) l1: INSERT dbo.log VALUES (93)",
			want: []string{
				"simple: GOTO l1",
				"simple: INSERT dbo.log VALUES (92)",
				"label: l1:",
				"simple: INSERT dbo.log VALUES (93)",
			},
		},
		{
			name: "a scope qualifier, which is not a label",
			text: "GRANT SELECT ON SCHEMA::dbo TO public",
			want: []string{
				"simple: GRANT SELECT ON SCHEMA::dbo TO public",
			},
		},
		{
			name: "a common table expression and the statement it serves",
			text: "WITH x AS (SELECT 21 AS v) INSERT dbo.log SELECT v FROM x INSERT dbo.log VALUES (2)",
			want: []string{
				"simple: WITH x AS (SELECT 21 AS v) INSERT dbo.log SELECT v FROM x",
				"simple: INSERT dbo.log VALUES (2)",
			},
		},
		{
			name: "a common table expression after a statement",
			text: "INSERT dbo.log VALUES (23) WITH x AS (SELECT 24 AS v) INSERT dbo.log SELECT v FROM x",
			want: []string{
				"simple: INSERT dbo.log VALUES (23)",
				"simple: WITH x AS (SELECT 24 AS v) INSERT dbo.log SELECT v FROM x",
			},
		},
		{
			name: "WITH XMLNAMESPACES",
			text: "WITH XMLNAMESPACES ('urn:a' AS a) SELECT 1 INSERT dbo.log VALUES (22)",
			want: []string{
				"simple: WITH XMLNAMESPACES ('urn:a' AS a) SELECT 1",
				"simple: INSERT dbo.log VALUES (22)",
			},
		},
		{
			name: "WITH CHANGE_TRACKING_CONTEXT",
			text: "WITH CHANGE_TRACKING_CONTEXT (0x01) INSERT dbo.log VALUES (108)",
			want: []string{
				"simple: WITH CHANGE_TRACKING_CONTEXT (0x01) INSERT dbo.log VALUES (108)",
			},
		},
		{
			name: "a table hint",
			text: "SELECT 1 AS a FROM dbo.log WITH (NOLOCK) INSERT dbo.log VALUES (138)",
			want: []string{
				"simple: SELECT 1 AS a FROM dbo.log WITH (NOLOCK)",
				"simple: INSERT dbo.log VALUES (138)",
			},
		},
		{
			name: "MERGE ends at its semicolon",
			text: "MERGE dbo.log AS t USING (SELECT 86 AS v) AS s ON t.v = s.v WHEN NOT MATCHED THEN INSERT (v) VALUES (s.v); INSERT dbo.log VALUES (87)",
			want: []string{
				"simple: MERGE dbo.log AS t USING (SELECT 86 AS v) AS s ON t.v = s.v WHEN NOT MATCHED THEN INSERT (v) VALUES (s.v);",
				"simple: INSERT dbo.log VALUES (87)",
			},
		},
		{
			name: "MERGE with a common table expression",
			text: "WITH x AS (SELECT 1 AS a) MERGE t USING x ON 1=0 WHEN MATCHED THEN UPDATE SET a = 1 WHEN NOT MATCHED THEN INSERT (a) VALUES (1);",
			want: []string{
				"simple: WITH x AS (SELECT 1 AS a) MERGE t USING x ON 1=0 WHEN MATCHED THEN UPDATE SET a = 1 WHEN NOT MATCHED THEN INSERT (a) VALUES (1);",
			},
		},
		{
			name: "the EXEC INSERT stores the rows of",
			text: "INSERT dbo.log EXEC dbo.p2 116 INSERT dbo.log VALUES (117)",
			want: []string{
				"simple: INSERT dbo.log EXEC dbo.p2 116",
				"  simple: EXEC dbo.p2 116",
				"simple: INSERT dbo.log VALUES (117)",
			},
		},
		{
			name: "a semicolon before ELSE",
			text: "IF 1=1 INSERT dbo.log VALUES (9); ELSE INSERT dbo.log VALUES (10)",
			want: []string{
				"if: IF 1=1 INSERT dbo.log VALUES (9); ELSE INSERT dbo.log VALUES (10)",
				"  simple: INSERT dbo.log VALUES (9);",
				"  simple: INSERT dbo.log VALUES (10)",
			},
		},
		{
			name: "IF and ELSE with blocks",
			text: "IF 1=1 BEGIN INSERT dbo.log VALUES (67) END ELSE BEGIN INSERT dbo.log VALUES (68) END INSERT dbo.log VALUES (69)",
			want: []string{
				"if: IF 1=1 BEGIN INSERT dbo.log VALUES (67) END ELSE BEGIN INSERT dbo.log VALUES (68) END",
				"  block: BEGIN INSERT dbo.log VALUES (67) END",
				"    simple: INSERT dbo.log VALUES (67)",
				"  block: BEGIN INSERT dbo.log VALUES (68) END",
				"    simple: INSERT dbo.log VALUES (68)",
				"simple: INSERT dbo.log VALUES (69)",
			},
		},
		{
			name: "WHILE runs one statement",
			text: "WHILE 1=0 INSERT dbo.log VALUES (70) INSERT dbo.log VALUES (71)",
			want: []string{
				"while: WHILE 1=0 INSERT dbo.log VALUES (70)",
				"  simple: INSERT dbo.log VALUES (70)",
				"simple: INSERT dbo.log VALUES (71)",
			},
		},
		{
			name: "ELSE belongs to the nearest IF",
			text: "IF 1=0 IF 1=1 INSERT dbo.log VALUES (72) ELSE INSERT dbo.log VALUES (73) INSERT dbo.log VALUES (74)",
			want: []string{
				"if: IF 1=0 IF 1=1 INSERT dbo.log VALUES (72) ELSE INSERT dbo.log VALUES (73)",
				"  if: IF 1=1 INSERT dbo.log VALUES (72) ELSE INSERT dbo.log VALUES (73)",
				"    simple: INSERT dbo.log VALUES (72)",
				"    simple: INSERT dbo.log VALUES (73)",
				"simple: INSERT dbo.log VALUES (74)",
			},
		},
		{
			name: "TRY and CATCH",
			text: "BEGIN TRY INSERT dbo.log VALUES (75) END TRY BEGIN CATCH INSERT dbo.log VALUES (76) END CATCH INSERT dbo.log VALUES (77)",
			want: []string{
				"block: BEGIN TRY INSERT dbo.log VALUES (75) END TRY BEGIN CATCH INSERT dbo.log VALUES (76) END CATCH",
				"  simple: INSERT dbo.log VALUES (75)",
				"  simple: INSERT dbo.log VALUES (76)",
				"simple: INSERT dbo.log VALUES (77)",
			},
		},
		{
			name: "TRY and CATCH are one statement for IF",
			text: "IF 1=0 BEGIN TRY INSERT dbo.log VALUES (82) END TRY BEGIN CATCH INSERT dbo.log VALUES (83) END CATCH ELSE INSERT dbo.log VALUES (84)",
			want: []string{
				"if: IF 1=0 BEGIN TRY INSERT dbo.log VALUES (82) END TRY BEGIN CATCH INSERT dbo.log VALUES (83) END CATCH ELSE INSERT dbo.log VALUES (84)",
				"  block: BEGIN TRY INSERT dbo.log VALUES (82) END TRY BEGIN CATCH INSERT dbo.log VALUES (83) END CATCH",
				"    simple: INSERT dbo.log VALUES (82)",
				"    simple: INSERT dbo.log VALUES (83)",
				"  simple: INSERT dbo.log VALUES (84)",
			},
		},
		{
			name: "a common table expression IF runs",
			text: "IF 1=1 WITH x AS (SELECT 40 AS v) INSERT dbo.log SELECT v FROM x",
			want: []string{
				"if: IF 1=1 WITH x AS (SELECT 40 AS v) INSERT dbo.log SELECT v FROM x",
				"  simple: WITH x AS (SELECT 40 AS v) INSERT dbo.log SELECT v FROM x",
			},
		},
		{
			name: "a common table expression in a block",
			text: "BEGIN WITH x AS (SELECT 41 AS v) INSERT dbo.log SELECT v FROM x END",
			want: []string{
				"block: BEGIN WITH x AS (SELECT 41 AS v) INSERT dbo.log SELECT v FROM x END",
				"  simple: WITH x AS (SELECT 41 AS v) INSERT dbo.log SELECT v FROM x",
			},
		},
		{
			name: "IF runs SET",
			text: "IF 1=1 SET NOCOUNT ON INSERT dbo.log VALUES (101)",
			want: []string{
				"if: IF 1=1 SET NOCOUNT ON",
				"  simple: SET NOCOUNT ON",
				"simple: INSERT dbo.log VALUES (101)",
			},
		},
		{
			name: "IF runs a parenthesized query",
			text: "IF 1=1 (SELECT 125 AS v INTO #x125) INSERT dbo.log SELECT v FROM #x125",
			want: []string{
				"if: IF 1=1 (SELECT 125 AS v INTO #x125)",
				"  simple: (SELECT 125 AS v INTO #x125)",
				"simple: INSERT dbo.log SELECT v FROM #x125",
			},
		},
		{
			name: "IF runs a label",
			text: "IF 1=1 lbl3: INSERT dbo.log VALUES (158)",
			want: []string{
				"if: IF 1=1 lbl3:",
				"  label: lbl3:",
				"simple: INSERT dbo.log VALUES (158)",
			},
		},
		{
			name: "the schema the planner creates",
			text: "IF SCHEMA_ID(N'app') IS NULL EXEC(N'CREATE SCHEMA [app]')",
			want: []string{
				"if: IF SCHEMA_ID(N'app') IS NULL EXEC(N'CREATE SCHEMA [app]')",
				"  simple: EXEC(N'CREATE SCHEMA [app]')",
			},
		},
		{
			name: "the table the planner creates",
			text: "IF OBJECT_ID(N'dbo.t', N'U') IS NULL CREATE TABLE dbo.t (id int)",
			want: []string{
				"if: IF OBJECT_ID(N'dbo.t', N'U') IS NULL CREATE TABLE dbo.t (id int)",
				"  simple: CREATE TABLE dbo.t (id int)",
			},
		},
		{
			name: "the property the planner adds",
			text: "IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE name = N'MS_Description') EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'x', @level0type = N'SCHEMA', @level0name = N'dbo'",
			want: []string{
				"if: IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE name = N'MS_Description') EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'x', @level0type = N'SCHEMA', @level0name = N'dbo'",
				"  simple: EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'x', @level0type = N'SCHEMA', @level0name = N'dbo'",
			},
		},
		{
			name: "BEGIN ATOMIC",
			text: "BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english') SELECT 1 END",
			want: []string{
				"block: BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english') SELECT 1 END",
				"  simple: SELECT 1",
			},
		},
		{
			name: "END CONVERSATION in a block",
			text: "BEGIN END CONVERSATION @h END",
			want: []string{
				"block: BEGIN END CONVERSATION @h END",
				"  simple: END CONVERSATION @h",
			},
		},
		{
			name: "CASE in a condition",
			text: "IF CASE WHEN 1=1 THEN 1 ELSE 0 END = 1 SELECT 1 ELSE SELECT 2",
			want: []string{
				"if: IF CASE WHEN 1=1 THEN 1 ELSE 0 END = 1 SELECT 1 ELSE SELECT 2",
				"  simple: SELECT 1",
				"  simple: SELECT 2",
			},
		},
		{
			name: "a view is the rest of the batch",
			text: "CREATE VIEW dbo.v4 AS SELECT 1 AS a; INSERT dbo.log VALUES (4)",
			want: []string{
				"module: CREATE VIEW dbo.v4 AS SELECT 1 AS a; INSERT dbo.log VALUES (4)",
			},
		},
		{
			name: "a procedure is the rest of the batch",
			text: "CREATE OR ALTER PROCEDURE dbo.p AS BEGIN SELECT 1; SELECT 2; END",
			want: []string{
				"module: CREATE OR ALTER PROCEDURE dbo.p AS BEGIN SELECT 1; SELECT 2; END",
			},
		},
		{
			name: "a function is the rest of the batch",
			text: "ALTER FUNCTION dbo.f5() RETURNS int AS BEGIN RETURN 1 END; INSERT dbo.log VALUES (5)",
			want: []string{
				"module: ALTER FUNCTION dbo.f5() RETURNS int AS BEGIN RETURN 1 END; INSERT dbo.log VALUES (5)",
			},
		},
		{
			name: "a procedure named with no EXEC",
			text: "dbo.p",
			want: []string{
				"call: dbo.p",
			},
		},
		{
			name: "a bracketed procedure name",
			text: "[dbo].[p] 1",
			want: []string{
				"call: [dbo].[p] 1",
			},
		},
		{
			name: "a double-quoted procedure name",
			text: "\"dbo\".\"p\"",
			want: []string{
				"call: \"dbo\".\"p\"",
			},
		},
		{
			name: "a procedure in another database",
			text: "m6688..p",
			want: []string{
				"call: m6688..p",
			},
		},
		{
			name: "a temporary procedure",
			text: "#tp",
			want: []string{
				"call: #tp",
			},
		},
		{
			name: "xp_cmdshell named with no EXEC",
			text: "master.dbo.xp_cmdshell 'dir'",
			want: []string{
				"call: master.dbo.xp_cmdshell 'dir'",
			},
		},
		{
			name: "sp_executesql named with no EXEC, then a statement",
			text: "sp_executesql N'SELECT 1' DROP DATABASE other",
			want: []string{
				"call: sp_executesql N'SELECT 1'",
				"simple: DROP DATABASE other",
			},
		},
		{
			name: "a procedure named after a semicolon",
			text: ";dbo.p",
			want: []string{
				"call: dbo.p",
			},
		},
		{
			name: "a no-break space",
			text: "INSERT dbo.log VALUES (177)\u00a0INSERT dbo.log VALUES (178)",
			want: []string{
				"simple: INSERT dbo.log VALUES (177)",
				"simple: INSERT dbo.log VALUES (178)",
			},
		},
		{
			name: "an ideographic space",
			text: "INSERT dbo.log VALUES (179)\u3000INSERT dbo.log VALUES (180)",
			want: []string{
				"simple: INSERT dbo.log VALUES (179)",
				"simple: INSERT dbo.log VALUES (180)",
			},
		},
		{
			name: "a carriage return ends a line comment",
			text: "SELECT 1 AS a -- c\rINSERT dbo.log VALUES (218)",
			want: []string{
				"simple: SELECT 1 AS a",
				"simple: INSERT dbo.log VALUES (218)",
			},
		},
		{
			name: "a vertical tab does not end a line comment",
			text: "SELECT 1 AS a -- c\vINSERT dbo.log VALUES (219)",
			want: []string{
				"simple: SELECT 1 AS a",
			},
		},
		{
			name: "block comments nest",
			text: "SELECT 1 AS a /* /* */ INSERT dbo.log VALUES (236) */ INSERT dbo.log VALUES (237)",
			want: []string{
				"simple: SELECT 1 AS a",
				"simple: INSERT dbo.log VALUES (237)",
			},
		},
		{
			name: "a comment opener in a string",
			text: "SELECT 'a /* b' AS a INSERT dbo.log VALUES (240) -- */",
			want: []string{
				"simple: SELECT 'a /* b' AS a",
				"simple: INSERT dbo.log VALUES (240)",
			},
		},
		{
			name: "a statement against a string",
			text: "SELECT N'x'INSERT dbo.log VALUES (231)",
			want: []string{
				"simple: SELECT N'x'",
				"simple: INSERT dbo.log VALUES (231)",
			},
		},
		{
			name: "a statement against a bracketed name",
			text: "SELECT 1 AS [a]INSERT dbo.log VALUES (232)",
			want: []string{
				"simple: SELECT 1 AS [a]",
				"simple: INSERT dbo.log VALUES (232)",
			},
		},
		{
			name: "numbers the lexer splits",
			text: "SELECT 0x1F AS a, 1e5 AS b, 1E+5 AS c, 1.5 AS d, .5 AS e, $1 AS f INSERT dbo.log VALUES (1)",
			want: []string{
				"simple: SELECT 0x1F AS a, 1e5 AS b, 1E+5 AS c, 1.5 AS d, .5 AS e, $1 AS f",
				"simple: INSERT dbo.log VALUES (1)",
			},
		},
		{
			name: "statement words in names",
			text: "DECLARE @insert int SET @insert = 1 SELECT t.[update], t.delete FROM dbo.t AS t",
			want: []string{
				"simple: DECLARE @insert int",
				"simple: SET @insert = 1",
				"simple: SELECT t.[update], t.delete FROM dbo.t AS t",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := sqlserverbatch.Read(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(render(statements, ""), qt.DeepEquals, test.want)
			assertOffsets(c, test.text, statements)
		})
	}
}

// assertOffsets asserts that Start and End of every statement, and of the
// statements it runs, locate its SQL in text.
func assertOffsets(c *qt.C, text string, statements []sqlserverbatch.Statement) {
	c.Helper()
	for _, statement := range statements {
		c.Assert(text[statement.Start:statement.End], qt.Equals, statement.SQL)
		assertOffsets(c, text, statement.Statements)
	}
}

// TestRead_ReadsNoStatementFromTextWithNone covers text with nothing the
// server runs.
func TestRead_ReadsNoStatementFromTextWithNone(t *testing.T) {
	for _, text := range []string{"", " \n\t", "-- comment\n/* block */", ";;"} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			statements, err := sqlserverbatch.Read(text)
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 0)
		})
	}
}

// TestRead_StatesWhatAStatementRunsBesideOthers pins the Head of a statement
// that runs others: IF or WHILE and its condition, and an INSERT up to the
// EXEC whose rows it stores.
func TestRead_StatesWhatAStatementRunsBesideOthers(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{
			name: "an IF",
			text: "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'app') EXEC(N'CREATE SCHEMA [app]') ELSE SELECT 1",
			want: "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'app')",
		},
		{
			name: "a WHILE",
			text: "WHILE @@FETCH_STATUS = 0 BEGIN FETCH NEXT FROM c END",
			want: "WHILE @@FETCH_STATUS = 0",
		},
		{
			name: "an INSERT of the rows an EXEC returns",
			text: "INSERT INTO dbo.log (v) EXEC dbo.p2 116",
			want: "INSERT INTO dbo.log (v)",
		},
		{
			name: "a statement that runs no other",
			text: "INSERT INTO dbo.log (v) VALUES (1);",
			want: "INSERT INTO dbo.log (v) VALUES (1);",
		},
		{
			name: "a block",
			text: "BEGIN SELECT 1 END",
			want: "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := sqlserverbatch.Read(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0].Head, qt.Equals, test.want)
		})
	}
}

// TestRead_FailurePath pins the text Read refuses, because it cannot find
// where a statement ends or because the lexer and the server would read the
// text differently. The server measurements in the rows, where there are
// any, are of the same text run on SQL Server 2022 and 2025.
func TestRead_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{
			name: "a BEGIN with no END",
			text: "BEGIN SELECT 1",
			want: "cannot find the T-SQL statement boundaries: a BEGIN with no END at offset 0",
		},
		{
			name: "an END that closes no BEGIN",
			text: "SELECT 1 END",
			want: "cannot find the T-SQL statement boundaries: an END that closes no BEGIN at offset 9",
		},
		{
			// The lexer reads 8.0.17 as one word, and the server reads the
			// number 8.0 first; the rest is text the reader would not see.
			name: "a number the lexer reads as a longer word",
			text: "SELECT 8.0.17",
			want: "cannot find the T-SQL statement boundaries: a number the lexer reads differently at offset 7",
		},
		{
			name: "an ELSE that follows no IF",
			text: "ELSE SELECT 1",
			want: "cannot find the T-SQL statement boundaries: an ELSE that follows no IF at offset 0",
		},
		{
			name: "an IF that runs no statement",
			text: "IF 1=1",
			want: "cannot find the T-SQL statement boundaries: an IF that runs no statement at offset 0",
		},
		{
			name: "a semicolon in a condition",
			text: "IF 1=1; SELECT 1",
			want: "cannot find the T-SQL statement boundaries: an IF that runs no statement at offset 0",
		},
		{
			name: "ELSE in a condition",
			text: "IF 1=1 ELSE SELECT 1",
			want: "cannot find the T-SQL statement boundaries: an IF that runs no statement at offset 0",
		},
		{
			name: "an ELSE that runs no statement",
			text: "IF 1=1 SELECT 1 ELSE",
			want: "cannot find the T-SQL statement boundaries: an ELSE that runs no statement at offset 16",
		},
		{
			name: "a TRY block with no CATCH block",
			text: "BEGIN TRY SELECT 1 END TRY",
			want: "cannot find the T-SQL statement boundaries: a TRY block with no CATCH block at offset 0",
		},
		{
			name: "a CATCH block that follows no TRY block",
			text: "BEGIN CATCH SELECT 1 END CATCH",
			want: "cannot find the T-SQL statement boundaries: a CATCH block that follows no TRY block at offset 0",
		},
		{
			name: "an END that does not close BEGIN TRY",
			text: "BEGIN TRY SELECT 1 END CATCH",
			want: "cannot find the T-SQL statement boundaries: an END that does not close BEGIN TRY at offset 19",
		},
		{
			name: "parentheses that do not close",
			text: "SELECT (1",
			want: "cannot find the T-SQL statement boundaries: parentheses or a CASE that the text does not close at offset 0",
		},
		{
			name: "a parenthesis that closes none",
			text: "SELECT 1)",
			want: "cannot find the T-SQL statement boundaries: a closing parenthesis that closes none at offset 8",
		},
		{
			name: "a CASE that does not close",
			text: "SELECT CASE WHEN 1 = 1 THEN 1",
			want: "cannot find the T-SQL statement boundaries: parentheses or a CASE that the text does not close at offset 0",
		},
		{
			name: "a semicolon inside parentheses",
			text: "SELECT (1; SELECT 2)",
			want: "cannot find the T-SQL statement boundaries: a semicolon inside parentheses or a CASE at offset 9",
		},
		{
			name: "an END inside parentheses",
			text: "SELECT (1 END)",
			want: "cannot find the T-SQL statement boundaries: an END inside parentheses at offset 10",
		},
		{
			name: "a statement that starts with a variable",
			text: "@x = 1",
			want: "cannot find the T-SQL statement boundaries: a statement that starts with \"@\" at offset 0",
		},
		{
			name: "a procedure named after the first statement",
			text: "SELECT 1; dbo.p",
			want: "cannot find the T-SQL statement boundaries: a statement that starts with \"dbo\" at offset 10",
		},
		{
			name: "an IF that runs a procedure named with no EXEC",
			text: "IF 1=1 dbo.p",
			want: "cannot find the T-SQL statement boundaries: an IF that runs no statement at offset 0",
		},
		{
			name: "a MERGE that does not end with a semicolon",
			text: "MERGE dbo.log AS t USING (SELECT 1 AS v) AS s ON 1=0 WHEN NOT MATCHED THEN INSERT (v) VALUES (154) EXEC dbo.p;",
			want: "cannot find the T-SQL statement boundaries: a MERGE that does not end with a semicolon at offset 99",
		},
		{
			name: "a bracketed name that does not close",
			text: "SELECT 1 [a",
			want: "cannot find the T-SQL statement boundaries: text the lexer cannot read at offset 9",
		},
		{
			name: "a dollar quote",
			text: "SELECT 1 $tag$ INSERT dbo.log VALUES (1) $tag$",
			want: "cannot find the T-SQL statement boundaries: quoting T-SQL does not have at offset 9",
		},
		{
			name: "a backtick",
			text: "SELECT `a`",
			want: "cannot find the T-SQL statement boundaries: quoting T-SQL does not have at offset 7",
		},
		{
			name: "a zero width space, which the server reads as whitespace",
			text: "INSERT dbo.log VALUES (183)\u200bINSERT dbo.log VALUES (184)",
			want: "cannot find the T-SQL statement boundaries: a character that is not a T-SQL operator at offset 27",
		},
		{
			name: "a NUL, which the server reads as whitespace",
			text: "INSERT dbo.log VALUES (198)\x00INSERT dbo.log VALUES (199)",
			want: "cannot find the T-SQL statement boundaries: a character that is not a T-SQL operator at offset 27",
		},
		{
			name: "a file separator, which the server reads as whitespace",
			text: "INSERT dbo.log VALUES (193)\x1cINSERT dbo.log VALUES (194)",
			want: "cannot find the T-SQL statement boundaries: a character that is not a T-SQL operator at offset 27",
		},
		{
			name: "a currency sign",
			text: "SELECT €1 AS a INSERT dbo.log VALUES (210)",
			want: "cannot find the T-SQL statement boundaries: a character that is not a T-SQL operator at offset 7",
		},
		{
			name: "a binary constant against a word",
			text: "SELECT 0x1INSERT dbo.log VALUES (213)",
			want: "cannot find the T-SQL statement boundaries: a number written against a word at offset 7",
		},
		{
			name: "an empty binary constant against a word",
			text: "SELECT 0xINSERT dbo.log VALUES (226)",
			want: "cannot find the T-SQL statement boundaries: a number written against a word at offset 7",
		},
		{
			name: "an exponent against a word",
			text: "SELECT 1e5INSERT dbo.log VALUES (214)",
			want: "cannot find the T-SQL statement boundaries: a number written against a word at offset 7",
		},
		{
			name: "an empty exponent against a word",
			text: "SELECT 1eINSERT dbo.log VALUES (228)",
			want: "cannot find the T-SQL statement boundaries: a number written against a word at offset 7",
		},
		{
			name: "a signed exponent against a word",
			text: "SELECT 1E+5INSERT dbo.log VALUES (227)",
			want: "cannot find the T-SQL statement boundaries: a number written against a word at offset 7",
		},
		{
			name: "money against a word",
			text: "SELECT $1INSERT dbo.log VALUES (215)",
			want: "cannot find the T-SQL statement boundaries: a number written against a word at offset 7",
		},
		{
			name: "an integer against a word",
			text: "SELECT 1INSERT dbo.log VALUES (216)",
			want: "cannot find the T-SQL statement boundaries: a number written against a word at offset 7",
		},
		{
			name: "a decimal point against a word",
			text: "SELECT 1.INSERT dbo.log VALUES (217)",
			want: "cannot find the T-SQL statement boundaries: a number written against a word at offset 7",
		},
		{
			name: "BEGIN ATOMIC WITH parentheses that do not close",
			text: "BEGIN ATOMIC WITH (x SELECT 1 END",
			want: "cannot find the T-SQL statement boundaries: a BEGIN ATOMIC WITH whose parentheses do not close at offset 0",
		},
		{
			name: "a procedure in a block",
			text: "BEGIN CREATE PROCEDURE p AS SELECT 1 END",
			want: "cannot find the T-SQL statement boundaries: a BEGIN with no END at offset 0",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := sqlserverbatch.Read(test.text)
			c.Assert(err, qt.ErrorIs, sqlserverbatch.ErrUnreadable)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.want))
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRead_ReadsEveryStatementOfALongBatch reads a batch with many
// statements in one pass, without separators, as a migration file with many
// statements is.
func TestRead_ReadsEveryStatementOfALongBatch(t *testing.T) {
	c := qt.New(t)
	text := strings.Repeat("INSERT dbo.log VALUES (1) ", 500)
	statements, err := sqlserverbatch.Read(text)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 500)
}
