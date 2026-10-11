package mssqlroutine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/routineext"
	"ptah.run/dialect/mssql/mssqlroutine"
)

// read reads one procedure body on the sqlserver target.
func read(c *qt.C, sql string) routineext.Reading {
	c.Helper()
	request := routineext.ReadRequest{Target: "sqlserver", Bodies: []routineext.Body{{Kind: "procedure", SQL: sql}}}
	result, err := mssqlroutine.Reader{}.ReadBodies(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(routineext.ValidateRead(request, result), qt.IsNil)
	c.Assert(result.Bodies, qt.HasLen, 1)
	return result.Bodies[0]
}

// labels lists the labels of a statement list.
func labels(statements []routineext.Statement) []string {
	listed := make([]string, 0, len(statements))
	for _, statement := range statements {
		listed = append(listed, statement.Label)
	}
	return listed
}

// A body is split into the statements of its outer BEGIN ... END, or of the
// body itself when it has none, each named by the word it begins with. A
// statement ends at a semicolon or where a keyword that only starts one
// begins. The bodies are the text the parser hands over for each routine
// shape.
func TestReader_SplitsATSQLBody(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want []string
	}{
		{name: "an inline table-valued function", body: "RETURN SELECT @a as [a], @b as [b]", want: []string{"return"}},
		{name: "a multi-statement table-valued function", body: "BEGIN\n  INSERT @t1\n  SELECT 1 AS [c1];\nRETURN;\nEND",
			want: []string{"insert", "return"}},
		{name: "keywords in brackets are names", body: "BEGIN\n  INSERT @t\n  SELECT @a AS [AS], @a AS [END]]value];\nRETURN;\nEND",
			want: []string{"insert", "return"}},
		{name: "a scalar function with a branch",
			body: "BEGIN\n    DECLARE @score int;\n    SET @score = 1;\n    IF @score > 0\n    BEGIN\n        SET @score = @score + 1;\n" +
				"    END;\n    RETURN @score;\nEND",
			want: []string{"declaration", "assignment", "if", "return"}},
		{name: "a RETURN without a semicolon", body: "BEGIN\n  RETURN @a\n\tEND", want: []string{"return"}},
		{name: "a procedure body in a block", body: "BEGIN\n  SELECT @user_id;\nEND", want: []string{"select"}},
		{name: "a procedure body with no block", body: "SELECT 1 AS [first];\nSELECT 2 AS [second]", want: []string{"select", "select"}},
		{name: "statements with no separators", body: "DECLARE @score int\nSET @score = 1\nRETURN @score",
			want: []string{"declaration", "assignment", "return"}},
		{name: "an EXEC with no separator before it", body: "SET @s = N'x'\nEXEC sp_executesql @s\nEXEC dbo.p",
			want: []string{"assignment", "exec", "raw"}},
		{name: "an EXEC an INSERT stores", body: "INSERT INTO t EXEC dbo.p", want: []string{"insert"}},
		{name: "the EXECUTE permission", body: "GRANT EXECUTE ON dbo.p TO u\nDENY EXECUTE ON dbo.p TO v\nREVOKE EXEC ON dbo.p FROM w",
			want: []string{"raw"}},
		{name: "a body that starts with a transaction", body: "BEGIN TRAN;\nUPDATE t SET a = 1;\nCOMMIT;",
			want: []string{"transaction", "raw", "raw"}},
		{name: "a distributed transaction", body: "BEGIN\n  BEGIN DISTRIBUTED TRANSACTION;\n  COMMIT;\nEND",
			want: []string{"transaction", "raw"}},
		{name: "a BEGIN and an END that open and close no block",
			body: "BEGIN\n  BEGIN TRANSACTION;\n  END CONVERSATION @handle;\n  SELECT 1;\nEND",
			want: []string{"transaction", "raw", "select"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reading := read(c, test.body)

			c.Assert(labels(reading.Statements), qt.DeepEquals, test.want)
		})
	}
}

// Each statement of a body is stated in the contract's kinds: what runs as
// written, what writes nothing, what composes SQL at run time, and what steers
// statements this reader does not split out of it.
func TestReader_StatesEachKind(t *testing.T) {
	const body = "BEGIN\n" +
		"  UPDATE t SET a = 1;\n" +
		"  INSERT INTO t VALUES (1);\n" +
		"  DECLARE @s nvarchar(max) = N'DELETE FROM t';\n" +
		"  SET @s = N'x';\n" +
		"  SELECT @s;\n" +
		"  BEGIN TRAN;\n" +
		"  COMMIT;\n" +
		"  IF @s = N'x' DELETE FROM t;\n" +
		"  WHILE @s = N'x' SET @s = N'y';\n" +
		"  BEGIN TRY DELETE FROM t; END TRY BEGIN CATCH SELECT 1; END CATCH;\n" +
		"  BEGIN DIALOG @h FROM SERVICE a TO SERVICE 'b';\n" +
		"  RETURN;\n" +
		"END"
	c := qt.New(t)

	reading := read(c, body)

	c.Assert(reading.Statements, qt.DeepEquals, []routineext.Statement{
		{Kind: routineext.StatementSQL, Label: "raw", SQL: "UPDATE t SET a = 1;"},
		{Kind: routineext.StatementSQL, Label: "insert", SQL: "INSERT INTO t VALUES (1);"},
		{Kind: routineext.StatementInert, Label: "declaration", SQL: "DECLARE @s nvarchar(max) = N'DELETE FROM t';"},
		{Kind: routineext.StatementInert, Label: "assignment", SQL: "SET @s = N'x';"},
		{Kind: routineext.StatementInert, Label: "select", SQL: "SELECT @s;"},
		{Kind: routineext.StatementInert, Label: "transaction", SQL: "BEGIN TRAN;"},
		{Kind: routineext.StatementSQL, Label: "raw", SQL: "COMMIT;"},
		{Kind: routineext.StatementControl, Label: "if", SQL: "IF @s = N'x' DELETE FROM t;", Opaque: true},
		{Kind: routineext.StatementControl, Label: "while", SQL: "WHILE @s = N'x' SET @s = N'y';", Opaque: true},
		{Kind: routineext.StatementControl, Label: "block", SQL: "BEGIN TRY DELETE FROM t; END TRY BEGIN CATCH SELECT 1; END CATCH;", Opaque: true},
		{Kind: routineext.StatementSQL, Label: "raw", SQL: "BEGIN DIALOG @h FROM SERVICE a TO SERVICE 'b';"},
		{Kind: routineext.StatementInert, Label: "return", SQL: "RETURN;"},
	})
}

// EXEC and EXECUTE compose SQL at run time when they run a string or
// sp_executesql, under either spelling and any qualification. Calling a
// procedure by name, by a variable that holds its name, or switching the
// security context composes nothing.
func TestReader_NamesTheStatementsThatComposeSQL(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want routineext.Statement
	}{
		{name: "sp_executesql", body: "EXEC sp_executesql @s;",
			want: routineext.Statement{Kind: routineext.StatementDynamic, Label: "exec", SQL: "EXEC sp_executesql @s;"}},
		{name: "the EXECUTE spelling", body: "execute sp_executesql @s;",
			want: routineext.Statement{Kind: routineext.StatementDynamic, Label: "execute", SQL: "execute sp_executesql @s;"}},
		{name: "a qualified sp_executesql", body: "EXEC sys.sp_executesql @s;",
			want: routineext.Statement{Kind: routineext.StatementDynamic, Label: "exec", SQL: "EXEC sys.sp_executesql @s;"}},
		{name: "a bracketed sp_executesql", body: "EXEC [sys].[sp_executesql] @s;",
			want: routineext.Statement{Kind: routineext.StatementDynamic, Label: "exec", SQL: "EXEC [sys].[sp_executesql] @s;"}},
		{name: "a double-quoted sp_executesql", body: `EXEC "sp_executesql" @s;`,
			want: routineext.Statement{Kind: routineext.StatementDynamic, Label: "exec", SQL: `EXEC "sp_executesql" @s;`}},
		{name: "a string is not a name", body: "EXEC 'sp_executesql';",
			want: routineext.Statement{Kind: routineext.StatementSQL, Label: "raw", SQL: "EXEC 'sp_executesql';"}},
		{name: "a return status", body: "EXEC @rc = sp_executesql @s;",
			want: routineext.Statement{Kind: routineext.StatementDynamic, Label: "exec", SQL: "EXEC @rc = sp_executesql @s;"}},
		{name: "a string", body: "EXEC('DELETE FROM t');",
			want: routineext.Statement{Kind: routineext.StatementDynamic, Label: "exec", SQL: "EXEC('DELETE FROM t');"}},
		{name: "a variable in parentheses", body: "EXECUTE (@s);",
			want: routineext.Statement{Kind: routineext.StatementDynamic, Label: "execute", SQL: "EXECUTE (@s);"}},
		{name: "a procedure by name", body: "EXEC dbo.other_proc;",
			want: routineext.Statement{Kind: routineext.StatementSQL, Label: "raw", SQL: "EXEC dbo.other_proc;"}},
		{name: "a procedure whose name a variable holds", body: "EXEC @proc_name;",
			want: routineext.Statement{Kind: routineext.StatementSQL, Label: "raw", SQL: "EXEC @proc_name;"}},
		{name: "a procedure a variable names, given a word", body: "EXEC @proc_name sp_executesql;",
			want: routineext.Statement{Kind: routineext.StatementSQL, Label: "raw", SQL: "EXEC @proc_name sp_executesql;"}},
		{name: "a return status from a procedure by name", body: "EXEC @rc = dbo.other_proc;",
			want: routineext.Statement{Kind: routineext.StatementSQL, Label: "raw", SQL: "EXEC @rc = dbo.other_proc;"}},
		{name: "a security context", body: "EXECUTE AS USER = 'x';",
			want: routineext.Statement{Kind: routineext.StatementSQL, Label: "raw", SQL: "EXECUTE AS USER = 'x';"}},
		{name: "nothing after the keyword", body: "EXEC",
			want: routineext.Statement{Kind: routineext.StatementSQL, Label: "raw", SQL: "EXEC"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(read(c, test.body).Statements, qt.DeepEquals, []routineext.Statement{test.want})
		})
	}
}

// A T-SQL variable carries an @ prefix, so every reading reports its column
// reads as attributable, and an empty body has no statements.
func TestReader_ReportsColumnReads(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want int
	}{
		{name: "a block", body: "BEGIN UPDATE t SET a = b; END", want: 1},
		{name: "a RETURN body", body: "RETURN @a", want: 1},
		{name: "an empty body", body: "  ", want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reading := read(c, test.body)

			c.Assert(reading.Statements, qt.HasLen, test.want)
			c.Assert(reading.ColumnReads, qt.IsTrue)
		})
	}
}

// The sqlserver target is served under any of its spellings, and every body
// of a request is read, in order.
func TestReader_ReadBodies_HappyPath(t *testing.T) {
	for _, target := range []string{"sqlserver", "mssql"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			request := routineext.ReadRequest{Target: target, Bodies: []routineext.Body{
				{Kind: "function", SQL: "RETURN 1"}, {Kind: "procedure", SQL: "BEGIN DELETE FROM t; END"},
			}}

			result, err := mssqlroutine.Reader{}.ReadBodies(c.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert([]any{labels(result.Bodies[0].Statements), labels(result.Bodies[1].Statements)}, qt.DeepEquals,
				[]any{[]string{"return"}, []string{"raw"}})
		})
	}
}

func TestReader_ReadBodies_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name    string
		ctx     context.Context
		target  string
		bodies  []routineext.Body
		wantIs  error
		wantErr string
	}{
		{name: "a target outside the family", ctx: context.Background(), target: "postgres", wantIs: ptaherr.ErrUnsupportedDialect,
			wantErr: `.*the SQL Server routine body reader on "postgres"`},
		{name: "no target", ctx: context.Background(), target: "", wantIs: ptaherr.ErrUnsupportedDialect,
			wantErr: `.*the SQL Server routine body reader on ""`},
		{name: "no context", target: "sqlserver", wantIs: routineext.ErrInvalid, wantErr: `.*reading routine bodies requires a context`},
		{name: "canceled", ctx: canceled, target: "sqlserver", bodies: []routineext.Body{{Kind: "procedure", SQL: "DELETE FROM t"}},
			wantIs: context.Canceled, wantErr: `context canceled`},
		{name: "canceled with no bodies", ctx: canceled, target: "sqlserver", wantIs: context.Canceled, wantErr: `context canceled`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := mssqlroutine.Reader{}.ReadBodies(test.ctx, routineext.ReadRequest{Target: test.target, Bodies: test.bodies})

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, routineext.ReadResult{})
		})
	}
}
