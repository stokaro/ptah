package mysqlroutine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/routineext"
	"ptah.run/dialect/mysql/mysqlroutine"
)

// read reads one body on the mysql target.
func read(c *qt.C, sql string) routineext.Reading {
	c.Helper()
	request := routineext.ReadRequest{Target: "mysql", Bodies: []routineext.Body{{Kind: "procedure", SQL: sql}}}
	result, err := mysqlroutine.Reader{}.ReadBodies(c.Context(), request)
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

// A compound body is split into the statements of its outer BEGIN ... END,
// each named by the word it begins with. A nested block, a branch, a loop or a
// label stays one statement, and a scalar IF() is an expression rather than a
// branch.
func TestReader_SplitsACompoundBody(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want []string
	}{
		{
			name: "every statement form",
			body: "BEGIN\n" +
				"    DECLARE done, retried BOOL DEFAULT FALSE;\n" +
				"    DECLARE duplicate_key CONDITION FOR SQLSTATE '23000';\n" +
				"    DECLARE cur CURSOR FOR SELECT id FROM users;\n" +
				"    DECLARE CONTINUE HANDLER FOR NOT FOUND, duplicate_key SET done = TRUE;\n" +
				"    SET @seen = 0;\n" +
				"    BEGIN\n        SET @seen = @seen + 10;\n    END;\n" +
				"    IF @seen = 0 THEN\n        SET @seen = 1;\n    END IF;\n" +
				"    CASE\n        WHEN @seen = 1 THEN SET @seen = 2;\n        ELSE SET @seen = 3;\n    END CASE;\n" +
				"    WHILE @seen < 5 DO\n        SET @seen = @seen + 1;\n    END WHILE;\n" +
				"    REPEAT\n        SET @seen = @seen - 1;\n    UNTIL @seen = 0 END REPEAT;\n" +
				"    scan_loop: LOOP\n        LEAVE scan_loop;\n    END LOOP scan_loop;\n" +
				"    SELECT IF(@seen = 0, 1, 0);\n" +
				"END",
			want: []string{
				"declaration", "declaration", "cursor", "handler", "set", "block", "if", "case", "while", "repeat",
				"label", "select",
			},
		},
		{
			name: "a parenthesized IF is a branch",
			body: "BEGIN\n    DECLARE seen INT DEFAULT 0;\n    IF (seen = 0) THEN\n        SET seen = 1;\n    END IF;\n" +
				"    SELECT IF(seen = 1, 'yes', 'no') AS result;\nEND",
			want: []string{"declaration", "if", "select"},
		},
		{
			name: "a labeled outer block and a handler that opens a block",
			body: "main: BEGIN\n    DECLARE done BOOL DEFAULT FALSE;\n    DECLARE cur CURSOR FOR SELECT id FROM users;\n" +
				"    DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN\n        SET done = TRUE;\n    END;\n" +
				"    OPEN cur;\n    FETCH cur INTO done;\n    CLOSE cur;\n    IF (done = FALSE) THEN\n        SET done = TRUE;\n" +
				"    END IF;\n    SELECT IF(done, 1, 0) AS value;\nEND main",
			want: []string{"declaration", "cursor", "handler", "open", "fetch", "close", "if", "select"},
		},
		{
			name: "a handler runs the statement after its conditions",
			body: "BEGIN\n    DECLARE v INT DEFAULT 0;\n    DECLARE cur CURSOR FOR SELECT id FROM users;\n" +
				"    DECLARE CONTINUE HANDLER FOR NOT FOUND CLOSE cur;\n" +
				"    DECLARE CONTINUE HANDLER FOR SQLSTATE VALUE '23000' FETCH cur INTO v;\n" +
				"    DECLARE EXIT HANDLER FOR SQLEXCEPTION GET DIAGNOSTICS CONDITION 1 v = MESSAGE_TEXT;\nEND",
			want: []string{"declaration", "cursor", "handler", "handler", "handler"},
		},
		{
			name: "a malformed declaration is still one",
			body: "BEGIN\n    DECLARE CONTINUE HANDLER FOR;\n    DECLARE v INT DEFAULT;\nEND",
			want: []string{"handler", "declaration"},
		},
		{name: "a function's RETURN body", body: "RETURN a + b", want: []string{"return"}},
		{name: "a compound function body", body: "BEGIN\n    INSERT INTO t1 VALUES (x);\n    RETURN x+2;\nEND",
			want: []string{"insert", "return"}},
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
// statements this reader does not split out of it. The text of each is cut at
// its own boundaries, a body with whitespace in front of it included.
func TestReader_StatesEachKind(t *testing.T) {
	const body = "\n  BEGIN\n  UPDATE t SET a = 1;\n  INSERT INTO t VALUES (1);\n  DELETE FROM t;\n  TRUNCATE TABLE t;\n" +
		"  SET @s = 'DELETE FROM t';\n  PREPARE stmt FROM @s;\n  EXECUTE stmt;\n  DEALLOCATE PREPARE stmt;\n" +
		"  DROP PREPARE stmt;\n  execute stmt;\n  IF a THEN\n    DELETE FROM t;\n  END IF;\n" +
		"  DECLARE EXIT HANDLER FOR SQLEXCEPTION DELETE FROM t;\n  RETURN;\nEND"
	c := qt.New(t)

	reading := read(c, body)

	c.Assert(reading.Statements, qt.DeepEquals, []routineext.Statement{
		{Kind: routineext.StatementSQL, Label: "update", SQL: "UPDATE t SET a = 1;"},
		{Kind: routineext.StatementSQL, Label: "insert", SQL: "INSERT INTO t VALUES (1);"},
		{Kind: routineext.StatementSQL, Label: "delete", SQL: "DELETE FROM t;"},
		{Kind: routineext.StatementSQL, Label: "raw", SQL: "TRUNCATE TABLE t;"},
		{Kind: routineext.StatementInert, Label: "set", SQL: "SET @s = 'DELETE FROM t';"},
		{Kind: routineext.StatementDynamic, Label: "prepare", SQL: "PREPARE stmt FROM @s;"},
		{Kind: routineext.StatementDynamic, Label: "execute", SQL: "EXECUTE stmt;"},
		{Kind: routineext.StatementInert, Label: "deallocate", SQL: "DEALLOCATE PREPARE stmt;"},
		{Kind: routineext.StatementInert, Label: "deallocate", SQL: "DROP PREPARE stmt;"},
		{Kind: routineext.StatementDynamic, Label: "execute", SQL: "execute stmt;"},
		{Kind: routineext.StatementControl, Label: "if", SQL: "IF a THEN\n    DELETE FROM t;\n  END IF;", Opaque: true},
		{Kind: routineext.StatementControl, Label: "handler", SQL: "DECLARE EXIT HANDLER FOR SQLEXCEPTION DELETE FROM t;", Opaque: true},
		{Kind: routineext.StatementInert, Label: "return", SQL: "RETURN;"},
	})
}

// The statements a body only reads or steers are inert, and every control-flow
// statement is opaque: this reader does not split the statements it runs, so a
// host cannot credit it with running none.
func TestReader_StatesEachInertAndControlKind(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want routineext.Statement
	}{
		{name: "a variable", body: "DECLARE v INT DEFAULT 0;",
			want: routineext.Statement{Kind: routineext.StatementInert, Label: "declaration", SQL: "DECLARE v INT DEFAULT 0;"}},
		{name: "a cursor", body: "DECLARE cur CURSOR FOR SELECT id FROM users;",
			want: routineext.Statement{Kind: routineext.StatementInert, Label: "cursor", SQL: "DECLARE cur CURSOR FOR SELECT id FROM users;"}},
		{name: "a SELECT", body: "SELECT id INTO v FROM users;",
			want: routineext.Statement{Kind: routineext.StatementInert, Label: "select", SQL: "SELECT id INTO v FROM users;"}},
		{name: "OPEN", body: "OPEN cur;", want: routineext.Statement{Kind: routineext.StatementInert, Label: "open", SQL: "OPEN cur;"}},
		{name: "FETCH", body: "FETCH cur INTO v;",
			want: routineext.Statement{Kind: routineext.StatementInert, Label: "fetch", SQL: "FETCH cur INTO v;"}},
		{name: "CLOSE", body: "CLOSE cur;", want: routineext.Statement{Kind: routineext.StatementInert, Label: "close", SQL: "CLOSE cur;"}},
		{name: "LEAVE", body: "LEAVE l;", want: routineext.Statement{Kind: routineext.StatementInert, Label: "leave", SQL: "LEAVE l;"}},
		{name: "ITERATE", body: "ITERATE l;",
			want: routineext.Statement{Kind: routineext.StatementInert, Label: "iterate", SQL: "ITERATE l;"}},
		{name: "a nested block", body: "BEGIN DELETE FROM t; END;",
			want: routineext.Statement{Kind: routineext.StatementControl, Label: "block", SQL: "BEGIN DELETE FROM t; END;", Opaque: true}},
		{name: "CASE", body: "CASE WHEN a THEN DELETE FROM t; END CASE;",
			want: routineext.Statement{Kind: routineext.StatementControl, Label: "case", SQL: "CASE WHEN a THEN DELETE FROM t; END CASE;", Opaque: true}},
		{name: "LOOP", body: "LOOP DELETE FROM t; END LOOP;",
			want: routineext.Statement{Kind: routineext.StatementControl, Label: "loop", SQL: "LOOP DELETE FROM t; END LOOP;", Opaque: true}},
		{name: "WHILE", body: "WHILE a DO DELETE FROM t; END WHILE;",
			want: routineext.Statement{Kind: routineext.StatementControl, Label: "while", SQL: "WHILE a DO DELETE FROM t; END WHILE;", Opaque: true}},
		{name: "REPEAT", body: "REPEAT DELETE FROM t; UNTIL a END REPEAT;",
			want: routineext.Statement{Kind: routineext.StatementControl, Label: "repeat", SQL: "REPEAT DELETE FROM t; UNTIL a END REPEAT;", Opaque: true}},
		{name: "a labeled loop", body: "l: LOOP DELETE FROM t; END LOOP l;",
			want: routineext.Statement{Kind: routineext.StatementControl, Label: "label", SQL: "l: LOOP DELETE FROM t; END LOOP l;", Opaque: true}},
		{name: "DROP of anything else runs as written", body: "DROP TABLE t;",
			want: routineext.Statement{Kind: routineext.StatementSQL, Label: "raw", SQL: "DROP TABLE t;"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(read(c, "BEGIN\n"+test.body+"\nEND").Statements, qt.DeepEquals, []routineext.Statement{test.want})
		})
	}
}

// MySQL resolves a name a declared variable and a column share to the variable,
// so no reading reports its column reads as attributable, and an empty body
// has no statements.
func TestReader_ReportsNoColumnReads(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want int
	}{
		{name: "a compound body", body: "BEGIN UPDATE t SET a = b; END", want: 1},
		{name: "a RETURN body", body: "RETURN a", want: 1},
		{name: "an empty body", body: "  ", want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reading := read(c, test.body)

			c.Assert(reading.Statements, qt.HasLen, test.want)
			c.Assert(reading.ColumnReads, qt.IsFalse)
		})
	}
}

// Both targets of the MySQL family are served, and every body of a request
// is read, in order.
func TestReader_ReadBodies_HappyPath(t *testing.T) {
	for _, target := range []string{"mysql", "mariadb"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			request := routineext.ReadRequest{Target: target, Bodies: []routineext.Body{
				{Kind: "function", SQL: "RETURN 1"}, {Kind: "procedure", SQL: "BEGIN DELETE FROM t; END"},
			}}

			result, err := mysqlroutine.Reader{}.ReadBodies(c.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert([]any{labels(result.Bodies[0].Statements), labels(result.Bodies[1].Statements)}, qt.DeepEquals,
				[]any{[]string{"return"}, []string{"delete"}})
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
			wantErr: `.*the MySQL routine body reader on "postgres"`},
		{name: "no target", ctx: context.Background(), target: "", wantIs: ptaherr.ErrUnsupportedDialect,
			wantErr: `.*the MySQL routine body reader on ""`},
		{name: "no context", target: "mysql", wantIs: routineext.ErrInvalid, wantErr: `.*reading routine bodies requires a context`},
		{name: "canceled", ctx: canceled, target: "mysql", bodies: []routineext.Body{{Kind: "procedure", SQL: "BEGIN DELETE FROM t; END"}}, wantIs: context.Canceled,
			wantErr: `context canceled`},
		{name: "canceled with no bodies", ctx: canceled, target: "mysql", wantIs: context.Canceled, wantErr: `context canceled`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := mysqlroutine.Reader{}.ReadBodies(test.ctx, routineext.ReadRequest{Target: test.target,
				Bodies: test.bodies})

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, routineext.ReadResult{})
		})
	}
}
