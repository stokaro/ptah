package pgroutine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/routineext"
	"ptah.run/dialect/postgres/pgroutine"
)

// read reads one body on the postgres target.
func read(c *qt.C, language, sql string) routineext.Reading {
	c.Helper()
	request := routineext.ReadRequest{Target: "postgres", Bodies: []routineext.Body{{Language: language, SQL: sql}}}
	result, err := pgroutine.Reader{}.ReadBodies(c.Context(), request)
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

// A PL/pgSQL body is split into the statements of its outer block, a DECLARE
// section first, each statement named by the word it begins with. The bodies
// are the text the parser hands over for each routine shape.
func TestReader_SplitsAPLpgSQLBody(t *testing.T) {
	for _, test := range []struct {
		name  string
		body  string
		want  []string
		first string
	}{
		{name: "a declaration, a dynamic statement and a return",
			body: "\nDECLARE\n    changed integer;\nBEGIN\n    EXECUTE 'SELECT 1';\n    RETURN user_id;\nEND;\n",
			want: []string{"declaration", "execute", "return"}, first: "DECLARE\n    changed integer;"},
		{name: "a dollar quote inside a string is not the delimiter",
			body: "\nBEGIN\n    PERFORM '$$ not the delimiter';\nEND;\n",
			want: []string{"perform"}, first: "PERFORM '$$ not the delimiter';"},
		{name: "a nested block",
			body: "\nBEGIN\n    BEGIN\n        PERFORM 1;\n    END;\n    RETURN;\nEND;\n",
			want: []string{"block", "return"}, first: "BEGIN\n        PERFORM 1;\n    END;"},
		{name: "FOR UPDATE opens no loop",
			body: "\nBEGIN\n    PERFORM 1 FROM users FOR UPDATE;\n    RETURN;\nEND;\n",
			want: []string{"perform", "return"}, first: "PERFORM 1 FROM users FOR UPDATE;"},
		{name: "a FOR loop",
			body: "\nBEGIN\n    FOR i IN 1..3 LOOP\n        PERFORM i;\n    END LOOP;\n    RETURN;\nEND;\n",
			want: []string{"loop", "return"}, first: "FOR i IN 1..3 LOOP\n        PERFORM i;\n    END LOOP;"},
		{name: "an assignment and a return",
			body: "\nBEGIN\n    NEW.updated_at := now();\n    RETURN NEW;\nEND;\n",
			want: []string{"raw", "return"}, first: "NEW.updated_at := now();"},
		{name: "every loop form keeps its depth",
			body: "\nBEGIN\n    FOR i IN 1..2 LOOP\n        RAISE NOTICE '%', i;\n    END LOOP;\n    WHILE false LOOP\n" +
				"        RAISE NOTICE 'never';\n    END LOOP;\n    FOREACH item IN ARRAY ARRAY[1, 2] LOOP\n" +
				"        RAISE NOTICE '%', item;\n    END LOOP;\n    RETURN;\nEND;\n",
			want: []string{"loop", "loop", "loop", "return"}, first: "FOR i IN 1..2 LOOP\n        RAISE NOTICE '%', i;\n    END LOOP;"},
		{name: "a DO block",
			body: "\nDECLARE\n    missing boolean;\nBEGIN\n    IF missing THEN\n        RAISE NOTICE 'missing';\n    END IF;\n" +
				"EXCEPTION\n    WHEN duplicate_object THEN NULL;\nEND;\n",
			want: []string{"declaration", "if", "exception"}, first: "DECLARE\n    missing boolean;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reading := read(c, "plpgsql", test.body)

			c.Assert(labels(reading.Statements), qt.DeepEquals, test.want)
			c.Assert(reading.Statements[0].SQL, qt.Equals, test.first)
			c.Assert(reading.ColumnReads, qt.IsTrue)
		})
	}
}

// Each statement of a PL/pgSQL body is stated in the contract's kinds: what
// runs as written, what writes nothing, what composes SQL at run time, and
// what steers others.
func TestReader_StatesEachKind(t *testing.T) {
	const body = "BEGIN\n  UPDATE t SET a = 1;\n  PERFORM 1;\n  RAISE NOTICE 'x';\n  EXECUTE 'DELETE FROM t';\n" +
		"  IF a THEN\n    DELETE FROM t;\n  END IF;\n  CASE WHEN a THEN\n    PERFORM 2;\n  END CASE;\n  RETURN;\nEND;"
	c := qt.New(t)

	reading := read(c, "plpgsql", body)

	c.Assert(reading.Statements, qt.DeepEquals, []routineext.Statement{
		{Kind: routineext.StatementSQL, Label: "raw", SQL: "UPDATE t SET a = 1;"},
		{Kind: routineext.StatementInert, Label: "perform", SQL: "PERFORM 1;"},
		{Kind: routineext.StatementInert, Label: "raise", SQL: "RAISE NOTICE 'x';"},
		{Kind: routineext.StatementDynamic, Label: "execute", SQL: "EXECUTE 'DELETE FROM t';"},
		{Kind: routineext.StatementControl, Label: "if", SQL: "IF a THEN\n    DELETE FROM t;\n  END IF;", Statements: []routineext.Statement{
			{Kind: routineext.StatementSQL, Label: "raw", SQL: "DELETE FROM t;"},
		}},
		{Kind: routineext.StatementControl, Label: "case", SQL: "CASE WHEN a THEN\n    PERFORM 2;\n  END CASE;", Statements: []routineext.Statement{
			{Kind: routineext.StatementInert, Label: "perform", SQL: "PERFORM 2;"},
		}},
		{Kind: routineext.StatementInert, Label: "return", SQL: "RETURN;"},
	})
}

// A control-flow statement carries the statements it runs, all the way down,
// and one whose contents could not be split says so rather than carrying
// none (stokaro/ptah#2393). A label in front of a block or a loop does not
// hide what follows it.
func TestReader_ControlFlowCarriesItsStatements(t *testing.T) {
	for _, test := range []struct {
		name       string
		body       string
		wantLabel  string
		wantNested []string
		wantOpaque bool
	}{
		{name: "IF carries its branch", body: "IF a THEN\n  EXECUTE 'x';\nEND IF;", wantLabel: "if", wantNested: []string{"execute"}},
		{name: "IF carries both branches", body: "IF a THEN\n  PERFORM 1;\nELSE\n  EXECUTE 'x';\nEND IF;",
			wantLabel: "if", wantNested: []string{"perform", "execute"}},
		{name: "LOOP carries its body", body: "LOOP\n  EXECUTE 'x';\nEND LOOP;", wantLabel: "loop", wantNested: []string{"execute"}},
		{name: "FOR carries its body", body: "FOR r IN SELECT 1 LOOP\n  PERFORM 2;\nEND LOOP;", wantLabel: "loop", wantNested: []string{"perform"}},
		{name: "an IF it cannot split is opaque", body: "IF a THEN\nEND IF;", wantLabel: "if", wantNested: make([]string, 0), wantOpaque: true},
		{name: "a labeled block", body: "<<outer>>\nBEGIN\n  EXECUTE 'x';\nEND;", wantLabel: "block", wantNested: []string{"execute"}},
		{name: "a labeled loop", body: "<< \"Odd Name\" >> LOOP\n  PERFORM 1;\nEND LOOP;", wantLabel: "loop", wantNested: []string{"perform"}},
		{name: "a comparison is no label", body: "IF a << 2 THEN\n  PERFORM 1;\nEND IF;", wantLabel: "if", wantNested: []string{"perform"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reading := read(c, "plpgsql", "BEGIN\n"+test.body+"\nEND;")

			c.Assert(reading.Statements, qt.HasLen, 1)
			c.Assert(reading.Statements[0].Kind, qt.Equals, routineext.StatementControl)
			c.Assert(reading.Statements[0].Label, qt.Equals, test.wantLabel)
			c.Assert(labels(reading.Statements[0].Statements), qt.DeepEquals, test.wantNested)
			c.Assert(reading.Statements[0].Opaque, qt.Equals, test.wantOpaque)
		})
	}
}

// A statement inside a statement inside a statement is reached.
func TestReader_NestingGoesAllTheWayDown(t *testing.T) {
	c := qt.New(t)

	reading := read(c, "plpgsql", "BEGIN\n  IF a THEN\n    LOOP\n      EXECUTE 'x';\n    END LOOP;\n  END IF;\nEND;")

	c.Assert(labels(reading.Statements), qt.DeepEquals, []string{"if"})
	c.Assert(labels(reading.Statements[0].Statements), qt.DeepEquals, []string{"loop"})
	c.Assert(labels(reading.Statements[0].Statements[0].Statements), qt.DeepEquals, []string{"execute"})
}

// Text that only looks like control flow carries no statements. A CASE
// expression is not control flow: the splitter finds its THEN as it finds a
// CASE statement's, and the fragments of the expression it splits are not
// statements, so a SELECT, a RETURN or a PERFORM that holds one carries
// nothing and a host reads its whole text.
func TestReader_ACaseExpressionCarriesNoStatements(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want routineext.Statement
	}{
		{name: "a SELECT", body: "SELECT CASE WHEN a THEN 1 ELSE 2 END INTO v FROM t;",
			want: routineext.Statement{Kind: routineext.StatementSQL, Label: "raw", SQL: "SELECT CASE WHEN a THEN 1 ELSE 2 END INTO v FROM t;"}},
		{name: "a RETURN", body: "RETURN CASE WHEN a THEN 1 ELSE 2 END;",
			want: routineext.Statement{Kind: routineext.StatementInert, Label: "return", SQL: "RETURN CASE WHEN a THEN 1 ELSE 2 END;"}},
		// Only `<<name>>` is a label. Text in that shape with other operators
		// is no label, so the LOOP after it does not make a loop.
		{name: "brackets in the shape of a label", body: "((x)) LOOP\n  PERFORM 1;\nEND LOOP;",
			want: routineext.Statement{Kind: routineext.StatementSQL, Label: "raw", SQL: "((x)) LOOP\n  PERFORM 1;\nEND LOOP;"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(read(c, "plpgsql", "BEGIN\n"+test.body+"\nEND;").Statements, qt.DeepEquals, []routineext.Statement{test.want})
		})
	}
}

// A body in any other language is one statement, its whole text, and an empty
// body has none. The language is matched in any letter case, as the server
// matches it.
func TestReader_ReadsAnotherLanguageWhole(t *testing.T) {
	for _, test := range []struct {
		name     string
		language string
		body     string
		want     []routineext.Statement
	}{
		{name: "SQL", language: "sql", body: " SELECT value + 1; ",
			want: []routineext.Statement{{Kind: routineext.StatementSQL, Label: "raw", SQL: "SELECT value + 1;"}}},
		{name: "an atomic SQL body", language: "sql", body: "BEGIN ATOMIC\n    INSERT INTO tbl VALUES (a);\nEND",
			want: []routineext.Statement{{Kind: routineext.StatementSQL, Label: "raw", SQL: "BEGIN ATOMIC\n    INSERT INTO tbl VALUES (a);\nEND"}}},
		{name: "PL/pgSQL in any letter case", language: "PLpgSQL", body: "BEGIN\n  EXECUTE 'x';\nEND;",
			want: []routineext.Statement{{Kind: routineext.StatementDynamic, Label: "execute", SQL: "EXECUTE 'x';"}}},
		{name: "no language", language: "", body: "BEGIN\n  EXECUTE 'x';\nEND;",
			want: []routineext.Statement{{Kind: routineext.StatementSQL, Label: "raw", SQL: "BEGIN\n  EXECUTE 'x';\nEND;"}}},
		{name: "an empty body", language: "sql", body: "  ", want: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(read(c, test.language, test.body).Statements, qt.DeepEquals, test.want)
		})
	}
}

// Every target of the PostgreSQL family is served, and every body of a
// request is read, in order.
func TestReader_ReadBodies_HappyPath(t *testing.T) {
	for _, target := range []string{"postgres", "cockroachdb", "yugabytedb", "spanner"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			request := routineext.ReadRequest{Target: target, Bodies: []routineext.Body{
				{Language: "sql", SQL: "SELECT 1"}, {Language: "plpgsql", SQL: "BEGIN RETURN; END;"},
			}}

			result, err := pgroutine.Reader{}.ReadBodies(c.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert([]any{labels(result.Bodies[0].Statements), labels(result.Bodies[1].Statements)}, qt.DeepEquals,
				[]any{[]string{"raw"}, []string{"return"}})
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
		{name: "a target outside the family", ctx: context.Background(), target: "mysql", wantIs: ptaherr.ErrUnsupportedDialect,
			wantErr: `.*the PostgreSQL routine body reader on "mysql"`},
		{name: "no context", target: "postgres", wantIs: routineext.ErrInvalid, wantErr: `.*reading routine bodies requires a context`},
		{name: "canceled", ctx: canceled, target: "postgres", bodies: []routineext.Body{{Language: "sql", SQL: "SELECT 1"}}, wantIs: context.Canceled,
			wantErr: `context canceled`},
		{name: "canceled with no bodies", ctx: canceled, target: "postgres", wantIs: context.Canceled, wantErr: `context canceled`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := pgroutine.Reader{}.ReadBodies(test.ctx, routineext.ReadRequest{Target: test.target,
				Bodies: test.bodies})

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, routineext.ReadResult{})
		})
	}
}
