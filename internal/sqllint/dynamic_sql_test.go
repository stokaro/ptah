package sqllint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/sqllint"
)

// TestDynamicSQL_TheBoundaryIsReportedWhereItIs is the first thing in Ptah that
// reads a parsed routine body.
//
// stokaro/ptah#1270 asks "does dynamic SQL introduce an analysis boundary that
// should be surfaced?", and the answer needed no new parsing: PL/pgSQL's
// EXECUTE is already its own statement Kind. What was missing was a consumer --
// `ast.PostgresRoutineBody.Statements` was produced by internal/parser and read
// by nothing.
//
// The rows that matter are the ones that DO NOT report. A rule that fired on
// every routine, or on every dialect, would pass a fixture set containing only
// the first row.
//
// Both PostgreSQL node types are here because a routine reaches the linter as
// one of two: a procedure as PostgresRoutineNode, a function as
// CreateFunctionNode with its body in RoutineBody. Covering one and calling the
// other unparsed is the mistake this file previously recorded as a fact.
func TestDynamicSQL_TheBoundaryIsReportedWhereItIs(t *testing.T) {
	const executes = "CREATE PROCEDURE p() AS $$\nBEGIN\n" +
		"EXECUTE 'TRUNCATE t';\nEND;\n$$ LANGUAGE plpgsql;"

	tests := []struct {
		name    string
		sql     string
		dialect string
		want    int
		why     string
	}{
		{
			name:    "a procedure that composes SQL",
			sql:     executes,
			dialect: platform.Postgres,
			want:    1,
			why:     "EXECUTE is its own Kind, so the boundary is decidable from the statement list",
		},
		{
			name: "a procedure that does not",
			sql: "CREATE PROCEDURE p() AS $$\nBEGIN\n" +
				"PERFORM pg_notify('done', 'x');\nEND;\n$$ LANGUAGE plpgsql;",
			dialect: platform.Postgres,
			want:    0,
			why:     "the control: a routine body alone is not a boundary, only an EXECUTE in it is",
		},
		{
			name: "two EXECUTEs are two boundaries",
			sql: "CREATE PROCEDURE p() AS $$\nBEGIN\n" +
				"EXECUTE 'TRUNCATE a';\nEXECUTE 'TRUNCATE b';\nEND;\n$$ LANGUAGE plpgsql;",
			dialect: platform.Postgres,
			want:    2,
			why:     "each is a separate place the analysis stops, and each gets its own position",
		},
		{
			// A function arrives as a DIFFERENT node than a procedure --
			// CreateFunctionNode rather than PostgresRoutineNode -- and its
			// body is parsed into RoutineBody by attachPostgresFunctionBody.
			// Reading only the procedure node left every function unexamined
			// while looking like a parser limitation, which is what an earlier
			// version of this row asserted and got wrong.
			name: "a function composing SQL",
			sql: "CREATE FUNCTION f() RETURNS void AS $$\nBEGIN\n" +
				"EXECUTE 'TRUNCATE t';\nEND;\n$$ LANGUAGE plpgsql;",
			dialect: platform.Postgres,
			want:    1,
			why:     "the body is parsed for a function too, on the other node",
		},
		{
			name: "a function that does not",
			sql: "CREATE FUNCTION f() RETURNS void AS $$\nBEGIN\n" +
				"PERFORM pg_notify('done', 'x');\nEND;\n$$ LANGUAGE plpgsql;",
			dialect: platform.Postgres,
			want:    0,
			why:     "the control for the row above: the node type is not what makes a boundary",
		},
		{
			name:    "a dialect with no execute Kind",
			sql:     executes,
			dialect: platform.MySQL,
			want:    0,
			why:     "MySQL routine statements carry no execute classification, so dynamic SQL is indistinguishable from raw",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := sqllint.LintSource(
				sqllint.Source{Name: "routine.sql", SQL: test.sql},
				sqllint.Options{Dialect: test.dialect},
			)

			c.Assert(err, qt.IsNil)
			// Filtered rather than disabled: SQL001 and SQL002 report that the
			// file could not be analyzed and are refused as --disable
			// selectors, so a test that wants only the boundary rule selects it
			// here (stokaro/ptah#1270).
			c.Assert(dynamicSQLFindings(findings), qt.HasLen, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestDynamicSQL_TheFindingPointsAtTheExecute pins the position.
//
// A routine statement carries no offset of its own -- it is a Kind and its raw
// text -- so the finding locates itself by searching that text in the source. A
// rule reporting the routine's own line instead would still look right in a
// one-line fixture, which is why this one puts the EXECUTE on line 3.
func TestDynamicSQL_TheFindingPointsAtTheExecute(t *testing.T) {
	c := qt.New(t)

	findings, err := sqllint.LintSource(
		sqllint.Source{Name: "routine.sql", SQL: "CREATE PROCEDURE p() AS $$\nBEGIN\n" +
			"    EXECUTE 'TRUNCATE t';\nEND;\n$$ LANGUAGE plpgsql;"},
		sqllint.Options{Dialect: platform.Postgres},
	)

	c.Assert(err, qt.IsNil)
	// Selected rather than assumed to be alone: the same routine also reports
	// that the linter does not model it, and the parse-path codes can no longer
	// be disabled to hide that (stokaro/ptah#1270).
	boundary := dynamicSQLFindings(findings)
	c.Assert(boundary, qt.HasLen, 1, qt.Commentf("findings: %#v", findings))
	c.Assert(boundary[0].Line, qt.Equals, 3)
	c.Assert(boundary[0].Column, qt.Equals, 5)
	c.Assert(boundary[0].Severity, qt.Equals, sqllint.SeverityInfo,
		qt.Commentf("a routine that composes SQL is doing something legitimate; this reports a limit, not a defect"))
}

// dynamicSQLFindings keeps the boundary rule's findings.
func dynamicSQLFindings(findings []sqllint.Finding) []sqllint.Finding {
	kept := make([]sqllint.Finding, 0, len(findings))
	for _, finding := range findings {
		if finding.Rule == sqllint.RuleDynamicSQL {
			kept = append(kept, finding)
		}
	}
	return kept
}

// TestDynamicSQL_TheBoundaryIsFoundAtAnyDepth pins that the rule does not care
// how deeply the EXECUTE sits.
//
// A routine that composes SQL inside a condition is the ordinary shape, since a
// condition is usually what the composition is for. Before the body model
// carried nesting, an EXECUTE at the top of a body reported SQL003 and the same
// EXECUTE one level in reported nothing: the boundary was there and the rule
// that exists to name it was silent (stokaro/ptah#2393).
func TestDynamicSQL_TheBoundaryIsFoundAtAnyDepth(t *testing.T) {
	tests := []struct {
		name string
		body string
		want int
	}{
		{name: "at the top of the body", body: "EXECUTE 'a';", want: 1},
		{name: "inside IF", body: "IF a THEN\n  EXECUTE 'a';\nEND IF;", want: 1},
		{
			name: "inside the ELSE branch",
			body: "IF a THEN\n  PERFORM 1;\nELSE\n  EXECUTE 'a';\nEND IF;", want: 1,
		},
		{
			name: "inside an ELSIF branch",
			body: "IF a THEN\n  PERFORM 1;\nELSIF b THEN\n  EXECUTE 'a';\nEND IF;", want: 1,
		},
		{name: "inside LOOP", body: "LOOP\n  EXECUTE 'a';\nEND LOOP;", want: 1},
		{
			name: "inside FOR ... LOOP",
			body: "FOR r IN SELECT 1 LOOP\n  EXECUTE 'a';\nEND LOOP;", want: 1,
		},
		{
			name: "inside WHILE ... LOOP",
			body: "WHILE a LOOP\n  EXECUTE 'a';\nEND LOOP;", want: 1,
		},
		{
			name: "two levels down",
			body: "IF a THEN\n  LOOP\n    EXECUTE 'a';\n  END LOOP;\nEND IF;", want: 1,
		},
		{
			name: "twice at different depths",
			body: "EXECUTE 'a';\nIF b THEN\n  EXECUTE 'c';\nEND IF;", want: 2,
		},
		{
			// The control. Without it, reporting every nested statement would
			// pass every row above.
			name: "a conditional that composes nothing",
			body: "IF a THEN\n  PERFORM pg_notify('x', 'y');\nEND IF;", want: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := sqllint.LintSource(
				sqllint.Source{
					Name: "routine.sql",
					SQL:  "CREATE PROCEDURE p() AS $$\nBEGIN\n" + test.body + "\nEND;\n$$ LANGUAGE plpgsql;",
				},
				sqllint.Options{Dialect: platform.Postgres},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(dynamicSQLFindings(findings), qt.HasLen, test.want,
				qt.Commentf("findings: %#v", findings))
		})
	}
}
