package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// parsedTrigger is the part of a parsed trigger these tests compare.
type parsedTrigger struct {
	Event    string
	ForEach  string
	When     string
	OldTable string
	NewTable string
	Function string
}

func parseOneTrigger(c *qt.C, dialect, sql string) parsedTrigger {
	statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	trigger, ok := statements.Statements[0].(*ast.CreateTriggerNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("parsed %T", statements.Statements[0]))
	return parsedTrigger{
		Event:    trigger.Event,
		ForEach:  trigger.ForEach,
		When:     trigger.When,
		OldTable: trigger.OldTable,
		NewTable: trigger.NewTable,
		Function: trigger.FunctionName,
	}
}

// TestParseCreateTrigger_PostgresEventForms_HappyPath reads every event list,
// level, condition and transition-table form PostgreSQL 18 accepts, including
// the statements stokaro/ptah#3674 reports refused and the spelling
// pg_get_triggerdef prints back.
func TestParseCreateTrigger_PostgresEventForms_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want parsedTrigger
	}{
		{
			name: "insert or update",
			sql:  `CREATE TRIGGER t BEFORE INSERT OR UPDATE ON x FOR EACH ROW EXECUTE FUNCTION f();`,
			want: parsedTrigger{Event: "INSERT OR UPDATE", ForEach: "ROW", Function: "f"},
		},
		{
			name: "truncate",
			sql:  `CREATE TRIGGER t AFTER TRUNCATE ON x FOR EACH STATEMENT EXECUTE FUNCTION f();`,
			want: parsedTrigger{Event: "TRUNCATE", ForEach: "STATEMENT", Function: "f"},
		},
		{
			name: "every event, in the order written",
			sql:  `CREATE TRIGGER t AFTER TRUNCATE OR DELETE OR UPDATE OR INSERT ON x FOR EACH STATEMENT EXECUTE FUNCTION f();`,
			want: parsedTrigger{Event: "TRUNCATE OR DELETE OR UPDATE OR INSERT", ForEach: "STATEMENT", Function: "f"},
		},
		{
			name: "lower-case keywords",
			sql:  `create trigger t before insert or delete on x for each row execute function f();`,
			want: parsedTrigger{Event: "INSERT OR DELETE", ForEach: "ROW", Function: "f"},
		},
		{
			name: "update of a column list, then another event",
			sql:  `CREATE TRIGGER t BEFORE UPDATE OF b,a OR DELETE ON x FOR EACH ROW EXECUTE FUNCTION f();`,
			want: parsedTrigger{Event: "UPDATE OF b, a OR DELETE", ForEach: "ROW", Function: "f"},
		},
		{
			name: "update of a quoted column keeps its quotes",
			sql:  `CREATE TRIGGER t BEFORE UPDATE OF "Total", b ON x FOR EACH ROW EXECUTE FUNCTION f();`,
			want: parsedTrigger{Event: `UPDATE OF "Total", b`, ForEach: "ROW", Function: "f"},
		},
		{
			name: "for statement without each",
			sql:  `CREATE TRIGGER t AFTER DELETE ON x FOR STATEMENT EXECUTE PROCEDURE f();`,
			want: parsedTrigger{Event: "DELETE", ForEach: "STATEMENT", Function: "f"},
		},
		{
			name: "no for clause is a row trigger",
			sql:  `CREATE TRIGGER t AFTER DELETE ON x EXECUTE FUNCTION f();`,
			want: parsedTrigger{Event: "DELETE", ForEach: "ROW", Function: "f"},
		},
		{
			name: "when",
			sql: `CREATE TRIGGER t BEFORE UPDATE ON x FOR EACH ROW
				WHEN (NEW.total IS DISTINCT FROM OLD.total AND new.a > 0) EXECUTE FUNCTION f();`,
			want: parsedTrigger{
				Event: "UPDATE", ForEach: "ROW", Function: "f",
				When: "NEW.total IS DISTINCT FROM OLD.total AND new.a > 0",
			},
		},
		{
			name: "when with a parenthesis in a string",
			sql:  `CREATE TRIGGER t BEFORE INSERT ON x FOR EACH ROW WHEN ((new.a = 1) AND new.s <> ')') EXECUTE FUNCTION f();`,
			want: parsedTrigger{Event: "INSERT", ForEach: "ROW", Function: "f", When: "(new.a = 1) AND new.s <> ')'"},
		},
		{
			name: "when as pg_get_triggerdef prints it",
			sql: `CREATE TRIGGER t BEFORE UPDATE ON public.x FOR EACH ROW
				WHEN (((new.total IS DISTINCT FROM old.total) AND (new.a > 0))) EXECUTE FUNCTION f();`,
			want: parsedTrigger{
				Event: "UPDATE", ForEach: "ROW", Function: "f",
				When: "((new.total IS DISTINCT FROM old.total) AND (new.a > 0))",
			},
		},
		{
			name: "referencing both tables, AS optional",
			sql: `CREATE TRIGGER t AFTER UPDATE ON x REFERENCING OLD TABLE oldrows NEW TABLE AS newrows
				FOR EACH STATEMENT EXECUTE FUNCTION f();`,
			want: parsedTrigger{Event: "UPDATE", ForEach: "STATEMENT", Function: "f", OldTable: "oldrows", NewTable: "newrows"},
		},
		{
			name: "referencing new before old",
			sql: `CREATE TRIGGER t AFTER UPDATE ON x REFERENCING NEW TABLE AS n OLD TABLE AS o
				FOR EACH ROW EXECUTE FUNCTION f();`,
			want: parsedTrigger{Event: "UPDATE", ForEach: "ROW", Function: "f", OldTable: "o", NewTable: "n"},
		},
		{
			name: "referencing one table",
			sql:  `CREATE TRIGGER t AFTER INSERT ON x REFERENCING NEW TABLE AS newrows FOR EACH STATEMENT EXECUTE FUNCTION f();`,
			want: parsedTrigger{Event: "INSERT", ForEach: "STATEMENT", Function: "f", NewTable: "newrows"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(parseOneTrigger(c, platform.Postgres, test.sql), qt.DeepEquals, test.want)
		})
	}
}

// TestParseCreateTrigger_PostgresEventForms_FailurePath refuses a clause that
// is malformed rather than reading part of it.
func TestParseCreateTrigger_PostgresEventForms_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "an event that is not one",
			sql:     `CREATE TRIGGER t BEFORE SELECT ON x FOR EACH ROW EXECUTE FUNCTION f();`,
			wantErr: `(?s).*expected trigger event.*`,
		},
		{
			name:    "a list ending in OR",
			sql:     `CREATE TRIGGER t BEFORE INSERT OR ON x FOR EACH ROW EXECUTE FUNCTION f();`,
			wantErr: `(?s).*expected trigger event.*`,
		},
		{
			name:    "update of with a trailing comma",
			sql:     `CREATE TRIGGER t BEFORE UPDATE OF a, ON x FOR EACH ROW EXECUTE FUNCTION f();`,
			wantErr: `(?s).*expected ON after trigger event.*`,
		},
		{
			name:    "the old table named twice",
			sql:     `CREATE TRIGGER t AFTER UPDATE ON x REFERENCING OLD TABLE a OLD TABLE b FOR EACH STATEMENT EXECUTE FUNCTION f();`,
			wantErr: `(?s).*REFERENCING names the OLD transition table twice.*`,
		},
		{
			name:    "referencing naming nothing",
			sql:     `CREATE TRIGGER t AFTER UPDATE ON x REFERENCING FOR EACH STATEMENT EXECUTE FUNCTION f();`,
			wantErr: `(?s).*expected OLD TABLE or NEW TABLE after REFERENCING.*`,
		},
		{
			name:    "referencing a row alias",
			sql:     `CREATE TRIGGER t AFTER UPDATE ON x REFERENCING OLD AS o FOR EACH ROW EXECUTE FUNCTION f();`,
			wantErr: `(?s).*expected TABLE after REFERENCING OLD.*`,
		},
		{
			name:    "when without parentheses",
			sql:     `CREATE TRIGGER t BEFORE UPDATE ON x FOR EACH ROW WHEN new.a > 0 EXECUTE FUNCTION f();`,
			wantErr: `(?s).*trigger WHEN condition.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestParseCreateTrigger_OtherDialectsKeepTheirWhenInTheBody shows that WHEN
// and REFERENCING are PostgreSQL's only on a PostgreSQL-family read. SQLite
// writes a WHEN without parentheses and Oracle a REFERENCING of row aliases;
// both stay in the body text those renderers write back.
func TestParseCreateTrigger_OtherDialectsKeepTheirWhenInTheBody(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		sql      string
		wantBody string
	}{
		{
			name:     "sqlite when",
			dialect:  platform.SQLite,
			sql:      `CREATE TRIGGER t BEFORE UPDATE OF a ON x FOR EACH ROW WHEN NEW.a > 0 BEGIN SELECT 1; END;`,
			wantBody: `(?s)WHEN NEW\.a > 0\s+BEGIN.*END`,
		},
		{
			name:     "oracle referencing",
			dialect:  platform.Oracle,
			sql:      `CREATE TRIGGER t BEFORE UPDATE ON x REFERENCING OLD AS o FOR EACH ROW BEGIN NULL; END;`,
			wantBody: `(?s)REFERENCING OLD AS o\s+FOR EACH ROW\s+BEGIN.*END`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			trigger, ok := statements.Statements[0].(*ast.CreateTriggerNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(trigger.When, qt.Equals, "")
			c.Assert(trigger.OldTable, qt.Equals, "")
			c.Assert(trigger.Body, qt.Matches, test.wantBody)
		})
	}
}
