package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/parser"
)

// transactSQLTrigger is the part of a parsed T-SQL trigger these tests
// compare.
type transactSQLTrigger struct {
	Name    string
	Table   string
	Timing  string
	Event   string
	ForEach string
	Body    string
	Replace bool
}

// parseTransactSQLTriggers reads sql as SQL Server and returns every trigger
// in it, failing on any other statement.
func parseTransactSQLTriggers(c *qt.C, sql string) []transactSQLTrigger {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.SQLServer)).Parse()
	c.Assert(err, qt.IsNil)
	triggers := make([]transactSQLTrigger, 0, len(statements.Statements))
	for _, statement := range statements.Statements {
		trigger, ok := statement.(*ast.CreateTriggerNode)
		c.Assert(ok, qt.IsTrue, qt.Commentf("parsed %T", statement))
		triggers = append(triggers, transactSQLTrigger{
			Name: trigger.Name, Table: trigger.Table, Timing: trigger.Timing, Event: trigger.Event,
			ForEach: trigger.ForEach, Body: trigger.Body, Replace: trigger.Replace,
		})
	}
	return triggers
}

// TestParseCreateTrigger_TransactSQL_HappyPath reads the T-SQL trigger header
// SQL Server 2025 accepts, with the table before the timing. FOR is AFTER, as
// sys.triggers records it; the events keep the order written, joined by OR;
// every trigger fires once per statement; and a schema in the trigger's name
// is the table's, so the name is kept without it.
func TestParseCreateTrigger_TransactSQL_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want transactSQLTrigger
	}{
		{
			name: "one event, the body to the end of the input",
			sql:  `CREATE TRIGGER trg ON t AFTER INSERT AS SET NOCOUNT ON;`,
			want: transactSQLTrigger{Name: "trg", Table: "t", Timing: "AFTER", Event: "INSERT", ForEach: "STATEMENT", Body: "SET NOCOUNT ON;"},
		},
		{
			name: "FOR is AFTER",
			sql:  `CREATE TRIGGER trg ON t FOR DELETE AS SET NOCOUNT ON;`,
			want: transactSQLTrigger{Name: "trg", Table: "t", Timing: "AFTER", Event: "DELETE", ForEach: "STATEMENT", Body: "SET NOCOUNT ON;"},
		},
		{
			name: "INSTEAD OF, with a block body",
			sql:  `CREATE TRIGGER trg ON t INSTEAD OF DELETE AS BEGIN INSERT INTO dbo.audit_log(msg) VALUES(N'd'); END;`,
			want: transactSQLTrigger{
				Name: "trg", Table: "t", Timing: "INSTEAD OF", Event: "DELETE", ForEach: "STATEMENT",
				Body: "BEGIN INSERT INTO dbo.audit_log(msg) VALUES(N'd'); END",
			},
		},
		{
			name: "every event, in the order written",
			sql:  `CREATE TRIGGER trg ON t AFTER UPDATE, DELETE,INSERT AS SET NOCOUNT ON;`,
			want: transactSQLTrigger{Name: "trg", Table: "t", Timing: "AFTER", Event: "UPDATE OR DELETE OR INSERT", ForEach: "STATEMENT", Body: "SET NOCOUNT ON;"},
		},
		{
			name: "lower-case keywords",
			sql:  `create trigger trg on t for insert, delete as begin set nocount on; end`,
			want: transactSQLTrigger{Name: "trg", Table: "t", Timing: "AFTER", Event: "INSERT OR DELETE", ForEach: "STATEMENT", Body: "begin set nocount on; end"},
		},
		{
			name: "bracketed names, as ptah db read prints them",
			sql:  `CREATE TRIGGER [trg_multi] ON [t] AFTER INSERT, UPDATE AS BEGIN INSERT INTO dbo.audit_log(msg) VALUES(N'm'); END;`,
			want: transactSQLTrigger{
				Name: "[trg_multi]", Table: "[t]", Timing: "AFTER", Event: "INSERT OR UPDATE", ForEach: "STATEMENT",
				Body: "BEGIN INSERT INTO dbo.audit_log(msg) VALUES(N'm'); END",
			},
		},
		{
			name: "a schema in the name is the table's",
			sql:  `CREATE TRIGGER app.trg ON app.t AFTER INSERT AS SET NOCOUNT ON;`,
			want: transactSQLTrigger{Name: "trg", Table: "app.t", Timing: "AFTER", Event: "INSERT", ForEach: "STATEMENT", Body: "SET NOCOUNT ON;"},
		},
		{
			name: "a bracketed schema in the name",
			sql:  `CREATE TRIGGER [app].[trg] ON [app].[t] AFTER INSERT AS SET NOCOUNT ON;`,
			want: transactSQLTrigger{Name: "[trg]", Table: "[app].[t]", Timing: "AFTER", Event: "INSERT", ForEach: "STATEMENT", Body: "SET NOCOUNT ON;"},
		},
		{
			name: "the default schema in the name, in another case, of an unqualified table",
			sql:  `CREATE TRIGGER DBO.trg ON t AFTER INSERT AS SET NOCOUNT ON;`,
			want: transactSQLTrigger{Name: "trg", Table: "t", Timing: "AFTER", Event: "INSERT", ForEach: "STATEMENT", Body: "SET NOCOUNT ON;"},
		},
		{
			name: "EXECUTE AS CALLER is the default and reads as nothing",
			sql:  `CREATE TRIGGER trg ON t WITH EXECUTE AS CALLER AFTER INSERT AS SET NOCOUNT ON;`,
			want: transactSQLTrigger{Name: "trg", Table: "t", Timing: "AFTER", Event: "INSERT", ForEach: "STATEMENT", Body: "SET NOCOUNT ON;"},
		},
		{
			name: "CREATE OR ALTER replaces",
			sql:  `CREATE OR ALTER TRIGGER trg ON t AFTER INSERT AS SET NOCOUNT ON;`,
			want: transactSQLTrigger{Name: "trg", Table: "t", Timing: "AFTER", Event: "INSERT", ForEach: "STATEMENT", Body: "SET NOCOUNT ON;", Replace: true},
		},
		{
			name: "comments between the header's words",
			sql:  "CREATE TRIGGER trg /* name */ ON t -- table\n AFTER /* timing */ INSERT -- event\n AS SET NOCOUNT ON;",
			want: transactSQLTrigger{Name: "trg", Table: "t", Timing: "AFTER", Event: "INSERT", ForEach: "STATEMENT", Body: "SET NOCOUNT ON;"},
		},
		{
			name: "a body of several statements runs to GO",
			sql:  "CREATE TRIGGER trg ON t AFTER INSERT AS\nSET NOCOUNT ON;\nUPDATE t SET a = 1;\nGO\n",
			want: transactSQLTrigger{
				Name: "trg", Table: "t", Timing: "AFTER", Event: "INSERT", ForEach: "STATEMENT",
				Body: "SET NOCOUNT ON;\nUPDATE t SET a = 1;",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(parseTransactSQLTriggers(c, test.sql), qt.DeepEquals, []transactSQLTrigger{test.want})
		})
	}
}

// TestParseCreateTrigger_TransactSQL_EndsEachBody reads a document of several
// triggers. A block body ends at the END that closes it, so the triggers
// `ptah db read` prints one after another, with no GO between them, are read
// one by one; a GO ends a body that is not a block.
func TestParseCreateTrigger_TransactSQL_EndsEachBody(t *testing.T) {
	c := qt.New(t)
	const document = "CREATE TRIGGER [trg_instead] ON [t] INSTEAD OF DELETE AS BEGIN INSERT INTO dbo.audit_log(msg) VALUES(N'd'); END;\n\n\n" +
		"CREATE TRIGGER [trg_multi] ON [t] AFTER INSERT, UPDATE AS BEGIN SET NOCOUNT ON; END;\n" +
		"CREATE TRIGGER trg ON t FOR DELETE AS\nSET NOCOUNT ON;\nGO\n" +
		"CREATE TRIGGER trg_last ON t AFTER UPDATE AS SET NOCOUNT ON;\n"

	c.Assert(parseTransactSQLTriggers(c, document), qt.DeepEquals, []transactSQLTrigger{
		{
			Name: "[trg_instead]", Table: "[t]", Timing: "INSTEAD OF", Event: "DELETE", ForEach: "STATEMENT",
			Body: "BEGIN INSERT INTO dbo.audit_log(msg) VALUES(N'd'); END",
		},
		{Name: "[trg_multi]", Table: "[t]", Timing: "AFTER", Event: "INSERT OR UPDATE", ForEach: "STATEMENT", Body: "BEGIN SET NOCOUNT ON; END"},
		{Name: "trg", Table: "t", Timing: "AFTER", Event: "DELETE", ForEach: "STATEMENT", Body: "SET NOCOUNT ON;"},
		{Name: "trg_last", Table: "t", Timing: "AFTER", Event: "UPDATE", ForEach: "STATEMENT", Body: "SET NOCOUNT ON;"},
	})
}

// TestParseCreateTrigger_TransactSQL_ReadsTheExampleOnTheSQLPage binds the
// SQL Server trigger the SQL schema page prints to what the parser reads from
// it: the page has to print the example, and the example has to read as the
// page describes it, FOR as AFTER and the body through its GO.
func TestParseCreateTrigger_TransactSQL_ReadsTheExampleOnTheSQLPage(t *testing.T) {
	c := qt.New(t)
	const example = "CREATE TRIGGER trg ON t FOR INSERT, UPDATE AS\nSET NOCOUNT ON;\nUPDATE t SET a = a WHERE 1 = 0;\nGO\n"
	c.Assert(readSQLSchemaPage(c), qt.Contains, example)

	c.Assert(parseTransactSQLTriggers(c, example), qt.DeepEquals, []transactSQLTrigger{{
		Name: "trg", Table: "t", Timing: "AFTER", Event: "INSERT OR UPDATE", ForEach: "STATEMENT",
		Body: "SET NOCOUNT ON;\nUPDATE t SET a = a WHERE 1 = 0;",
	}})
}

// TestParseCreateTrigger_TransactSQL_FailurePath refuses what SQL Server 2025
// refuses, with the server's message number where it has one, rather than
// reading part of the header.
func TestParseCreateTrigger_TransactSQL_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "IF NOT EXISTS",
			sql:     `CREATE TRIGGER IF NOT EXISTS trg ON t AFTER INSERT AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 16: IF after CREATE TRIGGER: SQL Server takes no IF NOT EXISTS on a trigger; write CREATE OR ALTER TRIGGER to replace one that exists`,
		},
		{
			name:    "the timing before the table",
			sql:     `CREATE TRIGGER trg AFTER INSERT ON t AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 20: expected ON after trigger name trg: expected 'ON', got 'AFTER'`,
		},
		{
			name:    "BEFORE",
			sql:     `CREATE TRIGGER trg ON t BEFORE INSERT AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 25: expected FOR, AFTER or INSTEAD OF in trigger trg, got Identifier "BEFORE"`,
		},
		{
			name:    "INSTEAD without OF",
			sql:     `CREATE TRIGGER trg ON t INSTEAD DELETE AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 33: expected OF after INSTEAD in trigger trg: expected 'OF', got 'DELETE'`,
		},
		{
			name:    "TRUNCATE",
			sql:     `CREATE TRIGGER trg ON t AFTER TRUNCATE AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 31: expected INSERT, UPDATE or DELETE as an event of trigger trg, got Identifier "TRUNCATE"`,
		},
		{
			name:    "events joined by OR",
			sql:     `CREATE TRIGGER trg ON t AFTER INSERT OR DELETE AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 38: expected AS before the body of trigger trg: expected 'AS', got 'OR'`,
		},
		{
			name:    "UPDATE OF a column",
			sql:     `CREATE TRIGGER trg ON t AFTER UPDATE OF a AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 38: expected AS before the body of trigger trg: expected 'AS', got 'OF'`,
		},
		{
			name:    "a list ending in a comma",
			sql:     `CREATE TRIGGER trg ON t AFTER INSERT, AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 39: expected INSERT, UPDATE or DELETE as an event of trigger trg, got Identifier "AS"`,
		},
		{
			name:    "an event written twice",
			sql:     `CREATE TRIGGER trg ON t AFTER INSERT, INSERT AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 39: trigger trg names the event INSERT twice, which SQL Server refuses with Msg 1034`,
		},
		{
			name: "WITH APPEND",
			sql:  `CREATE TRIGGER trg ON t FOR INSERT WITH APPEND AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 36: trigger trg states WITH APPEND, which SQL Server 2025 refuses with Msg 195, ` +
				`'APPEND' is not a recognized option; remove it`,
		},
		{
			name:    "no AS",
			sql:     `CREATE TRIGGER trg ON t AFTER INSERT SET NOCOUNT ON;`,
			wantErr: `line 1, column 38: expected AS before the body of trigger trg: expected 'AS', got 'SET'`,
		},
		{
			name:    "no body",
			sql:     "CREATE TRIGGER trg ON t AFTER INSERT AS\nGO\n",
			wantErr: `line 2, column 1: expected the body of trigger trg after AS`,
		},
		{
			name:    "an empty WITH list",
			sql:     `CREATE TRIGGER trg ON t WITH AFTER INSERT AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 30: expected a trigger option`,
		},
		{
			name: "a schema in the name other than the table's",
			sql:  `CREATE TRIGGER app.trg ON dbo.t AFTER INSERT AS SET NOCOUNT ON;`,
			wantErr: `trigger app.trg is in schema app and its table dbo.t in schema dbo; ` +
				`SQL Server refuses a trigger outside its table's schema with Msg 2103`,
		},
		{
			name: "a schema in the name other than the default schema of an unqualified table",
			sql:  `CREATE TRIGGER app.trg ON t AFTER INSERT AS SET NOCOUNT ON;`,
			wantErr: `trigger app.trg is in schema app and its table t in schema dbo; ` +
				`SQL Server refuses a trigger outside its table's schema with Msg 2103`,
		},
		{
			name:    "a database prefix on the name",
			sql:     `CREATE TRIGGER db.dbo.trg ON dbo.t AFTER INSERT AS SET NOCOUNT ON;`,
			wantErr: `trigger name db.dbo.trg has a database prefix, which SQL Server refuses with Msg 166; write \[schema.\]name`,
		},
		{
			name:    "OR REPLACE",
			sql:     `CREATE OR REPLACE TRIGGER trg ON t AFTER INSERT AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 19: unsupported CREATE OR REPLACE target: TRIGGER`,
		},
		{
			name:    "TEMP",
			sql:     `CREATE TEMP TRIGGER trg ON t AFTER INSERT AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 13: unsupported CREATE TEMP target: TRIGGER`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.SQLServer)).Parse()
			c.Assert(err, qt.ErrorMatches, `(?s).*`+test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestParseCreateTrigger_TransactSQL_RefusesWhatPtahDoesNotModel refuses, by
// name, a clause SQL Server 2025 takes and records and Ptah's trigger has no
// place for. Read as nothing, the clause would be dropped from the schema, and
// the trigger would compare equal to one without it.
func TestParseCreateTrigger_TransactSQL_RefusesWhatPtahDoesNotModel(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name: "WITH ENCRYPTION",
			sql:  `CREATE TRIGGER trg ON t WITH ENCRYPTION AFTER INSERT AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 30: trigger trg states WITH ENCRYPTION: SQL Server keeps no readable definition of an encrypted trigger, ` +
				`so Ptah could neither compare its body nor write it back; remove it, or keep the trigger out of the schema`,
		},
		{
			name: "ENCRYPTION after EXECUTE AS CALLER",
			sql:  `CREATE TRIGGER trg ON t WITH EXECUTE AS CALLER, ENCRYPTION AFTER INSERT AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 49: trigger trg states WITH ENCRYPTION: SQL Server keeps no readable definition of an encrypted trigger, ` +
				`so Ptah could neither compare its body nor write it back; remove it, or keep the trigger out of the schema`,
		},
		{
			name: "EXECUTE AS OWNER",
			sql:  `CREATE TRIGGER trg ON t WITH EXECUTE AS OWNER AFTER INSERT AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 30: trigger trg states WITH EXECUTE AS OWNER: Ptah does not model the principal a trigger runs as, ` +
				`so it could neither write it nor compare it back; remove it, or keep the trigger out of the schema`,
		},
		{
			name: "EXECUTE AS SELF",
			sql:  `CREATE TRIGGER trg ON t WITH EXECUTE AS SELF FOR INSERT AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 30: trigger trg states WITH EXECUTE AS SELF: Ptah does not model the principal a trigger runs as, ` +
				`so it could neither write it nor compare it back; remove it, or keep the trigger out of the schema`,
		},
		{
			name: "EXECUTE AS a user",
			sql:  `CREATE TRIGGER trg ON t WITH EXECUTE AS 'auditor' INSTEAD OF INSERT AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 30: trigger trg states WITH EXECUTE AS 'auditor': Ptah does not model the principal a trigger runs as, ` +
				`so it could neither write it nor compare it back; remove it, or keep the trigger out of the schema`,
		},
		{
			name: "NATIVE_COMPILATION",
			sql:  `CREATE TRIGGER trg ON t WITH NATIVE_COMPILATION, SCHEMABINDING AFTER INSERT AS BEGIN ATOMIC WITH (LANGUAGE = N'us_english') END;`,
			wantErr: `line 1, column 30: trigger trg states WITH NATIVE_COMPILATION: Ptah can neither write the option nor compare it back; ` +
				`remove it, or keep the trigger out of the schema`,
		},
		{
			name: "NOT FOR REPLICATION",
			sql:  `CREATE TRIGGER trg ON t AFTER UPDATE NOT FOR REPLICATION AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 38: trigger trg states NOT FOR REPLICATION: Ptah does not model whether replication fires a trigger, ` +
				`so it could neither write the setting nor compare it back; remove it, or keep the trigger out of the schema`,
		},
		{
			name: "a DDL trigger",
			sql:  `CREATE TRIGGER trg ON DATABASE FOR CREATE_TABLE AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 23: CREATE TRIGGER trg ON DATABASE is a DDL or logon trigger, which Ptah does not model; ` +
				`a schema declares DML triggers on tables alone, so keep this trigger out of the schema`,
		},
		{
			name: "a logon trigger",
			sql:  `CREATE TRIGGER trg ON ALL SERVER FOR LOGON AS SET NOCOUNT ON;`,
			wantErr: `line 1, column 23: CREATE TRIGGER trg ON ALL SERVER is a DDL or logon trigger, which Ptah does not model; ` +
				`a schema declares DML triggers on tables alone, so keep this trigger out of the schema`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.SQLServer)).Parse()
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*`+test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
