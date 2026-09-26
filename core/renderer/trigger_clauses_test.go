package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

// triggerSpec is the part of a trigger these tests vary.
type triggerSpec struct {
	timing   string
	event    string
	forEach  string
	when     string
	oldTable string
	newTable string
}

func (s triggerSpec) node() *ast.CreateTriggerNode {
	return ast.NewCreateTrigger("t", "x").
		SetTiming(s.timing).
		SetEvent(s.event).
		SetForEach(s.forEach).
		SetWhen(s.when).
		SetReferencing(s.oldTable, s.newTable).
		SetBody("BEGIN SELECT 1; END")
}

// TestRenderCreateTrigger_Clauses_HappyPath writes each clause a target has in
// that target's grammar.
func TestRenderCreateTrigger_Clauses_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		spec    triggerSpec
		want    string
	}{
		{
			name:    "postgres event list with a condition",
			dialect: platform.Postgres,
			spec:    triggerSpec{timing: "BEFORE", event: "INSERT OR UPDATE OF a, b", forEach: "ROW", when: "NEW.a > 0"},
			want:    `CREATE TRIGGER "t" BEFORE INSERT OR UPDATE OF a, b ON "x" FOR EACH ROW WHEN (NEW.a > 0) EXECUTE FUNCTION`,
		},
		{
			name:    "postgres transition tables",
			dialect: platform.Postgres,
			spec:    triggerSpec{timing: "AFTER", event: "UPDATE", forEach: "STATEMENT", oldTable: "o", newTable: "n"},
			want:    `CREATE TRIGGER "t" AFTER UPDATE ON "x" REFERENCING OLD TABLE AS "o" NEW TABLE AS "n" FOR EACH STATEMENT EXECUTE FUNCTION`,
		},
		{
			name:    "postgres truncate",
			dialect: platform.Postgres,
			spec:    triggerSpec{timing: "AFTER", event: "TRUNCATE", forEach: "STATEMENT"},
			want:    `CREATE TRIGGER "t" AFTER TRUNCATE ON "x" FOR EACH STATEMENT EXECUTE FUNCTION`,
		},
		{
			name:    "sqlite update of a column",
			dialect: platform.SQLite,
			spec:    triggerSpec{timing: "BEFORE", event: "UPDATE OF a", forEach: "ROW"},
			want:    `CREATE TRIGGER "t" BEFORE UPDATE OF a ON "x" FOR EACH ROW BEGIN SELECT 1; END;`,
		},
		{
			name:    "sql server separates the events with commas",
			dialect: platform.SQLServer,
			spec:    triggerSpec{timing: "AFTER", event: "INSERT OR DELETE", forEach: "STATEMENT"},
			want:    `CREATE TRIGGER [t] ON [x] AFTER INSERT, DELETE AS BEGIN SELECT 1; END;`,
		},
		{
			name:    "oracle event list with an update column list",
			dialect: platform.Oracle,
			spec:    triggerSpec{timing: "BEFORE", event: "INSERT OR UPDATE OF a", forEach: "ROW"},
			want:    `CREATE TRIGGER t BEFORE INSERT OR UPDATE OF a ON x FOR EACH ROW`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := renderer.RenderSQL(test.dialect, test.spec.node())
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestRenderCreateTrigger_Clauses_FailurePath refuses a clause the target
// cannot create. Rendering the trigger without it would create one that fires
// on other events, on other rows, or without the tables its function reads.
func TestRenderCreateTrigger_Clauses_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		spec    triggerSpec
		wantErr string
	}{
		{
			name:    "mysql event list",
			dialect: platform.MySQL,
			spec:    triggerSpec{timing: "BEFORE", event: "INSERT OR UPDATE", forEach: "ROW"},
			wantErr: `.*trigger "t" fires on several events \(INSERT OR UPDATE\); a trigger here fires on one`,
		},
		{
			name:    "mariadb truncate",
			dialect: platform.MariaDB,
			spec:    triggerSpec{timing: "AFTER", event: "TRUNCATE", forEach: "ROW"},
			wantErr: `.*trigger "t" fires on TRUNCATE, which has no trigger here`,
		},
		{
			name:    "mysql update of",
			dialect: platform.MySQL,
			spec:    triggerSpec{timing: "BEFORE", event: "UPDATE OF a", forEach: "ROW"},
			wantErr: `.*trigger "t" fires on an UPDATE of named columns, which has no trigger here`,
		},
		{
			name:    "mysql when",
			dialect: platform.MySQL,
			spec:    triggerSpec{timing: "BEFORE", event: "UPDATE", forEach: "ROW", when: "NEW.a > 0"},
			wantErr: `.*trigger "t" has a WHEN condition, which has no trigger here`,
		},
		{
			name:    "mysql transition tables",
			dialect: platform.MySQL,
			spec:    triggerSpec{timing: "AFTER", event: "UPDATE", forEach: "ROW", newTable: "n"},
			wantErr: `.*trigger "t" declares transition tables, which have no trigger here`,
		},
		{
			name:    "sqlite event list",
			dialect: platform.SQLite,
			spec:    triggerSpec{timing: "BEFORE", event: "INSERT OR DELETE", forEach: "ROW"},
			wantErr: `.*trigger "t" fires on several events \(INSERT OR DELETE\); a SQLite trigger fires on one`,
		},
		{
			name:    "sqlite truncate",
			dialect: platform.SQLite,
			spec:    triggerSpec{timing: "AFTER", event: "TRUNCATE", forEach: "ROW"},
			wantErr: `.*trigger "t" fires on TRUNCATE, which SQLite does not have`,
		},
		{
			name:    "sqlite postgres when",
			dialect: platform.SQLite,
			spec:    triggerSpec{timing: "BEFORE", event: "UPDATE", forEach: "ROW", when: "NEW.a > 0"},
			wantErr: `.*trigger "t" has a PostgreSQL WHEN condition`,
		},
		{
			name:    "sqlite transition tables",
			dialect: platform.SQLite,
			spec:    triggerSpec{timing: "AFTER", event: "UPDATE", forEach: "ROW", oldTable: "o"},
			wantErr: `.*trigger "t" declares transition tables, which SQLite does not have`,
		},
		{
			name:    "sql server truncate in a list",
			dialect: platform.SQLServer,
			spec:    triggerSpec{timing: "AFTER", event: "INSERT OR TRUNCATE", forEach: "STATEMENT"},
			wantErr: `.*trigger "t" fires on TRUNCATE, which has no trigger on SQL Server`,
		},
		{
			name:    "sql server update of",
			dialect: platform.SQLServer,
			spec:    triggerSpec{timing: "AFTER", event: "UPDATE OF a", forEach: "STATEMENT"},
			wantErr: `.*trigger "t" fires on an UPDATE of named columns; SQL Server fires on every UPDATE`,
		},
		{
			name:    "sql server when",
			dialect: platform.SQLServer,
			spec:    triggerSpec{timing: "AFTER", event: "UPDATE", forEach: "STATEMENT", when: "1 = 1"},
			wantErr: `.*trigger "t" has a PostgreSQL WHEN condition`,
		},
		{
			name:    "sql server transition tables",
			dialect: platform.SQLServer,
			spec:    triggerSpec{timing: "AFTER", event: "UPDATE", forEach: "STATEMENT", oldTable: "o"},
			wantErr: `.*trigger "t" declares transition tables; SQL Server names them inserted and deleted`,
		},
		{
			name:    "oracle truncate",
			dialect: platform.Oracle,
			spec:    triggerSpec{timing: "AFTER", event: "DELETE OR TRUNCATE", forEach: "ROW"},
			wantErr: `.*trigger "t" fires on TRUNCATE, which has no row trigger on Oracle`,
		},
		{
			name:    "oracle postgres when",
			dialect: platform.Oracle,
			spec:    triggerSpec{timing: "BEFORE", event: "UPDATE", forEach: "ROW", when: "NEW.a > 0"},
			wantErr: `.*trigger "t" has a PostgreSQL WHEN condition`,
		},
		{
			name:    "oracle transition tables",
			dialect: platform.Oracle,
			spec:    triggerSpec{timing: "AFTER", event: "UPDATE", forEach: "ROW", newTable: "n"},
			wantErr: `.*trigger "t" declares transition tables, which Oracle does not have`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := renderer.RenderSQL(test.dialect, test.spec.node())
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
