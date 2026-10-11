package mysqllint_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/dialect/mysql/mysqllint"
	"ptah.run/internal/lintscan"
)

// file is a migration file of the given direction holding sql, split the way
// the bundled MySQL owner splits it.
func file(name string, up bool, sql string) lintext.File {
	return lintext.File{Name: name, Up: up, Down: !up, Statements: lintscan.Split(sql, lintscan.ForDialect("mysql"))}
}

// Rules publishes the owner's families in code order, each rule valid as
// data and naming the engines it judges, and a call's rules share nothing
// with the next call's.
func TestRules(t *testing.T) {
	c := qt.New(t)
	rules := mysqllint.Rules()
	rules[0].Targets[0] = "postgres"

	again := mysqllint.Rules()
	codes := make([]string, 0, len(again))
	targets := make(map[string][]string, len(again))
	for _, rule := range again {
		c.Assert(rule.Validate(), qt.IsNil)
		codes = append(codes, rule.Code)
		targets[rule.Code] = rule.Targets
	}
	c.Assert(codes, qt.DeepEquals, []string{
		"MY101", "MY102", "MY110", "MY111", "MY112", "MY113", "MY120", "MY121", "MY122", "MY123",
		"MY130", "MY130P", "MY131", "MY132", "MY133", "MY134", "MY135", "MY136", "MY137", "MY138", "MY139",
		"MY140", "MY141", "MY143", "MY144", "MY145", "MY146", "MY147",
		"ON102",
	})
	c.Assert([]any{targets["MY101"], targets["MY145"], targets["MY146"], targets["ON102"]}, qt.DeepEquals,
		[]any{[]string{"mysql", "mariadb"}, []string{"mysql"}, []string{"mariadb"}, []string{"mysql", "mariadb"}})
}

// ON102 reports a statement that does not ask the server to apply it online,
// in either direction, and leaves one that does.
func TestService_CheckMigrations_Online(t *testing.T) {
	const message = "this statement does not ask the server to apply it without blocking writes; " +
		"add ALGORITHM=INPLACE, LOCK=NONE (or ALGORITHM=INSTANT) so the server refuses what it " +
		"cannot do online, or apply it in a window where a table copy is acceptable"
	for _, test := range []struct {
		name string
		file lintext.File
		want []lintext.Finding
	}{
		{"an up file", file("1.up.sql", true, "ALTER TABLE t ADD COLUMN c int;\nALTER TABLE t ADD COLUMN d int, ALGORITHM=INSTANT;\n"),
			[]lintext.Finding{{Code: "ON102", File: 0, Statement: 0, Message: message}}},
		{"a down file", file("1.down.sql", false, "ALTER TABLE t DROP COLUMN c, ALGORITHM=INPLACE, LOCK=SHARED;\n"),
			[]lintext.Finding{{Code: "ON102", File: 0, Statement: 0, Message: message}}},
		{"online", file("1.up.sql", true, "ALTER TABLE t ADD INDEX i (c), ALGORITHM=INPLACE, LOCK=NONE;\n"), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := lintext.CheckRequest{Target: lintext.Target{Name: "mysql"}, Codes: []string{"ON102"}, Files: []lintext.File{test.file}}

			result, err := mysqllint.Service{}.CheckMigrations(c.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, lintext.CheckResult{Complete: true, Findings: test.want})
			c.Assert(lintext.ValidateCheck(request, result), qt.IsNil)
		})
	}
}

// A type change the starting state shows copying the table is reported by
// MY130 on the second of the two calls, and named as a baseline subject on
// both.
func TestService_CheckMigrations_BaselineRule(t *testing.T) {
	c := qt.New(t)
	request := lintext.CheckRequest{Target: lintext.Target{Name: "mysql"}, Codes: []string{"MY130"},
		Files: []lintext.File{{Name: "2.up.sql", Version: 2, Up: true,
			Statements: lintscan.Split("ALTER TABLE t MODIFY a BIGINT;\n", lintscan.ForDialect("mysql"))}}}
	subjects := []lintext.BaselineSubject{{Code: "MY130", File: 0, Statements: []int{0}}}

	first, err := mysqllint.Service{}.CheckMigrations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(first, qt.DeepEquals, lintext.CheckResult{Complete: true, BaselineSubjects: subjects})

	request.Baseline = &lintext.Baseline{Columns: []lintext.BaselineColumn{{Version: 2, Table: "t", Name: "a", ColumnType: "int"}}}
	second, err := mysqllint.Service{}.CheckMigrations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(second.BaselineSubjects, qt.DeepEquals, subjects)
	c.Assert(second.Findings, qt.HasLen, 1)
	c.Assert([]any{second.Findings[0].Code, second.Findings[0].Statement, second.Findings[0].Subjects}, qt.DeepEquals,
		[]any{"MY130", 0, []lintext.Subject{{Kind: lintext.SubjectColumn, Name: "a", Parent: "t", DataType: "bigint"}}})
	c.Assert(lintext.ValidateCheck(request, second), qt.IsNil)
}

func TestService_CheckMigrations_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name    string
		ctx     context.Context
		codes   []string
		wantErr string
	}{
		{"a code the owner does not publish", context.Background(), []string{"PG101"}, `.*no rule of code PG101`},
		{"cancellation", canceled, []string{"ON102"}, `context canceled`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := mysqllint.Service{}.CheckMigrations(test.ctx, lintext.CheckRequest{Codes: test.codes,
				Files: []lintext.File{file("1.up.sql", true, "ALTER TABLE t ADD COLUMN c int;\n")}})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, lintext.CheckResult{})
		})
	}
}
