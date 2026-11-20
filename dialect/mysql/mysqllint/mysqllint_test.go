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
		codes = append(codes, rule.Name)
		targets[rule.Name] = rule.Targets
	}
	c.Assert(codes, qt.DeepEquals, []string{
		"locking/blocking-alter-table", "side-effect/inline-reference", "rewrite/remove-enum-value", "rewrite/reorder-enum-values", "rewrite/insert-enum-value", "rewrite/widen-enum", "rewrite/remove-set-member", "rewrite/reorder-set-members", "rewrite/insert-set-member", "rewrite/widen-set",
		"rewrite/alter-column-type", "rewrite/in-place-column-change", "locking/add-foreign-key", "rewrite/add-primary-key", "rewrite/drop-primary-key", "locking/add-fulltext-index", "locking/add-spatial-index", "rewrite/convert-charset", "rewrite/replace-primary-key", "rewrite/change-engine", "rewrite/change-partitioning",
		"rewrite/add-stored-generated-column", "locking/add-auto-increment-column", "rewrite/modify-generated-column", "locking/add-check", "locking/enforce-check", "destructive/drop-system-versioning", "rewrite/set-not-null",
		"online/unproven",
	})
	c.Assert([]any{targets["locking/blocking-alter-table"], targets["locking/enforce-check"], targets["destructive/drop-system-versioning"], targets["online/unproven"]}, qt.DeepEquals,
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
			[]lintext.Finding{{Name: "online/unproven", File: 0, Statement: 0, Message: message}}},
		{"a down file", file("1.down.sql", false, "ALTER TABLE t DROP COLUMN c, ALGORITHM=INPLACE, LOCK=SHARED;\n"),
			[]lintext.Finding{{Name: "online/unproven", File: 0, Statement: 0, Message: message}}},
		{"online", file("1.up.sql", true, "ALTER TABLE t ADD INDEX i (c), ALGORITHM=INPLACE, LOCK=NONE;\n"), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := lintext.CheckRequest{Target: lintext.Target{Name: "mysql"}, Names: []string{"online/unproven"}, Files: []lintext.File{test.file}}

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
	request := lintext.CheckRequest{Target: lintext.Target{Name: "mysql"}, Names: []string{"rewrite/alter-column-type"},
		Files: []lintext.File{{Name: "2.up.sql", Version: 2, Up: true,
			Statements: lintscan.Split("ALTER TABLE t MODIFY a BIGINT;\n", lintscan.ForDialect("mysql"))}}}
	subjects := []lintext.BaselineSubject{{Name: "rewrite/alter-column-type", File: 0, Statements: []int{0}}}

	first, err := mysqllint.Service{}.CheckMigrations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(first, qt.DeepEquals, lintext.CheckResult{Complete: true, BaselineSubjects: subjects})

	request.Baseline = &lintext.Baseline{Columns: []lintext.BaselineColumn{{Version: 2, Table: "t", Name: "a", ColumnType: "int"}}}
	second, err := mysqllint.Service{}.CheckMigrations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(second.BaselineSubjects, qt.DeepEquals, subjects)
	c.Assert(second.Findings, qt.HasLen, 1)
	c.Assert([]any{second.Findings[0].Name, second.Findings[0].Statement, second.Findings[0].Subjects}, qt.DeepEquals,
		[]any{"rewrite/alter-column-type", 0, []lintext.Subject{{Kind: lintext.SubjectColumn, Name: "a", Parent: "t", DataType: "bigint"}}})
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
		{"a code the owner does not publish", context.Background(), []string{"locking/create-index"}, `.*no rule of code PG101`},
		{"cancellation", canceled, []string{"online/unproven"}, `context canceled`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := mysqllint.Service{}.CheckMigrations(test.ctx, lintext.CheckRequest{Names: test.codes,
				Files: []lintext.File{file("1.up.sql", true, "ALTER TABLE t ADD COLUMN c int;\n")}})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, lintext.CheckResult{})
		})
	}
}
