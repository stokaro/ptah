package lintkit_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintscan"
)

// statements splits sql the way the bundled PostgreSQL owner does.
func statements(sql string) []lintext.Statement {
	return lintscan.Split(sql, lintscan.ForDialect("postgres"))
}

// wordRule reports a statement holding word, as a text rule of code.
func wordRule(code, word string, appliesToDown bool) lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{Code: code, Title: code, Severity: lintext.Warning, Targets: []string{"postgres"}, AppliesToDown: appliesToDown},
		Statement: func(_ *lintkit.File, statement *lintext.Statement) (bool, string) {
			return slices.Contains(statement.Words, word), "holds " + word
		},
	}
}

// renameRule wants the starting state for every RENAME, and reports the type
// the renamed column carried where the state has it.
func renameRule() lintkit.Rule {
	renames := func(file *lintkit.File) []int {
		var found []int
		for i, statement := range file.Statements {
			if slices.Contains(statement.Words, "RENAME") {
				found = append(found, i)
			}
		}
		return found
	}
	return lintkit.Rule{
		Rule:             lintext.Rule{Code: "XY301", Title: "t", Severity: lintext.Warning, Targets: []string{"postgres"}, Input: lintext.InputBaselineSchema},
		BaselineSubjects: renames,
		File: func(file *lintkit.File) []lintext.Violation {
			var violations []lintext.Violation
			for _, i := range renames(file) {
				column, ok := file.Baseline.Column("T", "A")
				if ok {
					violations = append(violations, lintext.Violation{Statement: i, Message: "a was " + column.DataType,
						Subjects: []lintext.Subject{{Kind: lintext.SubjectColumn, Name: "a", Parent: "t"}}})
				}
			}
			return violations
		},
	}
}

// renameRequest is a run over an up and a down file of version 2, an up file
// without a version, and an up file of version 3 with no rename, for codes.
func renameRequest(codes ...string) lintext.CheckRequest {
	return lintext.CheckRequest{Target: lintext.Target{Name: "postgres"}, Codes: codes, Files: []lintext.File{
		{Name: "2.up.sql", Version: 2, Up: true, Statements: statements("DROP TABLE x;\nALTER TABLE t RENAME COLUMN a TO b;\n")},
		{Name: "2.down.sql", Version: 2, Down: true, Statements: statements("DROP TABLE y;\nALTER TABLE t RENAME COLUMN b TO a;\n")},
		{Name: "loose.up.sql", Up: true, Statements: statements("ALTER TABLE t RENAME COLUMN c TO d;\n")},
		{Name: "3.up.sql", Version: 3, Up: true, Statements: statements("SELECT 1;\n")},
	}}
}

// A statement rule judges up files, and down files where it applies to down;
// a file rule sees every file and reports what it finds there.
func TestRules_CheckMigrations_StatementAndFileRules(t *testing.T) {
	c := qt.New(t)
	fileRule := lintkit.Rule{Rule: lintext.Rule{Code: "XY103", Title: "t", Severity: lintext.Info, Targets: []string{"postgres"}},
		File: func(file *lintkit.File) []lintext.Violation {
			return []lintext.Violation{{Statement: -1, Message: file.Name}}
		}}
	rules := lintkit.Rules{wordRule("XY101", "DROP", false), wordRule("XY102", "DROP", true), fileRule}

	result, err := rules.CheckMigrations(c.Context(), renameRequest("XY101", "XY102", "XY103"))

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, lintext.CheckResult{Complete: true, Findings: []lintext.Finding{
		{Code: "XY101", File: 0, Statement: 0, Message: "holds DROP"},
		{Code: "XY102", File: 0, Statement: 0, Message: "holds DROP"},
		{Code: "XY103", File: 0, Statement: -1, Message: "2.up.sql"},
		{Code: "XY102", File: 1, Statement: 0, Message: "holds DROP"},
		{Code: "XY103", File: 1, Statement: -1, Message: "2.down.sql"},
		{Code: "XY103", File: 2, Statement: -1, Message: "loose.up.sql"},
		{Code: "XY103", File: 3, Statement: -1, Message: "3.up.sql"},
	}})
	c.Assert(lintext.ValidateCheck(renameRequest("XY101", "XY102", "XY103"), result), qt.IsNil)
}

// A baseline rule names its statements on an up file with a version where it
// wants any, on every call; the state a file rule reads is the one its file's version
// starts from, and it resolves nothing on the call that carries none.
func TestRules_CheckMigrations_BaselineRule(t *testing.T) {
	subjects := []lintext.BaselineSubject{{Code: "XY301", File: 0, Statements: []int{1}}}
	for _, test := range []struct {
		name     string
		baseline *lintext.Baseline
		want     []lintext.Finding
	}{
		{"no baseline", nil, nil},
		{"the version's state, which the down file of the version reads too", &lintext.Baseline{Columns: []lintext.BaselineColumn{
			{Version: 2, Table: "t", Name: "a", DataType: "integer"}}}, []lintext.Finding{
			{Code: "XY301", File: 0, Statement: 1, Message: "a was integer", Subjects: []lintext.Subject{{Kind: lintext.SubjectColumn, Name: "a", Parent: "t"}}},
			{Code: "XY301", File: 1, Statement: 1, Message: "a was integer", Subjects: []lintext.Subject{{Kind: lintext.SubjectColumn, Name: "a", Parent: "t"}}},
		}},
		{"another version's state", &lintext.Baseline{Columns: []lintext.BaselineColumn{{Version: 1, Table: "t", Name: "a", DataType: "integer"}}}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := renameRequest("XY301")
			request.Baseline = test.baseline

			result, err := lintkit.Rules{renameRule()}.CheckMigrations(c.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, lintext.CheckResult{Complete: true, Findings: test.want, BaselineSubjects: subjects})
			c.Assert(lintext.ValidateCheck(request, result), qt.IsNil)
		})
	}
}

func TestRules_CheckMigrations_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name    string
		ctx     context.Context
		codes   []string
		wantErr string
	}{
		{"a code no rule carries", context.Background(), []string{"XY999"}, `.*no rule of code XY999`},
		{"cancellation", canceled, []string{"XY101"}, `context canceled`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := lintkit.Rules{wordRule("XY101", "DROP", false)}.CheckMigrations(test.ctx, renameRequest(test.codes...))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, lintext.CheckResult{})
		})
	}
}

func TestRules_Validate_FailurePath(t *testing.T) {
	both := wordRule("XY101", "DROP", false)
	both.File = func(*lintkit.File) []lintext.Violation { return nil }
	neither := wordRule("XY101", "DROP", false)
	neither.Statement = nil
	withoutSubjects := renameRule()
	withoutSubjects.BaselineSubjects = nil
	textWithSubjects := wordRule("XY101", "DROP", false)
	textWithSubjects.BaselineSubjects = func(*lintkit.File) []int { return nil }
	invalid := wordRule("xy101", "DROP", false)
	for _, test := range []struct {
		name    string
		rules   lintkit.Rules
		wantErr string
	}{
		{"both functions", lintkit.Rules{both}, `.*rule XY101 must set exactly one of Statement and File`},
		{"no function", lintkit.Rules{neither}, `.*rule XY101 must set exactly one of Statement and File`},
		{"a baseline rule naming no subjects", lintkit.Rules{withoutSubjects}, `.*rule XY301 sets BaselineSubjects exactly when its input reads a baseline`},
		{"a text rule naming subjects", lintkit.Rules{textWithSubjects}, `.*rule XY101 sets BaselineSubjects exactly when its input reads a baseline`},
		{"two rules of one code", lintkit.Rules{wordRule("XY101", "DROP", false), wordRule("XY101", "TRUNCATE", false)}, `.*two rules of code XY101`},
		{"a rule that does not validate", lintkit.Rules{invalid}, `.*rule code "xy101" must start.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.rules.Validate(), qt.ErrorIs, lintext.ErrInvalid)
			c.Assert(test.rules.Validate(), qt.ErrorMatches, test.wantErr)
		})
	}
}

// Published is the rules as data, sharing no slice with them.
func TestRules_Published(t *testing.T) {
	c := qt.New(t)
	rules := lintkit.Rules{wordRule("XY101", "DROP", true), renameRule()}

	published := rules.Published()
	published[0].Targets[0] = "mysql"

	c.Assert(rules.Published(), qt.DeepEquals, []lintext.Rule{
		{Code: "XY101", Title: "XY101", Severity: lintext.Warning, Targets: []string{"postgres"}, AppliesToDown: true},
		{Code: "XY301", Title: "t", Severity: lintext.Warning, Targets: []string{"postgres"}, Input: lintext.InputBaselineSchema},
	})
}
