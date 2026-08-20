package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
)

// TestAnalyzeFS_BaselineVersionsComeFromTheRules is the load-bearing half of
// the input declaration.
//
// The list used to be computed here from a hardcoded "does this file carry a
// column rename" check, which was correct while exactly one rule needed the
// starting schema state and would have gone quietly wrong for the second: its
// files would never be read, so its findings could never fire, and nothing
// would say so (stokaro/ptah#1632).
//
// Disabling the one rule that declares the input is what separates the two
// implementations: the rename is still there, so a hardcoded check still asks
// for version 2, and a list assembled from the enabled rules asks for nothing.
func TestAnalyzeFS_BaselineVersionsComeFromTheRules(t *testing.T) {
	tests := []struct {
		name     string
		disabled []string
		want     []int64
	}{
		{
			name: "the rule that declares the input asks for its version",
			want: []int64{2},
		},
		{
			name:     "disabling that rule asks for nothing",
			disabled: []string{"DD101"},
			want:     nil,
		},
		{
			name:     "disabling the whole family asks for nothing",
			disabled: []string{"DD"},
			want:     nil,
		},
		{
			// A rule that reads statement text has nothing to do with the
			// list, so silencing it must not change what is read.
			name:     "disabling a statement-text rule changes nothing",
			disabled: []string{"DS102"},
			want:     []int64{2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := renameBaselineOptions(nil)
			opts.Disabled = test.disabled

			analysis, err := lint.AnalyzeFS(fixture(renameBaselineFS()), opts)

			c.Assert(err, qt.IsNil)
			c.Assert(analysis.BaselineVersions(), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_NativeSurfaceAsksForNoBaseline separates the declaration from
// the surface.
//
// The native surface models a rename as a rename, so the add side is not its
// claim to make and the state that would resolve it is not worth a round trip.
// A declaration that ignored the surface would read a dev database on every
// native run with a rename in it.
func TestAnalyzeFS_NativeSurfaceAsksForNoBaseline(t *testing.T) {
	c := qt.New(t)
	opts := renameBaselineOptions(nil)
	opts.Compatibility = lint.CompatibilityProfile("")

	analysis, err := lint.AnalyzeFS(fixture(renameBaselineFS()), opts)

	c.Assert(err, qt.IsNil)
	c.Assert(analysis.BaselineVersions(), qt.IsNil)
	c.Assert(analysis.UnmetInputs(), qt.HasLen, 0)
}

// TestAnalyzeFS_UnmetInputsNameTheRuleThatWentWithout is the anti-silence half.
//
// A rule that needs the replayed schema and does not get it reports less and
// the run still exits 0 — the failure the issue behind this called the hardest
// kind of gap to notice from CI. Both rows matter: the first is the report, and
// the second is the control that it goes quiet once the input arrives, so an
// implementation that always warned would fail it.
func TestAnalyzeFS_UnmetInputsNameTheRuleThatWentWithout(t *testing.T) {
	tests := []struct {
		name     string
		baseline []lint.BaselineColumn
		want     []lint.UnmetInput
	}{
		{
			name: "no baseline names the rule, the file and the version",
			want: []lint.UnmetInput{{
				Rule:    "DD101",
				Input:   lint.InputBaselineSchema,
				File:    "2_rename.sql",
				Version: 2,
			}},
		},
		{
			name:     "a baseline that answers leaves nothing unmet",
			baseline: notNullBaseline(),
			want:     nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			analysis, err := lint.AnalyzeFS(fixture(renameBaselineFS()), renameBaselineOptions(test.baseline))

			c.Assert(err, qt.IsNil)
			c.Assert(analysis.UnmetInputs(), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_UnmetInputsFollowRuleSelection proves the report is about rules
// that would have run: silencing the rule silences the notice, because an
// analysis is not thinner for a rule the operator switched off.
func TestAnalyzeFS_UnmetInputsFollowRuleSelection(t *testing.T) {
	c := qt.New(t)
	opts := renameBaselineOptions(nil)
	opts.Disabled = []string{"DD101"}

	analysis, err := lint.AnalyzeFS(fixture(renameBaselineFS()), opts)

	c.Assert(err, qt.IsNil)
	c.Assert(analysis.UnmetInputs(), qt.HasLen, 0)
}

// TestRuleInputString pins the wording that reaches an operator, since the
// notice and the two validation refusals all render the input through it.
func TestRuleInputString(t *testing.T) {
	tests := []struct {
		name  string
		input lint.RuleInput
		want  string
	}{
		{name: "statement text is the zero value", input: lint.RuleInput(0), want: "statement text"},
		{name: "baseline schema", input: lint.InputBaselineSchema, want: "baseline schema"},
		{name: "out of range", input: lint.RuleInput(99), want: "unknown input"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.input.String(), qt.Equals, test.want)
		})
	}
}

// TestRegisterRefusesAnInconsistentInputDeclaration is what stops the
// declaration from being a comment.
//
// A rule that asks for the baseline and names no statements would ask for every
// version and resolve nothing; a rule that names statements while declaring the
// text input describes two different needs and leaves a reader to guess which
// one the analysis honors. Both are refused where the rule is registered, so
// neither can reach a run.
func TestRegisterRefusesAnInconsistentInputDeclaration(t *testing.T) {
	tests := []struct {
		name string
		rule lint.Rule
		want string
	}{
		{
			name: "baseline input without subjects",
			rule: lint.Rule{
				Code:      "ZZ901",
				Title:     "test rule",
				Severity:  lint.SeverityWarning,
				CheckFile: func(*lint.File) []lint.Finding { return nil },
				Input:     lint.InputBaselineSchema,
			},
			want: `rule ZZ901 declares the baseline schema input and must set BaselineSubjects ` +
				`to say which statements need it`,
		},
		{
			name: "subjects without the baseline input",
			rule: lint.Rule{
				Code:             "ZZ902",
				Title:            "test rule",
				Severity:         lint.SeverityWarning,
				CheckFile:        func(*lint.File) []lint.Finding { return nil },
				BaselineSubjects: func(*lint.File) []int { return nil },
			},
			want: `rule ZZ902 sets BaselineSubjects but declares the statement text input; ` +
				`set Input to InputBaselineSchema`,
		},
		{
			name: "an input nothing implements",
			rule: lint.Rule{
				Code:      "ZZ903",
				Title:     "test rule",
				Severity:  lint.SeverityWarning,
				CheckFile: func(*lint.File) []lint.Finding { return nil },
				Input:     lint.RuleInput(99),
			},
			want: `rule ZZ903 has unsupported input 99`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(lint.Register(test.rule), qt.ErrorMatches, test.want)
		})
	}
}

// TestExtraRulesRefuseAnInconsistentInputDeclaration covers the other door into
// the registry: a rule handed to one run rather than registered process-wide is
// held to the same declaration.
func TestExtraRulesRefuseAnInconsistentInputDeclaration(t *testing.T) {
	c := qt.New(t)
	opts := renameBaselineOptions(nil)
	opts.ExtraRules = []lint.Rule{{
		Code:      "ZZ904",
		Title:     "test rule",
		Severity:  lint.SeverityWarning,
		CheckFile: func(*lint.File) []lint.Finding { return nil },
		Input:     lint.InputBaselineSchema,
	}}

	_, err := lint.AnalyzeFS(fixture(renameBaselineFS()), opts)

	c.Assert(err, qt.ErrorMatches, `.*rule ZZ904 declares the baseline schema input.*`)
}

// TestAnalyzeFS_ReviewedScopeDecidesWhatIsWorthReading is why
// [lint.Rule.BaselineSubjects] answers in statement indexes rather than yes/no.
//
// A rename in a schema the run does not review produces no finding, so reading
// a dev database for it is a round trip spent to learn nothing, and reporting
// it as unresolved would warn about work the operator excluded on purpose. The
// in-scope row is the control: the same directory under a scope that contains
// the rename still asks, and still reports.
func TestAnalyzeFS_ReviewedScopeDecidesWhatIsWorthReading(t *testing.T) {
	scopedFS := map[string]string{
		"1_base.sql":   "CREATE SCHEMA app;\nCREATE TABLE app.users (id int NOT NULL);",
		"2_rename.sql": "ALTER TABLE app.users RENAME COLUMN id TO oid;",
	}
	tests := []struct {
		name      string
		scope     string
		wantRead  []int64
		wantUnmet int
	}{
		{
			name:      "a scope that excludes the rename asks for nothing",
			scope:     "public",
			wantRead:  nil,
			wantUnmet: 0,
		},
		{
			name:      "a scope that contains it asks, and says so when it goes unanswered",
			scope:     "app",
			wantRead:  []int64{2},
			wantUnmet: 1,
		},
		{
			name:      "an unrestricted run asks too",
			scope:     "",
			wantRead:  []int64{2},
			wantUnmet: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := renameBaselineOptions(nil)
			opts.SchemaScope = test.scope

			analysis, err := lint.AnalyzeFS(fixture(scopedFS), opts)

			c.Assert(err, qt.IsNil)
			c.Assert(analysis.BaselineVersions(), qt.DeepEquals, test.wantRead)
			c.Assert(analysis.UnmetInputs(), qt.HasLen, test.wantUnmet)
		})
	}
}
