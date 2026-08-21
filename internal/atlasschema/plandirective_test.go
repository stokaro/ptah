package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestParsePlanDirectiveAcceptsEverySpellingOfALine covers the two ways one
// directive is written down.
//
// An operator moving a directive from a migration file to `--directive` copies
// the whole line, comment marker included; one reading the flag's help types
// the body. Both have to mean the directive, and both have to normalize to the
// same stored form, or a plan file would record which spelling produced it.
func TestParsePlanDirectiveAcceptsEverySpellingOfALine(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "body", raw: "atlas:txmode none", want: "atlas:txmode none"},
		{name: "whole line", raw: "-- atlas:txmode none", want: "atlas:txmode none"},
		{name: "no space after the marker", raw: "--atlas:txmode none", want: "atlas:txmode none"},
		{name: "surrounding whitespace", raw: "   atlas:txmode none  ", want: "atlas:txmode none"},
		{name: "inner whitespace", raw: "atlas:txmode    none", want: "atlas:txmode none"},
		{name: "file mode", raw: "atlas:txmode file", want: "atlas:txmode file"},
		{name: "nolint bare", raw: "atlas:nolint", want: "atlas:nolint"},
		{name: "nolint with selectors", raw: "-- atlas:nolint destructive DS103", want: "atlas:nolint destructive DS103"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			directive, err := atlasschema.ParsePlanDirective(test.raw)

			c.Assert(err, qt.IsNil)
			c.Assert(string(directive), qt.Equals, test.want)
			// The stored form is the body; the line is how it is written down.
			c.Assert(directive.Line(), qt.Equals, "-- "+test.want)
		})
	}
}

// TestParsePlanDirectiveRefusesWhatNothingActsOn is the half that keeps the
// flag from writing decoration.
//
// A directive Ptah writes into a plan file and no subsystem reads is worse
// than a refused one: the reviewer approves an instruction, and the run
// ignores it. Each refusal here also has to say what to write instead, which
// is why the supported list is asserted alongside the reason.
func TestParsePlanDirectiveRefusesWhatNothingActsOn(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "empty",
			raw:  "",
			want: `--directive: the directive is empty; supported directives are .*`,
		},
		{
			name: "marker only",
			raw:  "--",
			want: `--directive: the directive is empty; supported directives are .*`,
		},
		{
			name: "outside the namespace",
			raw:  "txmode none",
			want: `--directive "txmode none": a plan directive is written in the atlas namespace; .*`,
		},
		{
			name: "ptah family",
			raw:  "+ptah no_transaction",
			want: `--directive "\+ptah no_transaction": a plan directive is written in the atlas namespace; .*`,
		},
		{
			name: "nothing honors it",
			raw:  "atlas:checkpoint",
			want: `--directive "atlas:checkpoint": Ptah writes only directives something acts on, ` +
				`and nothing acts on this one; .*`,
		},
		{
			// The key is recognized and the VALUE is refused, so the answer is
			// the value's, not the generic one: an operator who wrote `all`
			// needs to be told to write `file`, not that the directive is
			// unknown.
			name: "recognized key, refused value",
			raw:  "atlas:txmode all",
			want: `--directive "atlas:txmode all": txmode "all" is not allowed in file directive ` +
				`"--directive". Use "file" instead`,
		},
		{
			name: "recognized key, nonsense value",
			raw:  "atlas:txmode sideways",
			want: `--directive "atlas:txmode sideways": .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			directive, err := atlasschema.ParsePlanDirective(test.raw)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(string(directive), qt.Equals, "")
		})
	}
}

// TestParsePlanDirectiveRefusalNamesEverySupportedDirective proves the list in
// the refusal is the list the parser enforces, rather than a second copy of it
// that can go stale.
func TestParsePlanDirectiveRefusalNamesEverySupportedDirective(t *testing.T) {
	c := qt.New(t)

	_, err := atlasschema.ParsePlanDirective("atlas:frobnicate")

	c.Assert(err, qt.IsNotNil)
	c.Assert(atlasschema.SupportedPlanDirectives, qt.Not(qt.HasLen), 0)
	for _, supported := range atlasschema.SupportedPlanDirectives {
		c.Assert(err.Error(), qt.Contains, supported)
	}
}

func TestPlanDirectivesRefusesASecondTransactionMode(t *testing.T) {
	tests := []struct {
		name string
		raw  []string
		want string
	}{
		{
			name: "contradicting",
			raw:  []string{"atlas:txmode none", "atlas:txmode file"},
			want: `--directive "atlas:txmode file": the plan already sets a transaction mode, and it has one`,
		},
		{
			// Repeating the same mode is refused too. There is no winner to
			// pick wrong, but an operator who wrote it twice believes one of
			// the lines is doing something the other is not.
			name: "repeated",
			raw:  []string{"atlas:txmode none", "-- atlas:txmode none"},
			want: `--directive "-- atlas:txmode none": the plan already sets a transaction mode, and it has one`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			directives, err := atlasschema.PlanDirectives(test.raw)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(directives, qt.IsNil)
		})
	}
}

// TestPlanDirectivesAcceptsRepeatedNoLint separates the rule from the flag: it
// is one transaction mode that is unique, not one directive.
func TestPlanDirectivesAcceptsRepeatedNoLint(t *testing.T) {
	c := qt.New(t)

	directives, err := atlasschema.PlanDirectives([]string{
		"atlas:nolint destructive",
		"atlas:nolint DS103",
		"atlas:txmode none",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(directives, qt.HasLen, 3)
}

// TestPlanDirectiveHeaderWritesTheBlockTheReaderHonors pins the shape of the
// header, not just its content.
//
// The Atlas directive family is read in the unbroken comment run that starts
// the body and nowhere after it, so the blank line that closes the block is
// load-bearing: without it the first statement joins the comment run, and with
// it in the wrong place the directives leave it.
func TestPlanDirectiveHeaderWritesTheBlockTheReaderHonors(t *testing.T) {
	c := qt.New(t)

	header := atlasschema.PlanDirectiveHeader([]atlasschema.PlanDirective{
		"atlas:txmode none",
		"atlas:nolint destructive",
	})

	c.Assert(header, qt.Equals, "-- atlas:txmode none\n-- atlas:nolint destructive\n\n")
}

func TestPlanDirectiveHeaderIsEmptyWithoutDirectives(t *testing.T) {
	c := qt.New(t)

	c.Assert(atlasschema.PlanDirectiveHeader(nil), qt.Equals, "")
	c.Assert(atlasschema.PlanDirectiveHeader(make([]atlasschema.PlanDirective, 0)), qt.Equals, "")
}

// TestPlanTxModeReadsTheHeaderAndNothingBelowIt is the placement rule, read
// from the plan side.
//
// Both answers matter. A directive in the header decides how the plan is
// executed; the identical line below a statement decides nothing, because it
// is read after the statement it claims to govern. Ptah honoring the second
// one would make it disagree with every other reader of the same family.
func TestPlanTxModeReadsTheHeaderAndNothingBelowIt(t *testing.T) {
	const statement = "CREATE TABLE t (id INTEGER PRIMARY KEY);\n"
	tests := []struct {
		name      string
		migration string
		want      migrator.MigrationFileTxMode
	}{
		{
			name:      "first line",
			migration: "-- atlas:txmode none\n\n" + statement,
			want:      migrator.MigrationFileTxModeNone,
		},
		{
			name:      "second comment line",
			migration: "-- planned by hand\n-- atlas:txmode none\n\n" + statement,
			want:      migrator.MigrationFileTxModeNone,
		},
		{
			name:      "file mode",
			migration: "-- atlas:txmode file\n\n" + statement,
			want:      migrator.MigrationFileTxModeFile,
		},
		{
			name:      "below the statement",
			migration: statement + "-- atlas:txmode none\n",
			want:      migrator.MigrationFileTxModeUnspecified,
		},
		{
			name:      "no directive",
			migration: statement,
			want:      migrator.MigrationFileTxModeUnspecified,
		},
		{
			// A directive this layer does not own is not this layer's to
			// report, and not this layer's to refuse either: `schema plan
			// lint` reads it out of the same text.
			name:      "another family's directive",
			migration: "-- atlas:nolint destructive\n\n" + statement,
			want:      migrator.MigrationFileTxModeUnspecified,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			mode, err := atlasschema.PlanTxMode("plan.hcl", test.migration)

			c.Assert(err, qt.IsNil)
			c.Assert(mode, qt.Equals, test.want)
		})
	}
}

// TestPlanTxModeRefusesAnUnreadableValueInThePlan closes the read side of the
// no-silent-drop rule: a plan whose header says something about execution that
// cannot be read must not execute as though it said nothing.
func TestPlanTxModeRefusesAnUnreadableValueInThePlan(t *testing.T) {
	c := qt.New(t)

	mode, err := atlasschema.PlanTxMode(
		"hand.plan.hcl", "-- atlas:txmode all\n\nCREATE TABLE t (id INTEGER PRIMARY KEY);\n")

	c.Assert(err, qt.ErrorMatches, `txmode "all" is not allowed in file directive "hand.plan.hcl".*`)
	c.Assert(mode, qt.Equals, migrator.MigrationFileTxModeUnspecified)
}
