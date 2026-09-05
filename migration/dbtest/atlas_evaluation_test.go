package dbtest_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/dbtest"
)

// TestParseAtlasTestCases_ForEachOverACollection pins what a collection
// expands to (stokaro/ptah#2866).
//
// Two facts, and the second is the one a reader is most likely to get wrong:
// the instance is named for its 1-based position, and `each.key` is that
// position rather than the element. A fixture whose elements were "0" and "1"
// would agree with both readings, so the elements here are words.
func TestParseAtlasTestCases_ForEachOverACollection(t *testing.T) {
	const document = `
test "schema" "rows" {
  for_each = ["alpha", "beta"]
  exec {
    sql = "SELECT '${each.key}:${each.value}'"
  }
}
`

	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases([]byte(document), "s.test.hcl", dbtest.AtlasTestKindSchema)

	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 2)
	c.Assert(cases[0].Name, qt.Equals, "rows/1")
	c.Assert(cases[1].Name, qt.Equals, "rows/2")

	// The control on the whole feature: each instance carries its OWN statement.
	// Translating once and copying the result would expand the case, name the
	// instances correctly, and run the same SQL twice -- and every assertion
	// above would still pass.
	c.Assert(cases[0].Steps[0].Exec, qt.Equals, "SELECT '0:alpha'")
	c.Assert(cases[1].Steps[0].Exec, qt.Equals, "SELECT '1:beta'")
}

// TestParseAtlasTestCases_ForEachOverAMapping pins the other half of the same
// attribute, which does not behave like the first.
//
// Over a mapping `each.key` is the mapping's key, not a position, and the
// instances run in sorted key order. The keys here are deliberately written out
// of order, so an implementation that preserved the document's order produces a
// different sequence and fails rather than agreeing by luck.
func TestParseAtlasTestCases_ForEachOverAMapping(t *testing.T) {
	const document = `
test "schema" "rows" {
  for_each = {
    zulu  = "z"
    alpha = "a"
    mike  = "m"
  }
  exec {
    sql = "SELECT '${each.key}=${each.value}'"
  }
}
`

	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases([]byte(document), "s.test.hcl", dbtest.AtlasTestKindSchema)

	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 3)
	c.Assert(cases[0].Steps[0].Exec, qt.Equals, "SELECT 'alpha=a'")
	c.Assert(cases[1].Steps[0].Exec, qt.Equals, "SELECT 'mike=m'")
	c.Assert(cases[2].Steps[0].Exec, qt.Equals, "SELECT 'zulu=z'")
}

// TestParseAtlasTestCases_SelfNameIsTheExpandedName is what ties a case's own
// view of itself to the name a report and a name filter use.
//
// `self.name` inside an expanded case answers the instance, not the block's
// label. Answering the label would let a case log or assert against a name no
// report line carries, which is exactly the mismatch a reader uses `self.name`
// to avoid.
func TestParseAtlasTestCases_SelfNameIsTheExpandedName(t *testing.T) {
	const document = `
test "schema" "named" {
  for_each = ["only"]
  exec {
    sql = "SELECT '${self.name}'"
  }
}

test "schema" "plain" {
  exec {
    sql = "SELECT '${self.name}'"
  }
}
`

	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases([]byte(document), "s.test.hcl", dbtest.AtlasTestKindSchema)

	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 2)
	c.Assert(cases[0].Name, qt.Equals, "named/1")
	c.Assert(cases[0].Steps[0].Exec, qt.Equals, "SELECT 'named/1'")

	// The control: a case that does not iterate keeps its written name, so the
	// ordinal is not appended to every case in the file.
	c.Assert(cases[1].Name, qt.Equals, "plain")
	c.Assert(cases[1].Steps[0].Exec, qt.Equals, "SELECT 'plain'")
}

// TestParseAtlasTestCases_VariablesAndDevURL covers the two values that come
// from outside a step.
//
// `var.*` is the file's own declaration; `self.dev_url` is the caller's, and it
// is present only when the caller supplied one.
func TestParseAtlasTestCases_VariablesAndDevURL(t *testing.T) {
	const document = `
variable "name" {
  default = "ada"
}

test "schema" "uses" {
  exec {
    sql = "SELECT '${var.name}' FROM '${self.dev_url}'"
  }
}
`

	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases([]byte(document), "s.test.hcl", dbtest.AtlasTestKindSchema,
		dbtest.WithAtlasTestDevURL("sqlite://dev?mode=memory"))

	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 1)
	c.Assert(cases[0].Steps[0].Exec, qt.Equals, "SELECT 'ada' FROM 'sqlite://dev?mode=memory'")
}

// TestParseAtlasTestCases_FileReadsBesideTheTest covers `file()`.
//
// The happy path proves the argument resolves against the directory holding the
// test rather than the process's working directory, which is why the file is
// written into a temporary directory the test never chdirs into.
func TestParseAtlasTestCases_FileReadsBesideTheTest(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "payload.txt"), []byte("hello-from-file"), 0o600), qt.IsNil)

	const document = `
test "schema" "reads" {
  exec {
    sql = "SELECT '${file("payload.txt")}'"
  }
}
`

	cases, err := dbtest.ParseAtlasTestCases(
		[]byte(document), "s.test.hcl", dbtest.AtlasTestKindSchema, dbtest.WithAtlasTestDir(dir))

	c.Assert(err, qt.IsNil)
	c.Assert(cases[0].Steps[0].Exec, qt.Equals, "SELECT 'hello-from-file'")
}

// TestParseAtlasTestCases_FileRefusesOutsideItsDirectory_FailurePath is the
// other half, and it is a confinement rather than a convenience.
//
// A test file is repository-controlled and evaluated before anything runs, so a
// `file()` that read upward would turn authoring a test into an arbitrary read
// on whatever machine runs the suite.
func TestParseAtlasTestCases_FileRefusesOutsideItsDirectory_FailurePath(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	outside := filepath.Join(dir, "outside.txt")
	c.Assert(os.WriteFile(outside, []byte("secret"), 0o600), qt.IsNil)
	inside := filepath.Join(dir, "tests")
	c.Assert(os.Mkdir(inside, 0o700), qt.IsNil)

	const document = `
test "schema" "escapes" {
  exec {
    sql = "SELECT '${file("../outside.txt")}'"
  }
}
`

	_, err := dbtest.ParseAtlasTestCases(
		[]byte(document), "s.test.hcl", dbtest.AtlasTestKindSchema, dbtest.WithAtlasTestDir(inside))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "reads only inside the directory that holds it")
}

// TestParseAtlasTestCases_SkipIsComputed covers `skip` as an expression rather
// than a literal.
//
// A literal would be enough to make the attribute work and would leave the
// interesting use unreachable: skipping one instance of an expanded case,
// decided from `each` or `var`.
func TestParseAtlasTestCases_SkipIsComputed(t *testing.T) {
	const document = `
test "schema" "rows" {
  for_each = ["run", "skip"]
  skip     = each.value == "skip"
  exec {
    sql = "SELECT 1"
  }
}
`

	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases([]byte(document), "s.test.hcl", dbtest.AtlasTestKindSchema)

	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 2)
	// Both halves: a skip that was always true would satisfy the second
	// assertion alone, and one that was always false the first.
	c.Assert(cases[0].Skip, qt.IsFalse)
	c.Assert(cases[1].Skip, qt.IsTrue)
}

// TestRunTest_ASkippedCaseIsNeitherPassedNorFailed is the report half of the
// same attribute.
//
// Three properties, and no single one of them is the behavior: the case is
// reported, it does not redden the run, and it carries no steps. A skip
// implemented by dropping the case satisfies the second, a skip implemented as
// a pass satisfies the first two, and only the third says the statements never
// reached the database.
func TestRunTest_ASkippedCaseIsNeitherPassedNorFailed(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{
			{
				Name: "skipped",
				Skip: true,
				// Invalid SQL: reaching the database at all fails the run, which
				// is what makes "no steps" an assertion rather than a formality.
				Steps: []dbtest.Step{{Exec: "NOT VALID SQL AT ALL"}},
			},
			{
				Name:  "ran",
				Steps: []dbtest.Step{{Exec: "SELECT 1"}},
			},
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse)
	c.Assert(report.Cases, qt.HasLen, 2)

	c.Assert(report.Cases[0].Skipped, qt.IsTrue)
	c.Assert(report.Cases[0].Passed, qt.IsFalse)
	c.Assert(report.Cases[0].Steps, qt.HasLen, 0)

	// The control: the case beside it still ran, so skipping is selective.
	c.Assert(report.Cases[1].Skipped, qt.IsFalse)
	c.Assert(report.Cases[1].Passed, qt.IsTrue)
}

// TestReport_RepresentsASkippedCaseInEveryFormat keeps the state readable
// wherever a reader consumes the run.
//
// The counts line is the part that matters most: a skipped case counted among
// the passes is a report telling a reader that a check they rely on ran.
func TestReport_RepresentsASkippedCaseInEveryFormat(t *testing.T) {
	c := qt.New(t)

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{
			{Name: "skipped", Skip: true, Steps: []dbtest.Step{{Exec: "SELECT 1"}}},
			{Name: "ran", Steps: []dbtest.Step{{Exec: "SELECT 1"}}},
		},
	})
	c.Assert(err, qt.IsNil)

	text := report.Text()
	c.Assert(text, qt.Contains, `SKIP  case "skipped"`)
	c.Assert(text, qt.Contains, "2 cases, 1 passed, 0 failed, 1 skipped")

	rendered, err := report.JSON()
	c.Assert(err, qt.IsNil)
	var document struct {
		Passed  int `json:"passed"`
		Failed  int `json:"failed"`
		Skipped int `json:"skipped"`
		Cases   []struct {
			Skipped bool `json:"skipped"`
		} `json:"cases"`
	}
	c.Assert(json.Unmarshal([]byte(rendered), &document), qt.IsNil)
	c.Assert(document.Passed, qt.Equals, 1)
	c.Assert(document.Failed, qt.Equals, 0)
	c.Assert(document.Skipped, qt.Equals, 1)
	c.Assert(document.Cases[0].Skipped, qt.IsTrue)
	c.Assert(document.Cases[1].Skipped, qt.IsFalse)

	page, err := report.HTML()
	c.Assert(err, qt.IsNil)
	c.Assert(page, qt.Contains, `<strong class="skip">SKIP</strong>`)
	c.Assert(page, qt.Contains, "1 skipped")
}

// TestParseAtlasTestCases_EvaluationRefusals_FailurePath keeps every new
// construct failing closed, naming the file and line.
//
// Each row is a value that would otherwise reach a statement as something the
// author did not write: a variable resolving to null, an iteration over a value
// that cannot be iterated, a skip that is not a decision.
func TestParseAtlasTestCases_EvaluationRefusals_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     string
	}{
		{
			name:     "a variable with no default",
			document: "variable \"n\" {\n}\ntest \"schema\" \"a\" {\n  exec {\n    sql = \"SELECT 1\"\n  }\n}\n",
			want:     `.*variable "n" has no ` + "`default`" + `.*`,
		},
		{
			name:     "a variable with an unknown attribute",
			document: "variable \"n\" {\n  default = 1\n  nope    = 2\n}\n",
			want:     ".*`variable` does not take \\[nope\\].*",
		},
		{
			name:     "for_each over a scalar",
			document: "test \"schema\" \"a\" {\n  for_each = \"one\"\n  exec {\n    sql = \"SELECT 1\"\n  }\n}\n",
			want:     ".*`for_each` must be a collection or a mapping, got string.*",
		},
		{
			name:     "skip that is not a boolean",
			document: "test \"schema\" \"a\" {\n  skip = \"yes\"\n  exec {\n    sql = \"SELECT 1\"\n  }\n}\n",
			want:     ".*`skip` must be a boolean, got string.*",
		},
		{
			name:     "a dev url no caller supplied",
			document: "test \"schema\" \"a\" {\n  exec {\n    sql = \"SELECT '${self.dev_url}'\"\n  }\n}\n",
			want:     "(?s).*dev_url.*",
		},
		{
			name:     "each outside an iterating case",
			document: "test \"schema\" \"a\" {\n  exec {\n    sql = \"SELECT '${each.value}'\"\n  }\n}\n",
			want:     "(?s).*each.*",
		},
		{
			name:     "an unsupported top-level block",
			document: "locals {\n  x = 1\n}\n",
			want:     ".*only `test` and `variable` blocks are supported.*",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := dbtest.ParseAtlasTestCases(
				[]byte(test.document), "s.test.hcl", dbtest.AtlasTestKindSchema)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestLoadCasesOfKind_FileReadsBesideTheTest drives the loader rather than the
// parser, and it is the test the parser's own coverage did not amount to.
//
// `file()` was bound to a directory derived from the document's name. The
// parser's tests passed a full path and so derived the right one; the loader
// passes the BASENAME, because that is what its diagnostics print, and
// `filepath.Dir` answered "." -- the process's working directory. Every unit
// test passed and `ptah schema test` could not read a file sitting beside the
// suite. Worse than the failure: a working directory that happened to hold the
// same name would have been read instead, silently.
//
// So the assertion has to come through LoadCasesOfKind. Running it from a
// working directory that is NOT the suite is what makes the two directories
// distinguishable at all.
func TestLoadCasesOfKind_FileReadsBesideTheTest(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "payload.txt"), []byte("beside-the-suite"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "a.test.hcl"), []byte(`
test "schema" "reads" {
  exec { sql = "SELECT '${file("payload.txt")}'" }
}
`), 0o600), qt.IsNil)

	cases, err := dbtest.LoadCasesOfKind(dir, dbtest.AtlasTestKindSchema)

	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 1)
	c.Assert(cases[0].Steps[0].Exec, qt.Equals, "SELECT 'beside-the-suite'")
}

// TestParseAtlasTestCases_FileRefusesWithoutADirectory_FailurePath is the
// fail-closed half.
//
// A reader given no directory refuses the call rather than resolving it against
// the process's working directory. The refusal is what keeps the defect above
// from coming back as a wrong file instead of an error: a caller that forgets
// the directory is told so, and nothing is read.
func TestParseAtlasTestCases_FileRefusesWithoutADirectory_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, err := dbtest.ParseAtlasTestCases([]byte(`
test "schema" "reads" {
  exec { sql = "SELECT '${file("payload.txt")}'" }
}
`), "s.test.hcl", dbtest.AtlasTestKindSchema)

	c.Assert(err, qt.ErrorMatches, "(?s).*no directory to read from.*")
}

// TestParseAtlasTestCases_AMisspelledCaseAttributeIsRefused is the defect this
// closes, not merely a rule it states.
//
// The guard compared the COUNT of a block's attributes against the size of the
// allowed set, which was correct only while nothing was allowed. Once
// `for_each`, `skip` and `parallel` joined it, a body carrying fewer attributes
// than that never reached the check: measured, `paralel = true` loaded clean
// and the case ran serially, with the report saying nothing.
//
// A misspelling of a real attribute is the fixture rather than an invented
// name, because that is the mistake an author actually makes and the one whose
// silent acceptance costs them the behavior they asked for.
func TestParseAtlasTestCases_AMisspelledCaseAttributeIsRefused(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     string
	}{
		{
			name:     "one attribute, fewer than the allowed set",
			document: "test \"schema\" \"a\" {\n  paralel = true\n  exec { sql = \"SELECT 1\" }\n}\n",
			want:     `.*` + "`test` takes step blocks and \\[for_each parallel skip\\], not \\[paralel\\]" + `.*`,
		},
		{
			name:     "beside a real one",
			document: "test \"schema\" \"a\" {\n  parallel = true\n  skp      = true\n  exec { sql = \"SELECT 1\" }\n}\n",
			want:     ".*not \\[skp\\].*",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := dbtest.ParseAtlasTestCases(
				[]byte(test.document), "s.test.hcl", dbtest.AtlasTestKindSchema)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestParseAtlasTestCases_AMisspelledCaseAttributeIsRefusedInAPlanCase is the
// same rule for the third kind, and it needs its own test rather than a row.
//
// A parse scoped to one kind drops the blocks of the others before their
// attributes are read, so a plan block examined under AtlasTestKindSchema is
// never checked at all -- which is what the first version of this assertion
// did, and it passed by not looking.
func TestParseAtlasTestCases_AMisspelledCaseAttributeIsRefusedInAPlanCase(t *testing.T) {
	c := qt.New(t)

	_, err := dbtest.ParseAtlasTestCases([]byte(`
test "plan" "a" {
  nope = 1
  apply { url = "file://x" }
}
`), "s.test.hcl", dbtest.AtlasTestKindPlan)

	c.Assert(err, qt.ErrorMatches, ".*not \\[nope\\].*")
}

// TestParseAtlasTestCases_TheRealCaseAttributesStillLoad is the control.
//
// Refusing every attribute would satisfy the table above and delete the feature
// the attributes exist for, so a case using all three has to keep loading.
func TestParseAtlasTestCases_TheRealCaseAttributesStillLoad(t *testing.T) {
	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases([]byte(`
test "schema" "a" {
  for_each = ["one"]
  skip     = false
  parallel = true
  exec { sql = "SELECT 1" }
}
`), "s.test.hcl", dbtest.AtlasTestKindSchema)

	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 1)
	c.Assert(cases[0].Parallel, qt.IsTrue)
	c.Assert(cases[0].Skip, qt.IsFalse)
}

// TestLoadCasesOfKind_ANameIsCheckedInEveryCaseWhateverItsKind closes the half
// of the fail-closed rule the kind filter left open.
//
// A parse scoped to one kind dropped the other kinds' blocks BEFORE reading
// their names, so a misspelled `paralel` or an invented step block in a
// `test "migrate"` case loaded clean under `schema test` -- exit 0, green
// report. The author saw success from one verb and the refusal only from the
// other, or never, if that file is only ever run by the verb that ignores it.
//
// Names only, and the controls below are what keep that narrow: a case this run
// does not execute must not fail the run for a value it alone needs.
func TestLoadCasesOfKind_ANameIsCheckedInEveryCaseWhateverItsKind(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     string
	}{
		{
			name:     "a misspelled attribute in an unselected case",
			document: "test \"migrate\" \"other\" {\n  paralel = true\n  exec { sql = \"SELECT 1\" }\n}\n",
			want:     ".*not \\[paralel\\].*",
		},
		{
			name:     "an invented step block in an unselected case",
			document: "test \"migrate\" \"other\" {\n  frobnicate { sql = \"SELECT 1\" }\n}\n",
			want:     ".*unsupported step \"frobnicate\".*",
		},
		{
			name:     "a plan-only step in an unselected migrate case",
			document: "test \"migrate\" \"other\" {\n  schema { url = \"file://x\" }\n}\n",
			want:     ".*belongs to a `test \"plan\"` case.*",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			dir := t.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "a.test.hcl"), []byte(test.document), 0o600), qt.IsNil)

			_, err := dbtest.LoadCasesOfKind(dir, dbtest.AtlasTestKindSchema)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestLoadCasesOfKind_AnUnselectedCaseIsNotEvaluated is the control that keeps
// the check above from becoming a wider one.
//
// Only names are read. A case of another kind may legitimately need a value
// this run has no way to supply -- `self.dev_url` with no database named, a
// variable another verb's invocation would set -- and failing the run over it
// would make one verb unable to read a directory another verb authored.
func TestLoadCasesOfKind_AnUnselectedCaseIsNotEvaluated(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "a.test.hcl"), []byte(`
test "migrate" "other" {
  for_each = ["a"]
  exec { sql = "SELECT '${self.dev_url}'" }
  cleanup { sql = "SELECT 1" }
}

test "schema" "selected" {
  exec { sql = "SELECT 1" }
}
`), 0o600), qt.IsNil)

	cases, err := dbtest.LoadCasesOfKind(dir, dbtest.AtlasTestKindSchema)

	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 1)
	c.Assert(cases[0].Name, qt.Equals, "selected")
}
