package dbtest_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/dbtest"
)

func TestParseCases_MultiDocument(t *testing.T) {
	c := qt.New(t)
	// Two ---separated documents must both contribute their cases; the second
	// one must not be silently dropped.
	doc := "cases:\n" +
		"  - name: first\n" +
		"    steps:\n" +
		"      - exec: SELECT 1\n" +
		"---\n" +
		"cases:\n" +
		"  - name: second\n" +
		"    steps:\n" +
		"      - exec: SELECT 1\n"

	cases, err := dbtest.ParseCases([]byte(doc))
	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 2)
	c.Assert(cases[0].Name, qt.Equals, "first")
	c.Assert(cases[1].Name, qt.Equals, "second")
}

func TestParseCases_Valid(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want []dbtest.Case
	}{
		{
			name: "empty document",
			yaml: "",
			want: nil,
		},
		{
			name: "single exec step",
			yaml: "cases:\n" +
				"  - name: create table\n" +
				"    steps:\n" +
				"      - name: create\n" +
				"        exec: CREATE TABLE t (id INTEGER)\n",
			want: []dbtest.Case{{
				Name:  "create table",
				Steps: []dbtest.Step{{Name: "create", Exec: "CREATE TABLE t (id INTEGER)"}},
			}},
		},
		{
			name: "migrate, apply schema, assert row_count, scalar and error_contains",
			yaml: "cases:\n" +
				"  - name: full flow\n" +
				"    steps:\n" +
				"      - name: migrate\n" +
				"        migrate_to: latest\n" +
				"      - name: apply desired schema\n" +
				"        apply_schema: true\n" +
				"      - name: count\n" +
				"        assert:\n" +
				"          query: SELECT id FROM t\n" +
				"          row_count: 2\n" +
				"      - name: scalar\n" +
				"        assert:\n" +
				"          query: SELECT name FROM t LIMIT 1\n" +
				"          scalar: widget\n" +
				"      - name: error\n" +
				"        assert:\n" +
				"          query: SELECT * FROM missing\n" +
				"          error_contains: missing\n",
			want: []dbtest.Case{{
				Name: "full flow",
				Steps: []dbtest.Step{
					{Name: "migrate", MigrateTo: "latest"},
					{Name: "apply desired schema", ApplySchema: true},
					{Name: "count", Assert: &dbtest.Assertion{Query: "SELECT id FROM t", RowCount: new(2)}},
					{Name: "scalar", Assert: &dbtest.Assertion{Query: "SELECT name FROM t LIMIT 1", Scalar: new("widget")}},
					{Name: "error", Assert: &dbtest.Assertion{Query: "SELECT * FROM missing", ErrorContains: "missing"}},
				},
			}},
		},
		{
			name: "integer migrate_to target coerces to string",
			yaml: "cases:\n" +
				"  - name: migrate to version\n" +
				"    steps:\n" +
				"      - migrate_to: 5\n",
			want: []dbtest.Case{{
				Name:  "migrate to version",
				Steps: []dbtest.Step{{MigrateTo: "5"}},
			}},
		},
		{
			name: "seed may use the run-level directory",
			yaml: "cases:\n" +
				"  - name: seed fixtures\n" +
				"    steps:\n" +
				"      - seed:\n" +
				"          env: test\n",
			want: []dbtest.Case{{
				Name:  "seed fixtures",
				Steps: []dbtest.Step{{Seed: &dbtest.SeedStep{Env: "test"}}},
			}},
		},
	}

	c := qt.New(t)
	for _, tc := range tests {
		c.Run(tc.name, func(c *qt.C) {
			got, err := dbtest.ParseCases([]byte(tc.yaml))
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tc.want)
		})
	}
}

func TestParseCases_Invalid(t *testing.T) {
	tests := []struct {
		name        string
		yaml        string
		wantErrText string
	}{
		{
			name:        "case without name",
			yaml:        "cases:\n  - steps:\n      - exec: SELECT 1\n",
			wantErrText: "has no name",
		},
		{
			name:        "case without steps",
			yaml:        "cases:\n  - name: empty\n",
			wantErrText: "has no steps",
		},
		{
			name:        "step with no action",
			yaml:        "cases:\n  - name: c\n    steps:\n      - name: nothing\n",
			wantErrText: "none is set",
		},
		{
			name:        "step with two actions",
			yaml:        "cases:\n  - name: c\n    steps:\n      - exec: SELECT 1\n        migrate_to: latest\n",
			wantErrText: "2 are set",
		},
		{
			name:        "assert without query",
			yaml:        "cases:\n  - name: c\n    steps:\n      - assert:\n          row_count: 1\n",
			wantErrText: "assert requires a query",
		},
		{
			name:        "assert with two conditions",
			yaml:        "cases:\n  - name: c\n    steps:\n      - assert:\n          query: SELECT 1\n          row_count: 1\n          scalar: x\n",
			wantErrText: "2 are set",
		},
		{
			name:        "assert with no condition",
			yaml:        "cases:\n  - name: c\n    steps:\n      - assert:\n          query: SELECT 1\n",
			wantErrText: "none is set",
		},
		{
			name:        "unknown field",
			yaml:        "cases:\n  - name: c\n    steps:\n      - exec: SELECT 1\n        bogus: value\n",
			wantErrText: "field bogus not found",
		},
		{
			name:        "invalid migrate_to target",
			yaml:        "cases:\n  - name: c\n    steps:\n      - migrate_to: next\n",
			wantErrText: "invalid migrate_to target",
		},
		{
			name:        "negative migrate_to target",
			yaml:        "cases:\n  - name: c\n    steps:\n      - migrate_to: -1\n",
			wantErrText: "expected a non-negative integer",
		},
		{
			name:        "negative row_count",
			yaml:        "cases:\n  - name: c\n    steps:\n      - assert:\n          query: SELECT 1\n          row_count: -1\n",
			wantErrText: "row_count must be non-negative",
		},
	}

	c := qt.New(t)
	for _, tc := range tests {
		c.Run(tc.name, func(c *qt.C) {
			got, err := dbtest.ParseCases([]byte(tc.yaml))
			c.Assert(err, qt.IsNotNil)
			c.Assert(got, qt.IsNil)
			c.Assert(err.Error(), qt.Contains, tc.wantErrText)
		})
	}
}

func TestLoadCases(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	c.Assert(os.WriteFile(filepath.Join(dir, "20_second.yaml"),
		[]byte("cases:\n  - name: second\n    steps:\n      - exec: SELECT 2\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "10_first.yml"),
		[]byte("cases:\n  - name: first\n    steps:\n      - exec: SELECT 1\n"), 0o600), qt.IsNil)
	// Non-YAML files and subdirectories are ignored.
	c.Assert(os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("ignore me"), 0o600), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(dir, "nested.yaml"), 0o750), qt.IsNil)

	cases, err := dbtest.LoadCases(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 2)
	// Files are read in lexical order: 10_first.yml before 20_second.yaml.
	c.Assert(cases[0].Name, qt.Equals, "first")
	c.Assert(cases[1].Name, qt.Equals, "second")
}

func TestLoadCases_MissingDir(t *testing.T) {
	c := qt.New(t)
	_, err := dbtest.LoadCases(filepath.Join(t.TempDir(), "does-not-exist"))
	c.Assert(err, qt.IsNotNil)
}

// TestLoadCases_RejectsDuplicateNameAcrossFiles is the issue's primary
// reproduction: a name unique within each file but repeated across the
// directory. Before the union check, this loaded clean and returned two cases
// both named "dup", which --run then matched twice.
func TestLoadCases_RejectsDuplicateNameAcrossFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	c.Assert(os.WriteFile(filepath.Join(dir, "a.yaml"),
		[]byte("cases:\n  - name: dup\n    steps:\n      - exec: SELECT 1\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "b.yaml"),
		[]byte("cases:\n  - name: dup\n    steps:\n      - exec: SELECT 2\n"), 0o600), qt.IsNil)

	// The message names both files, because the name alone does not say where
	// the duplicate came from. No per-file prefix: `b.yaml: duplicate ... in
	// a.yaml and b.yaml` would name b.yaml twice.
	_, err := dbtest.LoadCases(dir)
	c.Assert(err, qt.ErrorMatches, `duplicate test case "dup" in a\.yaml and b\.yaml`)
}

// TestLoadCases_AllowsDistinctNamesAcrossFiles is the negative half of the pair
// above. The directory shape is identical -- two YAML files, one case each --
// and only the names differ. Without it the union check is indistinguishable
// from one that simply rejects any multi-file directory.
func TestLoadCases_AllowsDistinctNamesAcrossFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	c.Assert(os.WriteFile(filepath.Join(dir, "a.yaml"),
		[]byte("cases:\n  - name: alpha\n    steps:\n      - exec: SELECT 1\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "b.yaml"),
		[]byte("cases:\n  - name: beta\n    steps:\n      - exec: SELECT 2\n"), 0o600), qt.IsNil)

	cases, err := dbtest.LoadCases(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 2)
	c.Assert(cases[0].Name, qt.Equals, "alpha")
	c.Assert(cases[1].Name, qt.Equals, "beta")
}

// TestLoadCases_RejectsNamesDifferingOnlyBySurroundingWhitespace closes the
// member of the class that a trailing space used to slip through. It is the
// issue's first bullet verbatim: `--run dup` compiles unanchored, so it matched
// `dup` and `dup ` alike and ran both, and Report.HTML renders each name inside
// `case &ldquo;…&rdquo;`, where a browser collapses the trailing space into two
// visually identical rows.
func TestLoadCases_RejectsNamesDifferingOnlyBySurroundingWhitespace(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	c.Assert(os.WriteFile(filepath.Join(dir, "a.yaml"),
		[]byte("cases:\n  - name: \"dup\"\n    steps:\n      - exec: SELECT 1\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "b.yaml"),
		[]byte("cases:\n  - name: \"dup \"\n    steps:\n      - exec: SELECT 2\n"), 0o600), qt.IsNil)

	_, err := dbtest.LoadCases(dir)
	c.Assert(err, qt.ErrorMatches,
		`duplicate test case "dup" in a\.yaml and b\.yaml: "dup" and "dup " differ only in surrounding whitespace`)
}

// TestLoadCases_AllowsNamesDifferingInInteriorWhitespace is the negative half of
// the test above, and it fixes how far the normalization goes. Only surrounding
// whitespace is removed; a check that stripped whitespace everywhere, or one
// that collapsed runs of it, would collide these two and pass every other test
// in this file.
func TestLoadCases_AllowsNamesDifferingInInteriorWhitespace(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	c.Assert(os.WriteFile(filepath.Join(dir, "a.yaml"),
		[]byte("cases:\n  - name: \"users load\"\n    steps:\n      - exec: SELECT 1\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "b.yaml"),
		[]byte("cases:\n  - name: \"users  load\"\n    steps:\n      - exec: SELECT 2\n"), 0o600), qt.IsNil)

	cases, err := dbtest.LoadCases(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 2)
	c.Assert(cases[0].Name, qt.Equals, "users load")
	c.Assert(cases[1].Name, qt.Equals, "users  load")
}

// TestLoadCases_ReportsWithinFileDuplicateFromThePerFilePath pins the invariant
// that lets the union check assume its two origins always differ: each file is
// validated by ParseCases before the concatenation, so a collision inside one
// file is reported with that file's own prefix and never reaches the union.
// Were that ordering to change, this message would become the union's
// `duplicate test case "dup" in a.yaml and a.yaml`.
func TestLoadCases_ReportsWithinFileDuplicateFromThePerFilePath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	c.Assert(os.WriteFile(filepath.Join(dir, "a.yaml"),
		[]byte("cases:\n"+
			"  - name: dup\n    steps:\n      - exec: SELECT 1\n"+
			"  - name: dup\n    steps:\n      - exec: SELECT 2\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "b.yaml"),
		[]byte("cases:\n  - name: other\n    steps:\n      - exec: SELECT 3\n"), 0o600), qt.IsNil)

	_, err := dbtest.LoadCases(dir)
	c.Assert(err, qt.ErrorMatches, `a\.yaml: duplicate test case "dup"`)
}

// TestParseCases_RejectsDuplicateNameInOneDocument covers the single-document
// collision. The issue attributes the gap to validateCases running per document
// and only the union escaping, which implies this case was already handled; it
// was not, so the check had to be written rather than relocated. With no file
// names to report, the message carries the case name alone.
func TestParseCases_RejectsDuplicateNameInOneDocument(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "same name twice in one cases list",
			yaml: "cases:\n" +
				"  - name: dup\n" +
				"    steps:\n" +
				"      - exec: SELECT 1\n" +
				"  - name: dup\n" +
				"    steps:\n" +
				"      - exec: SELECT 2\n",
			want: `duplicate test case "dup"`,
		},
		{
			name: "same name across two documents in one file",
			yaml: "cases:\n" +
				"  - name: dup\n" +
				"    steps:\n" +
				"      - exec: SELECT 1\n" +
				"---\n" +
				"cases:\n" +
				"  - name: dup\n" +
				"    steps:\n" +
				"      - exec: SELECT 2\n",
			want: `duplicate test case "dup"`,
		},
		{
			// Case is not folded. FilterCases matches the raw name with a Go
			// regexp, so `--run dup` selects `dup` and not `DUP`, and no report
			// format renders the two alike; folding here would reject a pair
			// the filter keeps apart. The third case supplies the collision, so
			// this subtest still asserts an error and cannot pass by accident
			// on a build where the whole check is gone.
			name: "names differing only by case are distinct",
			yaml: "cases:\n" +
				"  - name: dup\n" +
				"    steps:\n" +
				"      - exec: SELECT 1\n" +
				"  - name: DUP\n" +
				"    steps:\n" +
				"      - exec: SELECT 2\n" +
				"  - name: dup\n" +
				"    steps:\n" +
				"      - exec: SELECT 3\n",
			want: `duplicate test case "dup"`,
		},
		{
			// Surrounding whitespace IS folded, and the message quotes both raw
			// forms rather than a name that appears verbatim in neither entry.
			name: "names differing only by trailing whitespace collide",
			yaml: "cases:\n" +
				"  - name: dup\n" +
				"    steps:\n" +
				"      - exec: SELECT 1\n" +
				"  - name: \"dup \"\n" +
				"    steps:\n" +
				"      - exec: SELECT 2\n",
			want: `duplicate test case "dup": "dup" and "dup " differ only in surrounding whitespace`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := dbtest.ParseCases([]byte(tt.yaml))
			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}
