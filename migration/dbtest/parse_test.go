package dbtest_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/dbtest"
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
			name: "migrate, assert row_count, scalar and error_contains",
			yaml: "cases:\n" +
				"  - name: full flow\n" +
				"    steps:\n" +
				"      - name: migrate\n" +
				"        migrate_to: latest\n" +
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
