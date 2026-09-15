package schemamodel_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// TestLoadManagedRowValues_PreservesTheDeclaration measures the loss this
// loader exists to avoid. Resolved through Go values, `007` is 7, `1.0` is 1
// and `2020-01-01` is a time.Time; the published artifact has to carry what the
// author wrote.
func TestLoadManagedRowValues_PreservesTheDeclaration(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeRowFile(c, dir, "countries.yaml", `
- code: NO
  zero: 007
  quoted: "007"
  rate: 1.0
  since: 2020-01-01
  active: true
  retired: null
- code: CZ
  quoted: "1"
`)

	rows, err := schemamodel.LoadManagedRowValues(dir, schemamodel.ManagedData{
		Table: "countries", Keys: []string{"code"}, File: "countries.yaml",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 2)
	c.Assert(rows[0]["code"], qt.Equals, schemamodel.ManagedValue{Tag: "str", Text: "NO"})
	c.Assert(rows[0]["zero"], qt.Equals, schemamodel.ManagedValue{Tag: "int", Text: "007"})
	c.Assert(rows[0]["quoted"], qt.Equals, schemamodel.ManagedValue{Tag: "str", Text: "007"})
	c.Assert(rows[0]["rate"], qt.Equals, schemamodel.ManagedValue{Tag: "float", Text: "1.0"})
	c.Assert(rows[0]["since"], qt.Equals, schemamodel.ManagedValue{Tag: "timestamp", Text: "2020-01-01"})
	c.Assert(rows[0]["active"], qt.Equals, schemamodel.ManagedValue{Tag: "bool", Text: "true"})
	c.Assert(rows[0]["retired"], qt.Equals, schemamodel.ManagedValue{Tag: "null", Null: true})
	_, declared := rows[1]["retired"]
	c.Assert(declared, qt.IsFalse, qt.Commentf("the second row never names the column"))
}

// TestLoadManagedRowValues_EmptyFile covers the three spellings of a file that
// declares nothing, which is not the same as a file nobody read.
func TestLoadManagedRowValues_EmptyFile(t *testing.T) {
	tests := []struct {
		name     string
		contents string
	}{
		{name: "empty", contents: ""},
		{name: "whitespace", contents: "\n  \n"},
		{name: "null document", contents: "null\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			writeRowFile(c, dir, "countries.yaml", test.contents)

			rows, err := schemamodel.LoadManagedRowValues(dir, schemamodel.ManagedData{
				Table: "countries", Keys: []string{"code"}, File: "countries.yaml",
			})

			c.Assert(err, qt.IsNil)
			c.Assert(rows, qt.HasLen, 0)
		})
	}
}

func TestLoadManagedRowValues_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		contents string
		message  string
	}{
		{
			name:     "not a list",
			contents: "code: NO\n",
			message:  `managed data file .* for table "countries" must be a YAML list of mappings`,
		},
		{
			name:     "row is not a mapping",
			contents: "- NO\n",
			message:  `.*row 1: is not a mapping`,
		},
		{
			name:     "nested value",
			contents: "- code:\n    nested: NO\n",
			message:  `.*row 1: column "code" is not a scalar`,
		},
		{
			name:     "alias value",
			contents: "- code: &anchor NO\n- code: *anchor\n",
			message:  `.*row 2: column "code" is a YAML alias`,
		},
		{
			name:     "malformed",
			contents: "- code: [\n",
			message:  `parse managed data file .* for table "countries": .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			writeRowFile(c, dir, "countries.yaml", test.contents)

			rows, err := schemamodel.LoadManagedRowValues(dir, schemamodel.ManagedData{
				Table: "countries", Keys: []string{"code"}, File: "countries.yaml",
			})

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(rows, qt.IsNil)
		})
	}
}

func TestLoadManagedRowValues_MissingFile(t *testing.T) {
	c := qt.New(t)

	rows, err := schemamodel.LoadManagedRowValues(t.TempDir(), schemamodel.ManagedData{
		Table: "countries", Keys: []string{"code"}, File: "countries.yaml",
	})

	c.Assert(err, qt.ErrorMatches, `read managed data file .* for table "countries": .*`)
	c.Assert(rows, qt.IsNil)
}

// TestResolveManagedRows_AnswersLikeTheFileLoader is the property that lets a
// caller accept both forms of a declaration: rows carried by a published
// artifact resolve to the values reading the same file would have produced.
//
// The fixture declares one column per YAML scalar kind the two loaders could
// disagree about, including the quoted "007" that separates a string from the
// integer it looks like.
func TestResolveManagedRows_AnswersLikeTheFileLoader(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeRowFile(c, dir, "countries.yaml", `
- code: NO
  zero: 007
  quoted: "007"
  rate: 1.0
  since: 2020-01-01
  active: true
  retired: null
- code: CZ
  quoted: "1"
`)
	declaration := schemamodel.ManagedData{
		Table: "countries", Keys: []string{"code"}, File: "countries.yaml",
	}

	fromFile, err := schemamodel.LoadManagedRows(dir, declaration)
	c.Assert(err, qt.IsNil)
	declared, err := schemamodel.LoadManagedRowValues(dir, declaration)
	c.Assert(err, qt.IsNil)
	declaration.Rows = declared

	resolved, err := schemamodel.ResolveManagedRows(declaration)

	c.Assert(err, qt.IsNil)
	c.Assert(resolved, qt.DeepEquals, fromFile)
	// The comparison above holds for an empty pair too, so the fixture's own
	// values are named here: a resolver that answered a string for every column
	// would still have to produce these.
	c.Assert(resolved[0]["zero"], qt.Equals, 7)
	c.Assert(resolved[0]["quoted"], qt.Equals, "007")
	c.Assert(resolved[0]["rate"], qt.Equals, 1.0)
	c.Assert(resolved[0]["active"], qt.Equals, true)
	c.Assert(resolved[0]["retired"], qt.IsNil)
	c.Assert(resolved[0]["since"], qt.Equals, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
}

// TestResolveManagedRows_UnreadDeclaration separates a declaration nobody read
// from one that declares no rows. The first has a file to read and the second
// has an answer.
func TestResolveManagedRows_UnreadDeclaration(t *testing.T) {
	c := qt.New(t)

	unread, err := schemamodel.ResolveManagedRows(schemamodel.ManagedData{
		Table: "countries", Keys: []string{"code"}, File: "countries.yaml",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(unread, qt.IsNil)

	empty, err := schemamodel.ResolveManagedRows(schemamodel.ManagedData{
		Table: "countries", Keys: []string{"code"}, Rows: make([]schemamodel.ManagedRow, 0),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(empty, qt.IsNotNil)
	c.Assert(empty, qt.HasLen, 0)
}

func TestResolveManagedRows_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		value   schemamodel.ManagedValue
		message string
	}{
		{
			name:    "text that is not an integer",
			value:   schemamodel.ManagedValue{Tag: "int", Text: "twelve"},
			message: `managed data for table "countries", row 1, column "population": "twelve" is tagged "int" and resolves as "str"`,
		},
		{
			name:    "text that is not a boolean",
			value:   schemamodel.ManagedValue{Tag: "bool", Text: "yes please"},
			message: `managed data for table "countries", row 1, column "population": "yes please" is tagged "bool" and resolves as "str"`,
		},
		{
			name:    "unsupported tag",
			value:   schemamodel.ManagedValue{Tag: "binary", Text: "aGk="},
			message: `managed data for table "countries", row 1, column "population": carries the unsupported YAML tag "binary"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rows, err := schemamodel.ResolveManagedRows(schemamodel.ManagedData{
				Table: "countries",
				Keys:  []string{"code"},
				Rows:  []schemamodel.ManagedRow{{"population": test.value}},
			})

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(rows, qt.IsNil)
		})
	}
}

func writeRowFile(c *qt.C, dir, name, contents string) {
	c.Helper()
	err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600)
	c.Assert(err, qt.IsNil)
}
