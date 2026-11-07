package manageddata_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/manageddata"
	"ptah.run/core/schemamodel"
)

// TestLoadRowValues_PreservesTheDeclaration measures the loss this
// loader exists to avoid. Resolved through Go values, `007` is 7, `1.0` is 1
// and `2020-01-01` is a time.Time; the published artifact has to carry what the
// author wrote.
func TestLoadRowValues_PreservesTheDeclaration(t *testing.T) {
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

	rows, err := manageddata.LoadRowValues(dir, schemamodel.ManagedData{
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

// TestLoadRowValues_EmptyFile covers the three spellings of a file that
// declares nothing, which is not the same as a file nobody read.
func TestLoadRowValues_EmptyFile(t *testing.T) {
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

			rows, err := manageddata.LoadRowValues(dir, schemamodel.ManagedData{
				Table: "countries", Keys: []string{"code"}, File: "countries.yaml",
			})

			c.Assert(err, qt.IsNil)
			c.Assert(rows, qt.HasLen, 0)
		})
	}
}

func TestLoadRowValues_FailurePath(t *testing.T) {
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

			rows, err := manageddata.LoadRowValues(dir, schemamodel.ManagedData{
				Table: "countries", Keys: []string{"code"}, File: "countries.yaml",
			})

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(rows, qt.IsNil)
		})
	}
}

func TestLoadRowValues_MissingFile(t *testing.T) {
	c := qt.New(t)

	rows, err := manageddata.LoadRowValues(t.TempDir(), schemamodel.ManagedData{
		Table: "countries", Keys: []string{"code"}, File: "countries.yaml",
	})

	c.Assert(err, qt.ErrorMatches, `read managed data file .* for table "countries": .*`)
	c.Assert(rows, qt.IsNil)
}

// TestResolveRows_AnswersLikeTheFileLoader is the property that lets a
// caller accept both forms of a declaration: rows carried by a published
// artifact resolve to the values reading the same file would have produced.
//
// The fixture declares one column per YAML scalar kind the two loaders could
// disagree about, including the quoted "007" that separates a string from the
// integer it looks like.
func TestResolveRows_AnswersLikeTheFileLoader(t *testing.T) {
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

	fromFile, err := manageddata.LoadRows(dir, declaration)
	c.Assert(err, qt.IsNil)
	declared, err := manageddata.LoadRowValues(dir, declaration)
	c.Assert(err, qt.IsNil)
	declaration.Rows = declared

	resolved, err := manageddata.ResolveRows(declaration)

	c.Assert(err, qt.IsNil)
	c.Assert(resolved, qt.DeepEquals, fromFile)
	// The comparison above holds for an empty pair too, so the fixture's own
	// values are named here: a resolver that answered a string for every column
	// would still have to produce these.
	c.Assert(resolved[0]["zero"], qt.Equals, 7)
	c.Assert(resolved[0]["quoted"], qt.Equals, "007")
	c.Assert(resolved[0]["rate"], qt.Equals, 1.0)
	c.Assert(resolved[0]["active"], qt.IsTrue)
	c.Assert(resolved[0]["retired"], qt.IsNil)
	c.Assert(resolved[0]["since"], qt.Equals, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
}

// TestResolveRows_UnreadDeclaration separates a declaration nobody read
// from one that declares no rows. The first has a file to read and the second
// has an answer.
func TestResolveRows_UnreadDeclaration(t *testing.T) {
	c := qt.New(t)

	unread, err := manageddata.ResolveRows(schemamodel.ManagedData{
		Table: "countries", Keys: []string{"code"}, File: "countries.yaml",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(unread, qt.IsNil)

	empty, err := manageddata.ResolveRows(schemamodel.ManagedData{
		Table: "countries", Keys: []string{"code"}, Rows: make([]schemamodel.ManagedRow, 0),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(empty, qt.IsNotNil)
	c.Assert(empty, qt.HasLen, 0)
}

func TestResolveRows_FailurePath(t *testing.T) {
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

			rows, err := manageddata.ResolveRows(schemamodel.ManagedData{
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

// loaders drives both readers of a row file through one table: the boundary is
// the property they share, and one of them losing it is the defect.
func loaders() []struct {
	name string
	load func(string, schemamodel.ManagedData) (int, error)
} {
	return []struct {
		name string
		load func(string, schemamodel.ManagedData) (int, error)
	}{
		{name: "LoadRows", load: func(root string, md schemamodel.ManagedData) (int, error) {
			rows, err := manageddata.LoadRows(root, md)
			return len(rows), err
		}},
		{name: "LoadRowValues", load: func(root string, md schemamodel.ManagedData) (int, error) {
			rows, err := manageddata.LoadRowValues(root, md)
			return len(rows), err
		}},
	}
}

// rowFileProject lays out one project and a row file on each side of its boundary.
func rowFileProject(c *qt.C) (project, sourceDir, outside string) {
	c.Helper()
	parent := c.TempDir()
	project = filepath.Join(parent, "project")
	sourceDir = filepath.Join(project, "models")
	c.Assert(os.MkdirAll(sourceDir, 0o750), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(project, "exported"), 0o750), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(sourceDir, "sub"), 0o750), qt.IsNil)
	writeRowFile(c, sourceDir, "rows.yaml", "- code: INSIDE\n")
	writeRowFile(c, filepath.Join(sourceDir, "sub"), "rows.yaml", "- code: INSIDE\n")
	outside = filepath.Join(parent, "secret.yaml")
	writeRowFile(c, parent, "secret.yaml", "- code: OUTSIDE\n")
	return project, sourceDir, outside
}

// TestLoaders_RefuseAFileOutsideTheRoot holds the spellings that leave the project.
// A Windows spelling refuses here on every platform, which is the rule: what a
// schema may name must not depend on the machine reading it.
func TestLoaders_RefuseAFileOutsideTheRoot(t *testing.T) {
	setup := qt.New(t)
	project, sourceDir, outside := rowFileProject(setup)

	tests := []struct {
		name string
		file string
	}{
		{name: "climbs out", file: "../../secret.yaml"},
		{name: "climbs out from a subdirectory", file: "sub/../../../secret.yaml"},
		{name: "leading slash on the real path", file: filepath.ToSlash(outside)},
		{name: "leading slash", file: "/etc/passwd"},
		{name: "leading backslash", file: `\etc\passwd`},
		{name: "network path", file: `\\server\share\rows.yaml`},
		{name: "drive letter with backslash", file: `C:\secret\rows.yaml`},
		{name: "drive letter with slash", file: "C:/secret/rows.yaml"},
		{name: "drive relative", file: "C:rows.yaml"},
		{name: "climbs out in Windows spelling", file: `..\..\secret.yaml`},
		{name: "climbs out in mixed spelling", file: `sub\../../../secret.yaml`},
	}
	for _, loader := range loaders() {
		for _, test := range tests {
			t.Run(loader.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				count, err := loader.load(project, schemamodel.ManagedData{
					Table: "countries", File: test.file, SourceDir: sourceDir,
				})
				c.Assert(err, qt.ErrorIs, manageddata.ErrOutsideRoot)
				c.Assert(err, qt.ErrorMatches, `managed data file .* for table countries: .*outside allowed root.*`)
				c.Assert(count, qt.Equals, 0)
			})
		}
	}
}

// TestLoaders_RefuseWithNoRoot is the fail-closed half: the file is beside the
// declaration and would read, so only the missing boundary can refuse it.
func TestLoaders_RefuseWithNoRoot(t *testing.T) {
	setup := qt.New(t)
	_, sourceDir, _ := rowFileProject(setup)

	for _, loader := range loaders() {
		t.Run(loader.name, func(t *testing.T) {
			c := qt.New(t)
			count, err := loader.load("", schemamodel.ManagedData{
				Table: "countries", File: "rows.yaml", SourceDir: sourceDir,
			})
			c.Assert(err, qt.ErrorIs, manageddata.ErrOutsideRoot)
			c.Assert(err, qt.ErrorMatches, `.*no root was given`)
			c.Assert(count, qt.Equals, 0)
		})
	}
}

// TestLoaders_ReadAFileInsideTheRoot is the control: the boundary is the project,
// not the declaration's own directory, because `schema export` writes a path back
// out of the directory it exports into.
func TestLoaders_ReadAFileInsideTheRoot(t *testing.T) {
	setup := qt.New(t)
	project, sourceDir, _ := rowFileProject(setup)

	tests := []struct {
		name      string
		file      string
		sourceDir string
	}{
		{name: "beside the declaration", file: "rows.yaml", sourceDir: sourceDir},
		{name: "below the declaration", file: "sub/rows.yaml", sourceDir: sourceDir},
		{name: "backslash separates on every platform", file: `sub\rows.yaml`, sourceDir: sourceDir},
		{name: "across the project", file: "../models/rows.yaml", sourceDir: filepath.Join(project, "exported")},
		{name: "relative source directory", file: "rows.yaml", sourceDir: "models"},
	}
	for _, loader := range loaders() {
		for _, test := range tests {
			t.Run(loader.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				count, err := loader.load(project, schemamodel.ManagedData{
					Table: "countries", File: test.file, SourceDir: test.sourceDir,
				})
				c.Assert(err, qt.IsNil)
				c.Assert(count, qt.Equals, 1)
			})
		}
	}
}
