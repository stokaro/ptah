package dbmlparse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbmlparse"
)

// TestParse_SaysWhatItReadAndDidNotApply pins the loss report.
//
// A Project block, a TableGroup and a Records block are all legitimate DBML.
// None of them says anything about schema state, so none is applied — and a
// reader who wrote one and finds it absent from the applied schema deserves to
// have been told. A loss with nowhere to say so is how a document and a
// database quietly stop agreeing (stokaro/ptah#2065).
func TestParse_SaysWhatItReadAndDidNotApply(t *testing.T) {
	rows := []struct {
		name     string
		document string
		contains []string
	}{
		{
			name:     "a Project block",
			document: "Project shop {\n  database_type: 'PostgreSQL'\n}\n\nTable t {\n  a int\n}\n",
			contains: []string{"project", "describes the diagram"},
		},
		{
			name:     "a TableGroup",
			document: "Table t {\n  a int\n}\n\nTableGroup g {\n  t\n}\n",
			contains: []string{"tablegroup", "describes the diagram"},
		},
		{
			name:     "a Records block",
			document: "Table t {\n  a int\n}\n\nRecords t {\n  (1)\n  (2)\n}\n",
			contains: []string{`records for "t"`, "seed data, not schema", "reference data"},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			var reported []string

			db, err := dbmlparse.Parse(row.document, dbmlparse.Options{
				File:         "schema.dbml",
				OnDiagnostic: func(message string) { reported = append(reported, message) },
			})

			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 1,
				qt.Commentf("the construct was refused rather than skipped"))
			c.Assert(reported, qt.HasLen, 1)
			for _, fragment := range row.contains {
				c.Assert(reported[0], qt.Contains, fragment)
			}
			c.Assert(reported[0], qt.Contains, "schema.dbml:")
		})
	}
}

// TestParse_OneRecordsBlockIsOneDiagnostic pins that the report is per block
// rather than per row.
//
// A Records block holding a thousand rows would otherwise bury everything else
// the parse reported, and the fact a reader needs is that the block exists and
// did nothing, not how large it was.
func TestParse_OneRecordsBlockIsOneDiagnostic(t *testing.T) {
	c := qt.New(t)
	document := "Table t {\n  a int\n}\n\nRecords t {\n  (1)\n  (2)\n  (3)\n  (4)\n  (5)\n}\n"
	var reported []string

	_, err := dbmlparse.Parse(document, dbmlparse.Options{
		OnDiagnostic: func(message string) { reported = append(reported, message) },
	})

	c.Assert(err, qt.IsNil)
	c.Assert(reported, qt.HasLen, 1)
}

// TestParse_ADocumentWithNothingToLoseReportsNothing is the control on the
// three rows above.
//
// Without it a parser that diagnosed every declaration would satisfy all of
// them, and every ordinary document would carry a warning nobody can act on.
func TestParse_ADocumentWithNothingToLoseReportsNothing(t *testing.T) {
	c := qt.New(t)
	var reported []string

	_, err := dbmlparse.Parse("Enum e {\n  a\n}\n\nTable t {\n  a int [pk]\n}\n", dbmlparse.Options{
		OnDiagnostic: func(message string) { reported = append(reported, message) },
	})

	c.Assert(err, qt.IsNil)
	c.Assert(reported, qt.HasLen, 0)
}

// TestParse_RefusesMultiFileComposition pins the constructs that must fail.
//
// A document that pulls in another one describes a schema this parser has not
// read. Skipping the directive would hand back a model missing whatever the
// other file declared — silently, and looking complete.
func TestParse_RefusesMultiFileComposition(t *testing.T) {
	rows := []struct {
		name     string
		document string
	}{
		{name: "use", document: "use './other.dbml'\n"},
		{name: "reuse", document: "reuse './other.dbml'\n"},
		{name: "include", document: "include './other.dbml'\n"},
		{name: "import", document: "import './other.dbml'\n"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := dbmlparse.Parse(row.document, dbmlparse.Options{File: "schema.dbml"})

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "is not supported")
			c.Assert(err.Error(), qt.Contains, "single file")
			c.Assert(err.Error(), qt.Contains, "schema.dbml:")
		})
	}
}

// TestParse_ANilDiagnosticChannelChangesNothing pins that a caller who does not
// want the report gets the same parse.
func TestParse_ANilDiagnosticChannelChangesNothing(t *testing.T) {
	c := qt.New(t)
	document := "Table t {\n  a int\n}\n\nRecords t {\n  (1)\n}\n"

	withChannel, err := dbmlparse.Parse(document, dbmlparse.Options{OnDiagnostic: func(string) {}})
	c.Assert(err, qt.IsNil)
	without, err := dbmlparse.Parse(document, dbmlparse.Options{})
	c.Assert(err, qt.IsNil)

	c.Assert(without.Tables, qt.DeepEquals, withChannel.Tables)
	c.Assert(without.Fields, qt.DeepEquals, withChannel.Fields)
}
