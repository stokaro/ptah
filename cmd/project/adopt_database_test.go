package project_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestProjectAdopt_TheDatabaseHalfIsNamedEvenWhenNobodyAsked is the promise
// that keeps a clean file from reading as a finished adoption.
//
// stokaro/ptah#1215's forbidden outcomes -- re-running applied SQL, marking SQL
// applied that never ran -- all happen in the DATABASE, and a report that
// analysed the project file, printed "native-ready" and said nothing about
// persisted state would be read as saying there was nothing to check. The
// section is therefore unconditional; only its contents depend on --preflight.
func TestProjectAdopt_TheDatabaseHalfIsNamedEvenWhenNobodyAsked(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  url = "postgres://localhost:5432/app?sslmode=disable"
  migration {
    dir = "file://migrations"
  }
}
`)

	stdout, _, err := runInspect(c, "adopt", "--check", "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Database adoption:")
	c.Assert(stdout, qt.Contains, "not inspected")
	// The verdict still belongs to the file: a run that was never given a
	// database has no gap to report, and turning every existing --check into a
	// failure would be a worse answer than the advisory.
	c.Assert(stdout, qt.Contains, "Native-ready:")
}

// TestProjectAdopt_AnUninspectedDatabaseIsAbsentFromTheDocument is the JSON
// half of the same promise, and it is the opposite shape on purpose.
//
// The text says "not inspected" in words. The document omits the member
// entirely, because a JSON consumer that saw `"database": {...}` with nothing
// wrong in it would have been told the database was checked. Absent is the only
// encoding of "not asked" that cannot be mistaken for "asked and clean".
func TestProjectAdopt_AnUninspectedDatabaseIsAbsentFromTheDocument(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  url = "postgres://localhost:5432/app?sslmode=disable"
  migration {
    dir = "file://migrations"
  }
}
`)

	stdout, _, err := runInspect(c,
		"adopt", "--check", "--format", "json", "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Contains(stdout, `"database"`), qt.IsFalse,
		qt.Commentf("an omitted member is what makes 'not asked' unmistakable"))
}

// TestProjectAdopt_PreflightWithoutADatabaseIsRefused pins that the preflight
// asks the project which database to look at, and refuses rather than
// inventing one.
//
// A preflight run against the wrong database would answer confidently about a
// history that is not the one being adopted, which is the shape of every
// mistake #1215 asks the verb to avoid.
func TestProjectAdopt_PreflightWithoutADatabaseIsRefused(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  migration {
    dir = "file://migrations"
  }
}
`)

	_, _, err := runInspect(c,
		"adopt", "--check", "--preflight", "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "--preflight needs the database this project targets")
}
