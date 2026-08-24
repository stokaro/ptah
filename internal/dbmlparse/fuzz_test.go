package dbmlparse_test

import (
	"testing"

	"go.5x5.cz/ptah/internal/dbmlparse"
)

// FuzzParse holds the parser to the one promise it can make about any input:
// it returns, and it returns either a schema or an error.
//
// A grammar read by hand is where an unterminated construct turns into a loop
// that never advances, and a nested one into recursion with no floor. Neither
// shows up in a fixture, because a fixture is written by somebody who knows the
// grammar (stokaro/ptah#2065 asks for this suite by name).
func FuzzParse(f *testing.F) {
	seeds := []string{
		"",
		"Table t { a int }",
		"Table t {",
		"Enum e { a b }",
		"Enum e {",
		"Ref: a.b > c.d",
		"Ref: a.b <> c.d",
		"Table t { a int [pk, note: 'x'] }",
		"Table t { a int [default: `now()`] }",
		"Table t { a varchar(255) }",
		"Table t { a int[] }",
		"Table t {\n  a int\n\n  Indexes {\n    (a) [unique]\n  }\n}",
		"/* unterminated",
		"'''unterminated",
		"Table \"t\" { \"a\" int }",
		"Project p { database_type: 'PostgreSQL' }",
		"Table t { a int [ref: > u.id] }",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, document string) {
		db, err := dbmlparse.Parse(document, dbmlparse.Options{File: "fuzz.dbml"})
		if err != nil {
			if db != nil {
				t.Fatalf("a failed parse returned a schema as well as %v", err)
			}
			return
		}
		if db == nil {
			t.Fatal("a successful parse returned no schema")
		}
	})
}
