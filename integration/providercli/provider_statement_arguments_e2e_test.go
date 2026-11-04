//go:build integration

package providercli_test

import (
	"database/sql"
	"database/sql/driver"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	// The test reads the fixture's database file back through a driver of its
	// own, so the answer does not pass through the provider the statement used.
	_ "modernc.org/sqlite"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
)

// A statement argument reaches the provider as the case its Go type names, and
// one the statement contract cannot carry exactly is refused before anything
// is sent. The fixture's SQLite is the engine, and the expected values come
// from reading its database file back.

// argumentStatements opens the operator statement handle of a fresh database
// of a fixture command that serves the operator statements, holding an empty
// table "arguments" with one untyped column, and returns it with the database
// file's path. The dev database fixture, devdb, does not list typed arguments.
func argumentStatements(c *qt.C, command string) (dbschema.OperatorStatements, string) {
	c.Helper()
	host, _ := yamlProviderCommand(c, command)
	path := filepath.Join(c.TempDir(), "arguments.db")
	address := (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/")}).String()
	operator, release, err := connectTransactionAddress(c, host, address).OperatorStatements()
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(release(), qt.IsNil) })
	_, err = operator.ExecContext(c.Context(), `CREATE TABLE "arguments" ("value")`)
	c.Assert(err, qt.IsNil)
	return operator, path
}

// storedArguments reads the rows of "arguments" back as text, in insertion
// order.
func storedArguments(c *qt.C, path string) []sql.NullString {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `SELECT CAST("value" AS TEXT) FROM "arguments" ORDER BY rowid`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var stored []sql.NullString
	for rows.Next() {
		var value sql.NullString
		c.Assert(rows.Scan(&value), qt.IsNil)
		stored = append(stored, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return stored
}

// liveCount is a named unsigned integer, which the provider receives as one.
type liveCount uint

// liveLabel decides with its Value method what is bound.
type liveLabel string

func (l liveLabel) Value() (driver.Value, error) { return "label:" + string(l), nil }

func TestProviderStatementArguments_HappyPath(t *testing.T) {
	const wide = 5000000000
	tests := []struct {
		name     string
		argument any
		want     sql.NullString
	}{
		{name: "an int", argument: int(wide), want: sql.NullString{String: "5000000000", Valid: true}},
		{name: "a uint", argument: uint(wide), want: sql.NullString{String: "5000000000", Valid: true}},
		{name: "a named uint", argument: liveCount(wide), want: sql.NullString{String: "5000000000", Valid: true}},
		{name: "a float64", argument: 1.5, want: sql.NullString{String: "1.5", Valid: true}},
		{name: "a bool", argument: true, want: sql.NullString{String: "1", Valid: true}},
		{name: "a string", argument: "CZ", want: sql.NullString{String: "CZ", Valid: true}},
		{name: "a byte slice", argument: []byte("CZ"), want: sql.NullString{String: "CZ", Valid: true}},
		{name: "a valuer", argument: liveLabel("CZ"), want: sql.NullString{String: "label:CZ", Valid: true}},
		{name: "a duration", argument: 90 * time.Second, want: sql.NullString{String: "90000000000", Valid: true}},
		{name: "nil", argument: nil, want: sql.NullString{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			operator, path := argumentStatements(c, "devdb")

			_, err := operator.ExecContext(c.Context(), `INSERT INTO "arguments" ("value") VALUES (?)`, test.argument)

			c.Assert(err, qt.IsNil)
			c.Assert(storedArguments(c, path), qt.DeepEquals, []sql.NullString{test.want})
		})
	}
}

// A refused argument stops the statement before the provider receives it, so
// the table stays empty, and the same statement with an argument the contract
// carries runs afterwards on the same handle.
func TestProviderStatementArguments_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		argument any
		wantErr  string
	}{
		{name: "an int32", argument: int32(5), wantErr: ` a statement argument of type int32, since it carries an integer only as int64 or uint64`},
		{name: "a float32", argument: float32(1.5), wantErr: ` a statement argument of type float32, since it carries a floating-point number only as float64`},
		{name: "a pointer", argument: new(5), wantErr: ` a statement argument of type \*int, since it cannot say that a type admits null`},
		{name: "a generic null", argument: sql.Null[int]{V: 5, Valid: true},
			wantErr: ` a statement argument of type sql.Null\[int\], since it cannot say that a type admits null`},
		{name: "a null int32", argument: sql.NullInt32{Int32: 5, Valid: true},
			wantErr: ` a statement argument of type sql.NullInt32, since it cannot say that a type admits null`},
		{name: "a slice", argument: []int{5}, wantErr: ` a statement argument of type \[\]int, since it carries no list`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			operator, path := argumentStatements(c, "devdb")

			_, refused := operator.ExecContext(c.Context(), `INSERT INTO "arguments" ("value") VALUES (?)`, test.argument)
			stored := storedArguments(c, path)
			_, err := operator.ExecContext(c.Context(), `INSERT INTO "arguments" ("value") VALUES (?)`, int64(5))

			c.Assert(refused, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(refused, qt.ErrorMatches, `sql: converting argument .* type: unsupported feature: the experimental provider contract does not support`+test.wantErr)
			c.Assert(stored, qt.HasLen, 0)
			c.Assert(err, qt.IsNil)
			c.Assert(storedArguments(c, path), qt.DeepEquals, []sql.NullString{{String: "5", Valid: true}})
		})
	}
}
