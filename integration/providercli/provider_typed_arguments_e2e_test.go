//go:build integration

package providercli_test

import (
	"database/sql"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
)

// Through a provider that lists sessions.typed-arguments, a statement argument
// reaches the provider with the type its caller bound it at, so the narrow,
// nullable and list arguments that a provider without the feature refuses are
// written. The fixture's SQLite binds the value and drops the type, so what
// these tests can read back is the value; the type is the YDB provider's to
// prove. The expected values come from reading the database file back.

func TestProviderTypedArguments_HappyPath(t *testing.T) {
	const wide = 5000000000
	tests := []struct {
		name     string
		argument any
		want     sql.NullString
	}{
		{name: "an int32", argument: int32(-5), want: sql.NullString{String: "-5", Valid: true}},
		{name: "an int8", argument: int8(7), want: sql.NullString{String: "7", Valid: true}},
		{name: "a uint", argument: uint(wide), want: sql.NullString{String: "5000000000", Valid: true}},
		{name: "a uint32", argument: uint32(4294967295), want: sql.NullString{String: "4294967295", Valid: true}},
		{name: "a float32", argument: float32(1.5), want: sql.NullString{String: "1.5", Valid: true}},
		{name: "a pointer", argument: new(5), want: sql.NullString{String: "5", Valid: true}},
		{name: "a nil pointer", argument: (*int)(nil), want: sql.NullString{}},
		{name: "a pointer to a string", argument: new("CZ"), want: sql.NullString{String: "CZ", Valid: true}},
		{name: "a nil pointer to a uint", argument: (*uint)(nil), want: sql.NullString{}},
		{name: "a valid generic null", argument: sql.Null[int64]{V: 5, Valid: true}, want: sql.NullString{String: "5", Valid: true}},
		{name: "an invalid generic null", argument: sql.Null[uint]{}, want: sql.NullString{}},
		{name: "a valid null int32", argument: sql.NullInt32{Int32: 5, Valid: true}, want: sql.NullString{String: "5", Valid: true}},
		{name: "an invalid null string", argument: sql.NullString{String: "CZ"}, want: sql.NullString{}},
		{name: "a duration", argument: 90 * time.Second, want: sql.NullString{String: "90000000000", Valid: true}},
		{name: "nil", argument: nil, want: sql.NullString{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			operator, path := argumentStatements(c, "typed-arguments")

			_, err := operator.ExecContext(c.Context(), `INSERT INTO "arguments" ("value") VALUES (?)`, test.argument)

			c.Assert(err, qt.IsNil)
			c.Assert(storedArguments(c, path), qt.DeepEquals, []sql.NullString{test.want})
		})
	}
}

// A slice reaches the provider as a list, which the fixture binds as the JSON
// array of its elements: each element is one row, and a nil pointer is null.
func TestProviderTypedArgumentLists_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		argument any
		want     []sql.NullString
	}{
		{name: "a slice of uint64", argument: []uint64{1, 2, 3}, want: []sql.NullString{{String: "1", Valid: true}, {String: "2", Valid: true}, {String: "3", Valid: true}}},
		{name: "a slice of int32", argument: []int32{-1, 7}, want: []sql.NullString{{String: "-1", Valid: true}, {String: "7", Valid: true}}},
		{name: "a slice of pointers", argument: []*string{new("CZ"), nil}, want: []sql.NullString{{String: "CZ", Valid: true}, {}}},
		{name: "a slice of null values", argument: []sql.NullInt64{{Int64: 5, Valid: true}, {}}, want: []sql.NullString{{String: "5", Valid: true}, {}}},
		{name: "an empty slice", argument: make([]int32, 0), want: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			operator, path := argumentStatements(c, "typed-arguments")

			_, err := operator.ExecContext(c.Context(), `INSERT INTO "arguments" ("value") SELECT value FROM json_each(?)`, test.argument)

			c.Assert(err, qt.IsNil)
			c.Assert(storedArguments(c, path), qt.DeepEquals, test.want)
		})
	}
}

// What even the typed form cannot carry is refused before the provider
// receives it, so the table stays empty, and a typed argument runs afterwards
// on the same handle.
func TestProviderTypedArguments_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		argument any
		wantErr  string
	}{
		{name: "a nested slice", argument: [][]int{{5}}, wantErr: ` a statement argument of type \[\]\[\]int, since it carries no nested list`},
		{name: "an array", argument: [1]int{5}, wantErr: ` a statement argument of type \[1\]int, since it carries no array`},
		{name: "a pointer to a pointer", argument: new(new(5)), wantErr: ` a statement argument of type \*\*int, since it carries null at one level only`},
		{name: "a map", argument: map[string]int{"a": 5}, wantErr: ` a statement argument of type map\[string\]int`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			operator, path := argumentStatements(c, "typed-arguments")

			_, refused := operator.ExecContext(c.Context(), `INSERT INTO "arguments" ("value") VALUES (?)`, test.argument)
			stored := storedArguments(c, path)
			_, err := operator.ExecContext(c.Context(), `INSERT INTO "arguments" ("value") VALUES (?)`, int32(5))

			c.Assert(refused, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(refused, qt.ErrorMatches, `sql: converting argument .* type: unsupported feature: the experimental provider contract does not support`+test.wantErr)
			c.Assert(stored, qt.HasLen, 0)
			c.Assert(err, qt.IsNil)
			c.Assert(storedArguments(c, path), qt.DeepEquals, []sql.NullString{{String: "5", Valid: true}})
		})
	}
}
