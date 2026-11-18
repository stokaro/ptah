package pgcollation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgcollation"
)

func TestFromWritten_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		want  pgcollation.Name
		model string
		sql   string
	}{
		{name: "a quoted name keeps its case", parts: []string{`"C"`},
			want: pgcollation.Name{Name: "C"}, model: "C", sql: `"C"`},
		{name: "an unquoted name folds to lower case", parts: []string{"Unicode"},
			want: pgcollation.Name{Name: "unicode"}, model: "unicode", sql: `"unicode"`},
		{name: "a dot inside quotes is part of the name", parts: []string{`"en_US.utf8"`},
			want: pgcollation.Name{Name: "en_US.utf8"}, model: "en_US.utf8", sql: `"en_US.utf8"`},
		{name: "a doubled quote is one quote", parts: []string{`"Mixed""Q"`},
			want: pgcollation.Name{Name: `Mixed"Q`}, model: `"Mixed""Q"`, sql: `"Mixed""Q"`},
		{name: "a schema and a mixed-case name", parts: []string{"App", `"MixedCase"`},
			want: pgcollation.Name{Schema: "app", Name: "MixedCase"}, model: `"app"."MixedCase"`, sql: `"app"."MixedCase"`},
		{name: "pg_catalog named", parts: []string{"pg_catalog", `"C"`},
			want: pgcollation.Name{Schema: "pg_catalog", Name: "C"}, model: `"pg_catalog"."C"`, sql: `"pg_catalog"."C"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := pgcollation.FromWritten(test.parts...)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
			c.Assert(got.Model(), qt.Equals, test.model)
			c.Assert(got.SQL(), qt.Equals, test.sql)
		})
	}
}

func TestFromWritten_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		want  string
	}{
		{name: "no part", want: `invalid collation name: "" names 0 parts, and a collation is a name or a schema and a name`},
		{name: "three parts", parts: []string{"a", "b", "c"},
			want: `invalid collation name: "a.b.c" names 3 parts, and a collation is a name or a schema and a name`},
		{name: "a string constant", parts: []string{`'C'`}, want: `invalid collation name: "'C'" is not one identifier`},
		{name: "a bracketed name", parts: []string{`[C]`}, want: `invalid collation name: "\[C\]" is not one identifier`},
		{name: "an unclosed quote", parts: []string{`"C`}, want: `invalid collation name: "C is not one quoted identifier`},
		{name: "a lone quote inside", parts: []string{`"a"b"`}, want: `invalid collation name: "a"b" holds a quote that is not doubled`},
		{name: "a zero-length name", parts: []string{`""`}, want: `invalid collation name: "" is a zero-length name`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := pgcollation.FromWritten(test.parts...)

			c.Assert(err, qt.ErrorIs, pgcollation.ErrInvalidName)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.Equals, pgcollation.Name{})
		})
	}
}

// TestRead_HappyPath pins the model spelling: a value without a quote is a name
// as written, dots and case included, and one with a quote is SQL.
func TestRead_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		model string
		want  pgcollation.Name
	}{
		{name: "a name as written", model: "C", want: pgcollation.Name{Name: "C"}},
		{name: "a dot in a bare name", model: "en_US.utf8", want: pgcollation.Name{Name: "en_US.utf8"}},
		{name: "a schema and a name", model: `"app"."MixedCase"`, want: pgcollation.Name{Schema: "app", Name: "MixedCase"}},
		{name: "a quoted name holding a quote", model: `"Mixed""Q"`, want: pgcollation.Name{Name: `Mixed"Q`}},
		{name: "a quoted name holding a dot", model: `"app"."a.b"`, want: pgcollation.Name{Schema: "app", Name: "a.b"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := pgcollation.Read(test.model)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
			c.Assert(got.Model(), qt.Equals, test.model)
		})
	}
}

func TestRead_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		model string
		want  string
	}{
		{name: "empty", model: "", want: "invalid collation name: the name is empty"},
		{name: "an unclosed quote", model: `"app"."C`, want: `invalid collation name: "app"."C has no closing quote`},
		{name: "an empty part", model: `"app".`, want: "invalid collation name: a part of the name is empty"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := pgcollation.Read(test.model)

			c.Assert(err, qt.ErrorIs, pgcollation.ErrInvalidName)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.Equals, pgcollation.Name{})
		})
	}
}

// TestSame pins when a declaration names the collation a read reports. A read
// names no schema for pg_catalog's collations, and a declaration that names
// none is whichever collation of that name the server found.
func TestSame(t *testing.T) {
	tests := []struct {
		name     string
		declared pgcollation.Name
		read     pgcollation.Name
		want     bool
	}{
		{name: "one name", declared: pgcollation.Name{Name: "C"}, read: pgcollation.Name{Name: "C"}, want: true},
		{name: "names differ in case", declared: pgcollation.Name{Name: "c"}, read: pgcollation.Name{Name: "C"}, want: false},
		{name: "pg_catalog named against a bare read", declared: pgcollation.Name{Schema: "pg_catalog", Name: "C"},
			read: pgcollation.Name{Name: "C"}, want: true},
		{name: "an unqualified declaration against a schema's collation", declared: pgcollation.Name{Name: "MixedCase"},
			read: pgcollation.Name{Schema: "app", Name: "MixedCase"}, want: true},
		{name: "another schema", declared: pgcollation.Name{Schema: "app", Name: "MixedCase"},
			read: pgcollation.Name{Schema: "other", Name: "MixedCase"}, want: false},
		{name: "a schema's collation against pg_catalog's", declared: pgcollation.Name{Schema: "app", Name: "C"},
			read: pgcollation.Name{Name: "C"}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(pgcollation.Same(test.declared, test.read), qt.Equals, test.want)
		})
	}
}
