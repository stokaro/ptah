package oracletype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/oracletype"
)

// TestMap_VectorFoldsToTheSpellingTheCatalogReports pins the declared side
// against what the server answers with.
//
// Every want below is a string ALL_TAB_COLS.VECTOR_INFO produced on Oracle Free
// 23.26.3.0.0 for a column declared the way the input spells it. The two sides
// have to agree exactly: the reader returns VECTOR_INFO verbatim, so a fold
// that produced any other spelling would make a declaration differ from the
// column it built, and the comparator would propose the same MODIFY forever.
func TestMap_VectorFoldsToTheSpellingTheCatalogReports(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     string
	}{
		{name: "dimension and format", declared: "VECTOR(1536, FLOAT32)", want: "VECTOR(1536,FLOAT32,DENSE)"},
		{name: "any dimension", declared: "VECTOR(*, FLOAT32)", want: "VECTOR(*,FLOAT32,DENSE)"},
		{name: "nothing specified", declared: "VECTOR", want: "VECTOR(*,*,DENSE)"},
		{name: "sparse storage", declared: "VECTOR(1000, INT8, SPARSE)", want: "VECTOR(1000,INT8,SPARSE)"},
		// The spelling of the declaration does not reach the catalog: the
		// server reports one form however the author wrote it.
		{name: "no spaces", declared: "VECTOR(512,INT8)", want: "VECTOR(512,INT8,DENSE)"},
		{name: "lower case", declared: "vector(512, float64)", want: "VECTOR(512,FLOAT64,DENSE)"},
		{name: "dimension alone", declared: "VECTOR(3)", want: "VECTOR(3,*,DENSE)"},
		{name: "explicit dense", declared: "VECTOR(512, FLOAT32, DENSE)", want: "VECTOR(512,FLOAT32,DENSE)"},
		// Four arguments is not a vector Oracle creates. Completing the list
		// here would hand the comparator a type the server would have refused,
		// so the author's text goes through and the server answers it.
		{name: "more arguments than the type takes", declared: "VECTOR(1, 2, 3, 4)", want: "VECTOR(1, 2, 3, 4)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oracletype.Map(test.declared), qt.Equals, test.want)
		})
	}
}

// TestMap_VectorIsAFixedPoint is the control that makes the fold safe to apply
// to a value that already went through it.
//
// The reader hands the catalog's own spelling to the comparator, and the
// declared side may hold that same spelling -- a schema file written from
// `ptah db read` output does. A fold that changed its own output would make
// such a file differ from the database it came from.
func TestMap_VectorIsAFixedPoint(t *testing.T) {
	catalogSpellings := []string{
		"VECTOR(1536,FLOAT32,DENSE)",
		"VECTOR(*,*,DENSE)",
		"VECTOR(1000,INT8,SPARSE)",
	}

	for _, spelling := range catalogSpellings {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oracletype.Map(spelling), qt.Equals, spelling)
		})
	}
}

// TestMap_ANonVectorTypeIsUntouched is the control the fold needs.
//
// Without it, a fold that returned a VECTOR spelling for everything would
// satisfy the tests above, and every Oracle column would read as a vector.
func TestMap_ANonVectorTypeIsUntouched(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     string
	}{
		{name: "integer keeps its Oracle width", declared: "INTEGER", want: "NUMBER(10)"},
		{name: "a parameterized number", declared: "NUMBER(10,2)", want: "NUMBER(10,2)"},
		{name: "a varchar", declared: "VARCHAR(200)", want: "VARCHAR2(200)"},
		// The name starts with the same letters and is a different type.
		{name: "a type whose name begins with the word", declared: "VECTORISED", want: "VECTORISED"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oracletype.Map(test.declared), qt.Equals, test.want)
		})
	}
}
