package oraclecollation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/oraclecollation"
)

func TestValid_HappyPath(t *testing.T) {
	for _, name := range []string{"BINARY_CI", "binary_ci", "USING_NLS_SORT_AI", "GERMAN_CI", "a1$#_"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oraclecollation.Valid(name), qt.IsTrue)
		})
	}
}

func TestValid_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "a digit first", value: "1BINARY"},
		{name: "an underscore first", value: "_BINARY"},
		{name: "a space", value: "BINARY CI"},
		{name: "a quote", value: `"BINARY_CI"`},
		{name: "a parenthesis", value: "BINARY_CI)"},
		{name: "a semicolon", value: "BINARY;"},
		{name: "a non-ASCII letter", value: "BINÄRY"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oraclecollation.Valid(tc.value), qt.IsFalse)
		})
	}
}

func TestCollatable_HappyPath(t *testing.T) {
	for _, typ := range []string{"VARCHAR2(20)", "VARCHAR2(20 CHAR)", "nvarchar2(5)", "CHAR(3)", "NCHAR(3)", " varchar2 (20)"} {
		t.Run(typ, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oraclecollation.Collatable(typ), qt.IsTrue)
		})
	}
}

func TestCollatable_FailurePath(t *testing.T) {
	for _, typ := range []string{"CLOB", "NCLOB", "NUMBER(10)", "RAW(16)", "DATE", "JSON", "BLOB", "TIMESTAMP(6)", ""} {
		t.Run(typ, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oraclecollation.Collatable(typ), qt.IsFalse)
		})
	}
}

// TestRead_HappyPath pins what the catalog's pair of answers becomes. A column
// that reads USING_NLS_COMP, or reads nothing, holds no collation. A column on
// a table with a default collation holds that collation and is not reported
// as declaring it, because the catalog cannot say which spelled it.
func TestRead_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		column        string
		tableDefault  string
		wantDeclared  string
		wantEffective string
	}{
		{name: "a column that names none", column: "USING_NLS_COMP", tableDefault: "USING_NLS_COMP"},
		{name: "a column that reads nothing", column: "", tableDefault: ""},
		{name: "USING_NLS_COMP in lower case", column: "using_nls_comp"},
		{name: "a declared collation", column: "BINARY_CI", tableDefault: "USING_NLS_COMP", wantDeclared: "BINARY_CI", wantEffective: "BINARY_CI"},
		{name: "a declared collation on a table that reads nothing", column: "BINARY_CI", wantDeclared: "BINARY_CI", wantEffective: "BINARY_CI"},
		{name: "a collation in lower case", column: "binary_ci", wantDeclared: "BINARY_CI", wantEffective: "BINARY_CI"},
		{name: "a collation the table's default also names", column: "BINARY_CI", tableDefault: "BINARY_CI", wantEffective: "BINARY_CI"},
		{name: "a collation other than the table's default", column: "BINARY_AI", tableDefault: "BINARY_CI", wantDeclared: "BINARY_AI", wantEffective: "BINARY_AI"},
		{name: "USING_NLS_COMP on a table whose default differs", column: "USING_NLS_COMP", tableDefault: "BINARY_CI"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			declared, effective := oraclecollation.Read(tc.column, tc.tableDefault)
			c.Assert(declared, qt.Equals, tc.wantDeclared)
			c.Assert(effective, qt.Equals, tc.wantEffective)
		})
	}
}

// TestSortKeyColumn_HappyPath reads the expressions Oracle Free 23.26 keeps
// for an index over a column with a collation.
func TestSortKeyColumn_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		hidden     string
		expression string
		want       string
		wantSort   string
	}{
		{name: "BINARY_CI", hidden: "SYS_NC00004$", expression: `NLSSORT("A",'nls_sort=''BINARY_CI''')`, want: "A", wantSort: "BINARY_CI"},
		{name: "a quoted column in lower case", hidden: "SYS_NC00005$", expression: `NLSSORT("code",'nls_sort=''GERMAN_CI''')`, want: "code", wantSort: "GERMAN_CI"},
		{name: "a space after the comma", hidden: "SYS_NC00004$", expression: `NLSSORT("A", 'nls_sort=''BINARY_AI''')`, want: "A", wantSort: "BINARY_AI"},
		{name: "trailing space", hidden: "SYS_NC00004$", expression: `NLSSORT("A",'nls_sort=''BINARY_CI''') `, want: "A", wantSort: "BINARY_CI"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, sort, ok := oraclecollation.SortKeyColumn(tc.hidden, tc.expression)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, tc.want)
			c.Assert(sort, qt.Equals, tc.wantSort)
		})
	}
}

func TestSortKeyColumn_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		hidden     string
		expression string
	}{
		{name: "a plain column", hidden: "CODE", expression: ""},
		{name: "a plain column with an expression row", hidden: "CODE", expression: `NLSSORT("CODE",'nls_sort=''BINARY_CI''')`},
		{name: "a function of the column", hidden: "SYS_NC00006$", expression: `UPPER("B")`},
		{name: "a sort key of an expression", hidden: "SYS_NC00006$", expression: `NLSSORT(UPPER("B"),'nls_sort=''BINARY_CI''')`},
		{name: "a sort key with more after it", hidden: "SYS_NC00006$", expression: `NLSSORT("B",'nls_sort=''BINARY_CI''')||'x'`},
		{name: "a hidden column with no expression", hidden: "SYS_NC00006$", expression: ""},
		{name: "a name that only looks hidden", hidden: "SYS_NC_X", expression: `NLSSORT("B",'nls_sort=''BINARY_CI''')`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, sort, ok := oraclecollation.SortKeyColumn(tc.hidden, tc.expression)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
			c.Assert(sort, qt.Equals, "")
		})
	}
}

// TestKnown_HappyPath holds the rule to what Oracle Free 23.26 accepted when
// every sort of V$NLS_VALID_VALUES was tried bare, with _CI and with _AI.
func TestKnown_HappyPath(t *testing.T) {
	sorts := []string{"BINARY", "FRENCH", "XFRENCH", "WEST_EUROPEAN"}
	tests := []struct{ name, collation string }{
		{name: "a sort", collation: "FRENCH"},
		{name: "a sort in lower case", collation: "french"},
		{name: "case insensitive", collation: "FRENCH_CI"},
		{name: "accent insensitive", collation: "xfrench_ai"},
		{name: "a sort with an underscore", collation: "WEST_EUROPEAN_CI"},
		{name: "BINARY", collation: "BINARY"},
		{name: "BINARY_CI", collation: "BINARY_CI"},
		{name: "BINARY_AI", collation: "BINARY_AI"},
		{name: "USING_NLS_COMP", collation: "USING_NLS_COMP"},
		{name: "spaces around it", collation: "  french_ci "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oraclecollation.Known(tc.collation, sorts), qt.IsTrue)
		})
	}
}

func TestKnown_FailurePath(t *testing.T) {
	sorts := []string{"BINARY", "FRENCH"}
	tests := []struct{ name, collation string }{
		{name: "a MySQL collation", collation: "utf8mb4_bin"},
		{name: "another MySQL collation", collation: "utf8mb4_0900_ai_ci"},
		{name: "an unknown sort", collation: "NOSUCH"},
		{name: "an unknown sort with a suffix", collation: "NOSUCH_CI"},
		{name: "a suffix Oracle does not take", collation: "FRENCH_CS"},
		{name: "USING_NLS_COMP with a suffix", collation: "USING_NLS_COMP_CI"},
		{name: "two suffixes", collation: "FRENCH_CI_AI"},
		{name: "empty", collation: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oraclecollation.Known(tc.collation, sorts), qt.IsFalse)
		})
	}
}
