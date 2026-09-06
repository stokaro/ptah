package oracleroutine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/oracleroutine"
)

// TestBody_TakesTheTextAfterTheHeader pins the split that recovers a routine's
// body from what ALL_SOURCE stored.
//
// Every input below is the exact text the catalog returned on 23.26.2.0.0 for a
// routine created through the renderer, joined from its lines. The declaration
// section case is the one that matters: the header of
// `FUNCTION f RETURN NUMBER IS x NUMBER := 0; BEGIN ... END;` ends before the
// declaration, not at BEGIN, and a rule that looked for BEGIN would report the
// declaration as part of the signature.
func TestBody_TakesTheTextAfterTheHeader(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   string
	}{
		{
			name:   "a function whose body starts at BEGIN",
			source: "FUNCTION fn_double(p IN NUMBER) RETURN NUMBER IS\nBEGIN\n  RETURN p * 2;\nEND;\n",
			want:   "BEGIN\n  RETURN p * 2;\nEND;",
		},
		{
			name:   "a function with a declaration section",
			source: "FUNCTION fn_dec(p IN NUMBER) RETURN NUMBER IS\n  x NUMBER := 0;\nBEGIN\n  RETURN x;\nEND;\n",
			want:   "x NUMBER := 0;\nBEGIN\n  RETURN x;\nEND;",
		},
		{
			name:   "a procedure, which has no RETURN clause",
			source: "PROCEDURE pr_touch(p IN NUMBER) IS\nBEGIN\n  NULL;\nEND;\n",
			want:   "BEGIN\n  NULL;\nEND;",
		},
		{
			name:   "a parameterless function",
			source: "FUNCTION fn_one RETURN NUMBER IS\nBEGIN\n  RETURN 1;\nEND;\n",
			want:   "BEGIN\n  RETURN 1;\nEND;",
		},
		{
			name:   "AS opening the body instead of IS",
			source: "FUNCTION fn_as RETURN NUMBER AS\nBEGIN\n  RETURN 1;\nEND;\n",
			want:   "BEGIN\n  RETURN 1;\nEND;",
		},
		{
			name:   "a header carrying DETERMINISTIC and AUTHID",
			source: "FUNCTION fn_det(p IN NUMBER) RETURN NUMBER DETERMINISTIC AUTHID CURRENT_USER IS\nBEGIN\n  RETURN p;\nEND;\n",
			want:   "BEGIN\n  RETURN p;\nEND;",
		},
		{
			name: "a body whose own AS stands at depth zero",
			source: "FUNCTION fn_alias RETURN NUMBER IS\n  n NUMBER;\nBEGIN\n" +
				"  SELECT 1 AS ok INTO n FROM dual;\n  RETURN n;\nEND;\n",
			want: "n NUMBER;\nBEGIN\n  SELECT 1 AS ok INTO n FROM dual;\n  RETURN n;\nEND;",
		},
		{
			name:   "a parameter default mentioning the word IS inside a literal",
			source: "FUNCTION fn_lit(p IN VARCHAR2 DEFAULT 'is not') RETURN VARCHAR2 IS\nBEGIN\n  RETURN p;\nEND;\n",
			want:   "BEGIN\n  RETURN p;\nEND;",
		},
		{
			name: "a default expression whose CAST carries a standalone AS",
			source: "FUNCTION fn_cast(p IN VARCHAR2 DEFAULT CAST(1 AS VARCHAR2(2))) RETURN VARCHAR2 IS\n" +
				"BEGIN\n  RETURN p;\nEND;\n",
			want: "BEGIN\n  RETURN p;\nEND;",
		},
		{
			name:   "a block comment in the header carrying the word",
			source: "FUNCTION fn_cmt /* is it? */ RETURN NUMBER IS\nBEGIN\n  RETURN 1;\nEND;\n",
			want:   "BEGIN\n  RETURN 1;\nEND;",
		},
		{
			name:   "a line comment in the header carrying the word",
			source: "FUNCTION fn_line -- is it?\n RETURN NUMBER IS\nBEGIN\n  RETURN 1;\nEND;\n",
			want:   "BEGIN\n  RETURN 1;\nEND;",
		},
		{
			name:   "a return type whose name contains the word",
			source: "FUNCTION fn_axis RETURN axis_t IS\nBEGIN\n  RETURN NULL;\nEND;\n",
			want:   "BEGIN\n  RETURN NULL;\nEND;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oracleroutine.Body(test.source), qt.Equals, test.want)
		})
	}
}

// TestBody_HandsBackTheWholeTextWhenNoHeaderEndIsFound keeps a text this cannot
// read from disappearing.
//
// Reporting the empty string would compare as "the body was removed" and plan a
// replacement whose CREATE carries no body at all.
func TestBody_HandsBackTheWholeTextWhenNoHeaderEndIsFound(t *testing.T) {
	c := qt.New(t)
	c.Assert(oracleroutine.Body("FUNCTION broken(p IN NUMBER)"), qt.Equals, "FUNCTION broken(p IN NUMBER)")
}

// TestArgument_SpellsThePLSQLWordOrder pins the order and the case, because
// both sides of the comparison are built from it.
//
// PL/SQL puts the mode after the name, which is the opposite of PostgreSQL, and
// ALL_ARGUMENTS reports IN OUT as the single token IN/OUT.
func TestArgument_SpellsThePLSQLWordOrder(t *testing.T) {
	tests := []struct {
		name     string
		argument string
		mode     string
		dataType string
		want     string
	}{
		{name: "in", argument: "P", mode: "IN", dataType: "NUMBER", want: "p in number"},
		{name: "out", argument: "B", mode: "OUT", dataType: "NUMBER", want: "b out number"},
		{name: "in out", argument: "C", mode: "IN/OUT", dataType: "VARCHAR2", want: "c in out varchar2"},
		{name: "an unreported mode", argument: "D", mode: " ", dataType: "DATE", want: "d date"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oracleroutine.Argument(test.argument, test.mode, test.dataType), qt.Equals, test.want)
		})
	}
}

// TestFoldDefaultArgumentMode_DropsINAndKeepsTheRest is the rule that lets a
// declaration written either way converge against the catalog.
//
// The list has TWO parameters in the cases that carry one, because a fold
// applied to the first argument only would pass a one-element list and fail on
// every real signature.
func TestFoldDefaultArgumentMode_DropsINAndKeepsTheRest(t *testing.T) {
	tests := []struct {
		name       string
		parameters string
		want       string
	}{
		{name: "empty", parameters: "", want: ""},
		{name: "in on both", parameters: "p in number, q in varchar2", want: "p number, q varchar2"},
		{name: "already folded", parameters: "p number, q varchar2", want: "p number, q varchar2"},
		{name: "out is kept", parameters: "p in number, b out number", want: "p number, b out number"},
		{name: "in out is kept", parameters: "p in number, c in out varchar2", want: "p number, c in out varchar2"},
		{
			name:       "a default expression carrying a comma",
			parameters: "p in number, q in varchar2 default to_char(1, '9')",
			want:       "p number, q varchar2 default to_char(1, '9')",
		},
		{name: "a parameter named in", parameters: "in in number, q in date", want: "in number, q date"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oracleroutine.FoldDefaultArgumentMode(test.parameters), qt.Equals, test.want)
		})
	}
}

// TestSecurityClause_RoundTripsThroughTheCatalog is the round trip stated as
// one test, because the two halves are only correct together.
func TestSecurityClause_RoundTripsThroughTheCatalog(t *testing.T) {
	tests := []struct {
		name     string
		security string
		clause   string
		authID   string
	}{
		{name: "definer", security: "DEFINER", clause: "AUTHID DEFINER", authID: "DEFINER"},
		{name: "invoker", security: "INVOKER", clause: "AUTHID CURRENT_USER", authID: "CURRENT_USER"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			clause, err := oracleroutine.SecurityClause(test.security)
			c.Assert(err, qt.IsNil)
			c.Assert(clause, qt.Equals, test.clause)
			c.Assert(oracleroutine.SecurityFromCatalog(test.authID), qt.Equals, test.security)
		})
	}
}

// TestSecurityClause_RefusesAModeItCannotWrite keeps a misspelled mode from
// silently becoming the broader one.
func TestSecurityClause_RefusesAModeItCannotWrite(t *testing.T) {
	c := qt.New(t)
	clause, err := oracleroutine.SecurityClause("INVKOER")
	c.Assert(err, qt.IsNotNil)
	c.Assert(clause, qt.Equals, "")
	c.Assert(err.Error(), qt.Contains, "AUTHID CURRENT_USER")
}

// TestSecurityClause_WritesNothingForAnUnsetMode leaves the server its own
// default for a node built without one.
func TestSecurityClause_WritesNothingForAnUnsetMode(t *testing.T) {
	c := qt.New(t)
	clause, err := oracleroutine.SecurityClause("")
	c.Assert(err, qt.IsNil)
	c.Assert(clause, qt.Equals, "")
}

// TestDeterminismClause_RoundTripsTheTwoValuesOracleHas pins both directions of
// the one axis this engine reports.
func TestDeterminismClause_RoundTripsTheTwoValuesOracleHas(t *testing.T) {
	tests := []struct {
		name          string
		volatility    string
		clause        string
		deterministic string
	}{
		{name: "immutable", volatility: "IMMUTABLE", clause: "DETERMINISTIC", deterministic: "YES"},
		{name: "volatile", volatility: "VOLATILE", clause: "", deterministic: "NO"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			clause, err := oracleroutine.DeterminismClause(test.volatility)
			c.Assert(err, qt.IsNil)
			c.Assert(clause, qt.Equals, test.clause)
			c.Assert(oracleroutine.VolatilityFromCatalog(test.deterministic), qt.Equals, test.volatility)
		})
	}
}

// TestDeterminismClause_RefusesSTABLEByName is the decision this package makes
// most visibly, so it is pinned rather than left to a reader of the doc.
//
// Neither available answer is acceptable: DETERMINISTIC is a promise a
// function-based index may be built on, and the absence of the clause reads
// back as VOLATILE and plans the same replacement on every run.
func TestDeterminismClause_RefusesSTABLEByName(t *testing.T) {
	c := qt.New(t)
	clause, err := oracleroutine.DeterminismClause("STABLE")
	c.Assert(err, qt.IsNotNil)
	c.Assert(clause, qt.Equals, "")
	c.Assert(err.Error(), qt.Contains, "IMMUTABLE")
	c.Assert(err.Error(), qt.Contains, "VOLATILE")
}

// TestDeterminismClause_RefusesAValueItDoesNotKnow keeps a typo from rendering
// a routine whose volatility says something else.
func TestDeterminismClause_RefusesAValueItDoesNotKnow(t *testing.T) {
	c := qt.New(t)
	_, err := oracleroutine.DeterminismClause("IMMUTBALE")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "IMMUTBALE")
}

// TestRunsLanguage_AcceptsPLSQLAndTheEmptyNode is the predicate the renderer and
// the planner share.
//
// plpgsql is in the table because Canonicalize defaults an unset annotation to
// it, so it is the value that actually arrives when an author writes no
// language at all.
func TestRunsLanguage_AcceptsPLSQLAndTheEmptyNode(t *testing.T) {
	tests := []struct {
		name     string
		language string
		want     bool
	}{
		{name: "plsql", language: "plsql", want: true},
		{name: "PLSQL", language: "PLSQL", want: true},
		{name: "a node built without one", language: "", want: true},
		{name: "plpgsql, the annotation default", language: "plpgsql", want: false},
		{name: "sql", language: "sql", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oracleroutine.RunsLanguage(test.language), qt.Equals, test.want)
		})
	}
}

// TestParameterCarriesDefault_FindsBothSpellings pins the shape the renderer
// refuses, because the catalog reports that a default exists and never what it
// is.
func TestParameterCarriesDefault_FindsBothSpellings(t *testing.T) {
	tests := []struct {
		name       string
		parameters string
		want       bool
	}{
		{name: "none", parameters: "p IN NUMBER, q IN VARCHAR2", want: false},
		{name: "the DEFAULT keyword", parameters: "p IN NUMBER DEFAULT 1", want: true},
		{name: "the assignment spelling", parameters: "p IN NUMBER := 1", want: true},
		{name: "lower case", parameters: "p in number default 1", want: true},
		{name: "a literal containing the word", parameters: "p IN VARCHAR2, q IN VARCHAR2", want: false},
		{name: "a parameter named defaulted", parameters: "defaulted IN NUMBER", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oracleroutine.ParameterCarriesDefault(test.parameters), qt.Equals, test.want)
		})
	}
}
