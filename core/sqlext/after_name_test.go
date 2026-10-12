package sqlext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
)

// typeShape is a statement pattern that reads a name after CREATE TYPE and
// claims the statement when AS and one of next follow it.
func typeShape(name string, next ...string) sqlext.Pattern {
	return sqlext.Pattern{Name: name, Site: sqlext.Statement, Words: []string{"CREATE", "TYPE"}, AfterName: []string{"AS"}, Next: next}
}

// TestCandidate_MatchesNamed compares the words before and after the name the
// frontend read, without regard to case, and the word after AfterName against
// Next. A pattern with AfterName never matches the flat word list, and one
// without it never matches named words.
func TestCandidate_MatchesNamed(t *testing.T) {
	set := must.Must(sqlext.NewSet(extension("example.org/a", "a", shortParser{}, typeShape("shape", "(", "RANGE"),
		sqlext.Pattern{Name: "plain", Site: sqlext.Statement, Words: []string{"CREATE", "WIDGET"}})))
	shape, plain := set.Candidates("a", sqlext.Statement)[0], set.Candidates("a", sqlext.Statement)[1]
	tests := []struct {
		name          string
		candidate     sqlext.Candidate
		before, after []string
		want          bool
	}{
		{name: "a composite shape", candidate: shape, before: []string{"create", "type"}, after: []string{"as", "("}, want: true},
		{name: "a range shape", candidate: shape, before: []string{"CREATE", "TYPE"}, after: []string{"AS", "range"}, want: true},
		{name: "an enum", candidate: shape, before: []string{"CREATE", "TYPE"}, after: []string{"AS", "ENUM"}, want: false},
		{name: "no word after AS where one is required", candidate: shape, before: []string{"CREATE", "TYPE"}, after: []string{"AS"}, want: false},
		{name: "another word after the name", candidate: shape, before: []string{"CREATE", "TYPE"}, after: []string{"UNDER", "("}, want: false},
		{name: "other opening words", candidate: shape, before: []string{"CREATE", "DOMAIN"}, after: []string{"AS", "("}, want: false},
		{name: "too few opening words", candidate: shape, before: []string{"CREATE"}, after: []string{"AS", "("}, want: false},
		{name: "a pattern without AfterName", candidate: plain, before: []string{"CREATE", "WIDGET"}, after: []string{"AS"}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.candidate.MatchesNamed(test.before, test.after), qt.Equals, test.want)
		})
	}
}

// TestCandidate_MatchesNeverMatchesANamedPattern pins that the flat word list
// cannot open a pattern that reads a name: only the frontend knows where the
// name ends.
func TestCandidate_MatchesNeverMatchesANamedPattern(t *testing.T) {
	c := qt.New(t)
	set := must.Must(sqlext.NewSet(extension("example.org/a", "a", shortParser{}, typeShape("shape"))))

	c.Assert(set.Candidates("a", sqlext.Statement)[0].Matches([]string{"CREATE", "TYPE", "x", "AS", "("}), qt.IsFalse)
}

// TestNewSet_RefusesNamedPatternsThatOverlap refuses two patterns some
// statement matches both of, when one or both read a name: a name is any
// identifier, so a pattern whose next word is the other's name admits it.
func TestNewSet_RefusesNamedPatternsThatOverlap(t *testing.T) {
	plain := func(words []string, next ...string) sqlext.Pattern {
		return sqlext.Pattern{Name: "plain", Site: sqlext.Statement, Words: words, Next: next}
	}
	tests := []struct {
		name  string
		other sqlext.Pattern
	}{
		{name: "same words, no AfterName, no Next", other: plain([]string{"CREATE", "TYPE"})},
		{name: "same words, no AfterName, a Next a name may spell", other: plain([]string{"CREATE", "TYPE"}, "ENUM")},
		{name: "shorter words whose next word the other opens with", other: plain([]string{"CREATE"}, "TYPE")},
		{name: "the same AfterName and a Next both admit", other: typeShape("other", "(")},
		{name: "the same AfterName and no Next", other: typeShape("other")},
		{name: "a longer AfterName whose last word the shorter one's Next admits", other: sqlext.Pattern{Name: "other", Site: sqlext.Statement,
			Words: []string{"CREATE", "TYPE"}, AfterName: []string{"AS", "("}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := sqlext.NewSet(extension("example.org/a", "a", shortParser{}, typeShape("shape", "(", "RANGE")),
				extension("example.org/b", "a", shortParser{}, test.other))

			c.Assert(err, qt.ErrorMatches, `duplicate.*can match the same statement syntax`)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

// TestNewSet_AcceptsNamedPatternsToldApart is the control: the word after
// AfterName, the word after the name or the opening words tell the patterns
// apart.
func TestNewSet_AcceptsNamedPatternsToldApart(t *testing.T) {
	tests := []struct {
		name  string
		other sqlext.Pattern
	}{
		{name: "another word after AS", other: typeShape("other", "ENUM")},
		{name: "another word after the name", other: sqlext.Pattern{Name: "other", Site: sqlext.Statement, Words: []string{"CREATE", "TYPE"},
			AfterName: []string{"UNDER"}}},
		{name: "other opening words", other: sqlext.Pattern{Name: "other", Site: sqlext.Statement, Words: []string{"CREATE", "DOMAIN"}}},
		{name: "a shorter pattern whose Next excludes the other's next word", other: sqlext.Pattern{Name: "other", Site: sqlext.Statement,
			Words: []string{"CREATE"}, Next: []string{"DOMAIN"}}},
		{name: "a longer AfterName whose last word the shorter one's Next excludes", other: sqlext.Pattern{Name: "other", Site: sqlext.Statement,
			Words: []string{"CREATE", "TYPE"}, AfterName: []string{"AS", "ENUM"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := sqlext.NewSet(extension("example.org/a", "a", shortParser{}, typeShape("shape", "(", "RANGE")),
				extension("example.org/b", "a", shortParser{}, test.other))

			c.Assert(err, qt.IsNil)
			c.Assert(set.Selected(), qt.IsTrue)
		})
	}
}

// TestNewSet_RefusesAfterNameOutsideAStatement pins that only a statement
// pattern reads a name, and that every word after it is one word.
func TestNewSet_RefusesAfterNameOutsideAStatement(t *testing.T) {
	tests := []struct {
		name    string
		pattern sqlext.Pattern
	}{
		{name: "a table element", pattern: sqlext.Pattern{Name: "shape", Site: sqlext.TableElement, Words: []string{"INDEX"}, AfterName: []string{"ON"}}},
		{name: "a blank word", pattern: sqlext.Pattern{Name: "shape", Site: sqlext.Statement, Words: []string{"CREATE", "TYPE"}, AfterName: []string{" "}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := sqlext.NewSet(extension("example.org/a", "a", shortParser{}, test.pattern))

			c.Assert(err, qt.ErrorMatches, `SQL pattern "shape" of example.org/a names words after a name, which only a statement pattern takes, and each a word`)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

// TestSet_Parse_AcceptsADeclinedStatement passes a statement's decline
// through, which the frontend reads again with the shared grammar.
func TestSet_Parse_AcceptsADeclinedStatement(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request
	set := gadgetSet(sqlext.Statement, answeringParser{value: sqlext.Value{Declined: true}, requests: &requests})
	matches := []sqlext.Match{{Candidate: set.Candidates("x", sqlext.Statement)[0],
		Claim: sqlext.Claim{Pattern: "gadget", Site: sqlext.Statement, Text: "COMMENT ON GADGET g IS 'x'"}}}

	values, err := set.Parse(t.Context(), "x", matches, sqlext.Document{})

	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, []sqlext.Value{{Declined: true}})
}

// TestSet_Parse_RefusesADeclineItCannotHonor refuses a decline of a claim
// other than a statement, which the frontend could not read again by itself,
// and a decline carrying a value beside it.
func TestSet_Parse_RefusesADeclineItCannotHonor(t *testing.T) {
	tests := []struct {
		name    string
		site    sqlext.Site
		value   sqlext.Value
		wantErr string
	}{
		{name: "a table element", site: sqlext.TableElement, value: sqlext.Value{Declined: true},
			wantErr: `invalid feature value: SQL parser of example.org/a declined its table-element claim "gadget"; only a statement claim may decline`},
		{name: "a column option", site: sqlext.ColumnOption, value: sqlext.Value{Declined: true},
			wantErr: `invalid feature value: SQL parser of example.org/a declined its column-option claim "gadget"; only a statement claim may decline`},
		{name: "objects beside it", site: sqlext.Statement, value: sqlext.Value{Declined: true, Objects: []schemaext.Object{gadgetObject(gadgetKind, "new")}},
			wantErr: `invalid feature value: SQL parser of example.org/a declined its statement claim "gadget" and answered a value beside it`},
		{name: "a statement beside it", site: sqlext.Statement, value: sqlext.Value{Declined: true, Statement: &gadgetStatement{}},
			wantErr: `invalid feature value: SQL parser of example.org/a declined its statement claim "gadget" and answered a value beside it`},
		{name: "table references beside it", site: sqlext.Statement, value: sqlext.Value{Declined: true, TableReferences: []string{"t"}},
			wantErr: `invalid feature value: SQL parser of example.org/a declined its statement claim "gadget" and answered a value beside it`},
		{name: "refusal beside it", site: sqlext.Statement, value: sqlext.Value{Declined: true, UnmodeledReason: "not a declaration"},
			wantErr: `invalid feature value: SQL parser of example.org/a declined its statement claim "gadget" and answered a value beside it`},
		{name: "table facets beside it", site: sqlext.Statement, value: sqlext.Value{Declined: true, TableFacets: &sqlext.TableFacets{Name: "t"}},
			wantErr: `invalid feature value: SQL parser of example.org/a declined its statement claim "gadget" and answered a value beside it`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []sqlext.Request
			set := gadgetSet(test.site, answeringParser{value: test.value, requests: &requests})
			matches := []sqlext.Match{{Candidate: set.Candidates("x", test.site)[0],
				Claim: sqlext.Claim{Pattern: "gadget", Site: test.site, Text: "COMMENT ON GADGET g IS 'x'"}}}

			values, err := set.Parse(t.Context(), "x", matches, sqlext.Document{})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}
