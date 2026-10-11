package sqlext_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
)

// echoParser answers each claim with an index named after the claim's text,
// and records the requests it was given.
type echoParser struct {
	requests *[]sqlext.Request
}

func (p echoParser) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	*p.requests = append(*p.requests, request)
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		values = append(values, sqlext.Value{Index: &sqlext.Index{Name: claim.Text}})
	}
	return sqlext.Result{Values: values}, nil
}

// shortParser answers every request with no values.
type shortParser struct{}

func (shortParser) ParseSQL(context.Context, sqlext.Request) (sqlext.Result, error) {
	return sqlext.Result{}, nil
}

var errRefused = errors.New("refused")

// refusingParser refuses every request.
type refusingParser struct{}

func (refusingParser) ParseSQL(context.Context, sqlext.Request) (sqlext.Result, error) {
	return sqlext.Result{}, errRefused
}

func extension(owner, target string, parser sqlext.Parser, patterns ...sqlext.Pattern) sqlext.Extension {
	return sqlext.Extension{Owner: owner, Targets: []string{target}, Patterns: patterns, Parser: parser}
}

func TestNewSet_FailurePath(t *testing.T) {
	parser := shortParser{}
	index := sqlext.Pattern{Name: "index", Site: sqlext.TableElement, Words: []string{"INDEX"}}
	tests := []struct {
		name       string
		extensions []sqlext.Extension
		wantErr    string
	}{
		{name: "no owner", extensions: []sqlext.Extension{extension("", "a", parser, index)},
			wantErr: `SQL extension names no owner`},
		{name: "no target", extensions: []sqlext.Extension{{Owner: "example.org/a", Patterns: []sqlext.Pattern{index}, Parser: parser}},
			wantErr: `SQL extension of example.org/a names no target`},
		{name: "no parser", extensions: []sqlext.Extension{extension("example.org/a", "a", nil, index)},
			wantErr: `SQL extension of example.org/a declares patterns without a parser`},
		{name: "a pattern without a name", extensions: []sqlext.Extension{extension("example.org/a", "a", parser,
			sqlext.Pattern{Site: sqlext.TableElement, Words: []string{"INDEX"}})},
			wantErr: `SQL extension of example.org/a declares a pattern without a name`},
		{name: "a pattern named twice", extensions: []sqlext.Extension{extension("example.org/a", "a", parser, index,
			sqlext.Pattern{Name: "index", Site: sqlext.ViewClause, Words: []string{"REFRESH"}})},
			wantErr: `duplicate.*: SQL extension of example.org/a declares pattern "index" twice`},
		{name: "an unknown site", extensions: []sqlext.Extension{extension("example.org/a", "a", parser,
			sqlext.Pattern{Name: "index", Site: "column", Words: []string{"INDEX"}})},
			wantErr: `SQL pattern "index" of example.org/a names unknown site "column"`},
		{name: "a word with a space", extensions: []sqlext.Extension{extension("example.org/a", "a", parser,
			sqlext.Pattern{Name: "index", Site: sqlext.TableElement, Words: []string{"ADD INDEX"}})},
			wantErr: `SQL pattern "index" of example.org/a opens with no words`},
		{name: "two owners of one target and the same words", extensions: []sqlext.Extension{
			extension("example.org/a", "a", parser, index), extension("example.org/b", "a", parser, index)},
			wantErr: `duplicate.*: SQL pattern "index" of example.org/a and pattern "index" of example.org/b can match the same table-element syntax`},
		{name: "two untargeted patterns of different targets", extensions: []sqlext.Extension{
			extension("example.org/a", "a", parser, index), extension("example.org/b", "b", parser, index)},
			wantErr: `duplicate.*can match the same table-element syntax`},
		{name: "a longer pattern the shorter one admits", extensions: []sqlext.Extension{
			extension("example.org/a", "a", parser, index),
			extension("example.org/b", "a", parser, sqlext.Pattern{Name: "fulltext", Site: sqlext.TableElement, Words: []string{"INDEX", "FULLTEXT"}})},
			wantErr: `duplicate.*can match the same table-element syntax`},
		{name: "next words both admit", extensions: []sqlext.Extension{
			extension("example.org/a", "a", parser, sqlext.Pattern{Name: "ttl", Site: sqlext.AlterOperation, Words: []string{"TTL"}, Next: []string{"INTERVAL", "ON"}}),
			extension("example.org/b", "a", parser, sqlext.Pattern{Name: "ttl", Site: sqlext.AlterOperation, Words: []string{"TTL"}, NotNext: []string{"ON"}})},
			wantErr: `duplicate.*can match the same alter-operation syntax`},
		{name: "patterns beside a grammar", extensions: []sqlext.Extension{
			{Owner: "example.org/a", Targets: []string{"a"}, Patterns: []sqlext.Pattern{index}, Parser: parser, Grammar: emptyGrammar{}}},
			wantErr: `SQL extension of example.org/a declares patterns beside a grammar`},
		{name: "two grammars of one target", extensions: []sqlext.Extension{
			{Owner: "example.org/a", Targets: []string{"a"}, Grammar: emptyGrammar{}},
			{Owner: "example.org/b", Targets: []string{"b", "a"}, Grammar: emptyGrammar{}}},
			wantErr: `duplicate.*: SQL grammar of target "a" is registered by example.org/a and example.org/b`},
		{name: "patterns for a grammar's target", extensions: []sqlext.Extension{
			{Owner: "example.org/a", Targets: []string{"a"}, Grammar: emptyGrammar{}},
			extension("example.org/b", "a", parser, index)},
			wantErr: `SQL extension of example.org/b declares patterns for target "a", whose documents the grammar of example.org/a reads`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := sqlext.NewSet(test.extensions...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

// TestNewSet_HappyPath accepts patterns no document consults together, and
// patterns whose next words tell them apart.
func TestNewSet_HappyPath(t *testing.T) {
	parser := shortParser{}
	index := sqlext.Pattern{Name: "index", Site: sqlext.TableElement, Words: []string{"INDEX"}, Targeted: true}
	tests := []struct {
		name       string
		extensions []sqlext.Extension
	}{
		{name: "targeted patterns of different targets", extensions: []sqlext.Extension{
			extension("example.org/a", "a", parser, index), extension("example.org/b", "b", parser, index)}},
		{name: "one site each", extensions: []sqlext.Extension{
			extension("example.org/a", "a", parser, sqlext.Pattern{Name: "index", Site: sqlext.TableElement, Words: []string{"INDEX"}}),
			extension("example.org/b", "a", parser, sqlext.Pattern{Name: "index", Site: sqlext.AlterOperation, Words: []string{"INDEX"}})}},
		{name: "next words told apart", extensions: []sqlext.Extension{
			extension("example.org/a", "a", parser, sqlext.Pattern{Name: "ttl", Site: sqlext.AlterOperation, Words: []string{"TTL"}, Next: []string{"INTERVAL"}}),
			extension("example.org/b", "a", parser, sqlext.Pattern{Name: "ttl", Site: sqlext.AlterOperation, Words: []string{"TTL"}, NotNext: []string{"INTERVAL"}})}},
		{name: "different opening words", extensions: []sqlext.Extension{
			extension("example.org/a", "a", parser, sqlext.Pattern{Name: "a", Site: sqlext.ViewClause, Words: []string{"REFRESH"}}),
			extension("example.org/b", "a", parser, sqlext.Pattern{Name: "b", Site: sqlext.ViewClause, Words: []string{"ENGINE"}})}},
		{name: "a grammar beside patterns of another target", extensions: []sqlext.Extension{
			{Owner: "example.org/a", Targets: []string{"a"}, Grammar: emptyGrammar{}},
			extension("example.org/b", "b", parser, index)}},
		{name: "a grammar beside an owner of its target that claims coverage", extensions: []sqlext.Extension{
			{Owner: "example.org/a", Targets: []string{"a"}, Grammar: emptyGrammar{}},
			{Owner: "example.org/b", Targets: []string{"a"}, Coverage: noClaim}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := sqlext.NewSet(test.extensions...)

			c.Assert(err, qt.IsNil)
			c.Assert(set.Selected(), qt.IsTrue)
		})
	}
}

// TestSet_Candidates hands a document the patterns of the target it names,
// and a document that names none every pattern that is not targeted.
func TestSet_Candidates(t *testing.T) {
	parser := shortParser{}
	set := must.Must(sqlext.NewSet(
		extension("example.org/a", "a", parser,
			sqlext.Pattern{Name: "untargeted", Site: sqlext.AlterOperation, Words: []string{"ADD", "INDEX"}},
			sqlext.Pattern{Name: "targeted", Site: sqlext.AlterOperation, Words: []string{"ADD", "PROJECTION"}, Targeted: true},
			sqlext.Pattern{Name: "elsewhere", Site: sqlext.ViewClause, Words: []string{"REFRESH"}}),
		extension("example.org/b", "b", parser,
			sqlext.Pattern{Name: "other", Site: sqlext.AlterOperation, Words: []string{"ADD", "FAMILY"}, Targeted: true}),
	))
	tests := []struct {
		name   string
		target string
		want   []string
	}{
		{name: "a target", target: "a", want: []string{"untargeted", "targeted"}},
		{name: "another target", target: "b", want: []string{"other"}},
		{name: "no target", target: "", want: []string{"untargeted"}},
		{name: "an unregistered target", target: "c", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(patternNames(set.Candidates(test.target, sqlext.AlterOperation)), qt.DeepEquals, test.want)
		})
	}
}

func patternNames(candidates []sqlext.Candidate) []string {
	var names []string
	for _, candidate := range candidates {
		names = append(names, candidate.Pattern.Name)
	}
	return names
}

// TestCandidate_Matches compares the opening words without regard to case,
// and the word after them against Next and NotNext.
func TestCandidate_Matches(t *testing.T) {
	set := must.Must(sqlext.NewSet(extension("example.org/a", "a", shortParser{},
		sqlext.Pattern{Name: "ttl", Site: sqlext.AlterOperation, Words: []string{"TTL"}, NotNext: []string{"INTERVAL"}},
		sqlext.Pattern{Name: "add", Site: sqlext.TableElement, Words: []string{"ADD", "INDEX"}, Next: []string{"skip"}},
	)))
	ttl := set.Candidates("a", sqlext.AlterOperation)[0]
	add := set.Candidates("a", sqlext.TableElement)[0]
	tests := []struct {
		name      string
		candidate sqlext.Candidate
		words     []string
		want      bool
	}{
		{name: "the words in another case", candidate: ttl, words: []string{"ttl", "created"}, want: true},
		{name: "the words alone", candidate: ttl, words: []string{"TTL"}, want: true},
		{name: "an excluded next word", candidate: ttl, words: []string{"TTL", "interval"}, want: false},
		{name: "other words", candidate: ttl, words: []string{"TTLS"}, want: false},
		{name: "too few words", candidate: add, words: []string{"ADD"}, want: false},
		{name: "a required next word", candidate: add, words: []string{"add", "index", "SKIP"}, want: true},
		{name: "another next word", candidate: add, words: []string{"ADD", "INDEX", "i"}, want: false},
		{name: "no next word where one is required", candidate: add, words: []string{"ADD", "INDEX"}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.candidate.Matches(test.words), qt.Equals, test.want)
		})
	}
}

// TestSet_Parse_HappyPath calls each extension once with its own claims in
// the document's order, and returns the values in the order of the matches.
func TestSet_Parse_HappyPath(t *testing.T) {
	c := qt.New(t)
	var first, second []sqlext.Request
	set := must.Must(sqlext.NewSet(
		extension("example.org/a", "x", echoParser{requests: &first}, sqlext.Pattern{Name: "a", Site: sqlext.TableElement, Words: []string{"A"}}),
		extension("example.org/b", "x", echoParser{requests: &second}, sqlext.Pattern{Name: "b", Site: sqlext.TableElement, Words: []string{"B"}}),
	))
	a := set.Candidates("x", sqlext.TableElement)[0]
	b := set.Candidates("x", sqlext.TableElement)[1]
	matches := []sqlext.Match{
		{Candidate: b, Claim: sqlext.Claim{Pattern: "b", Text: "B 1"}},
		{Candidate: a, Claim: sqlext.Claim{Pattern: "a", Text: "A 2"}},
		{Candidate: b, Claim: sqlext.Claim{Pattern: "b", Text: "B 3"}},
	}

	values, err := set.Parse(t.Context(), "x", matches, schemaext.Objects{})

	c.Assert(err, qt.IsNil)
	c.Assert(indexNames(values), qt.DeepEquals, []string{"B 1", "A 2", "B 3"})
	c.Assert(first, qt.HasLen, 1)
	c.Assert(second, qt.HasLen, 1)
	c.Assert(first[0].Target, qt.Equals, "x")
	c.Assert(claimTexts(second[0].Claims), qt.DeepEquals, []string{"B 1", "B 3"})
}

func indexNames(values []sqlext.Value) []string {
	names := make([]string, 0, len(values))
	for _, value := range values {
		names = append(names, value.Index.Name)
	}
	return names
}

func claimTexts(claims []sqlext.Claim) []string {
	texts := make([]string, 0, len(claims))
	for _, claim := range claims {
		texts = append(texts, claim.Text)
	}
	return texts
}

func TestSet_Parse_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		parser  sqlext.Parser
		wantErr string
	}{
		{name: "a refusal", parser: refusingParser{}, wantErr: "refused"},
		{name: "too few values", parser: shortParser{}, wantErr: `invalid.*: SQL parser of example.org/a answered 1 claims with 0 values`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := must.Must(sqlext.NewSet(extension("example.org/a", "x", test.parser,
				sqlext.Pattern{Name: "a", Site: sqlext.TableElement, Words: []string{"A"}})))
			matches := []sqlext.Match{{Candidate: set.Candidates("x", sqlext.TableElement)[0], Claim: sqlext.Claim{Pattern: "a", Text: "A"}}}

			values, err := set.Parse(t.Context(), "x", matches, schemaext.Objects{})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}

// TestSet_Coverage claims for a document of a target what its extensions
// claim, and nothing for a document of another target or of none.
func TestSet_Coverage(t *testing.T) {
	set := must.Must(sqlext.NewSet(sqlext.Extension{Owner: "example.org/a", Targets: []string{"a"}, Coverage: noClaim}))
	tests := []struct {
		name        string
		target      string
		wantClaimed bool
	}{
		{name: "its target", target: "a", wantClaimed: true},
		{name: "another target", target: "b", wantClaimed: false},
		{name: "no target", target: "", wantClaimed: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, claimed, err := set.Coverage(test.target, nil)

			c.Assert(err, qt.IsNil)
			c.Assert(claimed, qt.Equals, test.wantClaimed)
		})
	}
}

// TestNone_IsSelectedWithoutOwners selects no owner, where the zero value
// selects nothing at all.
func TestNone_IsSelectedWithoutOwners(t *testing.T) {
	c := qt.New(t)
	c.Assert(sqlext.None().Selected(), qt.IsTrue)
	c.Assert(sqlext.None().Candidates("a", sqlext.TableElement), qt.HasLen, 0)
	c.Assert(sqlext.Set{}.Selected(), qt.IsFalse)
	c.Assert(sqlext.ErrUnselected.Error(), qt.Contains, "SQL")
}
