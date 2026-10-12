package sqlext_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
)

// emptyGrammar reads every load as declaring nothing.
type emptyGrammar struct{}

func (emptyGrammar) ReadSQL(context.Context, sqlext.GrammarRequest) (sqlext.GrammarResult, error) {
	return sqlext.GrammarResult{}, nil
}

// noClaim claims an empty desired coverage whatever the header says.
func noClaim(sqlext.CoverageRequest) (schemaext.Coverage, error) {
	return schemaext.NewCoverage(schemaext.Desired, nil, nil)
}

// TestSet_Grammar returns the grammar registered for a target, and none for a
// target written in the shared grammar or for a document that names none.
func TestSet_Grammar(t *testing.T) {
	set := must.Must(sqlext.NewSet(
		sqlext.Extension{Owner: "example.org/a", Targets: []string{"a"}, Grammar: emptyGrammar{}},
		extension("example.org/b", "b", shortParser{}, sqlext.Pattern{Name: "index", Site: sqlext.TableElement, Words: []string{"INDEX"}}),
	))
	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{name: "the grammar's target", target: "a", want: true},
		{name: "a target of the shared grammar", target: "b", want: false},
		{name: "an unregistered target", target: "c", want: false},
		{name: "no target", target: "", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, found := set.Grammar(test.target)

			c.Assert(found, qt.Equals, test.want)
		})
	}
}

// TestSet_ReadsHeader answers whether an owner of the target reads a header
// kind, without regard to case and surrounding space. A kind that matches only
// under Unicode case folding is not the owner's, since the owner would record
// nothing for it.
func TestSet_ReadsHeader(t *testing.T) {
	set := must.Must(sqlext.NewSet(sqlext.Extension{Owner: "example.org/a", Targets: []string{"a"},
		Header: []coverage.Kind{"secret"}, Coverage: noClaim}))
	tests := []struct {
		name   string
		target string
		kind   coverage.Kind
		want   bool
	}{
		{name: "its kind", target: "a", kind: "secret", want: true},
		{name: "another case and space", target: "a", kind: " SECRET ", want: true},
		{name: "a long s", target: "a", kind: "ſecret", want: false},
		{name: "another kind", target: "a", kind: "table", want: false},
		{name: "another target", target: "b", kind: "secret", want: false},
		{name: "no target", target: "", kind: "secret", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(set.ReadsHeader(test.target, test.kind), qt.Equals, test.want)
		})
	}
}

// TestSet_Coverage_HandsEachOwnerItsHeaderEntries hands an owner's claim the
// header entries of the kinds it reads, and none of another owner's.
func TestSet_Coverage_HandsEachOwnerItsHeaderEntries(t *testing.T) {
	c := qt.New(t)
	var first, second []coverage.Object
	recording := func(into *[]coverage.Object) func(sqlext.CoverageRequest) (schemaext.Coverage, error) {
		return func(request sqlext.CoverageRequest) (schemaext.Coverage, error) {
			*into = request.Limits
			return noClaim(request)
		}
	}
	set := must.Must(sqlext.NewSet(
		sqlext.Extension{Owner: "example.org/a", Targets: []string{"a"}, Header: []coverage.Kind{"secret"}, Coverage: recording(&first)},
		sqlext.Extension{Owner: "example.org/b", Targets: []string{"a"}, Header: []coverage.Kind{"topic"}, Coverage: recording(&second)},
	))
	header := []coverage.Object{{Kind: "Secret", Name: "s"}, {Kind: "topic", Name: "t"}, {Kind: "secret"}}

	_, claimed, err := set.Coverage("a", sqlext.CoverageRequest{Limits: header})

	c.Assert(err, qt.IsNil)
	c.Assert(claimed, qt.IsTrue)
	c.Assert(first, qt.DeepEquals, []coverage.Object{{Kind: "Secret", Name: "s"}, {Kind: "secret"}})
	c.Assert(second, qt.DeepEquals, []coverage.Object{{Kind: "topic", Name: "t"}})
}

// TestFileError names the refused file and unwraps to the reason.
func TestFileError(t *testing.T) {
	c := qt.New(t)
	reason := errors.New("refused")

	err := error(&sqlext.FileError{Path: "schema/b.sql", Err: reason})

	c.Assert(err, qt.ErrorMatches, `schema/b.sql: refused`)
	c.Assert(err, qt.ErrorIs, reason)
}

// TestSet_Coverage_HandsOwnersTheDeclarations lets each owner derive its
// claim from the document's objects, including objects from another owner.
func TestSet_Coverage_HandsOwnersTheDeclarations(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.CoverageRequest
	record := func(request sqlext.CoverageRequest) (schemaext.Coverage, error) {
		requests = append(requests, request)
		return noClaim(request)
	}
	set := must.Must(sqlext.NewSet(
		sqlext.Extension{Owner: "example.org/a", Targets: []string{"a"}, Coverage: record},
		sqlext.Extension{Owner: "example.org/b", Targets: []string{"a"}, Coverage: record},
	))
	objects := must.Must(schemaext.NewObjects(gadgetObject(gadgetKind, "local"), gadgetObject("example.org/b/other", "foreign")))

	_, claimed, err := set.Coverage("a", sqlext.CoverageRequest{Declared: objects})

	c.Assert(err, qt.IsNil)
	c.Assert(claimed, qt.IsTrue)
	c.Assert(requests, qt.HasLen, 2)
	for _, request := range requests {
		c.Assert(must.Must(request.Declared.All()), qt.DeepEquals, must.Must(objects.All()))
	}
}
