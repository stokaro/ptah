package yamlext_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
)

func batchOwner(owner, key string) yamlext.Extension {
	return yamlext.Extension{Owner: owner, Sections: []yamlext.Section{{Key: key}},
		Document: func(context.Context, yamlext.Document) (yamlext.DocumentResult, error) {
			return yamlext.DocumentResult{}, nil
		}}
}

func TestDocumentOwnersReceiveIndependentInputs(t *testing.T) {
	c := qt.New(t)
	node := &yamlext.Node{Kind: yamlext.ScalarNode, Tag: "!!int", Value: "9007199254740993"}
	a, b := batchOwner("example.org/a", "first"), batchOwner("example.org/b", "second")
	var calls []string
	a.Document = func(_ context.Context, input yamlext.Document) (yamlext.DocumentResult, error) {
		calls = append(calls, "a")
		c.Assert(input.Fragments, qt.HasLen, 1)
		input.Source[0] = 'x'
		input.Tables[0].Name = "mutated"
		input.Fragments[0].Path[0] = "mutated"
		input.Fragments[0].Node.Value = "0"
		return yamlext.DocumentResult{}, nil
	}
	b.Document = func(_ context.Context, input yamlext.Document) (yamlext.DocumentResult, error) {
		calls = append(calls, "b")
		c.Assert(string(input.Source), qt.Equals, "source")
		c.Assert(input.Tables[0].Name, qt.Equals, "table")
		c.Assert(input.Fragments, qt.HasLen, 1)
		c.Assert(input.Fragments[0].Path, qt.DeepEquals, []string{"second"})
		c.Assert(input.Fragments[0].Node.Value, qt.Equals, "9007199254740993")
		return yamlext.DocumentResult{}, nil
	}
	set, err := yamlext.NewSet(a, b)
	c.Assert(err, qt.IsNil)
	input := yamlext.Document{Source: []byte("source"), Tables: yamlext.Tables{{Name: "table"}}, Fragments: []yamlext.Fragment{
		{Path: []string{"first"}, Table: -1, Node: node}, {Path: []string{"second"}, Table: -1, Node: node},
	}}
	_, err = set.DecodeDocuments(t.Context(), input)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.DeepEquals, []string{"a", "b"})
	c.Assert(string(input.Source), qt.Equals, "source")
	c.Assert(input.Tables[0].Name, qt.Equals, "table")
	c.Assert(input.Fragments[0].Path[0], qt.Equals, "first")
	c.Assert(node.Value, qt.Equals, "9007199254740993")
}

func TestDocumentOwnerFailureHasNoSuccessfulPrefix(t *testing.T) {
	c := qt.New(t)
	refused := errors.New("second owner failed")
	a, b := batchOwner("example.org/widget", "first"), batchOwner("example.org/b", "second")
	a.Kinds = []schemaext.Kind{levelKind}
	a.Document = func(context.Context, yamlext.Document) (yamlext.DocumentResult, error) {
		return yamlext.DocumentResult{Contributions: []yamlext.Contribution{{Facet: &level{Value: "first"}}}}, nil
	}
	b.Document = func(context.Context, yamlext.Document) (yamlext.DocumentResult, error) {
		return yamlext.DocumentResult{}, refused
	}
	set, err := yamlext.NewSet(a, b)
	c.Assert(err, qt.IsNil)
	result, err := set.DecodeDocuments(t.Context(), yamlext.Document{Tables: yamlext.Tables{{Name: "table"}}})
	c.Assert(err, qt.ErrorIs, refused)
	c.Assert(result.Contributions, qt.IsNil)
	c.Assert(result.Coverage.IsZero(), qt.IsTrue)
}

func TestDocumentOwnerRegistrationRefusesMixedInterpretation(t *testing.T) {
	cases := []struct {
		name   string
		change func(*yamlext.Extension)
	}{
		{"static coverage", func(e *yamlext.Extension) { e.Coverage = emptyClaim }},
		{"section callback", func(e *yamlext.Extension) { e.Sections[0].Decode = noDecode }},
		{"table callback", func(e *yamlext.Extension) {
			e.EntrySections = []yamlext.EntrySection{{Key: "paint", Decode: func(func(any) error, yamlext.Table) ([]yamlext.Contribution, error) { return nil, nil }}}
		}},
		{"entry attributes", func(e *yamlext.Extension) { e.EntryAttributes = []yamlext.EntryAttributes{{Entry: yamlext.EntryTable}} }},
		{"scoped entries", func(e *yamlext.Extension) { e.TargetScopes = []yamlext.TargetScope{{Key: "rls_policies"}} }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := batchOwner("example.org/widget", "levels")
			test.change(&owner)
			set, err := yamlext.NewSet(owner)
			c.Assert(err, qt.IsNotNil)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

func TestDocumentOwnerClaimsCannotOverlap(t *testing.T) {
	repeatedTop := batchOwner("example.org/a", "levels")
	repeatedTop.Sections = append(repeatedTop.Sections, repeatedTop.Sections[0])
	repeatedTable := batchOwner("example.org/a", "levels")
	repeatedTable.EntrySections = []yamlext.EntrySection{{Key: "paint"}, {Key: "paint"}}
	cases := []struct {
		name       string
		extensions []yamlext.Extension
	}{
		{"same section within an owner", []yamlext.Extension{repeatedTop}},
		{"same table key within an owner", []yamlext.Extension{repeatedTable}},
		{"same owner twice", []yamlext.Extension{batchOwner("example.org/a", "first"), batchOwner("example.org/a", "second")}},
		{"same section twice", []yamlext.Extension{batchOwner("example.org/a", "first"), batchOwner("example.org/b", "first")}},
		{"local and batch section", []yamlext.Extension{batchOwner("example.org/a", "levels"), sectionOwner("example.org/widget", "levels", noDecode)}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, err := yamlext.NewSet(test.extensions...)
			c.Assert(err, qt.IsNotNil)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

func TestDocumentOwnerRejectsInvalidResults(t *testing.T) {
	c := qt.New(t)
	claim, err := levelCoverage()
	c.Assert(err, qt.IsNil)
	observed, err := schemaext.NewCoverage(schemaext.Observed, nil, nil)
	c.Assert(err, qt.IsNil)
	cases := []struct {
		name   string
		result yamlext.DocumentResult
	}{
		{"another owner coverage", yamlext.DocumentResult{Coverage: claim}},
		{"empty observed coverage", yamlext.DocumentResult{Coverage: observed}},
		{"another model", yamlext.DocumentResult{Contributions: []yamlext.Contribution{{Facet: &other{}}}}},
		{"unbound table", yamlext.DocumentResult{Contributions: []yamlext.Contribution{{Facet: &level{}, Table: 1}}}},
		{"empty contribution", yamlext.DocumentResult{Contributions: []yamlext.Contribution{{}}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := batchOwner("example.org/a", "levels")
			owner.Kinds = []schemaext.Kind{levelKind}
			owner.Document = func(context.Context, yamlext.Document) (yamlext.DocumentResult, error) { return test.result, nil }
			set, err := yamlext.NewSet(owner)
			c.Assert(err, qt.IsNil)
			result, err := set.DecodeDocuments(t.Context(), yamlext.Document{Tables: yamlext.Tables{{Name: "table"}}})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Contributions, qt.IsNil)
			c.Assert(result.Coverage.IsZero(), qt.IsTrue)
		})
	}
}

func TestDocumentOwnerCannotBeCalledPerKey(t *testing.T) {
	c := qt.New(t)
	owner := batchOwner("example.org/a", "levels")
	owner.EntrySections = []yamlext.EntrySection{{Key: "paint"}}
	set, err := yamlext.NewSet(owner)
	c.Assert(err, qt.IsNil)
	_, err = set.Decode("levels", func(any) error { return nil }, nil)
	c.Assert(err, qt.ErrorMatches, ".*requires whole-document decoding")
	_, err = set.DecodeEntrySection("paint", func(any) error { return nil }, yamlext.Table{})
	c.Assert(err, qt.ErrorMatches, ".*requires whole-document decoding")
}

func TestDocumentFragmentsAreValidatedBeforeAnyOwnerCall(t *testing.T) {
	node := &yamlext.Node{Kind: yamlext.ScalarNode, Tag: "!!str", Value: "value"}
	valid := yamlext.Fragment{Path: []string{"levels"}, Table: -1, Node: node}
	cases := []struct {
		name      string
		fragments []yamlext.Fragment
	}{
		{"duplicate", []yamlext.Fragment{valid, valid}},
		{"unknown key", []yamlext.Fragment{{Path: []string{"unknown"}, Table: -1, Node: node}}},
		{"missing node", []yamlext.Fragment{{Path: []string{"levels"}, Table: -1}}},
		{"table index out of range", []yamlext.Fragment{{Path: []string{"tables", "a", "paint"}, Table: 1, Node: node}}},
		{"mismatched table key", []yamlext.Fragment{{Path: []string{"tables", "b", "paint"}, Table: 0, Node: node}}},
		{"missing path", []yamlext.Fragment{{Table: -1, Node: node}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			owner := batchOwner("example.org/a", "levels")
			owner.EntrySections = []yamlext.EntrySection{{Key: "paint"}}
			owner.Document = func(context.Context, yamlext.Document) (yamlext.DocumentResult, error) {
				calls++
				return yamlext.DocumentResult{}, nil
			}
			set, err := yamlext.NewSet(owner)
			c.Assert(err, qt.IsNil)
			result, err := set.DecodeDocuments(t.Context(), yamlext.Document{Tables: yamlext.Tables{{Key: "a"}}, Fragments: test.fragments})
			c.Assert(err, qt.IsNotNil)
			c.Assert(calls, qt.Equals, 0)
			c.Assert(result.Contributions, qt.IsNil)
			c.Assert(result.Coverage.IsZero(), qt.IsTrue)
		})
	}
}
