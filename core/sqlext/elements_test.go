package sqlext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
)

func childGadget() schemaext.Object {
	object := gadgetObject(gadgetKind, "g")
	object.Ref.Parent = objectidentity.Part{Source: "t", Normalized: "t"}
	return object
}

func TestSet_Parse_ChildDeclarations(t *testing.T) {
	tests := []struct {
		site  sqlext.Site
		value sqlext.Value
	}{
		{site: sqlext.TableElement, value: sqlext.Value{Objects: []schemaext.Object{childGadget()}}},
		{site: sqlext.AlterOperation, value: sqlext.Value{Objects: []schemaext.Object{childGadget()}, Operation: &gadgetStatement{}}},
	}
	for _, test := range tests {
		t.Run(string(test.site), func(t *testing.T) {
			c := qt.New(t)
			set := gadgetSet(test.site, answeringParser{value: test.value, requests: new([]sqlext.Request)})
			matches := []sqlext.Match{{Candidate: set.Candidates("x", test.site)[0], Claim: sqlext.Claim{Pattern: "gadget", Site: test.site, Subject: "t"}}}
			got, err := set.Parse(t.Context(), "x", matches, sqlext.Document{})
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, []sqlext.Value{test.value})
		})
	}
}

func TestSet_Parse_InvalidChildDeclarations(t *testing.T) {
	tests := []struct {
		name   string
		site   sqlext.Site
		value  sqlext.Value
		reason string
	}{
		{name: "empty element", site: sqlext.TableElement, reason: "neither an index nor objects, or both"},
		{name: "index and objects", site: sqlext.TableElement, value: sqlext.Value{Index: &sqlext.Index{}, Objects: []schemaext.Object{childGadget()}}, reason: "neither an index nor objects, or both"},
		{name: "operation at element", site: sqlext.TableElement, value: sqlext.Value{Objects: []schemaext.Object{childGadget()}, Operation: &gadgetStatement{}}, reason: "an operation at a table element"},
		{name: "no operation", site: sqlext.AlterOperation, value: sqlext.Value{Objects: []schemaext.Object{childGadget()}}, reason: "no operation"},
		{name: "operation index and objects", site: sqlext.AlterOperation, value: sqlext.Value{Index: &sqlext.Index{}, Objects: []schemaext.Object{childGadget()}, Operation: &gadgetStatement{}}, reason: "both an index and objects"},
		{name: "unscoped child", site: sqlext.TableElement, value: sqlext.Value{Objects: []schemaext.Object{gadgetObject(gadgetKind, "g")}}, reason: "object .* without a parent table"},
		{name: "wrong owner", site: sqlext.TableElement, value: sqlext.Value{Objects: []schemaext.Object{{Ref: objectidentity.ID{Kind: "example.org/b/other", Parent: objectidentity.Part{Source: "t", Normalized: "t"}, Name: objectidentity.Part{Source: "g", Normalized: "g"}}, Value: &gadget{kind: "example.org/b/other"}}}}, reason: ".*object of kind.*extension does not declare"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := gadgetSet(test.site, answeringParser{value: test.value, requests: new([]sqlext.Request)})
			matches := []sqlext.Match{{Candidate: set.Candidates("x", test.site)[0], Claim: sqlext.Claim{Pattern: "gadget", Site: test.site, Subject: "t"}}}
			got, err := set.Parse(t.Context(), "x", matches, sqlext.Document{})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, ".*"+test.reason)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestNewSet_FreezesNamedPatterns(t *testing.T) {
	c := qt.New(t)
	pattern := sqlext.Pattern{Name: "child", Site: sqlext.TableElement, Words: []string{"CONSTRAINT"}, AfterName: []string{"GADGET"}}
	set := must.Must(sqlext.NewSet(extension("example.org/a", "x", shortParser{}, pattern)))
	pattern.AfterName[0] = "UNIQUE"
	candidate := set.Candidates("x", sqlext.TableElement)[0]
	c.Assert(candidate.MatchesNamed([]string{"CONSTRAINT"}, []string{"GADGET"}), qt.IsTrue)
	c.Assert(candidate.MatchesNamed([]string{"CONSTRAINT"}, []string{"UNIQUE"}), qt.IsFalse)
}

func TestSet_CandidatesAreIndependent(t *testing.T) {
	c := qt.New(t)
	pattern := sqlext.Pattern{Name: "child", Site: sqlext.TableElement, Words: []string{"CONSTRAINT"}, AfterName: []string{"GADGET"}, Next: []string{"("}, NotNext: []string{"INVALID"}, Stops: []string{"END"}}
	set := must.Must(sqlext.NewSet(extension("example.org/a", "x", shortParser{}, pattern)))
	candidate := set.Candidates("x", sqlext.TableElement)[0]
	candidate.Pattern.Words[0] = "OTHER"
	candidate.Pattern.AfterName[0] = "OTHER"
	candidate.Pattern.Next[0] = "OTHER"
	candidate.Pattern.NotNext[0] = "OTHER"
	candidate.Pattern.Stops[0] = "OTHER"
	c.Assert(set.Candidates("x", sqlext.TableElement)[0].Pattern, qt.DeepEquals, pattern)
}
