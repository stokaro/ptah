package synonym_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/feature/synonym"
)

// TestSelections_ClearsADeclaredComment clears the comment of each declared
// synonym an exclusion names, and leaves an observed synonym, which carries
// none, as it is. A synonym requires nothing.
func TestSelections_ClearsADeclaredComment(t *testing.T) {
	c := qt.New(t)
	declared := synonym.DesiredObject(synonym.DesiredSynonym{Synonym: synonym.Synonym{Schema: "dbo", Name: "a", Target: "dbo.t"}, Comment: "c"})
	observed := synonym.ObservedObject(synonym.ObservedSynonym{Synonym: synonym.Synonym{Schema: "dbo", Name: "b", Target: "dbo.t"}})
	kept := must.Must(schemaext.NewObjects(declared, observed))

	result, err := synonym.Selections().Service.SelectObjects(c.Context(), selectext.Request{Kept: kept, Clears: []selectext.Clear{
		{Object: declared.Ref, Field: "comment"}, {Object: observed.Ref, Field: "comment"}}})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Requires, qt.HasLen, 0)
	c.Assert(result.Cleared, qt.HasLen, 1)
	c.Assert(result.Cleared[0].Ref, qt.Equals, declared.Ref)
	c.Assert(result.Cleared[0].Value, qt.DeepEquals, &synonym.DesiredSynonym{Synonym: synonym.Synonym{Schema: "dbo", Name: "a", Target: "dbo.t"}})
	c.Assert(declared.Value.(*synonym.DesiredSynonym).Comment, qt.Equals, "c")
}

// TestSelections_RefusesAClearOfASynonymNotKept refuses a clear of a synonym
// the request does not keep.
func TestSelections_RefusesAClearOfASynonymNotKept(t *testing.T) {
	c := qt.New(t)
	absent := synonym.DesiredObject(synonym.DesiredSynonym{Synonym: synonym.Synonym{Schema: "dbo", Name: "gone", Target: "dbo.t"}})

	result, err := synonym.Selections().Service.SelectObjects(c.Context(), selectext.Request{Clears: []selectext.Clear{{Object: absent.Ref, Field: "comment"}}})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, selectext.Result{})
}
