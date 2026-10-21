package objectidentity_test

import (
	"encoding/json"
	"runtime"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
)

func identityParts(values ...string) objectidentity.Parts {
	parts := make([]objectidentity.Part, len(values))
	for i, value := range values {
		parts[i] = objectidentity.Part{Source: value, Normalized: value}
	}
	return objectidentity.NewParts(parts...)
}

func TestPartsPreserveBoundaries(t *testing.T) {
	for _, row := range []struct {
		name        string
		left, right []string
	}{
		{"slash", []string{"a/b", "c"}, []string{"a", "b/c"}},
		{"dot", []string{"a.b", "c"}, []string{"a", "b.c"}},
		{"empty component", nil, []string{""}},
		{"length", []string{"a"}, []string{"a", ""}},
		{"order", []string{"a", "b"}, []string{"b", "a"}},
		{"nul", []string{"a\x00b", "c"}, []string{"a", "b\x00c"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			left, right := identityParts(row.left...), identityParts(row.right...)
			c.Assert(left.Equal(right), qt.IsFalse)
			c.Assert(left.Compare(right), qt.Not(qt.Equals), 0)
			c.Assert(left.Compare(right), qt.Equals, -right.Compare(left))
			c.Assert(left.Equal(identityParts(row.left...)), qt.IsTrue)
		})
	}
}

func TestPartsCaptureImmutableSourceAndComparisonForms(t *testing.T) {
	c := qt.New(t)
	input := []objectidentity.Part{{Source: "Mixed", Normalized: "mixed", Quoted: true, Defaulted: true}}
	captured := objectidentity.NewParts(input...)
	input[0].Source = "changed"
	output := captured.Values()
	c.Assert(output[0].Source, qt.Equals, "Mixed")
	output[0].Normalized = "changed"
	c.Assert(captured.Values()[0].Normalized, qt.Equals, "mixed")
	c.Assert(captured.Equal(identityParts("mixed")), qt.IsTrue)
	c.Assert(captured.Compare(identityParts("mixed")), qt.Equals, 0)
	encoded, err := json.Marshal(captured)
	c.Assert(err, qt.IsNil)
	var restored objectidentity.Parts
	c.Assert(json.Unmarshal(encoded, &restored), qt.IsNil)
	c.Assert(restored, qt.Equals, captured)
	c.Assert(restored.Values()[0].Quoted, qt.IsTrue)
	c.Assert(restored.Values()[0].Defaulted, qt.IsTrue)
	c.Assert((objectidentity.Parts{}).IsZero(), qt.IsTrue)
	c.Assert(identityParts("").IsZero(), qt.IsFalse)
}

func TestScopedIdentityRetainsTupleAfterGarbageCollection(t *testing.T) {
	c := qt.New(t)
	id := objectidentity.ID{Kind: objectidentity.KindTable,
		Database:  objectidentity.Part{Source: "Tenant", Normalized: "tenant"},
		Namespace: identityParts("a/b", "c"), Name: objectidentity.Part{Source: "rows", Normalized: "rows"}}
	key := id.Key()
	runtime.GC()
	c.Assert(key, qt.Equals, id.Key())
	encoded, err := json.Marshal(id)
	c.Assert(err, qt.IsNil)
	var restored objectidentity.ID
	c.Assert(json.Unmarshal(encoded, &restored), qt.IsNil)
	c.Assert(restored, qt.Equals, id)
	c.Assert(restored.Key(), qt.Equals, key)
	c.Assert(objectidentity.PrimaryKeyOf(id).SameNamespace(id), qt.IsTrue)
	c.Assert(id.Path(), qt.Equals, `[database="Tenant"; namespace=["a/b", "c"]] rows`)
	other := id
	other.Namespace = identityParts("a", "b/c")
	c.Assert(other.Equal(id), qt.IsFalse)
	c.Assert(other.SameNamespace(id), qt.IsFalse)
	c.Assert(other.Path(), qt.Not(qt.Equals), id.Path())
	other = id
	other.Database = objectidentity.Part{Source: "tenant", Normalized: "tenant"}
	c.Assert(other.Equal(id), qt.IsTrue)
	other.Database.Normalized = "elsewhere"
	c.Assert(other.Equal(id), qt.IsFalse)
	c.Assert(other.SameNamespace(id), qt.IsFalse)
}

func TestPartsJSONRefusesInvalidShapeWithoutChangingValue(t *testing.T) {
	for _, input := range []string{`"a/b"`, `{}`, `[1]`, `[{"source":true}]`, `[{"misspelled":"scope"}]`} {
		t.Run(input, func(t *testing.T) {
			c := qt.New(t)
			parts := identityParts("original")
			c.Assert(json.Unmarshal([]byte(input), &parts), qt.IsNotNil)
			c.Assert(parts, qt.Equals, identityParts("original"))
		})
	}
}
