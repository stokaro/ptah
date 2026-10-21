package objectidentity_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
)

func TestSignatureKeepsComponentBoundaries(t *testing.T) {
	for _, row := range []struct {
		name        string
		left, right []string
	}{
		{"comma", []string{"integer, text"}, []string{"integer", "text"}},
		{"order", []string{"integer", "text"}, []string{"text", "integer"}},
		{"empty", nil, []string{""}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			left := objectidentity.ID{Kind: objectidentity.KindFunction, Name: objectidentity.Part{Source: "f", Normalized: "f"}, Signature: identityParts(row.left...)}
			right := left
			right.Signature = identityParts(row.right...)
			c.Assert(left.Equal(right), qt.IsFalse)
			c.Assert(left.Key(), qt.Not(qt.Equals), right.Key())
			c.Assert(left.Path(), qt.Not(qt.Equals), right.Path())
			encoded, err := json.Marshal(left)
			c.Assert(err, qt.IsNil)
			var restored objectidentity.ID
			c.Assert(json.Unmarshal(encoded, &restored), qt.IsNil)
			c.Assert(restored, qt.Equals, left)
			c.Assert(restored.Key(), qt.Equals, left.Key())
		})
	}
}

func TestRoutineSignatureNormalizesUnquotedTextOnly(t *testing.T) {
	builder := objectidentity.NewBuilder(postgresSemantics())
	for _, row := range []struct {
		name, left, right string
		equal             bool
	}{
		{"keywords", "( DOUBLE  PRECISION, NUMERIC(10,2) )", "double precision,numeric(10, 2)", true},
		{"backtick comma", "`a,b`", "`a, b`", false},
		{"quoted comma", `"a,b"`, `"a, b"`, false},
		{"quoted case", `"Mixed"`, `"mixed"`, false},
		{"quoted spaces", `"a  b"`, `"a b"`, false},
		{"escaped quotes", `"a"",B"`, `"a"",b"`, false},
		{"type modifier", "NUMERIC(10,2), TEXT", "numeric(10, 2),text", true},
		{"no arguments", "()", "", true},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			left, right := builder.Function("public.f", row.left), builder.Function("public.f", row.right)
			c.Assert(left.Equal(right), qt.Equals, row.equal)
		})
	}
	c := qt.New(t)
	signature := builder.Function("public.f", `NUMERIC(10,2), "a,b"`).Signature.Values()
	c.Assert(signature, qt.DeepEquals, []objectidentity.Part{
		{Source: "NUMERIC(10,2)", Normalized: "numeric(10, 2)"},
		{Source: `"a,b"`, Normalized: `"a,b"`},
	})
}

func TestSignatureKeyUsesNormalizedComponents(t *testing.T) {
	c := qt.New(t)
	left := objectidentity.ID{Signature: objectidentity.NewParts(objectidentity.Part{Source: "INTEGER", Normalized: "integer"})}
	right := objectidentity.ID{Signature: identityParts("integer")}
	c.Assert(left.Equal(right), qt.IsTrue)
	c.Assert(left.SignatureText(), qt.Equals, "INTEGER")
	var id objectidentity.ID
	c.Assert(json.Unmarshal([]byte(`{"signature":"integer"}`), &id), qt.IsNotNil)
}

func TestRoutineSignatureKeepsBracketedTypeNames(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(postgresSemantics())
	signature := builder.Function("f", "[a)b], [c]]d,e], integer").Signature.Values()
	c.Assert(signature, qt.DeepEquals, []objectidentity.Part{
		{Source: "[a)b]", Normalized: "[a)b]"},
		{Source: "[c]]d,e]", Normalized: "[c]]d,e]"},
		{Source: "integer", Normalized: "integer"},
	})
}
