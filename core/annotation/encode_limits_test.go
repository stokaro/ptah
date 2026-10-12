package annotation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/schemaext"
)

// limiting is an extension of owner that reads not-described declarations of
// limits and whose encoder writes result.
func limiting(owner, directive string, kind schemaext.Kind, handed *[]annotation.EncodeRequest, result annotation.EncodeResult,
	limits ...string,
) annotation.Extension {
	extension := encoding(owner, directive, []schemaext.Kind{kind}, handed, result)
	extension.Limits = limits
	extension.Coverage = func([]coverage.Object) (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }
	return extension
}

// TestSet_Encode_Limits_HappyPath pins that each owner is handed the
// request's target and that the limits owners write come back owner by owner
// in the order of their names, whatever the order they were registered in.
func TestSet_Encode_Limits_HappyPath(t *testing.T) {
	c := qt.New(t)
	var handed []annotation.EncodeRequest
	levels := []annotation.EncodedLimit{{Kind: "gauge"}, {Kind: "gauge", Name: "spare"}}
	marks := []annotation.EncodedLimit{{Kind: "dial"}}
	set := must.Must(annotation.NewSet(
		limiting("example.org/marks", "ptah:schema:mark", markKind, &handed, annotation.EncodeResult{Limits: marks}, "dial"),
		limiting("example.org/levels", "ptah:schema:level", levelKind, &handed, annotation.EncodeResult{Limits: levels}, "gauge"),
	))

	result, err := set.Encode(c.Context(), annotation.EncodeRequest{Target: "ydb"})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Limits, qt.DeepEquals, append(levels, marks...))
	c.Assert(handed, qt.HasLen, 2)
	c.Assert([]string{handed[0].Target, handed[1].Target}, qt.DeepEquals, []string{"ydb", "ydb"})
}

// TestSet_Encode_Limits_FailurePath pins that an owner may write only a limit
// reading the source back hands to it, and each one once: another limit would
// be refused or claimed by another owner on the way back.
func TestSet_Encode_Limits_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		limits  []annotation.EncodedLimit
		wantErr string
	}{
		{name: "a kind nobody reads", limits: []annotation.EncodedLimit{{Kind: "knob"}},
			wantErr: `.*encoder of example.org/levels wrote the limit of kind "knob" and name "", which reading the source back does not hand to it, or wrote it twice`},
		{name: "a kind another owner reads", limits: []annotation.EncodedLimit{{Kind: "dial", Name: "front"}},
			wantErr: `.*encoder of example.org/levels wrote the limit of kind "dial" and name "front", which reading the source back does not hand to it, or wrote it twice`},
		{name: "a limit twice", limits: []annotation.EncodedLimit{{Kind: "gauge", Name: "spare"}, {Kind: "gauge", Name: "spare"}},
			wantErr: `.*encoder of example.org/levels wrote the limit of kind "gauge" and name "spare", which reading the source back does not hand to it, or wrote it twice`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var handed []annotation.EncodeRequest
			set := must.Must(annotation.NewSet(
				limiting("example.org/levels", "ptah:schema:level", levelKind, &handed, annotation.EncodeResult{Limits: test.limits}, "gauge"),
				limiting("example.org/marks", "ptah:schema:mark", markKind, &handed, annotation.EncodeResult{}, "dial"),
			))

			result, err := set.Encode(c.Context(), annotation.EncodeRequest{})

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result.Limits, qt.IsNil)
		})
	}
}

// TestSet_Encode_Limits_ReadsTheKindAsTheReaderDoes pins that a limit's kind
// is matched without case and surrounding space, as reading the source back
// matches it.
func TestSet_Encode_Limits_ReadsTheKindAsTheReaderDoes(t *testing.T) {
	c := qt.New(t)
	var handed []annotation.EncodeRequest
	limits := []annotation.EncodedLimit{{Kind: " Gauge "}}
	set := must.Must(annotation.NewSet(limiting("example.org/levels", "ptah:schema:level", levelKind, &handed,
		annotation.EncodeResult{Limits: limits}, "gauge")))

	result, err := set.Encode(c.Context(), annotation.EncodeRequest{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Limits, qt.DeepEquals, limits)
}
