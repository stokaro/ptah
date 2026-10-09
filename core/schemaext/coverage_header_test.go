package schemaext_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
)

func TestCoverageHeaderPreservesExactClaimsWithoutRuntimeEnrollment(t *testing.T) {
	c := qt.New(t)
	registry := must.Must(schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired))))
	known := must.Must(schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{
		Model: registry.Definitions()[0], Knowledge: schemaext.Knowledge{State: schemaext.Complete},
	}}, []schemaext.SubjectCoverage{{Kind: widgetKind, Subject: widgetRef("records", "events"),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unknown option\nfrom server"}}}))
	header, err := registry.EncodeCoverageHeader(t.Context(), schemaext.Desired, known)
	c.Assert(err, qt.IsNil)
	c.Assert(header, qt.Not(qt.Contains), "\n")
	grown := must.Must(schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)), owned(widgetCodec(otherKind, schemaext.Desired))))
	for _, prefix := range []string{"//", "#", "--"} {
		t.Run(prefix, func(t *testing.T) {
			c := qt.New(t)
			decoded, found, err := grown.DecodeCoverageHeader(t.Context(), schemaext.Desired, prefix+" "+header+"\n\nbody")
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(decoded.Equal(known), qt.IsTrue)
			c.Assert(decoded.Lookup(otherKind, widgetRef("records", "events")).State, qt.Equals, schemaext.Uninspected)
		})
	}
}

func TestCoverageHeaderAbsenceDoesNotAuthorizeAnyModel(t *testing.T) {
	registry := must.Must(schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired))))
	for _, source := range []string{"", "// ordinary comment\n", "body\n// " + schemaext.CoverageHeaderMarker + " invalid"} {
		t.Run(source, func(t *testing.T) {
			c := qt.New(t)
			known, found, err := registry.DecodeCoverageHeader(t.Context(), schemaext.Desired, source)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsFalse)
			c.Assert(known.IsZero(), qt.IsTrue)
			c.Assert(known.Representation(), qt.Equals, schemaext.Desired)
			header, err := registry.EncodeCoverageHeader(t.Context(), schemaext.Desired, known)
			c.Assert(err, qt.IsNil)
			c.Assert(header, qt.Equals, "")
		})
	}
}

func TestCoverageHeaderRefusesIncompatibleOrAmbiguousInput(t *testing.T) {
	registry := must.Must(schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired))))
	known := must.Must(schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{
		Model: registry.Definitions()[0], Knowledge: schemaext.Knowledge{State: schemaext.Complete},
	}}, nil))
	header := must.Must(registry.EncodeCoverageHeader(t.Context(), schemaext.Desired, known))
	for _, test := range []struct {
		name   string
		source string
	}{
		{"syntax", schemaext.CoverageHeaderMarker + " invalid"},
		{"duplicate header", header + "\n// " + header},
		{"format", strings.Replace(header, `"format":1`, `"format":2`, 1)},
		{"duplicate field", strings.Replace(header, `"format":1`, `"format":1,"format":1`, 1)},
		{"case alias", strings.Replace(header, `"format":1`, `"format":1,"Format":2`, 1)},
		{"model case alias", strings.Replace(header, `"version":1`, `"version":1,"Version":2`, 1)},
		{"unknown field", strings.Replace(header, `"format":1`, `"format":1,"extra":true`, 1)},
		{"direction", strings.ReplaceAll(header, `"desired"`, `"observed"`)},
		{"model version", strings.Replace(header, `"version":1`, `"version":2`, 1)},
		{"unknown model", strings.ReplaceAll(header, string(widgetKind), "example.test/missing")},
		{"owner", strings.Replace(header, registry.Definitions()[0].Owner, "example.test/other", 1)},
		{"definition", strings.Replace(header, registry.Definitions()[0].Definition, "another-definition", 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, found, err := registry.DecodeCoverageHeader(t.Context(), schemaext.Desired, "// "+test.source)
			c.Assert(err, qt.IsNotNil)
			c.Assert(found, qt.IsFalse)
			c.Assert(decoded.IsZero(), qt.IsTrue)
		})
	}
}
