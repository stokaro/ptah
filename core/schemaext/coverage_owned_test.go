package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

// TestOwnedCoverage_HappyPath enrolls the one model of the requested kind and
// representation among several codecs, so a claim about the widget says
// nothing about the other kind the same codecs define.
func TestOwnedCoverage_HappyPath(t *testing.T) {
	c := qt.New(t)
	codecs := []schemaext.Codec{
		widgetCodec(widgetKind, schemaext.Desired), widgetCodec(widgetKind, schemaext.Observed), widgetCodec(otherKind, schemaext.Observed),
	}
	subject := widgetRef("table", "name")
	subjects := []schemaext.SubjectCoverage{{Kind: widgetKind, Subject: subject,
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "cannot read it"}}}

	coverage, err := schemaext.OwnedCoverage("example.org/provider", codecs, widgetKind, schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Complete}, subjects)

	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Representation(), qt.Equals, schemaext.Observed)
	records := coverage.KindRecords()
	c.Assert(records, qt.HasLen, 1)
	c.Assert(records[0].Model.Owner, qt.Equals, "example.org/provider")
	c.Assert(records[0].Model.Kind, qt.Equals, widgetKind)
	c.Assert(records[0].Model.Representation, qt.Equals, schemaext.Observed)
	c.Assert(records[0].Knowledge, qt.DeepEquals, schemaext.Knowledge{State: schemaext.Complete})
	c.Assert(coverage.Lookup(widgetKind, subject).State, qt.Equals, schemaext.Unrepresentable)
	c.Assert(coverage.Lookup(widgetKind, widgetRef("table", "other")).State, qt.Equals, schemaext.Complete)
	c.Assert(coverage.Lookup(otherKind, subject).State, qt.Equals, schemaext.Uninspected)
}

// TestOwnedCoverage_FailurePath refuses a kind or representation the codecs
// define no model for, an owner the registry refuses, a representation no
// coverage can claim, and a subject of a kind the claim did not enroll.
func TestOwnedCoverage_FailurePath(t *testing.T) {
	codecs := []schemaext.Codec{widgetCodec(widgetKind, schemaext.Desired), widgetCodec(otherKind, schemaext.Observed)}
	tests := []struct {
		name           string
		owner          string
		kind           schemaext.Kind
		representation schemaext.Representation
		subjects       []schemaext.SubjectCoverage
		wantIs         error
		wantErr        string
	}{
		{name: "a kind only the other representation defines", owner: "example.org/provider", kind: widgetKind,
			representation: schemaext.Observed, wantIs: schemaext.ErrInvalidValue,
			wantErr: `invalid feature value: the codecs define no observed model of kind "example.org/widget"`},
		{name: "a kind no codec defines", owner: "example.org/provider", kind: "example.org/missing",
			representation: schemaext.Desired, wantIs: schemaext.ErrInvalidValue,
			wantErr: `invalid feature value: the codecs define no desired model of kind "example.org/missing"`},
		{name: "an invalid owner", owner: "", kind: widgetKind, representation: schemaext.Desired,
			wantIs: schemaext.ErrInvalidCodec, wantErr: `.*invalid owner, representation, or version`},
		{name: "a subject of another kind", owner: "example.org/provider", kind: widgetKind, representation: schemaext.Desired,
			subjects: []schemaext.SubjectCoverage{{Kind: otherKind, Subject: widgetRef("table", "name")}},
			wantIs:   schemaext.ErrInvalidValue, wantErr: `invalid feature value: subject coverage for unenrolled kind "example.org/other"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			coverage, err := schemaext.OwnedCoverage(test.owner, codecs, test.kind, test.representation,
				schemaext.Knowledge{State: schemaext.Complete}, test.subjects)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(coverage.IsZero(), qt.IsTrue)
		})
	}
}

// TestOwnedCoverageSource_HappyPath answers every call as OwnedCoverage does
// and asks for the codecs once, however many claims it records.
func TestOwnedCoverageSource_HappyPath(t *testing.T) {
	c := qt.New(t)
	codecs := []schemaext.Codec{
		widgetCodec(widgetKind, schemaext.Desired), widgetCodec(widgetKind, schemaext.Observed), widgetCodec(otherKind, schemaext.Observed),
	}
	calls := 0
	source := schemaext.OwnedCoverageSource("example.org/provider", func() []schemaext.Codec {
		calls++
		return codecs
	})
	for _, kind := range []schemaext.Kind{widgetKind, otherKind} {
		want, err := schemaext.OwnedCoverage("example.org/provider", codecs, kind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)
		c.Assert(err, qt.IsNil)

		got, err := source(kind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)

		c.Assert(err, qt.IsNil)
		c.Assert(got.KindRecords(), qt.DeepEquals, want.KindRecords())
	}
	c.Assert(calls, qt.Equals, 1)
}

// TestOwnedCoverageSource_FailurePath returns the registry refusal on every
// call, not only the first, and refuses a kind the codecs do not define.
func TestOwnedCoverageSource_FailurePath(t *testing.T) {
	c := qt.New(t)
	refused := schemaext.OwnedCoverageSource("", func() []schemaext.Codec {
		return []schemaext.Codec{widgetCodec(widgetKind, schemaext.Desired)}
	})
	for range 2 {
		coverage, err := refused(widgetKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidCodec)
		c.Assert(coverage.KindRecords(), qt.HasLen, 0)
	}

	source := schemaext.OwnedCoverageSource("example.org/provider", func() []schemaext.Codec {
		return []schemaext.Codec{widgetCodec(widgetKind, schemaext.Desired)}
	})
	coverage, err := source("example.org/missing", schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
	c.Assert(err, qt.ErrorMatches, `invalid feature value: the codecs define no desired model of kind "example.org/missing"`)
	c.Assert(coverage.KindRecords(), qt.HasLen, 0)
}
