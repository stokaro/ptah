package pgtableprobe_test

import (
	"bytes"
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/dialect/postgres/pgtableprobe"
	"ptah.run/internal/rundiag"
)

// The owner parsers read a string the way a session with
// standard_conforming_strings on does, and accept each text below. A session
// with it off reads `'\'` as an open string holding a quote, so the text
// would run on past the clause there; the probe is not asked, and a
// probeSession with no database panics if it is.

// backslashRun is a run whose warnings go to the returned buffer.
func backslashRun() (context.Context, *bytes.Buffer) {
	var stream bytes.Buffer
	return rundiag.With(context.Background(), rundiag.New(rundiag.Options{Stream: &stream, Listing: rundiag.ListedAtDebugLevel})), &stream
}

// backslashWarning is the warning the run prints for object.
func backslashWarning(object string) string {
	return "warning: " + object + " is compared as written, not in the form the server stores: " +
		"the probe that spells it does not run, because its text leaves a string, quoted name or comment open\n"
}

func TestNormalizeFacets_LeavesAKeyASessionCouldReadPastUnprobed(t *testing.T) {
	c := qt.New(t)
	ctx, stream := backslashRun()
	request := probeRequest(probeSession{})
	request.Desired.Records[0].Values = must.Must(schemaext.NewFacets(&pgtable.DesiredPartition{
		Type: "LIST", Parts: []pgtable.PartitionPart{{Expr: `lower(word) || '\'`}}}))

	result, err := (pgtableprobe.Service{}).NormalizeFacets(ctx, request)

	c.Assert(err, qt.IsNil)
	value, _, err := schemaext.FacetAs[*pgtable.DesiredPartition](result.Desired.Records[0].Values, pgtable.PartitionKind)
	c.Assert(err, qt.IsNil)
	c.Assert(value.Normalized, qt.IsNil)
	c.Assert(stream.String(), qt.Equals, backslashWarning("partition key of App.words"))
}

func TestNormalizeBound_LeavesABoundASessionCouldReadPastUnprobed(t *testing.T) {
	c := qt.New(t)
	ctx, stream := backslashRun()
	subject := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", "e1")
	request := schemaext.FacetNormalizationRequest{Target: "postgres", Session: probeSession{},
		Owners: []schemaext.ParentState{{Subject: subject, Desired: true, Current: true}},
		Desired: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: subject, Values: must.Must(schemaext.NewFacets(&pgtable.DesiredPartitionMember{
			PartitionMember: pgtable.PartitionMember{ParentSchema: "App", Parent: "events", Bound: `FOR VALUES IN ('\')`},
		}))}}}}

	result, err := (pgtableprobe.PartitionMemberService{}).NormalizeFacets(ctx, request)

	c.Assert(err, qt.IsNil)
	value, _, err := schemaext.FacetAs[*pgtable.DesiredPartitionMember](result.Desired.Records[0].Values, pgtable.PartitionMemberKind)
	c.Assert(err, qt.IsNil)
	c.Assert(value.Normalized, qt.IsNil)
	c.Assert(stream.String(), qt.Equals, backslashWarning("partition bound of public.e1"))
}

func TestExclusionNormalization_LeavesAClauseASessionCouldReadPastUnprobed(t *testing.T) {
	c := qt.New(t)
	ctx, stream := backslashRun()
	ref := pgtable.ExclusionRef(`"App.One"."Rooms"`, `"No.Overlap"`, identifier.ForDialect("postgres"))
	desired := &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "btree", Elements: `(room || '\') WITH =`}}
	observed := &pgtable.ObservedExclusion{UsingMethod: "btree", Elements: "room WITH ="}
	request := schemaext.NormalizationRequest{Target: "postgres", Session: probeSession{},
		Desired: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: desired}))},
		Current: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: observed}))}}

	result, err := (pgtableprobe.ExclusionService{}).NormalizeObjects(ctx, request)

	c.Assert(err, qt.IsNil)
	normalized, err := result.Desired.Objects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(normalized[0].Value.(*pgtable.DesiredExclusion).Normalized, qt.IsNil)
	c.Assert(stream.String(), qt.Equals, backslashWarning(`exclusion constraint "No.Overlap" on "App.One"."Rooms"`))
}
