package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// Withholding an addition is defensible. Withholding it without saying why is
// what left a user with nothing to act on: "the read was refused the role
// catalog", "your selection put extensions outside this run" and "this server
// has no extension mechanism" are three different problems with three different
// fixes, and until the withheld record carried the CURRENT side's reason a
// surface could print none of them (stokaro/ptah#1346).
//
// These tests measure the reason at the far end of the comparison, because that
// is where it stopped: the readers recorded it, and the comparator threw it away
// when it built the undecided list.

func TestWithheldAdditionCarriesTheReasonTheReadRecorded(t *testing.T) {
	tests := []struct {
		name           string
		notDescribed   coverage.Set
		wantReason     coverage.Reason
		wantProvenance coverage.Provenance
	}{
		{
			name:           "a read the server refused",
			notDescribed:   coverage.Set{}.With(coverage.Refused(coverage.Extension)),
			wantReason:     coverage.NotInspected,
			wantProvenance: coverage.Observed,
		},
		{
			name: "a target that cannot report the kind",
			notDescribed: coverage.Set{}.With(coverage.Object{
				Kind:       coverage.Extension,
				Reason:     coverage.Unsupported,
				Provenance: coverage.DerivedFromTarget,
			}),
			wantReason:     coverage.Unsupported,
			wantProvenance: coverage.DerivedFromTarget,
		},
		{
			name: "a selection that ruled the kind out",
			notDescribed: coverage.Set{}.With(coverage.Object{
				Kind:       coverage.Extension,
				Reason:     coverage.OutsideScope,
				Provenance: coverage.Configured,
			}),
			wantReason:     coverage.OutsideScope,
			wantProvenance: coverage.Configured,
		},
		{
			// A hand-authored directive names no reason, and the record must
			// not invent one: the document declined the kind and said no more.
			name:           "a document that gave no reason",
			notDescribed:   coverage.Set{}.WithKind(coverage.Extension),
			wantReason:     coverage.ReasonUnspecified,
			wantProvenance: coverage.ProvenanceUnspecified,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &goschema.Database{Extensions: []goschema.Extension{{
				Name:   "citext",
				Schema: "extensions",
			}}}
			current := &catalog.Database{NotDescribed: test.notDescribed}

			diff, undecided := schemadiff.CompareReportingUndecidedAdditions(desired, current, nil)

			c.Assert(diff.ExtensionsAdded, qt.HasLen, 0)
			c.Assert(undecided, qt.DeepEquals, []coverage.Object{{
				Kind:       coverage.Extension,
				Name:       "citext",
				Reason:     test.wantReason,
				Provenance: test.wantProvenance,
			}})
		})
	}
}

// TestWithheldTableCarriesTheSchemaRecordsReason pins the indirect path. A table
// is covered through the SCHEMA that owns it rather than by its own name, so the
// reason a withheld table reports has to come from the schema's record; taking
// it from a record about tables would find none and report nothing.
func TestWithheldTableCarriesTheSchemaRecordsReason(t *testing.T) {
	c := qt.New(t)
	desired := &goschema.Database{Tables: []goschema.Table{
		{Name: "reports", StructName: "Reports", Schema: "extra"},
	}}
	current := &catalog.Database{NotDescribed: coverage.Set{}.With(coverage.Object{
		Kind:       coverage.Schema,
		Name:       "extra",
		Reason:     coverage.OutsideScope,
		Provenance: coverage.Configured,
	})}

	diff, undecided := schemadiff.CompareReportingUndecidedAdditions(desired, current, nil)

	c.Assert(diff.TablesAdded, qt.HasLen, 0)
	c.Assert(undecided, qt.DeepEquals, []coverage.Object{{
		Kind:       coverage.Schema,
		Name:       "extra.reports",
		Reason:     coverage.OutsideScope,
		Provenance: coverage.Configured,
	}})
}

// TestWithheldAdditionCarriesTheReasonOfTheRecordThatCoveredIt pins which of two
// records explains the withholding. A whole-kind record and a record naming the
// object both cover it; the whole-kind one is the broader statement and the one
// a user needs.
func TestWithheldAdditionCarriesTheReasonOfTheRecordThatCoveredIt(t *testing.T) {
	c := qt.New(t)
	desired := &goschema.Database{Extensions: []goschema.Extension{{
		Name:   "citext",
		Schema: "extensions",
	}}}
	current := &catalog.Database{NotDescribed: coverage.Set{}.
		With(coverage.Object{
			Kind:       coverage.Extension,
			Name:       "citext",
			Reason:     coverage.SuppressedByPolicy,
			Provenance: coverage.Defaulted,
		}).
		With(coverage.Refused(coverage.Extension))}

	_, undecided := schemadiff.CompareReportingUndecidedAdditions(desired, current, nil)

	c.Assert(undecided, qt.DeepEquals, []coverage.Object{{
		Kind:       coverage.Extension,
		Name:       "citext",
		Reason:     coverage.NotInspected,
		Provenance: coverage.Observed,
	}})
}
