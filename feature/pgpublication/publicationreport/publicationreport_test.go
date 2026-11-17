package publicationreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpublication"
	"ptah.run/feature/pgpublication/publicationreport"
)

// TestReportValues_HappyPath pins that each publication counts once under the
// publications metric.
func TestReportValues_HappyPath(t *testing.T) {
	c := qt.New(t)

	reports, err := publicationreport.Service{}.ReportValues(c.Context(), schemaext.ReportingRequest{Representation: schemaext.Observed,
		Values: []schemaext.Value{&pgpublication.ObservedPublication{AllTables: true}, &pgpublication.ObservedPublication{}}})

	c.Assert(err, qt.IsNil)
	c.Assert(reports, qt.DeepEquals, []schemaext.ValueReport{
		{Kind: pgpublication.PublicationKind, Counts: []schemaext.MetricCount{{Name: "publications", Value: 1}}},
		{Kind: pgpublication.PublicationKind, Counts: []schemaext.MetricCount{{Name: "publications", Value: 1}}},
	})
	c.Assert(publicationreport.Definitions()[0].Metrics[0].Name, qt.Equals, "publications")
}

// TestReportValues_FailurePath pins that a value of the other representation
// is refused.
func TestReportValues_FailurePath(t *testing.T) {
	c := qt.New(t)

	reports, err := publicationreport.Service{}.ReportValues(c.Context(), schemaext.ReportingRequest{Representation: schemaext.Desired,
		Values: []schemaext.Value{&pgpublication.ObservedPublication{}}})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(reports, qt.IsNil)
}
