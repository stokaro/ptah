package postgres

// White-box testing required: readUserTypesInto is unexported, and the branch
// it takes on a server without pg_depend runs no query, so no public read
// reaches it with a catalog that holds user types.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/dbschema/dbtest"
)

// TestReadUserTypes_AnUnreachableCatalogRecordsWhatItLeftOut pins what a read
// without pg_depend records. A target that holds domains and composite types
// records each as a common not-described kind, which every document writes and
// the type owner folds into its coverage; a range record is the owner's own.
// A target that holds none records nothing, so its documents carry no line.
func TestReadUserTypes_AnUnreachableCatalogRecordsWhatItLeftOut(t *testing.T) {
	tests := []struct {
		name   string
		caps   capability.Capabilities
		limits []coverage.Kind
		ranges schemaext.KnowledgeState
	}{
		{name: "a server with the types", caps: capability.Postgres16().With(capability.CatalogDependencies, false),
			limits: []coverage.Kind{coverage.Composite, coverage.Domain}, ranges: schemaext.Uninspected},
		{name: "a server without them", caps: capability.SpannerPostgres(), ranges: schemaext.Complete},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, (&emptyQueryRecorder{}).handle)
			read := &catalog.Database{}

			err := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", test.caps).readUserTypesInto(t.Context(), read)

			c.Assert(err, qt.IsNil)
			var limits []coverage.Kind
			for _, object := range read.NotDescribed.Objects {
				limits = append(limits, object.Kind)
			}
			c.Assert(limits, qt.DeepEquals, test.limits)
			c.Assert(read.FeatureCoverage.Lookup(pgtype.DomainKind, pgtype.Ref(pgtype.DomainKind, "public", "d")).State, qt.Equals, schemaext.Complete)
			c.Assert(read.FeatureCoverage.Lookup(pgtype.CompositeKind, pgtype.Ref(pgtype.CompositeKind, "public", "c")).State, qt.Equals, schemaext.Complete)
			c.Assert(read.FeatureCoverage.Lookup(pgtype.RangeKind, pgtype.Ref(pgtype.RangeKind, "public", "r")).State, qt.Equals, test.ranges)
		})
	}
}
