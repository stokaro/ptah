package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// A document leaves a YDB async replication and a transfer out, since Atlas
// HCL has no block for either, and says so twice: a loss diagnostic for each,
// and a header recording that it describes neither, so applying it back plans
// no `DROP ASYNC REPLICATION ... CASCADE`. A document for another dialect whose
// schema holds neither records nothing.
func TestRenderForDialect_Replications(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		db          *schemamodel.Database
		diagnostics []atlashclrender.Diagnostic
		recorded    bool
	}{
		{name: "a YDB document holding both", dialect: platform.YDB,
			db: &schemamodel.Database{
				AsyncReplications: []schemamodel.AsyncReplication{{Name: "mirror", Schema: "dr"}},
				Transfers:         []schemamodel.Transfer{{Name: "ingest"}},
			},
			diagnostics: []atlashclrender.Diagnostic{
				{Severity: atlashclrender.SeverityWarning, Path: "async_replications.dr.mirror",
					Message: "a YDB async replication is not represented in HCL"},
				{Severity: atlashclrender.SeverityWarning, Path: "transfers.ingest",
					Message: "a YDB transfer is not represented in HCL"},
			},
			recorded: true},
		{name: "a YDB document holding none", dialect: platform.YDB, db: &schemamodel.Database{}, recorded: true},
		{name: "a PostgreSQL document holding none", dialect: platform.Postgres, db: &schemamodel.Database{},
			recorded: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := atlashclrender.RenderForDialect(test.db, test.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.DeepEquals, test.diagnostics)
			c.Assert(result.NotDescribed.Describes(coverage.Replication), qt.Equals, !test.recorded)
			c.Assert(result.NotDescribed.Describes(coverage.Transfer), qt.Equals, !test.recorded)
		})
	}
}
