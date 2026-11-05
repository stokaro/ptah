package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/convert/dbschematogo"
)

// reservedRead is a PostgreSQL read whose extension coverage marks one subject
// at a name provider-owned storage occupies, and one at an ordinary name, as
// unrepresentable, with the reserved name recorded beside them.
func reservedRead(c *qt.C) *catalog.Database {
	c.Helper()
	unrepresentable := schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "Name is occupied by provider-owned operational storage"}
	subjects := []schemaext.SubjectCoverage{
		{Kind: pgextension.ExtensionKind, Subject: pgextension.Ref("atlas_schema_revisions"), Knowledge: unrepresentable},
		{Kind: pgextension.ExtensionKind, Subject: pgextension.Ref("legacy"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the read cannot represent it"}},
	}
	return &catalog.Database{
		FeatureCoverage: must.Must(pgextension.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, subjects)),
		ReservedNames:   []catalog.ReservedName{{ProviderID: "example.org/fixture", Engine: "postgres", Purpose: "history", Identity: pgextension.Ref("atlas_schema_revisions")}},
	}
}

// A name the read reserved is not a declaration, and neither is what the read
// says of it: the document leaves out the knowledge a provider records at the
// name, which a source would otherwise write as an object of that kind it left
// out, and keeps what the read says of every other name.
func TestConvertDBSchemaToGoSchema_LeavesOutTheCoverageOfReservedNames(t *testing.T) {
	c := qt.New(t)

	converted, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), reservedRead(c), "postgres", must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	var names []string
	for _, subject := range converted.FeatureCoverage.SubjectRecords() {
		names = append(names, subject.Subject.Name.Source)
	}
	c.Assert(names, qt.DeepEquals, []string{"legacy"})
	_, reserved := converted.FeatureCoverage.SubjectKnowledge(pgextension.ExtensionKind, pgextension.Ref("atlas_schema_revisions"))
	c.Assert(reserved, qt.IsFalse)
}
