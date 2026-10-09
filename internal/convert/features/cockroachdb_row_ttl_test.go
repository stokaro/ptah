package features_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemaproperties"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematogo"
)

// observedRowTTL is a CockroachDB read of one table carrying a policy in the
// spelling the server stores, with complete knowledge for that table.
func observedRowTTL(policy crdbschema.Policy) *catalog.Database {
	subject := objectidentity.NewBuilder(identifier.ForDialect("cockroachdb")).TableParts("", "sessions")
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "sessions", Type: "BASE TABLE",
			Columns: []catalog.Column{{Name: "id", DataType: "bigint", UDTName: "int8", IsNullable: "NO", IsPrimaryKey: true}},
			Facets: must.Must(must.Must(schemaext.NewFacets(&crdbschema.ObservedRowTTL{Policy: policy})).
				WithTargetScope(crdbschema.RowTTLKind, "cockroachdb")),
		}},
		FeatureCoverage: must.Must(crdbschema.RowTTLCoverage(schemaext.Observed,
			schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables were read"},
			[]schemaext.SubjectCoverage{{Kind: crdbschema.RowTTLKind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}})),
	}
}

// TestCockroachDBRowTTL_GoExportRoundTrip pins that `ptah introspect` keeps a
// policy: the read is converted to a declaration, written as
// platform.cockroachdb properties, and parses back to the same policy, in the
// spelling the server stores. Before the owner modeled the policy, Go export
// dropped it without a word.
func TestCockroachDBRowTTL_GoExportRoundTrip(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	policy := crdbschema.Policy{
		ExpirationExpression: "expires_at + INTERVAL '1 day'", ExpireAfter: "72:00:00", RowStatsPollInterval: "10m0s",
		JobCron: "@daily", SelectBatchSize: new(int64(500)), Pause: true,
	}

	declared, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), observedRowTTL(policy), "cockroachdb", runtime)
	c.Assert(err, qt.IsNil)
	files, err := goschematogo.Render(t.Context(), declared, goschematogo.Options{SingleFile: true, Dialect: "cockroachdb", Runtime: runtime})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(string(files[0].Data), qt.Contains, `platform.cockroachdb.ttl_expire_after="72:00:00"`)

	parsed := must.Must(goschema.ParseSource("schema.go", string(files[0].Data)))
	decoded, err := schemaproperties.DecodeTables(t.Context(), &parsed, "cockroachdb", runtime)
	c.Assert(err, qt.IsNil)
	restored, found, err := schemaext.FacetAs[*crdbschema.DesiredRowTTL](decoded.Tables[0].Facets, crdbschema.RowTTLKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(restored.Policy, qt.DeepEquals, policy)
}

// TestCockroachDBRowTTL_HCLExportReportsTheLoss pins that a format without row
// -level TTL says it dropped the policy instead of writing a document that
// silently describes a table without one.
func TestCockroachDBRowTTL_HCLExportReportsTheLoss(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())

	declared, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), observedRowTTL(crdbschema.Policy{ExpireAfter: "3 days"}), "cockroachdb", runtime)
	c.Assert(err, qt.IsNil)
	result, err := atlashclrender.Render(declared)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Message, qt.Equals, "feature facet ptah.run/cockroachdb/row-ttl is not represented in HCL")
}
