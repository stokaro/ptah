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
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematogo"
)

// observedTTLTable is a read of one table named events carrying an observed
// facet of kind, with complete knowledge for that table.
func observedTTLTable(dialect string, value schemaext.Value, coverage schemaext.Coverage) *catalog.Database {
	facets := must.Must(must.Must(schemaext.NewFacets(value)).WithTargetScope(value.Kind(), dialect))
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "events", Type: "BASE TABLE", Facets: facets,
			Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", UDTName: "int8", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
				{Name: "created_at", DataType: "timestamp", UDTName: "timestamptz", IsNullable: "YES", OrdinalPosition: 2},
			},
		}},
		FeatureCoverage: coverage,
	}
}

func eventsSubject(dialect string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect(dialect)).TableParts("", "events")
}

func spannerRead(policy spannerschema.Policy) *catalog.Database {
	coverage := must.Must(spannerschema.RowDeletionCoverage(schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables were read"},
		[]schemaext.SubjectCoverage{{Kind: spannerschema.RowDeletionKind, Subject: eventsSubject("spanner"), Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}))
	return observedTTLTable("spanner", &spannerschema.ObservedRowDeletion{Policy: policy}, coverage)
}

func ydbRead(policy ydbschema.TTL) *catalog.Database {
	coverage := must.Must(ydbschema.TTLCoverage(schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables were read"},
		[]schemaext.SubjectCoverage{{Kind: ydbschema.TTLKind, Subject: eventsSubject("ydb"), Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}))
	return observedTTLTable("ydb", &ydbschema.ObservedTTL{Policy: policy}, coverage)
}

// TestSpannerRowDeletion_GoExportRoundTrip pins that `ptah introspect` keeps a
// Spanner policy: the read is converted to a declaration, written as
// platform.spanner properties in the spelling the server stores, and parses
// back to the same policy.
func TestSpannerRowDeletion_GoExportRoundTrip(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	policy := spannerschema.Policy{Column: "created_at", Interval: "4 WEEKS 2 DAYS"}

	declared, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), spannerRead(policy), "spanner", runtime)
	c.Assert(err, qt.IsNil)
	files, err := goschematogo.Render(t.Context(), declared, goschematogo.Options{SingleFile: true, Dialect: "spanner", Runtime: runtime})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(string(files[0].Data), qt.Contains, `platform.spanner.row_deletion_column="created_at"`)
	c.Assert(string(files[0].Data), qt.Contains, `platform.spanner.row_deletion_interval="4 WEEKS 2 DAYS"`)

	parsed := must.Must(goschema.ParseSource("schema.go", string(files[0].Data)))
	decoded, err := schemaproperties.DecodeTables(t.Context(), &parsed, "spanner", runtime)
	c.Assert(err, qt.IsNil)
	restored, found, err := schemaext.FacetAs[*spannerschema.DesiredRowDeletion](decoded.Tables[0].Facets, spannerschema.RowDeletionKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(restored.Policy, qt.DeepEquals, policy)
}

// TestYDBTTL_GoExportRoundTrip pins that `ptah introspect` keeps a YDB TTL on
// an integer column with its unit, written as platform.ydb properties.
func TestYDBTTL_GoExportRoundTrip(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	policy := ydbschema.TTL{Column: "created_at", Interval: "PT1H", Unit: "SECONDS"}

	declared, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), ydbRead(policy), "ydb", runtime)
	c.Assert(err, qt.IsNil)
	files, err := goschematogo.Render(t.Context(), declared, goschematogo.Options{SingleFile: true, Dialect: "ydb", Runtime: runtime})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(string(files[0].Data), qt.Contains, `platform.ydb.row_deletion_unit="SECONDS"`)

	parsed := must.Must(goschema.ParseSource("schema.go", string(files[0].Data)))
	decoded, err := schemaproperties.DecodeTables(t.Context(), &parsed, "ydb", runtime)
	c.Assert(err, qt.IsNil)
	restored, found, err := schemaext.FacetAs[*ydbschema.DesiredTTL](decoded.Tables[0].Facets, ydbschema.TTLKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(restored.Policy, qt.DeepEquals, policy)
}

// TestRowDeletion_HCLExportReportsTheLoss pins that HCL, which has no
// spelling for either policy, says it dropped one instead of writing a
// document that silently describes a table without it.
func TestRowDeletion_HCLExportReportsTheLoss(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		read    *catalog.Database
		want    string
	}{
		{name: "Spanner", dialect: "spanner", want: "feature facet ptah.run/spanner/row-deletion-policy is not represented in HCL",
			read: spannerRead(spannerschema.Policy{Column: "created_at", Interval: "30 days"})},
		{name: "YDB", dialect: "ydb", want: "feature facet ptah.run/ydb/ttl is not represented in HCL",
			read: ydbRead(ydbschema.TTL{Column: "created_at", Interval: "P30D"})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())

			declared, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), test.read, test.dialect, runtime)
			c.Assert(err, qt.IsNil)
			result, err := atlashclrender.Render(declared)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Message, qt.Equals, test.want)
		})
	}
}
