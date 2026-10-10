package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// A PostgreSQL read records that it knows every TimescaleDB model, even on a
// server without the extension. A comparison that names no target cannot run
// an owner, and with nothing declared and nothing found there is nothing for
// one to do, so the knowledge claim alone does not refuse it.
func TestCompare_HappyPath_AKnowledgeClaimAloneNeedsNoTarget(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items"}},
		Fields: []schemamodel.Field{{StructName: "Item", Name: "id", Type: "INTEGER"}},
	}
	current := &catalog.Database{
		Tables:          []catalog.Table{{Name: "items", Columns: []catalog.Column{{Name: "id", DataType: "integer"}}}},
		FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Observed)),
	}

	diff, err := schemadiff.Compare(t.Context(), desired, current, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.FeatureChanges, qt.HasLen, 0)
}

// A limit a source recorded about one subject is observed state an owner has
// to judge, so it still needs a target.
func TestCompare_FailurePath_ASubjectLimitNeedsATarget(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	known := must.Must(tsschema.CompleteCoverage(schemaext.Observed))
	limited := must.Must(schemaext.NewCoverage(schemaext.Observed, known.KindRecords(), []schemaext.SubjectCoverage{{
		Kind: tsschema.HypertableKind, Subject: builder.TableParts("public", "items"),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the catalog reported no dimension"},
	}}))
	current := &catalog.Database{Tables: []catalog.Table{{Name: "items", Schema: "public"}}, FeatureCoverage: limited}

	diff, err := schemadiff.Compare(t.Context(), &schemamodel.Database{}, current, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(diff, qt.IsNil)
}
