package planner_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestEveryPlannerButYDBRefusesATableSettingsChange drives a hand-built diff
// that changes a table's YDB settings through every registered planner but
// YDB's. Only the YDB owner plans the change, and a planner that dropped it
// would plan nothing while the comparison kept reporting the difference.
func TestEveryPlannerButYDBRefusesATableSettingsChange(t *testing.T) {
	c := qt.New(t)
	// The dialects Ptah ships, rather than every registered planner: other
	// tests in this package register planners of their own.
	dialects := slices.DeleteFunc(capability.DefaultDialects(), func(dialect string) bool { return dialect == platform.YDB })
	c.Assert(len(dialects) >= 8, qt.IsTrue, qt.Commentf("dialects: %v", dialects))

	subject := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "users")
	change := &ydbdiff.TablePartitioning{After: &ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{KeyBloomFilter: new(true)}}}
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:      "users",
		Desired:        schemacapture.TableDeclaration{Table: schemamodel.Table{StructName: "Users", Name: "users"}},
		Current:        schemacapture.TableObservation{Table: catalog.Table{Name: "users"}},
		FeatureChanges: []schemaext.ChangeRecord{{Subject: subject, Value: change}},
	}}}
	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffAST(
				context.Background(), must.Must(builtin.New()),
				diff, dialect,
			)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
