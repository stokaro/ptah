package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

func TestRenderRefusesNamespaceCoverageLoss(t *testing.T) {
	for _, kind := range []schemaext.Kind{pgdefaultprivilege.DefaultPrivilegeKind, pgtype.RangeKind, ydbcoordination.Kind} {
		t.Run(string(kind), func(t *testing.T) {
			c := qt.New(t)
			runtime := builtintest.Runtime()
			model, found := runtime.Codecs().Identity(kind, schemaext.Desired)
			c.Assert(found, qt.IsTrue)
			domain, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "outside inspection"}, schemaext.NamespaceKnowledge{
				Scope: schemaext.NamespaceScope{Schema: &objectidentity.Part{Source: "app", Normalized: "app"}}, Knowledge: schemaext.Knowledge{State: schemaext.Complete},
			})
			c.Assert(err, qt.IsNil)
			known, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: model, Knowledge: domain}}, nil)
			c.Assert(err, qt.IsNil)
			files, err := goschematogo.Render(t.Context(), &schemamodel.Database{FeatureCoverage: known}, goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*cannot preserve namespace coverage boundaries.*`)
			c.Assert(files, qt.HasLen, 0)
		})
	}
}
