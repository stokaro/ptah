package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestCompareWithDialect_ImplicitSchemaMatchesSchemaObjects(t *testing.T) {
	c := qt.New(t)
	desired, database := schemaObjectIdentityFixtures("public", "")

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, database, "postgres", must.Must(builtin.New())))

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

func TestCompareWithDialect_DifferentSchemaDoesNotMatchSchemaObjects(t *testing.T) {
	c := qt.New(t)
	desired, database := schemaObjectIdentityFixtures("public", "reporting")

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, database, "postgres", must.Must(builtin.New())))

	c.Assert(diff.FunctionsAdded.Names(), qt.DeepEquals, []string{"public.f_ctl"})
	c.Assert(diff.FunctionsRemoved.Names(), qt.DeepEquals, []string{"reporting.f_ctl"})
	c.Assert(diff.SequencesAdded.Names(), qt.DeepEquals, []string{"public.s_ctl"})
	c.Assert(diff.SequencesRemoved.Names(), qt.DeepEquals, []string{"reporting.s_ctl"})
	c.Assert(typeChangesOf(diff, pgtype.DomainChangeKind), qt.DeepEquals, []schemaext.ChangeRecord{
		{Subject: pgtype.Ref(pgtype.DomainKind, "public", "d_ctl"), Value: &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "text"}}},
		{Subject: pgtype.Ref(pgtype.DomainKind, "reporting", "d_ctl"), Value: &pgtype.DomainChange{Before: &pgtype.ObservedDomain{BaseType: "text"}}},
	})
	c.Assert(typeChangesOf(diff, pgtype.CompositeChangeKind), qt.DeepEquals, []schemaext.ChangeRecord{
		{Subject: pgtype.Ref(pgtype.CompositeKind, "public", "c_ctl"),
			Value: &pgtype.CompositeChange{After: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "value", Type: "text"}}}}},
		{Subject: pgtype.Ref(pgtype.CompositeKind, "reporting", "c_ctl"),
			Value: &pgtype.CompositeChange{Before: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "value", Type: "text"}}}}},
	})
	c.Assert(rangeChangesOf(diff), qt.DeepEquals, []schemaext.ChangeRecord{
		{Subject: pgtype.Ref(pgtype.RangeKind, "public", "r_ctl"), Value: &pgtype.RangeChange{After: &pgtype.DesiredRange{Subtype: "integer"}}},
		{Subject: pgtype.Ref(pgtype.RangeKind, "reporting", "r_ctl"), Value: &pgtype.RangeChange{Before: &pgtype.ObservedRange{Subtype: "integer"}}},
	})
	c.Assert(diff.ViewsAdded.Names(), qt.DeepEquals, []string{"public.v_ctl"})
	c.Assert(diff.ViewsRemoved.Names(), qt.DeepEquals, []string{"reporting.v_ctl"})
	c.Assert(diff.MaterializedViewsAdded.Names(), qt.DeepEquals, []string{"public.mv_ctl"})
	c.Assert(diff.MaterializedViewsRemoved.Names(), qt.DeepEquals, []string{"reporting.mv_ctl"})
	c.Assert(diff.TriggersAdded, qt.DeepEquals, []difftypes.TriggerRef{{
		TriggerName: "tr_ctl",
		TableName:   "public.items",
		// An addition carries the declaration the planner renders from
		// (stokaro/ptah#2315); a removal below needs only the two names.
		Desired: schemamodel.Trigger{
			Name: "tr_ctl", Table: "public.items",
			Timing: "BEFORE", Event: "INSERT", ForEach: "ROW", Body: "SELECT 1",
		},
	}})
	c.Assert(diff.TriggersRemoved, qt.DeepEquals, []difftypes.TriggerRef{{
		TriggerName: "tr_ctl",
		TableName:   "reporting.items",
	}})
}

func schemaObjectIdentityFixtures(
	desiredSchema,
	currentSchema string,
) (*schemamodel.Database, *catalog.Database) {
	desired := &schemamodel.Database{
		Functions: []schemamodel.Function{{
			Name:       desiredSchema + ".f_ctl",
			Returns:    "integer",
			Language:   "sql",
			Security:   "INVOKER",
			Volatility: "VOLATILE",
			Body:       "SELECT 1",
		}},
		Sequences: []schemamodel.Sequence{{
			Name:   "s_ctl",
			Schema: desiredSchema,
			AsType: "bigint",
		}},
		FeatureCoverage: must.Must(pgtype.CompleteCoverage(schemaext.Desired)),
		FeatureObjects: must.Must(schemaext.NewObjects(
			schemaext.Object{Ref: pgtype.Ref(pgtype.DomainKind, desiredSchema, "d_ctl"), Value: &pgtype.DesiredDomain{BaseType: "text"}},
			schemaext.Object{Ref: pgtype.Ref(pgtype.CompositeKind, desiredSchema, "c_ctl"),
				Value: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "value", Type: "text"}}}},
			schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, desiredSchema, "r_ctl"), Value: &pgtype.DesiredRange{Subtype: "integer"}})),
		Views: []schemamodel.View{{
			Name: desiredSchema + ".v_ctl",
			Body: "SELECT 1",
		}},
		MaterializedViews: []schemamodel.MaterializedView{{
			Name: desiredSchema + ".mv_ctl",
			Body: "SELECT 1",
		}},
		Triggers: []schemamodel.Trigger{{
			Name:    "tr_ctl",
			Table:   desiredSchema + ".items",
			Timing:  "BEFORE",
			Event:   "INSERT",
			ForEach: "ROW",
			Body:    "SELECT 1",
		}},
	}
	database := &catalog.Database{
		Functions: []catalog.Function{{
			Name:       "f_ctl",
			Schema:     currentSchema,
			Returns:    "integer",
			Language:   "sql",
			Security:   "INVOKER",
			Volatility: "VOLATILE",
			Body:       "SELECT 1",
		}},
		Sequences: []catalog.Sequence{{
			Name:     "s_ctl",
			Schema:   currentSchema,
			DataType: "bigint",
		}},
		FeatureCoverage: must.Must(pgtype.CompleteCoverage(schemaext.Observed)),
		FeatureObjects: must.Must(schemaext.NewObjects(
			schemaext.Object{Ref: pgtype.Ref(pgtype.DomainKind, currentSchema, "d_ctl"), Value: &pgtype.ObservedDomain{BaseType: "text"}},
			schemaext.Object{Ref: pgtype.Ref(pgtype.CompositeKind, currentSchema, "c_ctl"),
				Value: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "value", Type: "text"}}}},
			schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, currentSchema, "r_ctl"), Value: &pgtype.ObservedRange{Subtype: "integer"}})),
		Views: []catalog.View{{
			Name:        "v_ctl",
			Schema:      currentSchema,
			Body:        "SELECT 1",
			CheckOption: "NONE",
		}},
		MatViews: []catalog.MaterializedView{{
			Name:   "mv_ctl",
			Schema: currentSchema,
			Body:   "SELECT 1",
		}},
		Triggers: []catalog.Trigger{{
			Name:    "tr_ctl",
			Schema:  currentSchema,
			Table:   "items",
			Timing:  "BEFORE",
			Event:   "INSERT",
			ForEach: "ROW",
			Body:    "SELECT 1",
		}},
	}
	return desired, database
}
