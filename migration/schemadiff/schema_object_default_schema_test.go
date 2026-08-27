package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestCompareWithDialect_ImplicitSchemaMatchesSchemaObjects(t *testing.T) {
	c := qt.New(t)
	desired, database := schemaObjectIdentityFixtures("public", "")

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

func TestCompareWithDialect_DifferentSchemaDoesNotMatchSchemaObjects(t *testing.T) {
	c := qt.New(t)
	desired, database := schemaObjectIdentityFixtures("public", "reporting")

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")

	c.Assert(diff.FunctionsAdded, qt.DeepEquals, []string{"public.f_ctl"})
	c.Assert(diff.FunctionsRemoved, qt.DeepEquals, []string{"reporting.f_ctl"})
	c.Assert(diff.SequencesAdded.Names(), qt.DeepEquals, []string{"public.s_ctl"})
	c.Assert(diff.SequencesRemoved.Names(), qt.DeepEquals, []string{"reporting.s_ctl"})
	c.Assert(diff.DomainsAdded.Names(), qt.DeepEquals, []string{"public.d_ctl"})
	c.Assert(diff.DomainsRemoved.Names(), qt.DeepEquals, []string{"reporting.d_ctl"})
	c.Assert(diff.CompositeTypesAdded.Names(), qt.DeepEquals, []string{"public.c_ctl"})
	c.Assert(diff.CompositeTypesRemoved.Names(), qt.DeepEquals, []string{"reporting.c_ctl"})
	c.Assert(diff.RangesAdded.Names(), qt.DeepEquals, []string{"public.r_ctl"})
	c.Assert(diff.RangesRemoved.Names(), qt.DeepEquals, []string{"reporting.r_ctl"})
	c.Assert(diff.ViewsAdded.Names(), qt.DeepEquals, []string{"public.v_ctl"})
	c.Assert(diff.ViewsRemoved.Names(), qt.DeepEquals, []string{"reporting.v_ctl"})
	c.Assert(diff.MaterializedViewsAdded.Names(), qt.DeepEquals, []string{"public.mv_ctl"})
	c.Assert(diff.MaterializedViewsRemoved.Names(), qt.DeepEquals, []string{"reporting.mv_ctl"})
	c.Assert(diff.TriggersAdded, qt.DeepEquals, []difftypes.TriggerRef{{
		TriggerName: "tr_ctl",
		TableName:   "public.items",
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
		Domains: []schemamodel.Domain{{
			Name:     "d_ctl",
			Schema:   desiredSchema,
			BaseType: "text",
		}},
		CompositeTypes: []schemamodel.CompositeType{{
			Name:   "c_ctl",
			Schema: desiredSchema,
			Fields: []schemamodel.CompositeField{{Name: "value", Type: "text"}},
		}},
		Ranges: []schemamodel.Range{{
			Name:    "r_ctl",
			Schema:  desiredSchema,
			Subtype: "integer",
		}},
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
		Domains: []catalog.Domain{{
			Name:     "d_ctl",
			Schema:   currentSchema,
			BaseType: "text",
		}},
		Composites: []catalog.CompositeType{{
			Name:   "c_ctl",
			Schema: currentSchema,
			Fields: []catalog.CompositeField{{Name: "value", Type: "text"}},
		}},
		Ranges: []catalog.Range{{
			Name:    "r_ctl",
			Schema:  currentSchema,
			Subtype: "integer",
		}},
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
