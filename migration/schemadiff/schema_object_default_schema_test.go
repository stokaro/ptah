package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestCompareWithDialect_ImplicitSchemaMatchesSchemaObjects(t *testing.T) {
	c := qt.New(t)
	generated, database := schemaObjectIdentityFixtures("public", "")

	diff := schemadiff.CompareWithDialect(generated, database, "postgres")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

func TestCompareWithDialect_DifferentSchemaDoesNotMatchSchemaObjects(t *testing.T) {
	c := qt.New(t)
	generated, database := schemaObjectIdentityFixtures("public", "reporting")

	diff := schemadiff.CompareWithDialect(generated, database, "postgres")

	c.Assert(diff.FunctionsAdded, qt.DeepEquals, []string{"public.f_ctl"})
	c.Assert(diff.FunctionsRemoved, qt.DeepEquals, []string{"reporting.f_ctl"})
	c.Assert(diff.SequencesAdded, qt.DeepEquals, []string{"public.s_ctl"})
	c.Assert(diff.SequencesRemoved, qt.DeepEquals, []string{"reporting.s_ctl"})
	c.Assert(diff.DomainsAdded, qt.DeepEquals, []string{"public.d_ctl"})
	c.Assert(diff.DomainsRemoved, qt.DeepEquals, []string{"reporting.d_ctl"})
	c.Assert(diff.CompositeTypesAdded, qt.DeepEquals, []string{"public.c_ctl"})
	c.Assert(diff.CompositeTypesRemoved, qt.DeepEquals, []string{"reporting.c_ctl"})
	c.Assert(diff.RangesAdded, qt.DeepEquals, []string{"public.r_ctl"})
	c.Assert(diff.RangesRemoved, qt.DeepEquals, []string{"reporting.r_ctl"})
	c.Assert(diff.ViewsAdded, qt.DeepEquals, []string{"public.v_ctl"})
	c.Assert(diff.ViewsRemoved, qt.DeepEquals, []string{"reporting.v_ctl"})
	c.Assert(diff.MaterializedViewsAdded, qt.DeepEquals, []string{"public.mv_ctl"})
	c.Assert(diff.MaterializedViewsRemoved, qt.DeepEquals, []string{"reporting.mv_ctl"})
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
) (*goschema.Database, *types.DBSchema) {
	generated := &goschema.Database{
		Functions: []goschema.Function{{
			Name:       desiredSchema + ".f_ctl",
			Returns:    "integer",
			Language:   "sql",
			Security:   "INVOKER",
			Volatility: "VOLATILE",
			Body:       "SELECT 1",
		}},
		Sequences: []goschema.Sequence{{
			Name:   "s_ctl",
			Schema: desiredSchema,
			AsType: "bigint",
		}},
		Domains: []goschema.Domain{{
			Name:     "d_ctl",
			Schema:   desiredSchema,
			BaseType: "text",
		}},
		CompositeTypes: []goschema.CompositeType{{
			Name:   "c_ctl",
			Schema: desiredSchema,
			Fields: []goschema.CompositeTypeField{{Name: "value", Type: "text"}},
		}},
		Ranges: []goschema.Range{{
			Name:    "r_ctl",
			Schema:  desiredSchema,
			Subtype: "integer",
		}},
		Views: []goschema.View{{
			Name: desiredSchema + ".v_ctl",
			Body: "SELECT 1",
		}},
		MaterializedViews: []goschema.MaterializedView{{
			Name: desiredSchema + ".mv_ctl",
			Body: "SELECT 1",
		}},
		Triggers: []goschema.Trigger{{
			Name:    "tr_ctl",
			Table:   desiredSchema + ".items",
			Timing:  "BEFORE",
			Event:   "INSERT",
			ForEach: "ROW",
			Body:    "SELECT 1",
		}},
	}
	database := &types.DBSchema{
		Functions: []types.DBFunction{{
			Name:       "f_ctl",
			Schema:     currentSchema,
			Returns:    "integer",
			Language:   "sql",
			Security:   "INVOKER",
			Volatility: "VOLATILE",
			Body:       "SELECT 1",
		}},
		Sequences: []types.DBSequence{{
			Name:     "s_ctl",
			Schema:   currentSchema,
			DataType: "bigint",
		}},
		Domains: []types.DBDomain{{
			Name:     "d_ctl",
			Schema:   currentSchema,
			BaseType: "text",
		}},
		Composites: []types.DBComposite{{
			Name:   "c_ctl",
			Schema: currentSchema,
			Fields: []types.DBCompositeField{{Name: "value", Type: "text"}},
		}},
		Ranges: []types.DBRange{{
			Name:    "r_ctl",
			Schema:  currentSchema,
			Subtype: "integer",
		}},
		Views: []types.DBView{{
			Name:        "v_ctl",
			Schema:      currentSchema,
			Body:        "SELECT 1",
			CheckOption: "NONE",
		}},
		MatViews: []types.DBMatView{{
			Name:   "mv_ctl",
			Schema: currentSchema,
			Body:   "SELECT 1",
		}},
		Triggers: []types.DBTrigger{{
			Name:    "tr_ctl",
			Schema:  currentSchema,
			Table:   "items",
			Timing:  "BEFORE",
			Event:   "INSERT",
			ForEach: "ROW",
			Body:    "SELECT 1",
		}},
	}
	return generated, database
}
