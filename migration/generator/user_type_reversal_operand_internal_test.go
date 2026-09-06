package generator

// White-box testing required: what this pins is which definition the reversal
// resolves for a modified user type, and the reversal is unexported. The public
// API exposes only the SQL the whole pipeline produces, where a wrong operand
// and a missing one render the same absence of statements.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestReverseSchemaDiff_UserTypeOperandResolvesAcrossSchemaSpellings pins the
// lookup a rolled-back user-type modification depends on.
//
// The forward change spells the name the declaration produced and the schema it
// is resolved against is the one a database read produced, so the two do not
// have to agree on whether the schema is written down. Resolving them by string
// equality would leave the rollback with no definition, which is not a failure:
// the drop is withheld, the type keeps the shape the up migration gave it, and
// the plan says it rolled back.
func TestReverseSchemaDiff_UserTypeOperandResolvesAcrossSchemaSpellings(t *testing.T) {
	tests := []struct {
		name  string
		diff  *difftypes.SchemaDiff
		prior *schemamodel.Database
	}{
		{
			name: "a domain qualified in the change and bare in the prior schema",
			diff: &difftypes.SchemaDiff{DomainsModified: []difftypes.DomainDiff{{
				DomainName: "public.zip",
				Changes:    map[string]string{"type": "VARCHAR(5) -> VARCHAR(10)"},
			}}},
			prior: &schemamodel.Database{Domains: []schemamodel.Domain{
				{Name: "zip", BaseType: "VARCHAR(5)"},
			}},
		},
		{
			name: "a domain bare in the change and qualified in the prior schema",
			diff: &difftypes.SchemaDiff{DomainsModified: []difftypes.DomainDiff{{
				DomainName: "zip",
				Changes:    map[string]string{"type": "VARCHAR(5) -> VARCHAR(10)"},
			}}},
			prior: &schemamodel.Database{Domains: []schemamodel.Domain{
				{Name: "zip", Schema: "public", BaseType: "VARCHAR(5)"},
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			semantics := identifier.ForDialect(platform.Postgres)

			reversed := reverseDomainDiffs(
				test.diff.DomainsModified, &schemamodel.Database{}, test.prior, semantics)

			c.Assert(reversed, qt.HasLen, 1)
			c.Assert(reversed[0].Desired.BaseType, qt.Equals, "VARCHAR(5)",
				qt.Commentf("the rollback rebuilds the domain the database held"))
		})
	}
}

// TestReverseSchemaDiff_UserTypeOperandComesFromThePriorSchema is the row the
// spelling cases above cannot show: that the definition is taken from the
// PRE-CHANGE schema rather than from the declaration the change already
// carries.
//
// Both schemas hold a type of the same name here, and only the value tells them
// apart. A reversal carrying the forward operand through would rebuild the
// shape the rollback exists to undo, and every drop-before-create ordering
// assertion would still pass.
func TestReverseSchemaDiff_UserTypeOperandComesFromThePriorSchema(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect(platform.Postgres)

	prior := &schemamodel.Database{
		CompositeTypes: []schemamodel.CompositeType{{
			Name: "addr", Fields: []schemamodel.CompositeField{{Name: "line1", Type: "text"}},
		}},
		Ranges: []schemamodel.Range{{Name: "span", Subtype: "int4"}},
	}
	forward := &difftypes.SchemaDiff{
		CompositeTypesModified: []difftypes.CompositeTypeDiff{{
			TypeName: "addr",
			Changes:  map[string]string{"fields": "line1 text -> line1 text, line2 text"},
			Desired: schemamodel.CompositeType{Name: "addr", Fields: []schemamodel.CompositeField{
				{Name: "line1", Type: "text"}, {Name: "line2", Type: "text"},
			}},
		}},
		RangesModified: []difftypes.RangeDiff{{
			RangeName: "span",
			Changes:   map[string]string{"subtype": "int4 -> int8"},
			Desired:   schemamodel.Range{Name: "span", Subtype: "int8"},
		}},
	}

	composites := reverseCompositeTypeDiffs(
		forward.CompositeTypesModified, &schemamodel.Database{}, prior, semantics)
	c.Assert(composites, qt.HasLen, 1)
	c.Assert(composites[0].Desired.Fields, qt.HasLen, 1,
		qt.Commentf("the rollback rebuilds the one-field composite the database held"))

	ranges := reverseRangeDiffs(forward.RangesModified, &schemamodel.Database{}, prior, semantics)
	c.Assert(ranges, qt.HasLen, 1)
	c.Assert(ranges[0].Desired.Subtype, qt.Equals, "int4",
		qt.Commentf("the rollback rebuilds the subtype the database held"))
}
