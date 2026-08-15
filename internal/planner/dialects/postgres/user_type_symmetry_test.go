package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestModifiedUserTypeNeverDropsWhatItCannotRecreate pins the two halves of the
// drop-and-recreate pair to one another.
//
// PostgreSQL has no in-place ALTER for a domain's base type, a composite's field
// list or a range's subtype, so a modification is planned as DROP TYPE followed
// by CREATE TYPE. The drop was emitted from the diff alone while the recreate was
// emitted only where the target definition resolved out of the schema. Where it
// did not, the plan dropped the type and put nothing back -- and that is not a
// migration that fails, it is one that succeeds having deleted a type.
func TestModifiedUserTypeNeverDropsWhatItCannotRecreate(t *testing.T) {
	tests := []struct {
		name        string
		generated   *goschema.Database
		diff        *types.SchemaDiff
		wantDrop    bool
		wantCreate  bool
		wantWarning string
	}{
		{
			// Control. The definition resolves, so both halves are emitted and
			// the pair is what it has always been.
			name: "a modified domain the schema declares is dropped and recreated",
			generated: &goschema.Database{
				Domains: []goschema.Domain{{Name: "zip", Schema: "app", BaseType: "VARCHAR(10)"}},
			},
			diff: &types.SchemaDiff{DomainsModified: []types.DomainDiff{{
				DomainName:      "app.zip",
				Changes:         map[string]string{"type": "character varying(5) -> VARCHAR(10)"},
				CurrentBaseType: "character varying(5)",
			}}},
			wantDrop:   true,
			wantCreate: true,
		},
		{
			// The declaration and the diff spell the schema differently. Under
			// identifier semantics they are one domain, so both halves are
			// emitted -- and they name the same object.
			name: "a modified domain spelled bare in the schema still recreates",
			generated: &goschema.Database{
				Domains: []goschema.Domain{{Name: "zip", BaseType: "VARCHAR(10)"}},
			},
			diff: &types.SchemaDiff{DomainsModified: []types.DomainDiff{{
				DomainName:      "public.zip",
				Changes:         map[string]string{"type": "character varying(5) -> VARCHAR(10)"},
				CurrentBaseType: "character varying(5)",
			}}},
			wantDrop:   true,
			wantCreate: true,
		},
		{
			// The definition is genuinely absent. Neither half is emitted, and
			// the omission is stated rather than silent.
			name: "a modified domain the schema does not declare is left alone",
			generated: &goschema.Database{
				Domains: []goschema.Domain{{Name: "other", Schema: "app", BaseType: "TEXT"}},
			},
			diff: &types.SchemaDiff{DomainsModified: []types.DomainDiff{{
				DomainName:      "app.zip",
				Changes:         map[string]string{"type": "character varying(5) -> VARCHAR(10)"},
				CurrentBaseType: "character varying(5)",
			}}},
			wantDrop:    false,
			wantCreate:  false,
			wantWarning: "domain app.zip changed, but the target definition was not found",
		},
		{
			name: "a modified composite type the schema does not declare is left alone",
			generated: &goschema.Database{
				CompositeTypes: []goschema.CompositeType{{Name: "other", Schema: "app"}},
			},
			diff: &types.SchemaDiff{CompositeTypesModified: []types.CompositeTypeDiff{{
				TypeName: "app.addr",
				Changes:  map[string]string{"fields": "a text -> a text, b text"},
			}}},
			wantDrop:    false,
			wantCreate:  false,
			wantWarning: "composite type app.addr changed, but the target definition was not found",
		},
		{
			name: "a modified range type the schema does not declare is left alone",
			generated: &goschema.Database{
				Ranges: []goschema.Range{{Name: "other", Schema: "app", Subtype: "int8"}},
			},
			diff: &types.SchemaDiff{RangesModified: []types.RangeDiff{{
				RangeName:      "app.span",
				Changes:        map[string]string{"subtype": "int4 -> int8"},
				CurrentSubtype: "int4",
			}}},
			wantDrop:    false,
			wantCreate:  false,
			wantWarning: "range type app.span changed, but the target definition was not found",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.generated, "postgres")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(strings.Contains(plan, "DROP DOMAIN") || strings.Contains(plan, "DROP TYPE"),
				qt.Equals, test.wantDrop, qt.Commentf("plan:\n%s", plan))
			c.Assert(strings.Contains(plan, "CREATE DOMAIN") || strings.Contains(plan, "CREATE TYPE"),
				qt.Equals, test.wantCreate, qt.Commentf("plan:\n%s", plan))
			c.Assert(plan, qt.Contains, test.wantWarning)
		})
	}
}
