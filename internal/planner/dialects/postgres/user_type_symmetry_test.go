package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestModifiedUserTypeNeverDropsWhatItCannotRecreate pins the two halves of the
// drop-and-recreate pair to one another.
//
// PostgreSQL has no in-place ALTER for a domain's base type, a composite's field
// list or a range's subtype, so a modification is planned as DROP TYPE followed
// by CREATE TYPE. The drop is emitted from the diff alone and the recreate from
// the definition the change carries, so a change carrying none must emit
// neither -- a plan that drops the type and puts nothing back is not a
// migration that fails, it is one that succeeds having deleted a type.
//
// The schema passed alongside is empty in every row. The definition travels
// with the change (stokaro/ptah#2315), so the rows that recreate prove the
// planner needs nothing else, and the rows that do not prove the withholding
// is decided by the change rather than by what the planner was handed.
func TestModifiedUserTypeNeverDropsWhatItCannotRecreate(t *testing.T) {
	tests := []struct {
		name        string
		diff        *difftypes.SchemaDiff
		wantDrop    bool
		wantCreate  bool
		wantWarning string
	}{
		{
			// Control. The definition resolves, so both halves are emitted and
			// the pair is what it has always been.
			name: "a modified domain the change carries is dropped and recreated",
			diff: &difftypes.SchemaDiff{DomainsModified: []difftypes.DomainDiff{{
				DomainName:      "app.zip",
				Changes:         map[string]string{"type": "character varying(5) -> VARCHAR(10)"},
				CurrentBaseType: "character varying(5)",
				Desired:         schemamodel.Domain{Name: "zip", Schema: "app", BaseType: "VARCHAR(10)"},
			}}},
			wantDrop:   true,
			wantCreate: true,
		},
		{
			// The change's name and the operand spell the schema differently.
			// The drop is written from the first and the create from the
			// second, so this is the row where the pair could come apart.
			name: "a modified domain whose operand is spelled bare still recreates",
			diff: &difftypes.SchemaDiff{DomainsModified: []difftypes.DomainDiff{{
				DomainName:      "public.zip",
				Changes:         map[string]string{"type": "character varying(5) -> VARCHAR(10)"},
				CurrentBaseType: "character varying(5)",
				Desired:         schemamodel.Domain{Name: "zip", BaseType: "VARCHAR(10)"},
			}}},
			wantDrop:   true,
			wantCreate: true,
		},
		{
			// The definition is genuinely absent. Neither half is emitted, and
			// the omission is stated rather than silent.
			name: "a modified domain the change carries no definition for is left alone",
			diff: &difftypes.SchemaDiff{DomainsModified: []difftypes.DomainDiff{{
				DomainName:      "app.zip",
				Changes:         map[string]string{"type": "character varying(5) -> VARCHAR(10)"},
				CurrentBaseType: "character varying(5)",
			}}},
			wantDrop:    false,
			wantCreate:  false,
			wantWarning: "domain app.zip changed, but the change carries no definition",
		},
		{
			name: "a modified composite type the change carries no definition for is left alone",
			diff: &difftypes.SchemaDiff{CompositeTypesModified: []difftypes.CompositeTypeDiff{{
				TypeName: "app.addr",
				Changes:  map[string]string{"fields": "a text -> a text, b text"},
			}}},
			wantDrop:    false,
			wantCreate:  false,
			wantWarning: "composite type app.addr changed, but the change carries no definition",
		},
		{
			name: "a modified range type the change carries no definition for is left alone",
			diff: &difftypes.SchemaDiff{RangesModified: []difftypes.RangeDiff{{
				RangeName:      "app.span",
				Changes:        map[string]string{"subtype": "int4 -> int8"},
				CurrentSubtype: "int4",
			}}},
			wantDrop:    false,
			wantCreate:  false,
			wantWarning: "range type app.span changed, but the change carries no definition",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, "postgres")
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
