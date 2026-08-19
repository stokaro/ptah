package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// alterDomainSchema declares one domain of each shape the in-place path emits a
// statement for.
func alterDomainSchema() *goschema.Database {
	return &goschema.Database{Domains: []goschema.Domain{
		{Name: "positive", BaseType: "integer", Check: "VALUE > 0"},
		{Name: "labelled", BaseType: "text", Default: "x"},
		{Name: "expr_default", BaseType: "timestamptz", DefaultExpr: "now()"},
		{Name: "required", BaseType: "text", NotNull: true},
	}}
}

// TestPlanner_AltersDomainsInPlaceRatherThanRebuildingThem is why the
// comparison in stokaro/ptah#1717 is worth making at all.
//
// Every domain modification used to be reconciled by dropping the domain and
// creating it again, and PostgreSQL refuses a non-CASCADE drop of a domain a
// column uses. A changed CHECK was therefore not awkward to apply -- it could
// not be applied to any domain in use, which is every domain worth having.
func TestPlanner_AltersDomainsInPlaceRatherThanRebuildingThem(t *testing.T) {
	tests := []struct {
		name     string
		modified difftypes.DomainDiff
		want     []string
	}{
		{
			name: "a replaced CHECK drops the stored constraint by name",
			modified: difftypes.DomainDiff{
				DomainName:              "positive",
				Changes:                 map[string]string{"check": "(VALUE > 9) -> (VALUE > 0)"},
				CurrentCheckConstraints: []string{"positive_check"},
			},
			want: []string{
				"ALTER DOMAIN positive DROP CONSTRAINT positive_check;",
				"ALTER DOMAIN positive ADD CHECK (VALUE > 0);",
			},
		},
		{
			name: "every stored constraint goes, not only the first",
			modified: difftypes.DomainDiff{
				DomainName:              "positive",
				Changes:                 map[string]string{"check": "two -> one"},
				CurrentCheckConstraints: []string{"lower_check", "upper_check"},
			},
			want: []string{
				"ALTER DOMAIN positive DROP CONSTRAINT lower_check;",
				"ALTER DOMAIN positive DROP CONSTRAINT upper_check;",
				"ALTER DOMAIN positive ADD CHECK (VALUE > 0);",
			},
		},
		{
			name: "a literal default is quoted",
			modified: difftypes.DomainDiff{
				DomainName: "labelled",
				Changes:    map[string]string{"default": "'w'::text -> 'x'::text"},
			},
			want: []string{"ALTER DOMAIN labelled SET DEFAULT 'x';"},
		},
		{
			name: "an expression default is emitted as written",
			modified: difftypes.DomainDiff{
				DomainName: "expr_default",
				Changes:    map[string]string{"default": "CURRENT_TIMESTAMP -> now()"},
			},
			want: []string{"ALTER DOMAIN expr_default SET DEFAULT now();"},
		},
		{
			name: "NOT NULL is set in place",
			modified: difftypes.DomainDiff{
				DomainName: "required",
				Changes:    map[string]string{"not_null": "false -> true"},
			},
			want: []string{"ALTER DOMAIN required SET NOT NULL;"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			planner := postgres.New()

			diff := &difftypes.SchemaDiff{DomainsModified: []difftypes.DomainDiff{test.modified}}
			nodes, err := planner.GenerateMigrationASTChecked(diff, alterDomainSchema())
			c.Assert(err, qt.IsNil)
			sql, err := renderer.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)
			sql = legacyRenderedSQL(sql)

			c.Assert(statementsIn(sql), qt.DeepEquals, test.want)
		})
	}
}

// TestPlanner_RebuildsADomainWhoseBaseTypeChanged is the control that keeps the
// drop-and-recreate path where it belongs.
//
// PostgreSQL has no ALTER DOMAIN ... TYPE, so a domain over a different type is
// a different domain. Routing that one through the in-place path would emit
// statements that change nothing and report a migration that did.
func TestPlanner_RebuildsADomainWhoseBaseTypeChanged(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	diff := &difftypes.SchemaDiff{DomainsModified: []difftypes.DomainDiff{{
		DomainName:              "positive",
		Changes:                 map[string]string{"type": "smallint -> integer"},
		CurrentBaseType:         "smallint",
		CurrentCheckConstraints: []string{"positive_check"},
	}}}

	nodes, err := planner.GenerateMigrationASTChecked(diff, alterDomainSchema())
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	c.Assert(sql, qt.Contains, "DROP DOMAIN IF EXISTS positive")
	c.Assert(sql, qt.Contains, "CREATE DOMAIN positive AS integer")
	c.Assert(sql, qt.Not(qt.Contains), "ALTER DOMAIN")
}

// TestPlanner_RebuildsADomainThatMixesAnAlterableChangeWithARebuild pins the
// case both paths could claim.
//
// The rebuild carries the whole declaration, constraint and default included,
// so an ALTER emitted beside it would run against a domain that no longer
// exists by the time it is reached.
func TestPlanner_RebuildsADomainThatMixesAnAlterableChangeWithARebuild(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	diff := &difftypes.SchemaDiff{DomainsModified: []difftypes.DomainDiff{{
		DomainName: "positive",
		Changes: map[string]string{
			"type":  "smallint -> integer",
			"check": "(VALUE > 9) -> (VALUE > 0)",
		},
		CurrentBaseType:         "smallint",
		CurrentCheckConstraints: []string{"positive_check"},
	}}}

	nodes, err := planner.GenerateMigrationASTChecked(diff, alterDomainSchema())
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	c.Assert(sql, qt.Contains, "DROP DOMAIN IF EXISTS positive")
	c.Assert(sql, qt.Not(qt.Contains), "ALTER DOMAIN")
}

// statementsIn returns the rendered statements with comments and blank lines
// removed, so an assertion names the SQL and not the layout around it.
func statementsIn(sql string) []string {
	var statements []string
	for line := range strings.SplitSeq(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "", strings.HasPrefix(trimmed, "--"):
			continue
		default:
			statements = append(statements, trimmed)
		}
	}
	return statements
}
