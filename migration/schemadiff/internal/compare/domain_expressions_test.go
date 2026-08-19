package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestDomains_ComparesCheckThroughTheServersOwnSpelling is the defect
// stokaro/ptah#1717 names: a changed domain CHECK produced no diff and no
// diagnostic, so `schema apply` reported a synced schema over a database still
// enforcing the old rule.
//
// The declared and the stored form are never the same string. PostgreSQL 18.4
// stores a parsed CHECK and prints it back from the parse tree, measured
// against a live server:
//
//	CHECK (VALUE > 0)            reads back as  (VALUE > 0)
//	CHECK (VALUE <> '')          reads back as  (VALUE <> ''::text)
//	CHECK (VALUE IN ('x','y'))   reads back as  (VALUE = ANY (ARRAY['x'::text, 'y'::text]))
//	CHECK (VALUE BETWEEN 1 AND 10) reads back as ((VALUE >= 1) AND (VALUE <= 10))
//
// so the comparison holds the declaration after the same round trip rather than
// the declaration as written.
func TestDomains_ComparesCheckThroughTheServersOwnSpelling(t *testing.T) {
	tests := []struct {
		name        string
		declared    string
		normalized  string
		resolved    bool
		stored      string
		storedName  string
		wantChanged bool
	}{
		{
			name:        "unchanged constraint is not a difference",
			declared:    "VALUE IN ('x','y')",
			normalized:  "(VALUE = ANY (ARRAY['x'::text, 'y'::text]))",
			resolved:    true,
			stored:      "(VALUE = ANY (ARRAY['x'::text, 'y'::text]))",
			storedName:  "positive",
			wantChanged: false,
		},
		{
			name:        "changed constraint is a difference",
			declared:    "VALUE IN ('x','y','z')",
			normalized:  "(VALUE = ANY (ARRAY['x'::text, 'y'::text, 'z'::text]))",
			resolved:    true,
			stored:      "(VALUE = ANY (ARRAY['x'::text, 'y'::text]))",
			storedName:  "positive",
			wantChanged: true,
		},
		{
			// The declaration as written never equals the stored form, so a
			// comparison that skipped the normalization would call every
			// unchanged domain changed and replace its constraint on every run.
			name:        "raw declaration is not compared against the stored form",
			declared:    "VALUE > 0",
			normalized:  "(VALUE > 0)",
			resolved:    true,
			stored:      "(VALUE > 0)",
			storedName:  "positive",
			wantChanged: false,
		},
		{
			// No server was asked -- an offline comparison has none -- so the
			// attribute stays uncompared rather than being guessed at.
			name:        "an unresolved declaration is not compared",
			declared:    "VALUE > 0",
			normalized:  "",
			resolved:    false,
			stored:      "(VALUE > 999)",
			storedName:  "positive",
			wantChanged: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			generated := &goschema.Database{Domains: []goschema.Domain{
				{Name: "positive", BaseType: "integer", Check: test.declared},
			}}
			database := &types.DBSchema{Domains: []types.DBDomain{{
				Name:     "positive",
				BaseType: "integer",
				Check:    test.stored,
				CheckConstraints: []types.DBDomainCheck{
					{Name: test.storedName, Expression: test.stored},
				},
			}}}
			diff := &difftypes.SchemaDiff{}

			compare.DomainsWithSemantics(
				generated,
				database,
				diff,
				compare.CoverageOf(generated, database),
				identifier.ForDialect("postgres"),
				map[string]config.DomainExpression{
					"positive": {Check: test.normalized, Resolved: test.resolved},
				},
			)

			c.Assert(len(diff.DomainsModified) == 1, qt.Equals, test.wantChanged)
		})
	}
}

// TestDomains_ComparesEachStoredConstraintRatherThanTheJoinedForm pins why the
// comparison reads the constraints one by one.
//
// The reader also carries the expressions joined with AND, which is what a
// renderer needs. Comparing against that joined form has a seam: a domain
// holding two constraints joins as `(a) AND (b)`, while a declaration of
// `a AND b` normalizes to `((a) AND (b))` -- the same rule, one pair of
// parentheses apart. A comparison of the joined forms would plan a replacement
// on every run and converge on nothing.
func TestDomains_ComparesEachStoredConstraintRatherThanTheJoinedForm(t *testing.T) {
	tests := []struct {
		name        string
		constraints []types.DBDomainCheck
		wantChanged bool
		wantDropped []string
	}{
		{
			name: "one stored constraint matching the declaration",
			constraints: []types.DBDomainCheck{
				{Name: "bounded_check", Expression: "((VALUE > 0) AND (VALUE < 100))"},
			},
			wantChanged: false,
		},
		{
			// Two constraints against a declaration holding one is a domain
			// with one constraint too many, and both have to go for the
			// replacement to converge.
			name: "two stored constraints joining to the same rule",
			constraints: []types.DBDomainCheck{
				{Name: "lower_check", Expression: "(VALUE > 0)"},
				{Name: "upper_check", Expression: "(VALUE < 100)"},
			},
			wantChanged: true,
			wantDropped: []string{"lower_check", "upper_check"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			generated := &goschema.Database{Domains: []goschema.Domain{
				{Name: "bounded", BaseType: "integer", Check: "VALUE > 0 AND VALUE < 100"},
			}}
			database := &types.DBSchema{Domains: []types.DBDomain{{
				Name:             "bounded",
				BaseType:         "integer",
				Check:            "(VALUE > 0) AND (VALUE < 100)",
				CheckConstraints: test.constraints,
			}}}
			diff := &difftypes.SchemaDiff{}

			compare.DomainsWithSemantics(
				generated,
				database,
				diff,
				compare.CoverageOf(generated, database),
				identifier.ForDialect("postgres"),
				map[string]config.DomainExpression{
					"bounded": {Check: "((VALUE > 0) AND (VALUE < 100))", Resolved: true},
				},
			)

			c.Assert(len(diff.DomainsModified) == 1, qt.Equals, test.wantChanged)
			c.Assert(collectDroppedConstraints(diff), qt.DeepEquals, test.wantDropped)
		})
	}
}

// TestDomains_LeavesUndeclaredAttributesAlone keeps this comparator's rule for
// a declaration that states nothing.
//
// Reading "no CHECK declared" as "remove the CHECK the catalog holds" would
// make adopting Ptah over an existing database drop constraints nobody asked
// about, which is the opposite failure to the one #1717 reports.
func TestDomains_LeavesUndeclaredAttributesAlone(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{Domains: []goschema.Domain{
		{Name: "positive", BaseType: "integer"},
	}}
	database := &types.DBSchema{Domains: []types.DBDomain{{
		Name:             "positive",
		BaseType:         "integer",
		Check:            "(VALUE > 0)",
		Default:          "7",
		CheckConstraints: []types.DBDomainCheck{{Name: "positive_check", Expression: "(VALUE > 0)"}},
	}}}
	diff := &difftypes.SchemaDiff{}

	compare.DomainsWithSemantics(
		generated,
		database,
		diff,
		compare.CoverageOf(generated, database),
		identifier.ForDialect("postgres"),
		map[string]config.DomainExpression{"positive": {Resolved: true}},
	)

	c.Assert(diff.DomainsModified, qt.HasLen, 0)
}

// TestDomains_ComparesDefaultThroughTheServersOwnSpelling covers the other
// attribute the comparison used to decline. A declared `'x'` is stored as
// `'x'::text`, so it needs the same round trip the CHECK does.
func TestDomains_ComparesDefaultThroughTheServersOwnSpelling(t *testing.T) {
	tests := []struct {
		name        string
		normalized  string
		stored      string
		wantChanged bool
	}{
		{name: "unchanged default", normalized: "'x'::text", stored: "'x'::text", wantChanged: false},
		{name: "changed default", normalized: "'y'::text", stored: "'x'::text", wantChanged: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			generated := &goschema.Database{Domains: []goschema.Domain{
				{Name: "labelled", BaseType: "text", Default: "x"},
			}}
			database := &types.DBSchema{Domains: []types.DBDomain{
				{Name: "labelled", BaseType: "text", Default: test.stored},
			}}
			diff := &difftypes.SchemaDiff{}

			compare.DomainsWithSemantics(
				generated,
				database,
				diff,
				compare.CoverageOf(generated, database),
				identifier.ForDialect("postgres"),
				map[string]config.DomainExpression{
					"labelled": {Default: test.normalized, Resolved: true},
				},
			)

			c.Assert(len(diff.DomainsModified) == 1, qt.Equals, test.wantChanged)
		})
	}
}

// collectDroppedConstraints returns the constraint names the diff carries for
// the planner to remove, or nil when nothing was reported as modified.
func collectDroppedConstraints(diff *difftypes.SchemaDiff) []string {
	var names []string
	for _, domain := range diff.DomainsModified {
		names = append(names, domain.CurrentCheckConstraints...)
	}
	return names
}
