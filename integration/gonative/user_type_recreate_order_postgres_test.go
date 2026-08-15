//go:build integration

// Live guard for the drop half of the user-defined type ordering added for
// stokaro/ptah#1242.
//
// A modified domain or composite has no in-place ALTER, so it is reconciled as
// a non-CASCADE DROP followed by a CREATE. The two halves are ordered by
// different graphs and have to be: the CREATE builds the desired shape, while
// the DROP executes against the database as it stands, where only the
// references it holds now can block it.
//
// Nothing short of a live server settles this. Every emitted order is
// self-consistent, so a text assertion just restates the emitter; a self-apply
// plans no statements and executes nothing; and a replay from empty drops
// nothing. Each case below seeds the CURRENT shape into one throwaway database,
// reconciles it against a second database holding the DESIRED shape, and lets
// PostgreSQL judge.

package gonative_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestPostgreSQLUserTypeRecreate_DropsAgainstTheCurrentShape reconciles a
// database whose user-defined types have to be recreated, and asserts the plan
// runs.
//
// The three rows differ only in where the reference between the two types sits
// on each side, which is the axis that decides whether one order can serve
// both:
//
//   - it inverts: the domain names the composite now, the composite will name
//     the domain. Ordering the drops by the desired side takes the composite
//     first and PostgreSQL answers `cannot drop type cc because other objects
//     depend on it / DETAIL: type dd depends on type cc`.
//   - it disappears: the composite names the domain now and will name a
//     built-in. The desired side has no edge at all, so the order degrades to
//     the caller's -- domains first -- and the server answers `column q of
//     composite type meas depends on type qty`.
//   - it survives: both sides agree, and the two orders are mirror images.
//     This is the row that was already green before the drop graph was
//     separated from the create graph, and it is here so a fix for the first
//     two cannot be had by giving this one up.
func TestPostgreSQLUserTypeRecreate_DropsAgainstTheCurrentShape(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)

	tests := []struct {
		name             string
		current          []string
		desired          []string
		wantDomain       string
		wantBaseType     string
		wantComposite    string
		wantCompositeSQL []string
	}{
		{
			name: "the reference between the two types inverts",
			current: []string{
				"CREATE TYPE cc AS (f integer)",
				"CREATE DOMAIN dd AS cc",
			},
			desired: []string{
				"CREATE DOMAIN dd AS integer",
				"CREATE TYPE cc AS (f dd)",
			},
			wantDomain:       "dd",
			wantBaseType:     "integer",
			wantComposite:    "cc",
			wantCompositeSQL: []string{"dd"},
		},
		{
			name: "the desired shape stops naming the domain",
			current: []string{
				"CREATE DOMAIN qty AS integer CHECK (VALUE > 0)",
				"CREATE TYPE meas AS (q qty, label text)",
			},
			desired: []string{
				"CREATE DOMAIN qty AS bigint CHECK (VALUE > 0)",
				"CREATE TYPE meas AS (q bigint, label text)",
			},
			wantDomain:       "qty",
			wantBaseType:     "bigint",
			wantComposite:    "meas",
			wantCompositeSQL: []string{"bigint", "text"},
		},
		{
			name: "the reference survives the modification",
			current: []string{
				"CREATE DOMAIN qty AS integer CHECK (VALUE > 0)",
				"CREATE TYPE meas AS (q qty, label text)",
			},
			desired: []string{
				"CREATE DOMAIN qty AS bigint CHECK (VALUE > 0)",
				"CREATE TYPE meas AS (q qty, label text, extra text)",
			},
			wantDomain:       "qty",
			wantBaseType:     "bigint",
			wantComposite:    "meas",
			wantCompositeSQL: []string{"qty", "text", "text"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, desiredURL := newUserTypeRecreateTarget(c, dsn, "user_type_recreate", test.current, test.desired)

			plan := prepareUserTypeRecreatePlan(c, target, desiredURL)
			// Without this the row would pass on a planner that emitted
			// nothing, which is the cheapest way to make a drop order look
			// correct.
			c.Assert(plan.HasChanges(), qt.IsTrue)
			c.Assert(plan.Execute(c.Context()), qt.IsNil, qt.Commentf("emitted script:\n%s", plan.SQL()))

			// Exit status alone is not convergence: a plan could run and leave
			// the types on their old shape.
			applied, err := dbschema.ReadSchemaWithSchemas(target, nil)
			c.Assert(err, qt.IsNil)
			c.Assert(findLiveDomain(c, applied.Domains, test.wantDomain).BaseType, qt.Equals, test.wantBaseType)
			c.Assert(liveCompositeFieldTypes(c, applied.Composites, test.wantComposite), qt.DeepEquals, test.wantCompositeSQL)

			// The other direction: the converged database plans nothing
			// against the same target, so the plan above was not a churn that
			// happens to be executable.
			settled := prepareUserTypeRecreatePlan(c, target, desiredURL)
			c.Assert(settled.HasChanges(), qt.IsFalse, qt.Commentf("second plan:\n%s", settled.SQL()))
		})
	}
}

// TestPostgreSQLUserTypeRecreate_DropsADomainBeforeTheDomainItNames is the same
// live guard for a dependent pair of the SAME kind, which is where no order of
// kinds can help at all.
//
// Every row of the table above pairs a domain with a composite, so a rule as
// crude as "composites before domains" would pass all three. Here both types
// are domains and the reference between them lives inside one kind. The
// comparator hands the planner each modified list sorted by name, and the row
// is named so that order is the failing one: d_base comes first alphabetically,
// and dropping it while d_over still stands draws `cannot drop type d_base
// because other objects depend on it`.
//
// The desired side puts d_over on a built-in instead, so the desired graph is
// empty here as well. Only the current definitions can place these statements.
func TestPostgreSQLUserTypeRecreate_DropsADomainBeforeTheDomainItNames(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)

	tests := []struct {
		name              string
		current           []string
		desired           []string
		wantBase          string
		wantBaseType      string
		wantDependent     string
		wantDependentType string
	}{
		{
			name: "both types are domains",
			current: []string{
				"CREATE DOMAIN d_base AS integer",
				"CREATE DOMAIN d_over AS d_base",
			},
			desired: []string{
				"CREATE DOMAIN d_base AS text",
				"CREATE DOMAIN d_over AS text",
			},
			wantBase:          "d_base",
			wantBaseType:      "text",
			wantDependent:     "d_over",
			wantDependentType: "text",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, desiredURL := newUserTypeRecreateTarget(c, dsn, "user_type_one_kind", test.current, test.desired)

			plan := prepareUserTypeRecreatePlan(c, target, desiredURL)
			c.Assert(plan.HasChanges(), qt.IsTrue)
			c.Assert(plan.Execute(c.Context()), qt.IsNil, qt.Commentf("emitted script:\n%s", plan.SQL()))

			applied, err := dbschema.ReadSchemaWithSchemas(target, nil)
			c.Assert(err, qt.IsNil)
			c.Assert(findLiveDomain(c, applied.Domains, test.wantBase).BaseType, qt.Equals, test.wantBaseType)
			c.Assert(findLiveDomain(c, applied.Domains, test.wantDependent).BaseType, qt.Equals, test.wantDependentType)

			settled := prepareUserTypeRecreatePlan(c, target, desiredURL)
			c.Assert(settled.HasChanges(), qt.IsFalse, qt.Commentf("second plan:\n%s", settled.SQL()))
		})
	}
}

// TestPostgreSQLUserTypeRecreate_DropsACompositeBeforeTheCompositeItNames is
// the composite half of the same-kind guard: c_over carries a field of type
// c_base, so the two are ordered by a reference no kind ordering can see.
//
// Sorted by name, c_base comes first, and dropping it while c_over still holds
// a field of that type draws `cannot drop type c_base because other objects
// depend on it`. The desired side gives c_over a built-in field type, so the
// desired graph is empty and only the current definitions can place these
// statements.
func TestPostgreSQLUserTypeRecreate_DropsACompositeBeforeTheCompositeItNames(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)

	tests := []struct {
		name                  string
		current               []string
		desired               []string
		wantBase              string
		wantBaseFieldSQL      []string
		wantDependent         string
		wantDependentFieldSQL []string
	}{
		{
			name: "both types are composites",
			current: []string{
				"CREATE TYPE c_base AS (f integer)",
				"CREATE TYPE c_over AS (g c_base)",
			},
			desired: []string{
				"CREATE TYPE c_base AS (f text)",
				"CREATE TYPE c_over AS (g text)",
			},
			wantBase:              "c_base",
			wantBaseFieldSQL:      []string{"text"},
			wantDependent:         "c_over",
			wantDependentFieldSQL: []string{"text"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, desiredURL := newUserTypeRecreateTarget(c, dsn, "user_type_one_kind", test.current, test.desired)

			plan := prepareUserTypeRecreatePlan(c, target, desiredURL)
			c.Assert(plan.HasChanges(), qt.IsTrue)
			c.Assert(plan.Execute(c.Context()), qt.IsNil, qt.Commentf("emitted script:\n%s", plan.SQL()))

			applied, err := dbschema.ReadSchemaWithSchemas(target, nil)
			c.Assert(err, qt.IsNil)
			c.Assert(liveCompositeFieldTypes(c, applied.Composites, test.wantBase), qt.DeepEquals, test.wantBaseFieldSQL)
			c.Assert(liveCompositeFieldTypes(c, applied.Composites, test.wantDependent), qt.DeepEquals, test.wantDependentFieldSQL)

			settled := prepareUserTypeRecreatePlan(c, target, desiredURL)
			c.Assert(settled.HasChanges(), qt.IsFalse, qt.Commentf("second plan:\n%s", settled.SQL()))
		})
	}
}

// newUserTypeRecreateTarget seeds the desired shape into one throwaway database
// and the current shape into another, and returns an open connection to the
// current one together with the URL of the desired one. Both carry the same
// search_path, so the reconciliation compares public against public.
//
// name reaches the database name, which is how a leaked database says which
// case leaked it. Keep it short: the server truncates an identifier past 63
// bytes, and a truncated name loses its trailing timestamp -- the part that
// keeps this run's databases apart from a previous run's leftovers.
func newUserTypeRecreateTarget(
	c *qt.C,
	dsn, name string,
	current, desired []string,
) (*dbschema.DatabaseConnection, string) {
	c.Helper()

	desiredURL := newBoundaryDatabase(c, dsn, boundaryCase{
		name:  name + "_desired",
		seed:  desired,
		query: "search_path=public",
	})
	currentURL := newBoundaryDatabase(c, dsn, boundaryCase{
		name:  name + "_current",
		seed:  current,
		query: "search_path=public",
	})

	target, err := dbschema.ConnectToDatabase(c.Context(), currentURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })

	return target, desiredURL
}

// prepareUserTypeRecreatePlan plans target against desiredURL and asserts only
// that planning itself got that far. What each caller judges is what the plan
// then does to a live server, so that assertion stays in the test.
func prepareUserTypeRecreatePlan(
	c *qt.C,
	target *dbschema.DatabaseConnection,
	desiredURL string,
) atlasschema.ApplyRuntimePlan {
	c.Helper()

	plan, err := atlasschema.PrepareApply(c.Context(), target, atlasschema.ApplyRuntimeOptions{
		ToURLs: []string{desiredURL},
		TxMode: migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)

	return plan
}

func findLiveDomain(c *qt.C, domains []dbschematypes.DBDomain, name string) dbschematypes.DBDomain {
	c.Helper()

	for _, candidate := range domains {
		if candidate.Name == name {
			return candidate
		}
	}
	c.Fatalf("the read schema has no domain %q", name)
	return dbschematypes.DBDomain{}
}

func liveCompositeFieldTypes(c *qt.C, composites []dbschematypes.DBComposite, name string) []string {
	c.Helper()

	for _, candidate := range composites {
		if candidate.Name != name {
			continue
		}
		fieldTypes := make([]string, len(candidate.Fields))
		for i, field := range candidate.Fields {
			fieldTypes[i] = field.Type
		}
		return fieldTypes
	}
	c.Fatalf("the read schema has no composite type %q", name)
	return nil
}
