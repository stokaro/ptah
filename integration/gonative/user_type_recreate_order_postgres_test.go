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
	c := qt.New(t)

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
		c.Run(test.name, func(c *qt.C) {
			desiredURL := newBoundaryDatabase(c, dsn, boundaryCase{
				name:  "user_type_recreate_desired",
				seed:  test.desired,
				query: "search_path=public",
			})
			currentURL := newBoundaryDatabase(c, dsn, boundaryCase{
				name:  "user_type_recreate_current",
				seed:  test.current,
				query: "search_path=public",
			})

			target, err := dbschema.ConnectToDatabase(c.Context(), currentURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(target) })

			plan, err := atlasschema.PrepareApply(c.Context(), target, atlasschema.ApplyRuntimeOptions{
				ToURLs: []string{desiredURL},
				TxMode: migrator.MigrationTxModeFile,
			})
			c.Assert(err, qt.IsNil)
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
			settled, err := atlasschema.PrepareApply(c.Context(), target, atlasschema.ApplyRuntimeOptions{
				ToURLs: []string{desiredURL},
				TxMode: migrator.MigrationTxModeFile,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(settled.HasChanges(), qt.IsFalse, qt.Commentf("second plan:\n%s", settled.SQL()))
		})
	}
}

// TestPostgreSQLUserTypeRecreate_DropsAgainstTheCurrentShapeWithinOneKind is the
// same live guard for a dependent pair of the SAME kind, which is where no order
// of kinds can help at all.
//
// Every row of the table above pairs a domain with a composite, so a rule as
// crude as "composites before domains" would pass all three. Here the two types
// are both domains, or both composites, and the reference between them lives
// inside one kind. The comparator hands the planner each modified list sorted by
// name, and both rows are named so that order is the failing one: the base type
// comes first alphabetically, and dropping it while its dependent still stands
// draws `cannot drop type ... because other objects depend on it`.
//
// Neither desired side names the other type, so the desired graph is empty here
// as well. Only the current definitions can place these statements.
func TestPostgreSQLUserTypeRecreate_DropsAgainstTheCurrentShapeWithinOneKind(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	tests := []struct {
		name            string
		current         []string
		desired         []string
		assertConverged func(c *qt.C, applied *dbschematypes.DBSchema)
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
			assertConverged: func(c *qt.C, applied *dbschematypes.DBSchema) {
				c.Assert(findLiveDomain(c, applied.Domains, "d_base").BaseType, qt.Equals, "text")
				c.Assert(findLiveDomain(c, applied.Domains, "d_over").BaseType, qt.Equals, "text")
			},
		},
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
			assertConverged: func(c *qt.C, applied *dbschematypes.DBSchema) {
				c.Assert(liveCompositeFieldTypes(c, applied.Composites, "c_base"), qt.DeepEquals, []string{"text"})
				c.Assert(liveCompositeFieldTypes(c, applied.Composites, "c_over"), qt.DeepEquals, []string{"text"})
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			desiredURL := newBoundaryDatabase(c, dsn, boundaryCase{
				name:  "user_type_one_kind_desired",
				seed:  test.desired,
				query: "search_path=public",
			})
			currentURL := newBoundaryDatabase(c, dsn, boundaryCase{
				name:  "user_type_one_kind_current",
				seed:  test.current,
				query: "search_path=public",
			})

			target, err := dbschema.ConnectToDatabase(c.Context(), currentURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(target) })

			plan, err := atlasschema.PrepareApply(c.Context(), target, atlasschema.ApplyRuntimeOptions{
				ToURLs: []string{desiredURL},
				TxMode: migrator.MigrationTxModeFile,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(plan.HasChanges(), qt.IsTrue)
			c.Assert(plan.Execute(c.Context()), qt.IsNil, qt.Commentf("emitted script:\n%s", plan.SQL()))

			applied, err := dbschema.ReadSchemaWithSchemas(target, nil)
			c.Assert(err, qt.IsNil)
			test.assertConverged(c, applied)

			settled, err := atlasschema.PrepareApply(c.Context(), target, atlasschema.ApplyRuntimeOptions{
				ToURLs: []string{desiredURL},
				TxMode: migrator.MigrationTxModeFile,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(settled.HasChanges(), qt.IsFalse, qt.Commentf("second plan:\n%s", settled.SQL()))
		})
	}
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
