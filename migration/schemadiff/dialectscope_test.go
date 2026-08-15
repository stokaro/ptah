package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// scopedDesiredState declares one PostgreSQL-only function and one role, both
// scoped to postgres, against an empty database.
func scopedDesiredState() *goschema.Database {
	return &goschema.Database{
		Functions: []goschema.Function{{
			StructName: "Fn",
			Name:       "get_current_tenant_id",
			Returns:    "TEXT",
			Language:   "plpgsql",
			Body:       "BEGIN RETURN 'x'; END;",
			Dialects:   []string{"postgres"},
		}},
		Roles: []goschema.Role{
			{StructName: "Rol", Name: "app_reader", Inherit: true, Dialects: []string{"postgres"}},
		},
	}
}

// TestCompare_AScopedObjectIsNotReportedAsAddedOnATargetItDoesNotName is the
// convergence guarantee, stated where convergence is decided.
//
// The comparator is what made a scoped-away object a permanent diff: the object
// was in the desired state, never in the database, and therefore in
// FunctionsAdded on every run. Measured on MariaDB 12.3.2 before this existed,
// `schema apply` exited 0 having created nothing and the very next
// `schema compare --exit-code` returned 1 naming the same object, forever.
func TestCompare_AScopedObjectIsNotReportedAsAddedOnATargetItDoesNotName(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		added   int
	}{
		{name: "the named dialect still sees the object", dialect: "postgres", added: 1},
		{name: "an accepted spelling of the named dialect", dialect: "postgresql", added: 1},
		{name: "mysql", dialect: "mysql", added: 0},
		{name: "mariadb", dialect: "mariadb", added: 0},
		{name: "sqlite", dialect: "sqlite", added: 0},
		{name: "a postgres-family target the scope did not name", dialect: "yugabytedb", added: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(scopedDesiredState(), &types.DBSchema{}, test.dialect)

			c.Assert(diff.FunctionsAdded, qt.HasLen, test.added)
			c.Assert(diff.RolesAdded, qt.HasLen, test.added)
		})
	}
}

// TestCompare_AnUnscopedObjectIsStillReportedAsAdded is the compatibility
// control. Without it, a projection that emptied every desired state would pass
// the table above on five of its six rows.
func TestCompare_AnUnscopedObjectIsStillReportedAsAdded(t *testing.T) {
	c := qt.New(t)

	unscoped := scopedDesiredState()
	unscoped.Functions[0].Dialects = nil
	unscoped.Roles[0].Dialects = nil

	diff := schemadiff.CompareWithDialect(unscoped, &types.DBSchema{}, "mariadb")

	c.Assert(diff.FunctionsAdded, qt.HasLen, 1)
	c.Assert(diff.RolesAdded, qt.HasLen, 1)
}

// TestCompare_ADialectlessComparisonKeepsEveryScopedObject pins the one
// comparison that cannot project: with no dialect there is no target to measure
// a scope against, and dropping objects on a guess would report a synced schema
// for work nobody decided to skip.
func TestCompare_ADialectlessComparisonKeepsEveryScopedObject(t *testing.T) {
	c := qt.New(t)

	opts := config.DefaultCompareOptions()
	opts.Dialect = ""

	diff := schemadiff.CompareWithOptions(scopedDesiredState(), &types.DBSchema{}, opts)

	c.Assert(diff.FunctionsAdded, qt.HasLen, 1)
	c.Assert(diff.RolesAdded, qt.HasLen, 1)
}

// TestCompare_AScopedObjectAlreadyInTheDatabaseIsNotPlannedForRemoval is the
// other half of the convergence guarantee, and the half that can destroy data.
//
// Projecting the scope onto the desired state alone leaves the current side
// still holding the object, so the comparison reads it as present in the
// database and absent from the declaration -- which is exactly the shape of a
// drop. A function declared `dialects="mysql"` that already exists in a
// PostgreSQL target therefore lands in FunctionsRemoved, and `schema apply`
// plans a DROP FUNCTION for an object the feature promised not to compare.
//
// The scope says the declaration does not describe that target. It does not say
// the target should not have the object. An object outside the scope is not
// compared in either direction.
func TestCompare_AScopedObjectAlreadyInTheDatabaseIsNotPlannedForRemoval(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "a target the scope does not name", dialect: "postgres"},
		{name: "an accepted spelling of a target the scope does not name", dialect: "postgresql"},
		{name: "another target the scope does not name", dialect: "sqlite"},
		{name: "the named dialect, where both sides hold it", dialect: "mysql"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(
				scopedFunctionDeclaredFor("mysql"),
				databaseHoldingTheScopedFunction(),
				test.dialect,
			)

			c.Assert(diff.FunctionsRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompare_AnUndeclaredObjectIsStillRemovedOnAScopedTarget is the control
// that keeps the guarantee above from being read as "never plan a removal".
//
// The database holds a function no declaration mentions at all, on a target
// where scoping is active. That is an ordinary drop and must stay one: a fix
// that suppressed the current side wholesale would pass every row above and
// quietly stop removing anything.
func TestCompare_AnUndeclaredObjectIsStillRemovedOnAScopedTarget(t *testing.T) {
	c := qt.New(t)

	desired := scopedFunctionDeclaredFor("mysql")
	desired.Functions[0].Name = "something_else_entirely"

	diff := schemadiff.CompareWithDialect(desired, databaseHoldingTheScopedFunction(), "postgres")

	c.Assert(diff.FunctionsRemoved, qt.HasLen, 1)
}

// scopedFunctionDeclaredFor declares one function scoped to dialect.
func scopedFunctionDeclaredFor(dialect string) *goschema.Database {
	return &goschema.Database{
		Functions: []goschema.Function{{
			StructName: "Fn",
			Name:       "get_current_tenant_id",
			Returns:    "TEXT",
			Language:   "plpgsql",
			Body:       "BEGIN RETURN 'x'; END;",
			Dialects:   []string{dialect},
		}},
	}
}

// databaseHoldingTheScopedFunction is a target that already has it.
func databaseHoldingTheScopedFunction() *types.DBSchema {
	return &types.DBSchema{
		Functions: []types.DBFunction{{Name: "get_current_tenant_id"}},
	}
}

// TestCompare_AScopedAwayNameDoesNotSuppressAnotherSchemasObject keeps the
// suppression from reaching past the object it is about.
//
// A declaration spells a sequence as schema.name and a reader blanks the schema
// for the connection's own, so the two sides carry different strings for one
// object and the match has to look at the unqualified halves. Doing only that
// lets a scoped-away app.seq suppress an unrelated other.seq -- a drop that goes
// missing, which is the failure this whole area exists to prevent, arrived at
// from the other side.
func TestCompare_AScopedAwayNameDoesNotSuppressAnotherSchemasObject(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		removed int
	}{
		{name: "the same schema is the same object", schema: "app", removed: 0},
		{name: "a blank schema is the connection's own and still matches", schema: "", removed: 0},
		{name: "another schema is another object", schema: "other", removed: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			desired := &goschema.Database{
				Sequences: []goschema.Sequence{{
					StructName: "Seq",
					Name:       "tenant_seq",
					Schema:     "app",
					Dialects:   []string{"mysql"},
				}},
			}
			current := &types.DBSchema{
				Sequences: []types.DBSequence{{Name: "tenant_seq", Schema: test.schema}},
			}

			diff := schemadiff.CompareWithDialect(desired, current, "postgres")

			c.Assert(diff.SequencesRemoved, qt.HasLen, test.removed)
		})
	}
}
