package sqlite_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// rebuiltUsers declares users(id, email), with an index of its own, for a plan
// that drops the column name and so rebuilds the table.
func rebuiltUsers() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
		Fields: []schemamodel.Field{
			{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
			{Name: "email", Type: "TEXT", StructName: "User"},
		},
		Indexes: []schemamodel.Index{{Name: "users_declared", StructName: "User", Fields: []string{"email"}}},
	}
}

// usersRebuildObserving is the plan for [rebuiltUsers] over a table holding the
// given indexes.
func usersRebuildObserving(indexes ...catalog.Index) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "users",
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "name"}},
			Current: schemacapture.TableObservation{
				Table:   catalog.Table{Name: "users"},
				Indexes: indexes,
			},
		}},
	}
}

// DROP TABLE takes every index of the old table with it, so an index the plan
// leaves in place has to be created again, in the catalog's own words. The
// other observed indexes each have somebody else answering for them.
func TestPlannerRebuildPutsBackAnIndexThePlanKeeps_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := usersRebuildObserving(
		catalog.Index{
			Name:       "users_kept",
			Parts:      []catalog.IndexPart{{Name: "email"}},
			Definition: `CREATE INDEX users_kept ON users (email COLLATE NOCASE) WHERE email IS NOT NULL`,
		},
		catalog.Index{
			Name:       "users_expression",
			Parts:      []catalog.IndexPart{{Expr: "lower(name)"}},
			Definition: `CREATE INDEX users_expression ON users (lower(email))`,
		},
		catalog.Index{Name: "users_declared", Parts: []catalog.IndexPart{{Name: "name"}}, Definition: `CREATE INDEX users_declared ON users (name)`},
		catalog.Index{Name: "users_removed", Parts: []catalog.IndexPart{{Name: "name"}}, Definition: `CREATE INDEX users_removed ON users (name)`},
		catalog.Index{Name: "sqlite_autoindex_users_1", Parts: []catalog.IndexPart{{Name: "email"}}, IsUnique: true},
	)
	diff.SetIndexRemovals([]difftypes.IndexRef{{Name: "users_removed", TableName: "users"}})

	sql, err := planner.GenerateSchemaDiffSQL(
		context.Background(), must.Must(builtin.New()),
		withDeclaredTable(diff, rebuiltUsers()), platform.SQLite,
	)

	c.Assert(err, qt.IsNil)
	rename := strings.Index(sql, `ALTER TABLE "new_users" RENAME TO "users";`)
	kept := strings.Index(sql, `CREATE INDEX users_kept ON users (email COLLATE NOCASE) WHERE email IS NOT NULL;`)
	c.Assert(rename >= 0, qt.IsTrue, qt.Commentf("plan:\n%s", sql))
	c.Assert(kept > rename, qt.IsTrue, qt.Commentf("plan:\n%s", sql))
	c.Assert(sql, qt.Contains, `CREATE INDEX users_expression ON users (lower(email));`)
	c.Assert(sql, qt.Contains, `CREATE INDEX IF NOT EXISTS "users_declared" ON "users" ("email");`)
	c.Assert(sql, qt.Not(qt.Contains), `CREATE INDEX users_declared`)
	c.Assert(sql, qt.Not(qt.Contains), `users_removed`)
	c.Assert(sql, qt.Not(qt.Contains), `sqlite_autoindex_users_1`)
}

// A table reached only through a constraint change has no modification to hold
// its observed side; the constraint host does.
func TestPlannerConstraintRebuildPutsBackAnIndexThePlanKeeps_HappyPath(t *testing.T) {
	c := qt.New(t)
	desired := rebuiltUsers()
	desired.Constraints = []schemamodel.Constraint{{
		Name: "email_set", Type: "CHECK", StructName: "User", Table: "users", CheckExpression: "email <> ''",
	}}
	diff := &difftypes.SchemaDiff{
		ConstraintsAdded: []difftypes.ConstraintAdditionInfo{{
			Name: "email_set", TableName: "users", Type: "CHECK", CheckExpression: "email <> ''",
		}},
		ObservedConstraintHosts: []schemacapture.TableObservation{{
			Table: catalog.Table{Name: "users"},
			Indexes: []catalog.Index{{
				Name:       "users_kept",
				Parts:      []catalog.IndexPart{{Name: "email"}},
				Definition: `CREATE INDEX users_kept ON users (email)`,
			}},
		}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(
		context.Background(), must.Must(builtin.New()),
		withDeclaredTable(diff, desired), platform.SQLite,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TABLE "new_users"`)
	c.Assert(sql, qt.Contains, `CREATE INDEX users_kept ON users (email);`)
}

// The rebuilt table has no column for the index to cover, so the statement
// could only fail after the rows were copied. Keeping the index is what the plan
// says it does, so saying nothing and letting it go is the one wrong answer.
func TestPlannerRebuildPutsBackAnIndexThePlanKeeps_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := usersRebuildObserving(catalog.Index{
		Name:       "users_name",
		Parts:      []catalog.IndexPart{{Name: "NAME"}},
		Definition: `CREATE INDEX users_name ON users (NAME)`,
	})

	sql, err := planner.GenerateSchemaDiffSQL(
		context.Background(), must.Must(builtin.New()),
		withDeclaredTable(diff, rebuiltUsers()), platform.SQLite,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `sqlite: rebuilding table users cannot keep index users_name: the rebuilt table has no column NAME\. .*`)
	c.Assert(sql, qt.Equals, "")
}
