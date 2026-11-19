package sqlite_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// legacyRename is the bracket around the drop and the rename of the rebuild of
// users, with the statements between.
const legacyRename = "PRAGMA legacy_alter_table = on;\n" +
	"CREATE TABLE \"new_users\" (\n  \"id\" INTEGER NOT NULL PRIMARY KEY,\n  \"email\" TEXT\n);\n" +
	"INSERT INTO \"new_users\" (\"id\", \"email\") SELECT \"id\", \"email\" FROM \"users\";\n" +
	"DROP TABLE \"users\";\n" +
	"ALTER TABLE \"new_users\" RENAME TO \"users\";\n" +
	"PRAGMA legacy_alter_table = off;"

// usersRebuildBeside is the rebuild of users that drops its column name, in a
// schema that also holds what dependents sets.
func usersRebuildBeside(dependents func(*difftypes.SchemaDiff)) *difftypes.SchemaDiff {
	diff := withDeclaredTable(usersRebuildCarrying(catalog.TableParts{}), carriedUsers(false))
	dependents(diff)
	return diff
}

// From SQLite 3.26 a table rename checks every view and trigger, so renaming
// the copy fails while a view or a trigger of another table names the table
// the rebuild just dropped. The rebuild of such a table renames with
// legacy_alter_table on, and only such a table: a rebuild nothing names keeps
// the shape the community binary writes.
func TestPlannerRebuildRenamesBesideWhatNamesTheTable_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		dependents func(*difftypes.SchemaDiff)
	}{
		{
			name: "a declared view that selects from the table",
			dependents: func(diff *difftypes.SchemaDiff) {
				diff.DeclaredViewLikes.Views = []schemamodel.View{{Name: "emails", Body: "SELECT id, email FROM users"}}
			},
		},
		{
			name: "a view out of scope that selects from the table",
			dependents: func(diff *difftypes.SchemaDiff) {
				diff.CurrentOutOfScope.Views = []catalog.View{{Name: "emails", Body: "SELECT email FROM users"}}
			},
		},
		{
			name: "a declared trigger of another table that writes the table",
			dependents: func(diff *difftypes.SchemaDiff) {
				diff.DeclaredTriggers = []schemamodel.Trigger{{Name: "log_touch", Table: "log",
					Body: "BEGIN UPDATE users SET email = NEW.msg WHERE id = NEW.id; END"}}
			},
		},
		{
			name: "a trigger of another table out of scope that writes the table",
			dependents: func(diff *difftypes.SchemaDiff) {
				diff.CurrentOutOfScope.Triggers = []catalog.Trigger{{Name: "log_touch", Table: "log",
					Definition: "CREATE TRIGGER log_touch AFTER INSERT ON log BEGIN DELETE FROM users WHERE id = NEW.id; END"}}
			},
		},
		{
			name: "a view that names the dropped column of another table only",
			dependents: func(diff *difftypes.SchemaDiff) {
				diff.DeclaredViewLikes.Views = []schemamodel.View{{Name: "names",
					Body: "SELECT log.name FROM log JOIN users ON users.id = log.id"}}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()), usersRebuildBeside(test.dependents), platform.SQLite)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, legacyRename)
		})
	}
}

// A rebuild that no view and no trigger of another table names renames as the
// community binary does, with no pragma around the rename: a view that reads
// another table, a comment or a string that spells the table's name, and a
// trigger of the table itself, which the drop takes and the plan creates again.
func TestPlannerRebuildRenamesAloneWhereNothingNamesTheTable(t *testing.T) {
	tests := []struct {
		name       string
		dependents func(*difftypes.SchemaDiff)
	}{
		{
			name: "a view that reads another table",
			dependents: func(diff *difftypes.SchemaDiff) {
				diff.DeclaredViewLikes.Views = []schemamodel.View{{Name: "names", Body: "SELECT name FROM log /* users */ WHERE name <> 'users'"}}
			},
		},
		{
			name: "a trigger of the table itself",
			dependents: func(diff *difftypes.SchemaDiff) {
				diff.DeclaredTriggers = []schemamodel.Trigger{{Name: "users_touch", Table: "users",
					Body: "BEGIN DELETE FROM users WHERE id = OLD.id; END"}}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()), usersRebuildBeside(test.dependents), platform.SQLite)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "INSERT INTO \"new_users\" (\"id\", \"email\") SELECT \"id\", \"email\" FROM \"users\";\n"+
				"DROP TABLE \"users\";\nALTER TABLE \"new_users\" RENAME TO \"users\";")
			c.Assert(sql, qt.Not(qt.Contains), "legacy_alter_table")
		})
	}
}

// A view or a trigger of another table that reads the column the rebuild drops
// would fail when it runs, once the rename no longer checks it. The plan
// refuses it by name.
func TestPlannerRebuildRenamesBesideWhatNamesTheTable_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		dependents func(*difftypes.SchemaDiff)
		want       string
	}{
		{
			name: "a declared view that selects the dropped column",
			dependents: func(diff *difftypes.SchemaDiff) {
				diff.DeclaredViewLikes.Views = []schemamodel.View{{Name: "names", Body: "SELECT id, name FROM users"}}
			},
			want: `sqlite: rebuilding table users would break view names: it reads column name, which the rebuilt table drops\. ` +
				`Keep the column, or change the view first`,
		},
		{
			name: "a view out of scope that reads the dropped column through an alias",
			dependents: func(diff *difftypes.SchemaDiff) {
				diff.CurrentOutOfScope.Views = []catalog.View{{Name: "names", Body: "SELECT u.name FROM log JOIN users AS u ON u.id = log.id"}}
			},
			want: `sqlite: rebuilding table users would break view names: it reads column name, .*`,
		},
		{
			name: "a trigger of another table out of scope that sets the dropped column",
			dependents: func(diff *difftypes.SchemaDiff) {
				diff.CurrentOutOfScope.Triggers = []catalog.Trigger{{Name: "log_touch", Table: "log",
					Definition: "CREATE TRIGGER log_touch AFTER INSERT ON log BEGIN UPDATE users SET name = NEW.msg; END"}}
			},
			want: `sqlite: rebuilding table users would break trigger log_touch: it reads column name, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()), usersRebuildBeside(test.dependents), platform.SQLite)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
