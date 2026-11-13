package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// The steps of a SQLite table replacement, under any table name.
const (
	replaceUsers = "CREATE TABLE new_users (id integer NOT NULL, PRIMARY KEY (id));\n" +
		"INSERT INTO new_users (id) SELECT id FROM users;\n" +
		"DROP TABLE users;\n" +
		"ALTER TABLE new_users RENAME TO users;\n"
	replacePets = "CREATE TABLE new_pets (id integer NOT NULL, PRIMARY KEY (id));\n" +
		"INSERT INTO new_pets (id) SELECT id FROM pets;\n" +
		"DROP TABLE pets;\n" +
		"ALTER TABLE new_pets RENAME TO pets;\n"
)

// TestAnalyzeFS_AtlasSQLiteReplacementIsOneChange pins the schema changes of a
// SQLite table replacement on the Atlas surface to what the pinned community
// binary v1.3.0 counts. Its SQLite analyzer folds `CREATE TABLE new_t`, the
// next statement, `DROP TABLE t` and `ALTER TABLE new_t RENAME TO t` into one
// change to t. Each row's count is that binary's summary on the same version.
//
// The rows that keep four changes are the analyzer's own limits. It compares
// names exactly as the server spells them: the `new_` prefix and the rename
// target as written, and the old table as the dev database reports it, so
// `users_tmp`, `NEW_users`, `RENAME TO USERS` and a table the server spells
// `Users` are not folded. The native profile and an engine whose descriptor
// declares no fold keep the statements' changes too. Without the fold every
// row that wants one modify change counts four or more.
func TestAnalyzeFS_AtlasSQLiteReplacementIsOneChange(t *testing.T) {
	tests := []struct {
		name          string
		dialect       string
		compatibility lint.CompatibilityProfile
		sql           string
		// baseline is the users table as the dev database spells it, empty
		// for a run without one.
		baseline []lintext.BaselineColumn
		want     []changeProjection
	}{
		{
			name:          "a replacement between foreign key pragmas",
			dialect:       "sqlite",
			compatibility: lint.CompatibilityProfileAtlas,
			sql:           "PRAGMA foreign_keys = off;\n" + replaceUsers + "PRAGMA foreign_keys = on;\n",
			want:          []changeProjection{{Kind: lint.SchemaChangeModify, Object: "users"}},
		},
		{
			name:          "quoted names",
			dialect:       "sqlite",
			compatibility: lint.CompatibilityProfileAtlas,
			sql: "CREATE TABLE `new_users` (`id` integer NOT NULL, PRIMARY KEY (`id`));\n" +
				"INSERT INTO `new_users` (`id`) SELECT `id` FROM `users`;\n" +
				"DROP TABLE `users`;\n" +
				"ALTER TABLE `new_users` RENAME TO `users`;\n",
			// The object is spelled as the DROP TABLE spells it, as every
			// change in the model is.
			want: []changeProjection{{Kind: lint.SchemaChangeModify, Object: "`users`"}},
		},
		{
			name:          "any statement between the create and the drop",
			dialect:       "sqlite",
			compatibility: lint.CompatibilityProfileAtlas,
			sql: "CREATE TABLE new_users (id integer NOT NULL, PRIMARY KEY (id));\n" +
				"CREATE TABLE side (id int);\n" +
				"DROP TABLE users;\n" +
				"ALTER TABLE new_users RENAME TO users;\n",
			want: []changeProjection{{Kind: lint.SchemaChangeModify, Object: "users"}},
		},
		{
			name:          "two replacements",
			dialect:       "sqlite",
			compatibility: lint.CompatibilityProfileAtlas,
			sql:           replaceUsers + replacePets,
			want: []changeProjection{
				{Kind: lint.SchemaChangeModify, Object: "users"},
				{Kind: lint.SchemaChangeModify, Object: "pets"},
			},
		},
		{
			name:          "a replacement followed by another change",
			dialect:       "sqlite",
			compatibility: lint.CompatibilityProfileAtlas,
			sql:           replaceUsers + "CREATE TABLE extra (id int);\n",
			want: []changeProjection{
				{Kind: lint.SchemaChangeModify, Object: "users"},
				{Kind: lint.SchemaChangeAdd, Object: "extra"},
			},
		},
		{
			name:          "schema-qualified names",
			dialect:       "sqlite",
			compatibility: lint.CompatibilityProfileAtlas,
			sql: "CREATE TABLE main.new_users (id integer NOT NULL, PRIMARY KEY (id));\n" +
				"INSERT INTO main.new_users (id) SELECT id FROM main.users;\n" +
				"DROP TABLE main.users;\n" +
				"ALTER TABLE main.new_users RENAME TO users;\n",
			want: []changeProjection{{Kind: lint.SchemaChangeModify, Object: "main.users"}},
		},
		{
			name:          "the copy renamed through another spelling of its name",
			dialect:       "sqlite",
			compatibility: lint.CompatibilityProfileAtlas,
			sql: "CREATE TABLE new_users (id integer NOT NULL, PRIMARY KEY (id));\n" +
				"INSERT INTO new_users (id) SELECT id FROM users;\n" +
				"DROP TABLE users;\n" +
				"ALTER TABLE NEW_USERS RENAME TO users;\n",
			want: []changeProjection{{Kind: lint.SchemaChangeModify, Object: "users"}},
		},
		{
			name:          "the old table dropped through another spelling of the name the server reports",
			dialect:       "sqlite",
			compatibility: lint.CompatibilityProfileAtlas,
			sql: "CREATE TABLE new_users (id integer NOT NULL, PRIMARY KEY (id));\n" +
				"INSERT INTO new_users (id) SELECT id FROM users;\n" +
				"DROP TABLE Users;\n" +
				"ALTER TABLE new_users RENAME TO users;\n",
			baseline: []lintext.BaselineColumn{{Version: 1, Table: "users", Name: "id"}},
			want:     []changeProjection{{Kind: lint.SchemaChangeModify, Object: "Users"}},
		},
		{
			name:          "the old table spelled differently by the server",
			dialect:       "sqlite",
			compatibility: lint.CompatibilityProfileAtlas,
			sql:           replaceUsers,
			baseline:      []lintext.BaselineColumn{{Version: 1, Table: "Users", Name: "id"}},
			want: []changeProjection{
				{Kind: lint.SchemaChangeAdd, Object: "new_users"},
				{Kind: lint.SchemaChangeDrop, Object: "users"},
				{Kind: lint.SchemaChangeDrop, Object: "new_users"},
				{Kind: lint.SchemaChangeAdd, Object: "users"},
			},
		},
		{
			name:          "the copy renamed to another spelling",
			dialect:       "sqlite",
			compatibility: lint.CompatibilityProfileAtlas,
			sql: "CREATE TABLE new_users (id integer NOT NULL, PRIMARY KEY (id));\n" +
				"INSERT INTO new_users (id) SELECT id FROM users;\n" +
				"DROP TABLE users;\n" +
				"ALTER TABLE new_users RENAME TO USERS;\n",
			want: []changeProjection{
				{Kind: lint.SchemaChangeAdd, Object: "new_users"},
				{Kind: lint.SchemaChangeDrop, Object: "users"},
				{Kind: lint.SchemaChangeDrop, Object: "new_users"},
				{Kind: lint.SchemaChangeAdd, Object: "USERS"},
			},
		},
		{
			name:          "a copy under another name",
			dialect:       "sqlite",
			compatibility: lint.CompatibilityProfileAtlas,
			sql: "CREATE TABLE users_tmp (id integer NOT NULL, PRIMARY KEY (id));\n" +
				"INSERT INTO users_tmp (id) SELECT id FROM users;\n" +
				"DROP TABLE users;\n" +
				"ALTER TABLE users_tmp RENAME TO users;\n",
			want: []changeProjection{
				{Kind: lint.SchemaChangeAdd, Object: "users_tmp"},
				{Kind: lint.SchemaChangeDrop, Object: "users"},
				{Kind: lint.SchemaChangeDrop, Object: "users_tmp"},
				{Kind: lint.SchemaChangeAdd, Object: "users"},
			},
		},
		{
			name:          "the prefix spelled in capitals",
			dialect:       "sqlite",
			compatibility: lint.CompatibilityProfileAtlas,
			sql: "CREATE TABLE NEW_users (id integer NOT NULL, PRIMARY KEY (id));\n" +
				"INSERT INTO NEW_users (id) SELECT id FROM users;\n" +
				"DROP TABLE users;\n" +
				"ALTER TABLE NEW_users RENAME TO users;\n",
			want: []changeProjection{
				{Kind: lint.SchemaChangeAdd, Object: "NEW_users"},
				{Kind: lint.SchemaChangeDrop, Object: "users"},
				{Kind: lint.SchemaChangeDrop, Object: "NEW_users"},
				{Kind: lint.SchemaChangeAdd, Object: "users"},
			},
		},
		{
			name:          "the native profile",
			dialect:       "sqlite",
			compatibility: lint.CompatibilityProfileNative,
			sql:           replaceUsers,
			want: []changeProjection{
				{Kind: lint.SchemaChangeAdd, Object: "new_users"},
				{Kind: lint.SchemaChangeDrop, Object: "users"},
				{Kind: lint.SchemaChangeDrop, Object: "new_users"},
				{Kind: lint.SchemaChangeAdd, Object: "users"},
			},
		},
		{
			name:          "an engine that declares no fold",
			dialect:       "postgres",
			compatibility: lint.CompatibilityProfileAtlas,
			sql:           replaceUsers,
			want: []changeProjection{
				{Kind: lint.SchemaChangeAdd, Object: "new_users"},
				{Kind: lint.SchemaChangeDrop, Object: "users"},
				{Kind: lint.SchemaChangeDrop, Object: "new_users"},
				{Kind: lint.SchemaChangeAdd, Object: "users"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(),
				fixture(map[string]string{"1_replace.sql": test.sql}),
				lint.Options{
					Compatibility: test.compatibility,
					DirFormat:     migrationfile.DirFormatAtlas,
					Dialect:       test.dialect,
					Baseline:      test.baseline,
				},
			)
			c.Assert(err, qt.IsNil)
			c.Assert(projectChanges(fileByName(c, analysis, "1_replace.sql").Changes), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_AtlasSQLiteReplacementKeepsTheCreateLine pins where the folded
// change sits: the CREATE TABLE statement, as the community binary places it.
func TestAnalyzeFS_AtlasSQLiteReplacementKeepsTheCreateLine(t *testing.T) {
	c := qt.New(t)
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(),
		fixture(map[string]string{"1_replace.sql": "PRAGMA foreign_keys = off;\n" + replaceUsers}),
		lint.Options{
			Compatibility: lint.CompatibilityProfileAtlas,
			DirFormat:     migrationfile.DirFormatAtlas,
			Dialect:       "sqlite",
		},
	)
	c.Assert(err, qt.IsNil)
	changes := fileByName(c, analysis, "1_replace.sql").Changes
	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].StatementIndex, qt.Equals, 1)
	c.Assert(changes[0].Line, qt.Equals, 2)
}
