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
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// carriedUsers declares users(id, email), keyed on id unless keyless is set.
func carriedUsers(keyless bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
		Fields: []schemamodel.Field{
			{Name: "id", Type: "INTEGER", StructName: "User", Primary: !keyless, Nullable: keyless},
			{Name: "email", Type: "TEXT", StructName: "User", Nullable: true},
		},
	}
}

// usersRebuildCarrying is the plan that drops column name from users and so
// rebuilds the table, over a read that kept parts out of scope. The live
// columns are id, name, then email in the fourth place, which leaves the
// third to a column out of scope.
func usersRebuildCarrying(parts catalog.TableParts) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "users",
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "name"}},
			Current: schemacapture.TableObservation{
				Table: catalog.Table{Name: "users", Columns: []catalog.Column{
					{Name: "id", OrdinalPosition: 1},
					{Name: "name", OrdinalPosition: 2},
					{Name: "email", OrdinalPosition: 4},
				}},
			},
		}},
		CurrentOutOfScope: parts,
	}
}

// carriedColumn is a nullable TEXT column of users in the third place.
func carriedColumn(name string) catalog.Column {
	return catalog.Column{Name: name, ColumnType: "TEXT", IsNullable: "YES", OrdinalPosition: 3}
}

// carriedTable holds columns of users out of scope.
func carriedTable(columns ...catalog.Column) []catalog.Table {
	return []catalog.Table{{Name: "users", Columns: columns}}
}

// A rebuild writes the new table from the declaration, which an exclude
// selector left without the parts it keeps out of scope. Each part is carried
// into the new table from its live definition, so the rebuild keeps it.
func TestPlannerRebuildCarriesWhatAnExclusionKeeps_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		keyless bool
		parts   catalog.TableParts
		want    []string
		absent  string
	}{
		{
			name:  "a column, in its live place, with its rows and a literal default",
			parts: catalog.TableParts{Tables: carriedTable(withDefault(carriedColumn("extra"), "'x'"))},
			want: []string{
				`"id" INTEGER NOT NULL PRIMARY KEY,` + "\n" + `  "extra" TEXT DEFAULT 'x',` + "\n" + `  "email" TEXT`,
				`INSERT INTO "new_users" ("id", "email", "extra") SELECT "id", "email", "extra" FROM "users";`,
			},
			absent: `DEFAULT ('x')`,
		},
		{
			name:   "a column whose default is one double-quoted identifier, as the string SQLite reads it",
			parts:  catalog.TableParts{Tables: carriedTable(withDefault(carriedColumn("extra"), `"dq"`))},
			want:   []string{`"extra" TEXT DEFAULT 'dq'`},
			absent: `DEFAULT ("dq")`,
		},
		{
			name:   "a column whose default is one bracketed identifier, with a quote to double",
			parts:  catalog.TableParts{Tables: carriedTable(withDefault(carriedColumn("extra"), `[it's]`))},
			want:   []string{`"extra" TEXT DEFAULT 'it''s'`},
			absent: `DEFAULT ([it's])`,
		},
		{
			name:    "an AUTOINCREMENT column whose key is out of scope too, on its own key",
			keyless: true,
			parts: catalog.TableParts{
				Tables: carriedTable(autoIncrement(keyColumn(integerColumn(carriedColumn("serial"))))),
				Constraints: []catalog.Constraint{{
					TableName: "users", Name: "users_pkey", Type: "PRIMARY KEY", ColumnNames: []string{"serial"},
				}},
			},
			want:   []string{`"serial" INTEGER PRIMARY KEY AUTOINCREMENT`},
			absent: `PRIMARY KEY ("serial")`,
		},
		{
			name: "a trigger that writes a column of the dropped name into another table",
			parts: catalog.TableParts{Triggers: []catalog.Trigger{{
				Table: "users", Name: "users_log",
				Definition: `CREATE TRIGGER users_log AFTER DELETE ON users BEGIN INSERT INTO log(name) VALUES (OLD.email); END`,
			}}},
			want:   []string{`CREATE TRIGGER users_log AFTER DELETE ON users BEGIN INSERT INTO log(name) VALUES (OLD.email); END;`},
			absent: `DROP TRIGGER`,
		},
		{
			name: "a trigger that sets a column of the dropped name in another table from a query of its own",
			parts: catalog.TableParts{Triggers: []catalog.Trigger{{
				Table: "users", Name: "users_sync",
				Definition: `CREATE TRIGGER users_sync AFTER UPDATE ON users BEGIN ` +
					`UPDATE log SET name = (SELECT email FROM users WHERE id = NEW.id); END`,
			}}},
			want:   []string{`UPDATE log SET name = (SELECT email FROM users WHERE id = NEW.id); END;`},
			absent: `DROP TRIGGER`,
		},
		{
			name: "a trigger that names the dropped column in a comment, a string and after another table",
			parts: catalog.TableParts{Triggers: []catalog.Trigger{{
				Table: "users", Name: "users_note",
				Definition: "CREATE TRIGGER users_note AFTER DELETE ON users BEGIN -- name\n" +
					"UPDATE log SET name = 'name' WHERE log.name = OLD.email; END",
			}}},
			want:   []string{`UPDATE log SET name = 'name' WHERE log.name = OLD.email; END;`},
			absent: `DROP TRIGGER`,
		},
		{
			name:   "a column whose default is an expression",
			parts:  catalog.TableParts{Tables: carriedTable(withDefault(carriedColumn("extra"), "1+2"))},
			want:   []string{`"extra" TEXT DEFAULT (1+2)`},
			absent: `DEFAULT 1+2`,
		},
		{
			name:   "a column with a collation",
			parts:  catalog.TableParts{Tables: carriedTable(collated(carriedColumn("nick"), "NOCASE"))},
			want:   []string{`"nick" TEXT COLLATE NOCASE`},
			absent: `"nick" TEXT,`,
		},
		{
			name:   "a generated column, which the copy leaves to SQLite",
			parts:  catalog.TableParts{Tables: carriedTable(generated(carriedColumn("lowered"), "lower(email)"))},
			want:   []string{`"lowered" TEXT GENERATED ALWAYS AS (lower(email)) VIRTUAL`},
			absent: `"lowered" FROM`,
		},
		{
			name: "an index, created again from its stored statement",
			parts: catalog.TableParts{Indexes: []catalog.Index{{
				TableName: "users", Name: "users_email", Parts: []catalog.IndexPart{{Name: "email"}},
				Definition: `CREATE INDEX users_email ON users (email)`,
			}}},
			want:   []string{`ALTER TABLE "new_users" RENAME TO "users";` + "\n" + `CREATE INDEX users_email ON users (email);`},
			absent: `CREATE INDEX IF NOT EXISTS "users_email"`,
		},
		{
			name: "a trigger, created again from its stored statement",
			parts: catalog.TableParts{Triggers: []catalog.Trigger{{
				Table: "users", Name: "users_touch",
				Definition: `CREATE TRIGGER users_touch AFTER DELETE ON users BEGIN SELECT email FROM users; END`,
			}}},
			want:   []string{`CREATE TRIGGER users_touch AFTER DELETE ON users BEGIN SELECT email FROM users; END;`},
			absent: `DROP TRIGGER`,
		},
		{
			name: "a CHECK",
			parts: catalog.TableParts{Constraints: []catalog.Constraint{{
				TableName: "users", Name: "email_set", Type: "CHECK", CheckClause: new("email <> ''"),
			}}},
			want:   []string{`CONSTRAINT "email_set" CHECK (email <> '')`},
			absent: `CHECK ((`,
		},
		{
			name: "a foreign key",
			parts: catalog.TableParts{Constraints: []catalog.Constraint{{
				TableName: "users", Name: "users_team_fk", Type: "FOREIGN KEY", ColumnNames: []string{"email"},
				ForeignTable: new("teams"), ForeignColumns: []string{"email"}, DeleteRule: new("CASCADE"),
			}}},
			want:   []string{`CONSTRAINT "users_team_fk" FOREIGN KEY ("email") REFERENCES "teams" ("email") ON DELETE CASCADE`},
			absent: `ON UPDATE`,
		},
		{
			name: "a foreign key the table's statement left unnamed",
			parts: catalog.TableParts{Constraints: []catalog.Constraint{{
				TableName: "users", Name: "fk_users_email", Type: "FOREIGN KEY", ColumnNames: []string{"email"}, ColumnName: "email",
				ForeignTable: new("teams"), ForeignColumns: []string{"email"},
			}}},
			want:   []string{`  FOREIGN KEY ("email") REFERENCES "teams" ("email")`},
			absent: `fk_users_email`,
		},
		{
			name: "a UNIQUE the table's statement left unnamed",
			parts: catalog.TableParts{Constraints: []catalog.Constraint{{
				TableName: "users", Name: "sqlite_autoindex_users_1", Type: "UNIQUE", ColumnNames: []string{"email"},
			}}},
			want:   []string{`  UNIQUE ("email")`},
			absent: `sqlite_autoindex_users_1`,
		},
		{
			name:    "a named primary key the declaration leaves out",
			keyless: true,
			parts: catalog.TableParts{Constraints: []catalog.Constraint{{
				TableName: "users", Name: "users_pk", Type: "PRIMARY KEY", ColumnNames: []string{"id"},
			}}},
			want:   []string{`CONSTRAINT "users_pk" PRIMARY KEY ("id")`},
			absent: `INTEGER PRIMARY KEY`,
		},
		{
			name:    "a primary key the table's statement left unnamed",
			keyless: true,
			parts: catalog.TableParts{Constraints: []catalog.Constraint{{
				TableName: "users", Name: "users_pkey", Type: "PRIMARY KEY", ColumnNames: []string{"id"},
			}}},
			want:   []string{`  PRIMARY KEY ("id")`},
			absent: `users_pkey`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := withDeclaredTable(usersRebuildCarrying(test.parts), carriedUsers(test.keyless))

			sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()), diff, platform.SQLite)

			c.Assert(err, qt.IsNil)
			for _, want := range test.want {
				c.Assert(sql, qt.Contains, want)
			}
			c.Assert(sql, qt.Not(qt.Contains), test.absent)
		})
	}
}

// Where a part cannot be carried as it stands, the rebuild would lose it or
// change what it does, so the plan refuses by name and says what to do.
func TestPlannerRebuildCarriesWhatAnExclusionKeeps_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		parts catalog.TableParts
		want  string
	}{
		{
			name: "a primary key beside the declared one",
			parts: catalog.TableParts{Constraints: []catalog.Constraint{{
				TableName: "users", Name: "users_pk", Type: "PRIMARY KEY", ColumnNames: []string{"id"},
			}}},
			want: `sqlite: rebuilding table users cannot keep primary key users_pk as it is: .* takes its key from the declaration\. .*`,
		},
		{
			name:  "a column of the live key beside the declared one",
			parts: catalog.TableParts{Tables: carriedTable(keyColumn(carriedColumn("code")))},
			want:  `sqlite: rebuilding table users cannot keep column code as it is: .* part of the primary key, .*`,
		},
		{
			name:  "a column with AUTOINCREMENT",
			parts: catalog.TableParts{Tables: carriedTable(autoIncrement(carriedColumn("serial")))},
			want:  `sqlite: rebuilding table users cannot keep column serial as it is: .* AUTOINCREMENT, .*`,
		},
		{
			name: "a UNIQUE over the column the plan drops",
			parts: catalog.TableParts{Constraints: []catalog.Constraint{{
				TableName: "users", Name: "users_name_key", Type: "UNIQUE", ColumnNames: []string{"name"},
			}}},
			want: `sqlite: rebuilding table users cannot keep constraint users_name_key as it is: .* has no column name\. .*`,
		},
		{
			name: "a CHECK that reads the column the plan drops",
			parts: catalog.TableParts{Constraints: []catalog.Constraint{{
				TableName: "users", Name: "name_set", Type: "CHECK", CheckClause: new(`"name" <> ''`),
			}}},
			want: `sqlite: rebuilding table users cannot keep constraint name_set as it is: .* reads column name, which the rebuilt table drops\. .*`,
		},
		{
			name: "a trigger that names the column the plan drops",
			parts: catalog.TableParts{Triggers: []catalog.Trigger{{
				Table: "users", Name: "users_touch",
				Definition: `CREATE TRIGGER users_touch AFTER DELETE ON users BEGIN SELECT name FROM users; END`,
			}}},
			want: `sqlite: rebuilding table users cannot keep trigger users_touch as it is: .* reads column name, which the rebuilt table drops\. .*`,
		},
		{
			name: "a trigger that reads the dropped column of the row it fires on",
			parts: catalog.TableParts{Triggers: []catalog.Trigger{{
				Table: "users", Name: "users_touch",
				Definition: `CREATE TRIGGER users_touch AFTER UPDATE ON users BEGIN INSERT INTO log(msg) VALUES (NEW.name); END`,
			}}},
			want: `sqlite: rebuilding table users cannot keep trigger users_touch as it is: .* reads column name, which the rebuilt table drops\. .*`,
		},
		{
			name: "a trigger that reads the dropped column qualified by its table",
			parts: catalog.TableParts{Triggers: []catalog.Trigger{{
				Table: "users", Name: "users_touch",
				Definition: `CREATE TRIGGER users_touch AFTER DELETE ON users BEGIN UPDATE log SET msg = (SELECT users.name FROM users); END`,
			}}},
			want: `sqlite: rebuilding table users cannot keep trigger users_touch as it is: .* reads column name, which the rebuilt table drops\. .*`,
		},
		{
			name: "a trigger that fires on an update of the dropped column",
			parts: catalog.TableParts{Triggers: []catalog.Trigger{{
				Table: "users", Name: "users_touch",
				Definition: `CREATE TRIGGER users_touch AFTER UPDATE OF name ON users BEGIN SELECT 1; END`,
			}}},
			want: `sqlite: rebuilding table users cannot keep trigger users_touch as it is: .* reads column name, which the rebuilt table drops\. .*`,
		},
		{
			name:  "a generated column that reads the dropped column",
			parts: catalog.TableParts{Tables: carriedTable(generated(carriedColumn("lowered"), "lower(name)"))},
			want:  `sqlite: rebuilding table users cannot keep column lowered as it is: .* generated expression reads column name, which the rebuilt table drops\. .*`,
		},
		{
			name: "an index whose key expression reads the dropped column",
			parts: catalog.TableParts{Indexes: []catalog.Index{{
				TableName: "users", Name: "users_lower", Parts: []catalog.IndexPart{{Expr: "lower(name)"}},
				Definition: `CREATE INDEX users_lower ON users (lower(name))`,
			}}},
			want: `sqlite: rebuilding table users cannot keep index users_lower: its statement reads column name, which the rebuilt table drops\. .*`,
		},
		{
			name: "a partial index whose WHERE clause reads the dropped column",
			parts: catalog.TableParts{Indexes: []catalog.Index{{
				TableName: "users", Name: "users_email", Parts: []catalog.IndexPart{{Name: "email"}},
				Definition: `CREATE INDEX users_email ON users (email) WHERE name IS NOT NULL`,
			}}},
			want: `sqlite: rebuilding table users cannot keep index users_email: its statement reads column name, which the rebuilt table drops\. .*`,
		},
		{
			name:  "a trigger the read kept no statement for",
			parts: catalog.TableParts{Triggers: []catalog.Trigger{{Table: "users", Name: "users_touch"}}},
			want:  `sqlite: rebuilding table users cannot keep trigger users_touch as it is: .* no statement to create it again from\. .*`,
		},
		{
			name: "an index over the column the plan drops",
			parts: catalog.TableParts{Indexes: []catalog.Index{{
				TableName: "users", Name: "users_name", Parts: []catalog.IndexPart{{Name: "name"}},
				Definition: `CREATE INDEX users_name ON users (name)`,
			}}},
			want: `sqlite: rebuilding table users cannot keep index users_name: the rebuilt table has no column name\. .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := withDeclaredTable(usersRebuildCarrying(test.parts), carriedUsers(false))

			sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()), diff, platform.SQLite)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

func withDefault(column catalog.Column, value string) catalog.Column {
	column.ColumnDefault = &value
	return column
}

func generated(column catalog.Column, expression string) catalog.Column {
	column.GeneratedExpression = &expression
	column.GeneratedKind = "VIRTUAL"
	return column
}

func integerColumn(column catalog.Column) catalog.Column {
	column.ColumnType = "INTEGER"
	return column
}

func keyColumn(column catalog.Column) catalog.Column {
	column.IsPrimaryKey = true
	return column
}

func autoIncrement(column catalog.Column) catalog.Column {
	column.IsAutoIncrement = true
	return column
}

func collated(column catalog.Column, collation string) catalog.Column {
	column.Collate = collation
	return column
}
