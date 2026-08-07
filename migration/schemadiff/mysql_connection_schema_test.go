package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompareWithDatabaseInfo_MySQLConnectionSchemaMatchesTheDesiredSchema pins
// that constraint comparison honors the default schema the connection resolved
// rather than the one the dialect name implies (stokaro/ptah#1244).
//
// MySQL and MariaDB are the dialects where those two differ. A schema there is
// a database, so [identifier.ForDialect] has no static default to return and
// returns an empty one; the connected database name is the only answer, and
// only a connection has it. Constraint comparison used to rebuild the offline
// rules from the dialect string, so it saw the empty default no matter what the
// connection had resolved. The catalog reports a table with no schema and a
// desired state written as Atlas HCL carries `schema = schema.<database>`, so
// the two keys never met and every constraint the database had was reported as
// removed.
//
// Measured on live MySQL 9.7.1: `ptah-compat schema apply` fed the database's
// own unedited `atlas schema inspect` output planned CREATE TABLE and DROP
// TABLE for every table, plus DROP PRIMARY KEY and DROP FOREIGN KEY, where the
// pinned Atlas community binary v1.3.0 answered "Schema is synced, no changes
// to be made" and exited 0.
//
// The rows with a non-empty wantRemoved are the control and they carry the
// weight. A "fix" that stopped reporting constraint removals, or that collapsed
// every database name onto one key, satisfies every no-change row here and
// fails these.
func TestCompareWithDatabaseInfo_MySQLConnectionSchemaMatchesTheDesiredSchema(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		connSchema  string
		genSchema   string
		dbSchema    string
		dbTableName string
		wantRemoved []string
	}{
		{
			name:        "mysql reports the schema as nothing where the desired side names the database",
			dialect:     "mysql",
			connSchema:  "shop",
			genSchema:   "shop",
			dbSchema:    "",
			dbTableName: "users",
		},
		{
			name:        "mariadb reports the schema as nothing where the desired side names the database",
			dialect:     "mariadb",
			connSchema:  "shop",
			genSchema:   "shop",
			dbSchema:    "",
			dbTableName: "users",
		},
		{
			// The fill-in has to work in this direction too: a desired state
			// built from Go annotations names no schema, while a reader that
			// does report the database must not drift against it.
			name:        "the database naming it and the desired side not is unaffected",
			dialect:     "mysql",
			connSchema:  "shop",
			genSchema:   "",
			dbSchema:    "shop",
			dbTableName: "users",
		},
		{
			name:        "both sides naming the database is unaffected",
			dialect:     "mysql",
			connSchema:  "shop",
			genSchema:   "shop",
			dbSchema:    "shop",
			dbTableName: "users",
		},
		{
			name:        "neither side naming it is unaffected",
			dialect:     "mysql",
			connSchema:  "shop",
			genSchema:   "",
			dbSchema:    "",
			dbTableName: "users",
		},
		{
			// The control. A constraint on a table the desired schema genuinely
			// does not have must still be reported, or idempotency has been
			// bought by making removal impossible.
			name:        "a constraint on a table the desired schema does not have is still removed",
			dialect:     "mysql",
			connSchema:  "shop",
			genSchema:   "shop",
			dbSchema:    "",
			dbTableName: "orphaned",
			wantRemoved: []string{"orphaned_pkey"},
		},
		{
			// The other control. Another database is another table, and the
			// fill-in must not collapse them onto one key.
			name:        "a table in another database is not the desired one",
			dialect:     "mysql",
			connSchema:  "shop",
			genSchema:   "shop",
			dbSchema:    "reporting",
			dbTableName: "users",
			wantRemoved: []string{"users_pkey"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				implicitSchemaDesired(test.genSchema),
				implicitSchemaDatabase(test.dbSchema, test.dbTableName),
				mysqlConnectionInfo(test.dialect, test.connSchema),
				nil,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, test.wantRemoved,
				qt.Commentf("diff: %#v", diff))
		})
	}
}

// TestCompareWithDatabaseInfo_MySQLConnectionSchemaPlansNothingAtAll is the
// assertion a partial fix cannot satisfy.
//
// Supplying the connection's default schema without also letting constraint
// comparison see it removed the CREATE TABLE / DROP TABLE churn and left
// `ALTER TABLE users DROP PRIMARY KEY` behind — the same silent-corruption
// shape #1232 describes on PostgreSQL, reached from a different direction. So
// the assertion is that the comparison reports no change at all, not merely
// that the tables match.
func TestCompareWithDatabaseInfo_MySQLConnectionSchemaPlansNothingAtAll(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				implicitSchemaDesired("shop"),
				implicitSchemaDatabase("", "users"),
				mysqlConnectionInfo(test.dialect, "shop"),
				nil,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
			c.Assert(diff.TablesAdded, qt.HasLen, 0)
			c.Assert(diff.TablesRemoved, qt.HasLen, 0)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompareWithDatabaseInfo_MySQLForeignKeyOnTheConnectionSchema covers the
// field-level foreign key, which reaches the diff through a different route:
// it is synthesized only for columns the database already has, and that lookup
// keys the owning table the same way. Unsynthesized, the database's own foreign
// key has no counterpart and is dropped.
func TestCompareWithDatabaseInfo_MySQLForeignKeyOnTheConnectionSchema(t *testing.T) {
	tests := []struct {
		name        string
		genSchema   string
		dbSchema    string
		wantRemoved []string
	}{
		{name: "desired side names the database", genSchema: "shop", dbSchema: ""},
		{name: "neither side names it", genSchema: "", dbSchema: ""},
		{
			name:        "another database is another table",
			genSchema:   "shop",
			dbSchema:    "reporting",
			wantRemoved: []string{"fk_posts_user"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				mysqlForeignKeyDesired(test.genSchema),
				mysqlForeignKeyDatabase(test.dbSchema),
				mysqlConnectionInfo("mysql", "shop"),
				nil,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, test.wantRemoved,
				qt.Commentf("diff: %#v", diff))
		})
	}
}

// TestCompareWithDialect_PrimaryKeyKeepsTheNameTheDatabaseGaveIt pins the
// second half of the same defect on the dialect where it is easiest to hit.
//
// A synthesized primary key adopts the name the database already uses, so an
// unchanged schema compares equal. That lookup compared the two table spellings
// as raw strings, so it never matched a database that reports the schema as
// implicit, and the name always came from the fallback. Wherever the fallback
// agrees with what the engine would have picked the mistake is invisible; a
// schema that names its own primary key exposes it as a drop of the real
// constraint plus an add of a differently named one.
func TestCompareWithDialect_PrimaryKeyKeepsTheNameTheDatabaseGaveIt(t *testing.T) {
	tests := []struct {
		name           string
		dialect        string
		genSchema      string
		constraintName string
	}{
		{name: "postgres conventional name", dialect: "postgres", genSchema: "public", constraintName: "users_pkey"},
		{name: "postgres schema-chosen name", dialect: "postgres", genSchema: "public", constraintName: "pk_users"},
		{name: "sqlite schema-chosen name", dialect: "sqlite", genSchema: "main", constraintName: "pk_users"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			database := implicitSchemaDatabase("", "users")
			database.Constraints[0].Name = test.constraintName

			diff := schemadiff.CompareWithDialect(
				implicitSchemaDesired(test.genSchema),
				database,
				test.dialect,
			)

			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0, qt.Commentf("diff: %#v", diff))
			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0, qt.Commentf("diff: %#v", diff))
		})
	}
}

// mysqlConnectionInfo is the metadata a live MySQL-family connection carries:
// the connected database as the schema, and the same value as the default
// schema that owns unqualified objects. getDatabaseInfo pins the second field
// from the first; dbschema's live test covers that it does.
func mysqlConnectionInfo(dialect, schema string) types.DBInfo {
	semantics := identifier.ForDialect(dialect)
	semantics.DefaultSchema = schema
	return types.DBInfo{
		Dialect:             dialect,
		Schema:              schema,
		IdentifierSemantics: semantics,
	}
}

// mysqlForeignKeyDesired builds a desired side whose foreign key is declared on
// the field, which is how an Atlas HCL `foreign_key` block arrives.
func mysqlForeignKeyDesired(schema string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Users", Name: "users", Schema: schema},
			{StructName: "Posts", Name: "posts", Schema: schema},
		},
		Fields: []goschema.Field{
			{StructName: "Users", Name: "id", Type: "BIGINT", Nullable: false},
			{StructName: "Posts", Name: "id", Type: "BIGINT", Nullable: false},
			{
				StructName:     "Posts",
				Name:           "user_id",
				Type:           "BIGINT",
				Nullable:       false,
				Foreign:        "users(id)",
				ForeignKeyName: "fk_posts_user",
			},
		},
	}
}

// mysqlForeignKeyDatabase builds the matching database side as a MySQL reader
// reports it.
func mysqlForeignKeyDatabase(schema string) *types.DBSchema {
	return &types.DBSchema{
		Tables: []types.DBTable{
			{
				Name:   "users",
				Schema: schema,
				Type:   "TABLE",
				Columns: []types.DBColumn{
					{Name: "id", DataType: "BIGINT", IsNullable: "NO"},
				},
			},
			{
				Name:   "posts",
				Schema: schema,
				Type:   "TABLE",
				Columns: []types.DBColumn{
					{Name: "id", DataType: "BIGINT", IsNullable: "NO"},
					{Name: "user_id", DataType: "BIGINT", IsNullable: "NO"},
				},
			},
		},
		Constraints: []types.DBConstraint{{
			Name:          "fk_posts_user",
			TableName:     "posts",
			Schema:        schema,
			Type:          "FOREIGN KEY",
			ColumnNames:   []string{"user_id"},
			ForeignTable:  new("users"),
			ForeignColumn: new("id"),
		}},
	}
}
