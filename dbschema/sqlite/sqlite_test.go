package sqlite_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema/sqlite"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func openMemoryDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func execSQL(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), statement); err != nil {
		t.Fatalf("exec %q: %v", statement, err)
	}
}

func TestReaderReadSchema(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)

	execSQL(t, db, `CREATE TABLE accounts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE
	)`)
	execSQL(t, db, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		account_id INTEGER NOT NULL,
		email TEXT NOT NULL CONSTRAINT users_email_check CHECK (email <> ''),
		status TEXT CHECK (status IN ('active', 'disabled')),
		CONSTRAINT fk_users_account_id FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
	) STRICT`)
	execSQL(t, db, `CREATE UNIQUE INDEX idx_users_email_active ON users(email) WHERE status = 'active'`)
	execSQL(t, db, `CREATE VIEW active_users AS SELECT id, email FROM users WHERE status = 'active'`)
	execSQL(t, db, `CREATE TRIGGER trg_users_ai AFTER INSERT ON users
		BEGIN
			UPDATE users SET email = NEW.email WHERE id = NEW.id;
		END`)

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 2)
	c.Assert(schema.Views, qt.HasLen, 1)
	c.Assert(schema.Triggers, qt.HasLen, 1)

	accounts := findTable(schema.Tables, "accounts")
	c.Assert(accounts, qt.IsNotNil)
	accountID := findColumn(accounts.Columns, "id")
	c.Assert(accountID.IsPrimaryKey, qt.IsTrue)
	c.Assert(accountID.IsAutoIncrement, qt.IsTrue)
	accountName := findColumn(accounts.Columns, "name")
	c.Assert(accountName.IsNullable, qt.Equals, "NO")
	c.Assert(accountName.IsUnique, qt.IsTrue)

	users := findTable(schema.Tables, "users")
	c.Assert(users, qt.IsNotNil)
	status := findColumn(users.Columns, "status")
	c.Assert(status.DataType, qt.Equals, "TEXT")

	index := findIndex(schema.Indexes, "idx_users_email_active")
	c.Assert(index, qt.IsNotNil)
	c.Assert(index.TableName, qt.Equals, "users")
	c.Assert(index.Columns, qt.DeepEquals, []string{"email"})
	c.Assert(index.IsUnique, qt.IsTrue)
	c.Assert(index.Definition, qt.Contains, "CREATE UNIQUE INDEX idx_users_email_active")

	fk := findConstraint(schema.Constraints, "fk_users_account_id")
	c.Assert(fk, qt.IsNotNil)
	c.Assert(fk.Type, qt.Equals, "FOREIGN KEY")
	c.Assert(fk.ColumnNames, qt.DeepEquals, []string{"account_id"})
	c.Assert(*fk.ForeignTable, qt.Equals, "accounts")
	c.Assert(*fk.ForeignColumn, qt.Equals, "id")
	c.Assert(*fk.DeleteRule, qt.Equals, "CASCADE")

	check := findConstraint(schema.Constraints, "users_email_check")
	c.Assert(check, qt.IsNotNil)
	c.Assert(check.Type, qt.Equals, "CHECK")
	c.Assert(*check.CheckClause, qt.Equals, "email <> ''")

	enumCheck := findConstraint(schema.Constraints, "users_status_check")
	c.Assert(enumCheck, qt.IsNotNil)
	c.Assert(*enumCheck.CheckClause, qt.Equals, "status IN ('active', 'disabled')")

	c.Assert(schema.Views[0].Name, qt.Equals, "active_users")
	c.Assert(schema.Views[0].Body, qt.Equals, "SELECT id, email FROM users WHERE status = 'active'")
	c.Assert(schema.Triggers[0].Name, qt.Equals, "trg_users_ai")
	c.Assert(schema.Triggers[0].Table, qt.Equals, "users")
	c.Assert(schema.Triggers[0].Timing, qt.Equals, "AFTER")
	c.Assert(schema.Triggers[0].Event, qt.Equals, "INSERT")
}

func TestWriterDropAllTables(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)

	execSQL(t, db, `CREATE TABLE parents (id INTEGER PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE children (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER NOT NULL REFERENCES parents(id)
	)`)

	writer := sqlite.NewSQLiteWriter(db, "main")
	err := writer.DropAllTables()
	c.Assert(err, qt.IsNil)

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 0)

	var foreignKeys int
	err = db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys)
	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeys, qt.Equals, 1)
}

func TestReaderCompositeKeys(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)

	execSQL(t, db, `CREATE TABLE parents (
		code TEXT NOT NULL,
		tenant_id INTEGER NOT NULL,
		CONSTRAINT parents_pkey PRIMARY KEY (tenant_id, code)
	)`)
	execSQL(t, db, `CREATE TABLE children (
		id INTEGER PRIMARY KEY,
		tenant_id INTEGER NOT NULL,
		parent_code TEXT NOT NULL,
		CONSTRAINT fk_children_parent FOREIGN KEY (tenant_id, parent_code) REFERENCES parents(tenant_id, code)
	)`)

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)

	pk := findConstraint(schema.Constraints, "parents_pkey")
	c.Assert(pk, qt.IsNotNil)
	c.Assert(pk.Type, qt.Equals, "PRIMARY KEY")
	c.Assert(pk.ColumnNames, qt.DeepEquals, []string{"tenant_id", "code"})

	fk := findConstraint(schema.Constraints, "fk_children_parent")
	c.Assert(fk, qt.IsNotNil)
	c.Assert(fk.Type, qt.Equals, "FOREIGN KEY")
	c.Assert(fk.ColumnNames, qt.DeepEquals, []string{"tenant_id", "parent_code"})
	c.Assert(fk.ForeignColumns, qt.DeepEquals, []string{"tenant_id", "code"})
}

func TestReaderInlineNamedForeignKey(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)

	execSQL(t, db, `CREATE TABLE accounts (id INTEGER PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		account_id INTEGER CONSTRAINT fk_users_account REFERENCES accounts(id)
	)`)

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)

	fk := findConstraint(schema.Constraints, "fk_users_account")
	c.Assert(fk, qt.IsNotNil)
	c.Assert(fk.Type, qt.Equals, "FOREIGN KEY")
	c.Assert(fk.ColumnNames, qt.DeepEquals, []string{"account_id"})
	c.Assert(fk.ForeignColumns, qt.DeepEquals, []string{"id"})
}

func TestRoundTripGeneratedSchemaThroughSQLite(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)

	generated := sqliteRoundTripSchema()
	initial, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)

	diff := schemadiff.CompareWithDialect(generated, initial, platform.SQLite)
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, generated, platform.SQLite)
	c.Assert(err, qt.IsNil)
	c.Assert(len(statements) > 0, qt.IsTrue)

	writer := sqlite.NewSQLiteWriter(db, "main")
	c.Assert(writer.BeginTransaction(), qt.IsNil)
	for _, statement := range statements {
		c.Assert(writer.ExecuteSQL(context.Background(), statement), qt.IsNil)
	}
	c.Assert(writer.CommitTransaction(), qt.IsNil)

	actual, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)
	secondDiff := schemadiff.CompareWithDialect(generated, actual, platform.SQLite)
	c.Assert(secondDiff.HasChanges(), qt.IsFalse, qt.Commentf("unexpected SQLite drift: %+v", secondDiff))
}

func sqliteRoundTripSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{Name: "accounts", StructName: "Account", Strict: true},
			{Name: "users", StructName: "User", Strict: true},
		},
		Fields: []goschema.Field{
			{Name: "id", Type: "INTEGER", StructName: "Account", Primary: true, AutoInc: true},
			{Name: "name", Type: "TEXT", StructName: "Account", Nullable: false, Unique: true},
			{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
			{Name: "account_id", Type: "INTEGER", StructName: "User", Nullable: false, Foreign: "accounts(id)", OnDelete: "CASCADE"},
			{Name: "email", Type: "TEXT", StructName: "User", Nullable: false, Check: "email <> ''", CheckName: "users_email_check"},
			{Name: "status", Type: "enum_user_status", StructName: "User", Nullable: false, Enum: []string{"active", "disabled"}},
			{Name: "email_norm", Type: "TEXT", StructName: "User", Nullable: true, GeneratedExpression: "lower(email)"},
			{Name: "email_len", Type: "INTEGER", StructName: "User", Nullable: true, GeneratedExpression: "length(email)", GeneratedKind: "STORED"},
		},
		Enums: []goschema.Enum{{Name: "enum_user_status", Values: []string{"active", "disabled"}}},
		Indexes: []goschema.Index{{
			Name:       "idx_users_email_active",
			StructName: "User",
			Fields:     []string{"email"},
			Unique:     true,
			Condition:  "status = 'active'",
		}},
		Views: []goschema.View{{
			Name: "active_users",
			Body: "SELECT id, email FROM users WHERE status = 'active'",
		}},
	}
}

func findTable(tables []types.DBTable, name string) *types.DBTable {
	for i := range tables {
		if tables[i].Name == name {
			return &tables[i]
		}
	}
	return nil
}

func findColumn(columns []types.DBColumn, name string) types.DBColumn {
	for _, column := range columns {
		if column.Name == name {
			return column
		}
	}
	return types.DBColumn{}
}

func findIndex(indexes []types.DBIndex, name string) *types.DBIndex {
	for i := range indexes {
		if indexes[i].Name == name {
			return &indexes[i]
		}
	}
	return nil
}

func findConstraint(constraints []types.DBConstraint, name string) *types.DBConstraint {
	for i := range constraints {
		if constraints[i].Name == name {
			return &constraints[i]
		}
	}
	return nil
}
