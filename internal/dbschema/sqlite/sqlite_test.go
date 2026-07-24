package sqlite_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"
	moderncsqlite "modernc.org/sqlite"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/dbschema/sqlite"
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
		note TEXT,
		status TEXT CHECK (status IN ('active', 'disabled')),
		CONSTRAINT fk_users_account_id FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
	) STRICT`)
	execSQL(t, db, `CREATE TABLE invoices (
		id INTEGER PRIMARY KEY,
		account_id INTEGER CONSTRAINT fk_invoices_account REFERENCES accounts(id) ON DELETE SET NULL
	)`)
	execSQL(t, db, `CREATE UNIQUE INDEX idx_users_email_active ON users(email) WHERE status = 'active'`)
	execSQL(t, db, `CREATE INDEX idx_users_note_where ON users(note) WHERE note = 'contains WHERE token' AND status = 'active'`)
	execSQL(t, db, `CREATE VIEW active_users AS SELECT id, email FROM users WHERE status = 'active'`)
	execSQL(t, db, `CREATE TRIGGER trg_users_ai AFTER INSERT ON users
		BEGIN
			UPDATE users SET email = NEW.email WHERE id = NEW.id;
		END`)

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 3)
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
	c.Assert(index.Condition, qt.Equals, "status = 'active'")

	noteIndex := findIndex(schema.Indexes, "idx_users_note_where")
	c.Assert(noteIndex, qt.IsNotNil)
	c.Assert(noteIndex.Condition, qt.Equals, "note = 'contains WHERE token' AND status = 'active'")

	fk := findConstraint(schema.Constraints, "fk_users_account_id")
	c.Assert(fk, qt.IsNotNil)
	c.Assert(fk.Type, qt.Equals, "FOREIGN KEY")
	c.Assert(fk.ColumnNames, qt.DeepEquals, []string{"account_id"})
	c.Assert(*fk.ForeignTable, qt.Equals, "accounts")
	c.Assert(*fk.ForeignColumn, qt.Equals, "id")
	c.Assert(*fk.DeleteRule, qt.Equals, "CASCADE")

	inlineFK := findConstraint(schema.Constraints, "fk_invoices_account")
	c.Assert(inlineFK, qt.IsNotNil)
	c.Assert(inlineFK.TableName, qt.Equals, "invoices")
	c.Assert(inlineFK.ColumnNames, qt.DeepEquals, []string{"account_id"})
	c.Assert(*inlineFK.ForeignTable, qt.Equals, "accounts")
	c.Assert(*inlineFK.ForeignColumn, qt.Equals, "id")
	c.Assert(*inlineFK.DeleteRule, qt.Equals, "SET NULL")

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

func TestReaderExpressionAndPartialIndexes(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)

	execSQL(t, db, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		account_id INTEGER NOT NULL,
		email TEXT NOT NULL
	)`)
	execSQL(t, db, `CREATE INDEX idx_users_email_expr_active ON users(lower(email), account_id) WHERE account_id > 0`)

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)

	index := findIndex(schema.Indexes, "idx_users_email_expr_active")
	c.Assert(index, qt.IsNotNil)
	c.Assert(index.Columns, qt.DeepEquals, []string{"lower(email)", "account_id"})
	c.Assert(index.Condition, qt.Equals, "account_id > 0")
	c.Assert(index.Definition, qt.Contains, "CREATE INDEX idx_users_email_expr_active")
}

func TestReaderNamedUniqueConstraintsFollowColumns(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)

	execSQL(t, db, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		email TEXT NOT NULL,
		username TEXT NOT NULL,
		CONSTRAINT users_email_uq UNIQUE(email),
		CONSTRAINT users_username_uq UNIQUE(username)
	)`)

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)

	email := findConstraint(schema.Constraints, "users_email_uq")
	c.Assert(email, qt.IsNotNil)
	c.Assert(email.Type, qt.Equals, "UNIQUE")
	c.Assert(email.ColumnNames, qt.DeepEquals, []string{"email"})

	username := findConstraint(schema.Constraints, "users_username_uq")
	c.Assert(username, qt.IsNotNil)
	c.Assert(username.Type, qt.Equals, "UNIQUE")
	c.Assert(username.ColumnNames, qt.DeepEquals, []string{"username"})
}

func TestReaderUsesBatchedCatalogQueries(t *testing.T) {
	c := qt.New(t)
	db := openCountingMemoryDB(t)

	execSQL(t, db.SQL, `CREATE TABLE accounts (
		tenant_id INTEGER NOT NULL,
		code TEXT NOT NULL,
		CONSTRAINT accounts_pkey PRIMARY KEY (tenant_id, code),
		CONSTRAINT accounts_code_check CHECK (code <> '')
	)`)
	for idx := range 50 {
		tableName := "users_" + strconv.Itoa(idx)
		execSQL(t, db.SQL, fmt.Sprintf(`CREATE TABLE %s (
			id INTEGER PRIMARY KEY,
			tenant_id INTEGER NOT NULL,
			account_code TEXT NOT NULL,
			email TEXT NOT NULL,
			email_norm TEXT GENERATED ALWAYS AS (lower(email)) VIRTUAL,
			CONSTRAINT %s_account_fk FOREIGN KEY (tenant_id, account_code) REFERENCES accounts(tenant_id, code)
		)`, tableName, tableName))
		execSQL(t, db.SQL, fmt.Sprintf(
			`CREATE INDEX %s_email_active_idx ON %s(lower(email), account_code) WHERE email <> ''`,
			tableName,
			tableName,
		))
	}

	before := db.QueryCount()
	schema, err := sqlite.NewSQLiteReader(db.SQL, "main").ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 51)
	c.Assert(schema.Indexes, qt.HasLen, 51)
	c.Assert(schema.Constraints, qt.HasLen, 102)

	queryCount := db.QueryCount() - before
	// Catalog, table_xinfo, index_list, index_xinfo, and foreign_key_list stay
	// batched regardless of table or index count.
	c.Assert(queryCount, qt.Equals, 5)
}

func TestReaderReadsAttachedSchema(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)

	execSQL(t, db, `CREATE TABLE main_users (id INTEGER PRIMARY KEY)`)
	execSQL(t, db, `ATTACH DATABASE ':memory:' AS tenant`)
	execSQL(t, db, `CREATE TABLE tenant.users (
		id INTEGER PRIMARY KEY,
		email TEXT NOT NULL
	)`)
	execSQL(t, db, `CREATE INDEX tenant.users_email_idx ON users(email)`)

	schema, err := sqlite.NewSQLiteReader(db, "tenant").ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 1)
	c.Assert(schema.Tables[0].Name, qt.Equals, "users")
	c.Assert(schema.Tables[0].Schema, qt.Equals, "tenant")
	c.Assert(schema.Indexes, qt.HasLen, 1)
	c.Assert(schema.Indexes[0].Name, qt.Equals, "users_email_idx")
	c.Assert(schema.Indexes[0].Schema, qt.Equals, "tenant")
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
	tx, err := writer.BeginTransaction(context.Background())
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		c.Assert(tx.ExecuteSQL(context.Background(), statement), qt.IsNil)
	}
	c.Assert(tx.Commit(), qt.IsNil)

	actual, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)
	secondDiff := schemadiff.CompareWithDialect(generated, actual, platform.SQLite)
	c.Assert(secondDiff.HasChanges(), qt.IsFalse, qt.Commentf("unexpected SQLite drift: %+v", secondDiff))
}

type countingSQLiteDB struct {
	SQL        *sql.DB
	queryCount *atomic.Int64
}

func openCountingMemoryDB(t *testing.T) *countingSQLiteDB {
	t.Helper()

	queryCount := new(atomic.Int64)
	name := "ptah_sqlite_counting_" + strconv.FormatInt(countingSQLiteDriverID.Add(1), 10)
	sql.Register(name, &countingSQLiteDriver{queryCount: queryCount})

	db, err := sql.Open(name, ":memory:?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open counting sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return &countingSQLiteDB{SQL: db, queryCount: queryCount}
}

func (db *countingSQLiteDB) QueryCount() int {
	return int(db.queryCount.Load())
}

var countingSQLiteDriverID atomic.Int64

type countingSQLiteDriver struct {
	queryCount *atomic.Int64
}

func (d *countingSQLiteDriver) Open(name string) (driver.Conn, error) {
	conn, err := new(moderncsqlite.Driver).Open(name)
	if err != nil {
		return nil, err
	}
	return &countingSQLiteConn{Conn: conn, queryCount: d.queryCount}, nil
}

type countingSQLiteConn struct {
	driver.Conn
	queryCount *atomic.Int64
}

func (c *countingSQLiteConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	c.queryCount.Add(1)
	return queryer.QueryContext(ctx, query, args)
}

func (c *countingSQLiteConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return execer.ExecContext(ctx, query, args)
}

func (c *countingSQLiteConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	beginner, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, driver.ErrSkip
	}
	return beginner.BeginTx(ctx, opts)
}

func TestSQLiteWriterConcurrentTransactions(t *testing.T) {
	c := qt.New(t)

	db, err := sql.Open("sqlite", ":memory:")
	c.Assert(err, qt.IsNil)
	defer db.Close()
	db.SetMaxOpenConns(8)

	writer := sqlite.NewSQLiteWriter(db, "main")
	const goroutines = 16
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	start := make(chan struct{})

	for range goroutines {
		wg.Go(func() {
			<-start
			tx, err := writer.BeginTransaction(context.Background())
			if err != nil {
				errs <- err
				return
			}
			errs <- tx.Rollback()
		})
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		c.Assert(err, qt.IsNil)
	}
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
