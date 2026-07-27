package sqlite_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/dbschema/sqlite"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestReaderAndSchemaDiff_PreserveAttachedSchemaIndexIdentity(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)
	execSQL(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY, main_value TEXT NOT NULL)`)
	execSQL(t, db, `CREATE INDEX idx_shared_email ON users(main_value)`)
	execSQL(t, db, `ATTACH DATABASE ':memory:' AS tenant`)
	execSQL(t, db, `CREATE TABLE tenant.accounts (id INTEGER PRIMARY KEY)`)
	execSQL(t, db, `CREATE TABLE tenant.users (
		id INTEGER PRIMARY KEY,
		email TEXT NOT NULL,
		account_id INTEGER REFERENCES accounts(id)
	)`)
	execSQL(t, db, `CREATE INDEX tenant.idx_shared_email ON users(email)`)

	mainSchema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)
	tenantSchema, err := sqlite.NewSQLiteReader(db, "tenant").ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(mainSchema.Tables, qt.HasLen, 1)
	c.Assert(mainSchema.Tables[0].Columns[1].Name, qt.Equals, "main_value")
	c.Assert(mainSchema.Indexes, qt.HasLen, 1)
	c.Assert(mainSchema.Indexes[0].Columns, qt.DeepEquals, []string{"main_value"})
	c.Assert(slices.ContainsFunc(mainSchema.Constraints, func(constraint dbschematypes.DBConstraint) bool {
		return constraint.Type == "FOREIGN KEY"
	}), qt.IsFalse)
	c.Assert(tenantSchema.Tables, qt.HasLen, 2)
	c.Assert(tenantSchema.Indexes, qt.HasLen, 1)
	c.Assert(tenantSchema.Indexes[0].Columns, qt.DeepEquals, []string{"email"})
	c.Assert(slices.ContainsFunc(tenantSchema.Constraints, func(constraint dbschematypes.DBConstraint) bool {
		return constraint.Type == "FOREIGN KEY" && constraint.TableName == "users"
	}), qt.IsTrue)
	live := &dbschematypes.DBSchema{
		Indexes: append(mainSchema.Indexes, tenantSchema.Indexes...),
	}
	target := attachedSchemaIndexTarget()

	initialDiff := schemadiff.CompareWithDialect(target, live, platform.SQLite)
	c.Assert(initialDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(initialDiff.IndexRemovals(), qt.HasLen, 0)

	execSQL(t, db, `DROP INDEX tenant.idx_shared_email`)
	var mainIndexCount int
	err = db.QueryRow(`SELECT count(*) FROM main.sqlite_schema WHERE type = 'index' AND name = 'idx_shared_email'`).Scan(&mainIndexCount)
	c.Assert(err, qt.IsNil)
	c.Assert(mainIndexCount, qt.Equals, 1)
	var tenantIndexCount int
	err = db.QueryRow(`SELECT count(*) FROM tenant.sqlite_schema WHERE type = 'index' AND name = 'idx_shared_email'`).Scan(&tenantIndexCount)
	c.Assert(err, qt.IsNil)
	c.Assert(tenantIndexCount, qt.Equals, 0)
	tenantSchema, err = sqlite.NewSQLiteReader(db, "tenant").ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(tenantSchema.Indexes, qt.HasLen, 0)
	live.Indexes = append(mainSchema.Indexes, tenantSchema.Indexes...)
	c.Assert(live.Indexes, qt.DeepEquals, mainSchema.Indexes)

	got := schemadiff.CompareWithDialect(target, live, platform.SQLite)
	c.Assert(got.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared_email", TableName: "tenant.users"},
	})
	c.Assert(got.IndexRemovals(), qt.HasLen, 0)

	indexOnlyAddition := &difftypes.SchemaDiff{}
	indexOnlyAddition.SetIndexAdditions(got.IndexAdditions())
	addStatements, err := planner.GenerateSchemaDiffSQLStatements(indexOnlyAddition, target, platform.SQLite)
	c.Assert(err, qt.IsNil)
	c.Assert(addStatements, qt.DeepEquals, []string{
		`CREATE INDEX IF NOT EXISTS "tenant"."idx_shared_email" ON "users" ("email")`,
	})
	execSQL(t, db, addStatements[0])

	tenantSchema, err = sqlite.NewSQLiteReader(db, "tenant").ReadSchema()
	c.Assert(err, qt.IsNil)
	live.Indexes = append(mainSchema.Indexes, tenantSchema.Indexes...)
	restoredDiff := schemadiff.CompareWithDialect(target, live, platform.SQLite)
	c.Assert(restoredDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(restoredDiff.IndexRemovals(), qt.HasLen, 0)

	target = attachedSchemaIndexTargetWithoutTenantIndex()
	removalDiff := schemadiff.CompareWithDialect(target, live, platform.SQLite)
	c.Assert(removalDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(removalDiff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared_email", TableName: "tenant.users"},
	})
	indexOnlyRemoval := &difftypes.SchemaDiff{}
	indexOnlyRemoval.SetIndexRemovals(removalDiff.IndexRemovals())
	removeStatements, err := planner.GenerateSchemaDiffSQLStatements(indexOnlyRemoval, target, platform.SQLite)
	c.Assert(err, qt.IsNil)
	c.Assert(removeStatements, qt.DeepEquals, []string{
		`DROP INDEX IF EXISTS "tenant"."idx_shared_email"`,
	})
	execSQL(t, db, removeStatements[0])

	tenantSchema, err = sqlite.NewSQLiteReader(db, "tenant").ReadSchema()
	c.Assert(err, qt.IsNil)
	live.Indexes = append(mainSchema.Indexes, tenantSchema.Indexes...)
	finalDiff := schemadiff.CompareWithDialect(target, live, platform.SQLite)
	c.Assert(finalDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(finalDiff.IndexRemovals(), qt.HasLen, 0)
}

func TestReaderAndSchemaDiff_MoveSameSchemaIndexWithoutNameCollision(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)
	execSQL(t, db, `CREATE TABLE users (email TEXT NOT NULL)`)
	execSQL(t, db, `CREATE TABLE orders (reference TEXT NOT NULL)`)
	execSQL(t, db, `CREATE INDEX idx_shared ON users(email)`)
	target := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Order", Name: "orders"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "email", Type: "TEXT"},
			{StructName: "Order", Name: "reference", Type: "TEXT"},
		},
		Indexes: []goschema.Index{
			{StructName: "Order", Name: "idx_shared", Fields: []string{"reference"}},
		},
	}

	live, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)
	diff := schemadiff.CompareWithDialect(target, live, platform.SQLite)
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "users"},
	})

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, target, platform.SQLite)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		`DROP INDEX IF EXISTS "idx_shared"`,
		`CREATE INDEX IF NOT EXISTS "idx_shared" ON "orders" ("reference")`,
	})
	execSQL(t, db, statements[0])
	execSQL(t, db, statements[1])

	live, err = sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)
	finalDiff := schemadiff.CompareWithDialect(target, live, platform.SQLite)
	c.Assert(finalDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(finalDiff.IndexRemovals(), qt.HasLen, 0)
}

func attachedSchemaIndexTarget() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "MainUser", Name: "users"},
			{StructName: "TenantUser", Schema: "tenant", Name: "users"},
		},
		Indexes: []goschema.Index{
			{StructName: "MainUser", Name: "idx_shared_email", Fields: []string{"main_value"}},
			{StructName: "TenantUser", Name: "idx_shared_email", Fields: []string{"email"}},
		},
	}
}

func attachedSchemaIndexTargetWithoutTenantIndex() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "MainUser", Name: "users"},
			{StructName: "TenantUser", Schema: "tenant", Name: "users"},
		},
		Indexes: []goschema.Index{
			{StructName: "MainUser", Name: "idx_shared_email", Fields: []string{"main_value"}},
		},
	}
}
