package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// usersV1 is the current side of the CompareSchemas fixtures: one table with a
// serial primary key and an email column.
func usersV1() *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
		},
	}
	schemamodel.Finalize(db)
	return db
}

// usersV2 is usersV1 plus a created_at column and an index over it.
func usersV2() *schemamodel.Database {
	db := usersV1()
	db.Fields = append(db.Fields, schemamodel.Field{
		StructName: "User", Name: "created_at", Type: "TIMESTAMP", Nullable: true,
	})
	db.Indexes = append(db.Indexes, schemamodel.Index{
		StructName: "User",
		Name:       "idx_users_created_at",
		Fields:     []string{"created_at"},
	})
	schemamodel.Finalize(db)
	return db
}

func TestCompareSchemas_PlansAddedColumnAndIndex(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareSchemas(t.Context(), usersV2(), usersV1(), platform.Postgres, must.Must(builtin.New())))

	c.Assert(diff.TablesAdded, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved.Names(), qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	modified := diff.TablesModified[0]
	c.Assert(modified.TableName, qt.Equals, "users")
	c.Assert(modified.ColumnsAdded.Names(), qt.DeepEquals, []string{"created_at"})
	c.Assert(modified.ColumnsRemoved, qt.HasLen, 0)
	c.Assert(modified.ColumnsModified, qt.HasLen, 0)
	c.Assert(modified.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(modified.ConstraintsRemoved, qt.HasLen, 0)
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_users_created_at", TableName: "users"},
	})
	c.Assert(diff.IndexesRemoved, qt.HasLen, 0)
}

func TestCompareSchemas_IdenticalInputsReportNothing(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareSchemas(t.Context(), usersV2(), usersV2(), platform.Postgres, must.Must(builtin.New())))

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %+v", diff))
}

// TestCompareSchemas_YDBSelfCompareReportsNothing pins the file-to-file path
// on YDB, where the current side is a declaration rather than a catalog: its
// types are SQL spellings and its index kind sits where a catalog's would. A
// comparison that read only the desired side through the YDB type map would
// plan VARCHAR(255) -> Utf8, and one that read no kind from the current side
// would rebuild every asynchronous index, on a schema compared with itself.
func TestCompareSchemas_YDBSelfCompareReportsNothing(t *testing.T) {
	c := qt.New(t)
	db := usersV2()
	db.Fields = append(db.Fields,
		schemamodel.Field{StructName: "User", Name: "balance", Type: "DECIMAL(12,2)", Nullable: true},
		schemamodel.Field{StructName: "User", Name: "nickname", Type: "TEXT", Nullable: true},
	)
	db.Indexes = append(db.Indexes,
		schemamodel.Index{StructName: "User", Name: "idx_users_balance", Fields: []string{"balance"},
			Type: "async", IncludeColumns: []string{"nickname"}},
		schemamodel.Index{StructName: "User", Name: "uidx_users_email", Fields: []string{"email"}, Unique: true},
	)
	schemamodel.Finalize(db)

	diff := must.Must(schemadiff.CompareSchemas(t.Context(), db, db, platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %+v", diff))
}

// TestCompareSchemas_MatchesExplicitConversionThenCompare pins CompareSchemas
// to the path it documents: converting the current side with
// goschematodb.ToDBSchema and comparing with CompareWithDialect must produce
// the same diff, on a fixture whose diff is not empty.
func TestCompareSchemas_MatchesExplicitConversionThenCompare(t *testing.T) {
	c := qt.New(t)
	current := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Order", Name: "status", Type: "order_status"},
		},
		Enums: []schemamodel.Enum{{Name: "order_status", Values: []string{"new", "paid"}}},
	}
	schemamodel.Finalize(current)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Order", Name: "status", Type: "order_status"},
		},
		Enums: []schemamodel.Enum{{Name: "order_status", Values: []string{"new", "paid", "shipped"}}},
		Indexes: []schemamodel.Index{{
			StructName: "Order",
			Name:       "idx_orders_status",
			Fields:     []string{"status"},
		}},
	}
	schemamodel.Finalize(desired)

	got := must.Must(schemadiff.CompareSchemas(t.Context(), desired, current, platform.Postgres, must.Must(builtin.New())))
	want := must.Must(schemadiff.CompareWithDialect(
		t.Context(), desired, must.Must(goschematodb.ToDBSchema(t.Context(), current, platform.Postgres, must.Must(builtin.New()))), platform.Postgres, must.Must(builtin.New()),
	))

	c.Assert(got.HasChanges(), qt.IsTrue, qt.Commentf("fixture must produce a non-empty diff"))
	c.Assert(got, qt.DeepEquals, want)
}

// TestCompareSchemas_DialectReachesTheConversion pins that the dialect argument
// drives the conversion of the current side, not only the comparison. The
// conversion reports schemamodel.Index.Type as a PostgreSQL access method only on
// a PostgreSQL-family target, so a self-compare of a hash index is clean under
// the postgres dialect, while the same current side converted for SQLite
// loses the method and the same postgres comparison plans a rebuild.
func TestCompareSchemas_DialectReachesTheConversion(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
		},
		Indexes: []schemamodel.Index{{
			StructName: "User",
			Name:       "idx_users_email_hash",
			Fields:     []string{"email"},
			Type:       "hash",
		}},
	}
	schemamodel.Finalize(db)

	diff := must.Must(schemadiff.CompareSchemas(t.Context(), db, db, platform.Postgres, must.Must(builtin.New())))
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %+v", diff))

	otherTarget := must.Must(schemadiff.CompareWithDialect(
		t.Context(), db, must.Must(goschematodb.ToDBSchema(t.Context(), db, platform.SQLite, must.Must(builtin.New()))), platform.Postgres, must.Must(builtin.New()),
	))
	c.Assert(otherTarget.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_users_email_hash", TableName: "users"},
	})
	c.Assert(otherTarget.IndexesRemoved, qt.DeepEquals, otherTarget.IndexAdditions())
}
