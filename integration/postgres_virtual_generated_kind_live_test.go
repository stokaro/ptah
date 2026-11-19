//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// TestPostgresVirtualGeneratedColumnKindLive compares a VIRTUAL generated
// column, which PostgreSQL 18 builds, with a declaration of each kind. The
// column is written by hand, so the read reports what the server built rather
// than what a render wrote. A declaration that names no kind is STORED on
// PostgreSQL, so it and one that names STORED are a column of another kind and
// plan a change; one that names VIRTUAL is the column the server holds.
//
// A comparator test builds its catalog by hand and says nothing about whether
// the reader reports the kind. With the reader's "v" arm removed, the read
// names no kind and every row fails here.
func TestPostgresVirtualGeneratedColumnKindLive(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		changes  bool
	}{
		{name: "declared with no kind", declared: "", changes: true},
		{name: "declared stored", declared: "STORED", changes: true},
		{name: "declared virtual", declared: "VIRTUAL", changes: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
			c.Cleanup(cancel)
			adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
			admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
			name := fmt.Sprintf("ptah_virtual_kind_%d", time.Now().UnixNano())
			createE2EDatabase(c, ctx, admin, name)
			c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
			dbURL := replaceDatabaseName(c, adminURL, name)
			execPostgresFamilySQL(c, ctx, dbURL,
				"CREATE TABLE t (id bigint PRIMARY KEY, a bigint, m bigint GENERATED ALWAYS AS (a * 2) VIRTUAL)")
			conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			read, err := conn.Reader().ReadSchemaContext(ctx)
			c.Assert(err, qt.IsNil)

			diff, err := schemadiff.CompareWithDatabaseInfo(ctx, virtualKindDesired(test.declared), read, conn.Info(), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(generatedKindOf(c, read, "m"), qt.Equals, "VIRTUAL")
			c.Assert(diff.HasChanges(), qt.Equals, test.changes, qt.Commentf("%+v", diff.TablesModified))
		})
	}
}

// virtualKindDesired declares table t with m generated from a, of kind.
func virtualKindDesired(kind string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", FieldName: "A", Name: "a", Type: "BIGINT", Nullable: true},
			{StructName: "T", FieldName: "M", Name: "m", Type: "BIGINT", Nullable: true, GeneratedExpression: "a * 2", GeneratedKind: kind},
		},
	}
}

// generatedKindOf answers the generated kind the read reports for column of
// table t.
func generatedKindOf(c *qt.C, read *catalog.Database, column string) string {
	c.Helper()
	for _, table := range read.Tables {
		for _, held := range table.Columns {
			if table.Name == "t" && held.Name == column {
				return held.GeneratedKind
			}
		}
	}
	c.Fatalf("table t holds no column %s", column)
	return ""
}
