//go:build integration

package ydb_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/dbtarget"
)

// familyTableSQL creates table docs with column body in family cold, as a
// hand-written migration or another tool would.
const familyTableSQL = "CREATE TABLE `" + familySchema + "/docs` (`id` Int64 NOT NULL, `n` Int32, " +
	"`body` Utf8 FAMILY `cold`, PRIMARY KEY (`id`), FAMILY `cold` (COMPRESSION = 'lz4'))"

// familyDocsHCL is table docs in HCL, which has no spelling for a column
// family, with column n as nType.
func familyDocsHCL(nType string) string {
	return `schema "` + familySchema + `" {
}

table "docs" {
  schema = schema.` + familySchema + `
  column "id" {
    type = Int64
  }
  column "n" {
    type = ` + nType + `
    null = true
  }
  column "body" {
    type = Utf8
    null = true
  }
  primary_key {
    columns = [column.id]
  }
}
`
}

// HCL has no spelling for a column family. `ptah-compat schema inspect` says
// so for each table whose families it leaves out, and the document applied
// back to the database it came from plans nothing: the loader records that
// HCL cannot express a family, so each column stays where it is rather than
// moving back to the default family.
func TestYDBCompatBinary_KeepsColumnFamiliesHCLCannotWrite(t *testing.T) {
	c := qt.New(t)
	binary := buildCompatBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropTables(c, conn, familySchemas)
			c.Cleanup(func() { dropTables(c, conn, familySchemas) })
			c.Assert(conn.Writer().ExecuteSQL(ctx, familyTableSQL), qt.IsNil)

			inspected, notes, inspectErr := runCompat(ctx, binary, "schema", "inspect", "--url", url, "--schema", familySchema)
			c.Assert(inspectErr, qt.IsNil, qt.Commentf("schema inspect:\n%s", notes))
			c.Assert(notes, qt.Contains, "warning: table."+familySchema+".docs: column family cold is not represented in HCL")
			reapplied, _, reapplyErr := runCompat(ctx, binary, "schema", "apply", "--url", url, "--schema", familySchema,
				"--to", "file://"+writeCompatFile(c, c.TempDir(), "inspected.hcl", inspected), "--dry-run")
			c.Assert(reapplyErr, qt.IsNil, qt.Commentf("schema apply of the inspected document:\n%s", reapplied))
			c.Assert(reapplied, qt.Equals, "Schema is synced, no changes to be made\n")
			c.Assert(familiesOf(c, conn), qt.DeepEquals,
				[]ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4", Columns: []string{"body"}}, {Name: "default", Compression: "off"}})
		})
	}
}

// A table rebuilt from an HCL desired state keeps its column families: the
// document cannot spell one, so the plan takes the database's families as
// declared and writes them on the new table rather than dropping them with the
// old one.
func TestYDBCompatBinary_RebuildKeepsColumnFamiliesHCLCannotWrite(t *testing.T) {
	c := qt.New(t)
	binary := buildCompatBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropTables(c, conn, familySchemas)
			c.Cleanup(func() { dropTables(c, conn, familySchemas) })
			c.Assert(conn.Writer().ExecuteSQL(ctx, familyTableSQL), qt.IsNil)
			desired := "file://" + writeCompatFile(c, c.TempDir(), "desired.hcl", familyDocsHCL("Int64"))

			rebuilt, notes, rebuildErr := runCompatWithEnv(ctx, binary, []string{"PTAH_ALLOW_TABLE_REBUILD=1"},
				"schema", "apply", "--url", url, "--schema", familySchema, "--to", desired, "--auto-approve")

			c.Assert(rebuildErr, qt.IsNil, qt.Commentf("schema apply:\n%s\n%s", rebuilt, notes))
			c.Assert(rebuilt, qt.Contains, "    `body` Utf8 FAMILY `cold`,\n")
			c.Assert(rebuilt, qt.Contains, "    FAMILY `cold` (COMPRESSION = 'lz4'),\n    FAMILY `default` (COMPRESSION = 'off')\n"+
				") WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, ")
			c.Assert(familiesOf(c, conn), qt.DeepEquals,
				[]ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4", Columns: []string{"body"}}, {Name: "default", Compression: "off"}})
			synced, _, syncedErr := runCompat(ctx, binary,
				"schema", "apply", "--url", url, "--schema", familySchema, "--to", desired, "--dry-run")
			c.Assert(syncedErr, qt.IsNil, qt.Commentf("schema apply again:\n%s", synced))
			c.Assert(synced, qt.Equals, "Schema is synced, no changes to be made\n")
		})
	}
}
