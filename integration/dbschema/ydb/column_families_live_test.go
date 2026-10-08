//go:build integration

package ydb_test

import (
	"context"
	"fmt"
	"path"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/ydbfamily"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// familySchema is the directory the column family tests write into.
const familySchema = "ptah_ydb_families"

var familySchemas = []string{familySchema}

// familyPool is the storage pool kind local-ydb has: the only one its
// database takes, measured on 25.1.4.7 and 26.2.1.14 (`ssd` and every other
// kind answer `database doesn't have required storage pools`).
const familyPool = "hdd"

// familyDocs is table docs with a key, a NOT NULL column with a default and
// nullable columns, its columns in families. extra names nullable columns
// added after them.
func familyDocs(families []ast.YDBColumnFamilySpec, extra ...string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs", Schema: familySchema, YDBColumnFamilies: families}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Doc", Name: "title", Type: "TEXT", Default: "untitled", DefaultSet: true},
			{StructName: "Doc", Name: "body", Type: "TEXT", Nullable: true},
			{StructName: "Doc", Name: "blob", Type: "BYTEA", Nullable: true},
		},
	}
	for _, name := range extra {
		db.Fields = append(db.Fields, schemamodel.Field{StructName: "Doc", Name: name, Type: "TEXT", Nullable: true})
	}
	schemamodel.Finalize(db)
	return db
}

// lineFamilies are the families a round trip declares on a line: the default
// family compressed, a family on a named pool holding two columns, one stating
// no setting holding the NOT NULL column, and one holding no column. Where the
// line takes a cache mode, the third keeps its columns in memory.
func lineFamilies(caps capability.Capabilities) []ast.YDBColumnFamilySpec {
	cacheMode := map[bool]string{true: ydbfamily.CacheModeInMemory}[caps.Has(capability.ColumnFamilyCacheMode)]
	return []ast.YDBColumnFamilySpec{
		{Name: "default", Compression: "lz4"},
		{Name: "cold", Data: familyPool, Compression: "lz4", Columns: []string{"body", "blob"}},
		{Name: "hot", CacheMode: cacheMode, Columns: []string{"title"}},
		{Name: "spare"},
	}
}

// lineFamiliesRead are [lineFamilies] as DescribeTable reports them: a family
// stating no compression holds none (`off`), and a cache mode is reported only
// where one was set.
func lineFamiliesRead(caps capability.Capabilities) []ast.YDBColumnFamilySpec {
	cacheMode := map[bool]string{true: ydbfamily.CacheModeInMemory}[caps.Has(capability.ColumnFamilyCacheMode)]
	return []ast.YDBColumnFamilySpec{
		{Name: "cold", Data: familyPool, Compression: "lz4", Columns: []string{"blob", "body"}},
		{Name: "default", Compression: "lz4"},
		{Name: "hot", Compression: "off", CacheMode: cacheMode, Columns: []string{"title"}},
		{Name: "spare", Compression: "off"},
	}
}

// familiesOf reads table docs' column families back from the server.
func familiesOf(c *qt.C, conn *dbschema.DatabaseConnection) []ast.YDBColumnFamilySpec {
	c.Helper()
	return tableNamed(c, readScoped(c, conn, familySchemas), familySchema, "docs").YDBColumnFamilies
}

// TestYDBColumnFamilies_RoundTrip applies a table whose families use every
// setting the line takes, reads them back from DescribeTable with the columns
// each holds and every setting the table holds, and plans nothing after;
// applying the same declaration again plans nothing too.
func TestYDBColumnFamilies_RoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, familySchemas)
			c.Cleanup(func() { dropTables(c, conn, familySchemas) })
			declared := familyDocs(lineFamilies(line.preset()))

			apply(c, conn, planAgainst(c, conn, declared, familySchemas))
			c.Assert(planAgainst(c, conn, declared, familySchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, familySchemas))
			c.Assert(planAgainst(c, conn, declared, familySchemas), qt.HasLen, 0)

			c.Assert(familiesOf(c, conn), qt.DeepEquals, lineFamiliesRead(line.preset()))
			live := readScoped(c, conn, familySchemas)
			c.Assert(live.NotDescribed.Describes(coverage.ColumnFamily, familySchema+".docs"), qt.IsTrue)
		})
	}
}

// TestYDBColumnFamilies_ChangesInPlace changes a table's families on a table
// holding rows, by one ALTER TABLE a step: a family added, a column added into
// it, a setting changed, a pool given, and columns moved between families and
// back to the default one. Each step plans the statements in the order YDB
// takes them -- the family statement after the column it moves is added, and
// before the column it held is dropped -- and ends with nothing left to plan
// and the rows in place. The last step leaves out every setting and two
// families, and plans nothing: the table keeps what it holds.
func TestYDBColumnFamilies_ChangesInPlace(t *testing.T) {
	table := "`" + familySchema + "/docs`"
	steps := []struct {
		name     string
		declared *schemamodel.Database
		want     []string
		wantRead []ast.YDBColumnFamilySpec
	}{
		{
			name: "a family added, holding a column added and one moved",
			declared: familyDocs([]ast.YDBColumnFamilySpec{
				{Name: "cold", Compression: "lz4", Columns: []string{"body"}},
				{Name: "warm", Columns: []string{"blob", "note"}},
			}, "note"),
			want: []string{
				"ALTER TABLE " + table + " ADD COLUMN `note` Utf8",
				"ALTER TABLE " + table + " ADD FAMILY `warm` (), ALTER COLUMN `blob` SET FAMILY `warm`, " +
					"ALTER COLUMN `note` SET FAMILY `warm`",
			},
			wantRead: []ast.YDBColumnFamilySpec{
				{Name: "cold", Compression: "lz4", Columns: []string{"body"}},
				{Name: "default", Compression: "off"},
				{Name: "warm", Compression: "off", Columns: []string{"blob", "note"}},
			},
		},
		{
			name: "settings changed, a pool given, a column back in the default family, another dropped",
			declared: familyDocs([]ast.YDBColumnFamilySpec{
				{Name: "default", Compression: "lz4"},
				{Name: "cold", Data: familyPool, Compression: "off", Columns: []string{"blob"}},
				{Name: "warm", Compression: "lz4"},
			}),
			want: []string{
				"ALTER TABLE " + table + " ALTER FAMILY `cold` SET DATA 'hdd', ALTER FAMILY `cold` SET COMPRESSION 'off', " +
					"ALTER FAMILY `default` SET COMPRESSION 'lz4', ALTER FAMILY `warm` SET COMPRESSION 'lz4', " +
					"ALTER COLUMN `blob` SET FAMILY `cold`, ALTER COLUMN `body` SET FAMILY `default`",
				"ALTER TABLE " + table + " DROP COLUMN `note`",
			},
			wantRead: []ast.YDBColumnFamilySpec{
				{Name: "cold", Data: familyPool, Compression: "off", Columns: []string{"blob"}},
				{Name: "default", Compression: "lz4"},
				{Name: "warm", Compression: "lz4"},
			},
		},
		{
			name:     "settings and families left out",
			declared: familyDocs([]ast.YDBColumnFamilySpec{{Name: "cold", Columns: []string{"blob"}}}),
			want:     make([]string, 0),
			wantRead: []ast.YDBColumnFamilySpec{
				{Name: "cold", Data: familyPool, Compression: "off", Columns: []string{"blob"}},
				{Name: "default", Compression: "lz4"},
				{Name: "warm", Compression: "lz4"},
			},
		},
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, familySchemas)
			c.Cleanup(func() { dropTables(c, conn, familySchemas) })
			apply(c, conn, planAgainst(c, conn,
				familyDocs([]ast.YDBColumnFamilySpec{{Name: "cold", Compression: "lz4", Columns: []string{"body", "blob"}}}),
				familySchemas))
			apply(c, conn, []string{"UPSERT INTO " + table + " (id, title, body, blob) " +
				"VALUES (1l, 'a'u, 'one'u, 'x'), (2l, 'b'u, NULL, NULL)"})

			for _, step := range steps {
				planned := planAgainst(c, conn, step.declared, familySchemas)
				c.Assert(planned, qt.DeepEquals, step.want, qt.Commentf("step %q", step.name))
				apply(c, conn, planned)
				c.Assert(planAgainst(c, conn, step.declared, familySchemas), qt.HasLen, 0, qt.Commentf("step %q", step.name))
				c.Assert(familiesOf(c, conn), qt.DeepEquals, step.wantRead, qt.Commentf("step %q", step.name))
			}
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM "+table+" WHERE body = 'one'u AND blob = 'x'"), qt.Equals, int64(1))
		})
	}
}

// planRebuildAgainst plans the declaration with table rebuilds allowed.
func planRebuildAgainst(c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database) ([]string, error) {
	c.Helper()
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), declared, readScoped(c, conn, familySchemas), info, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	return planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, info.Dialect,
		planner.Options{Capabilities: info.Capabilities, AllowTableRebuild: true},
	)
}

// A family and a storage pool the declaration leaves out stay, since a
// cluster's table profile can give them to every new table and YQL drops
// neither: the plan moves the columns the declaration places elsewhere, the
// same with or without a rebuild allowed, and no rebuild. The table keeps the
// family, the pool and the rows.
func TestYDBColumnFamilies_WhatTheDeclarationLeavesOutStays(t *testing.T) {
	table := "`" + familySchema + "/docs`"
	tests := []struct {
		name     string
		desired  []ast.YDBColumnFamilySpec
		want     []string
		wantRead []ast.YDBColumnFamilySpec
	}{
		{
			name:    "a family left out",
			desired: []ast.YDBColumnFamilySpec{{Name: "cold", Data: familyPool, Columns: []string{"body"}}},
			want:    []string{"ALTER TABLE " + table + " ALTER COLUMN `blob` SET FAMILY `default`"},
			wantRead: []ast.YDBColumnFamilySpec{
				{Name: "cold", Data: familyPool, Compression: "off", Columns: []string{"body"}},
				{Name: "default", Compression: "off"},
				{Name: "warm", Compression: "off"},
			},
		},
		{
			name: "a pool left out",
			desired: []ast.YDBColumnFamilySpec{
				{Name: "cold", Columns: []string{"body"}},
				{Name: "warm", Columns: []string{"blob"}},
			},
			want: make([]string, 0),
			wantRead: []ast.YDBColumnFamilySpec{
				{Name: "cold", Data: familyPool, Compression: "off", Columns: []string{"body"}},
				{Name: "default", Compression: "off"},
				{Name: "warm", Compression: "off", Columns: []string{"blob"}},
			},
		},
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					conn := openYDB(c, line)
					dropTables(c, conn, familySchemas)
					c.Cleanup(func() { dropTables(c, conn, familySchemas) })
					apply(c, conn, planAgainst(c, conn, familyDocs([]ast.YDBColumnFamilySpec{
						{Name: "cold", Data: familyPool, Columns: []string{"body"}},
						{Name: "warm", Columns: []string{"blob"}},
					}), familySchemas))
					apply(c, conn, []string{"UPSERT INTO " + table + " (id, title, body) VALUES (1l, 'a'u, 'one'u)"})
					declared := familyDocs(test.desired)

					rebuildAllowed, err := planRebuildAgainst(c, conn, declared)
					c.Assert(err, qt.IsNil)
					c.Assert(rebuildAllowed, qt.DeepEquals, test.want)
					planned := planAgainst(c, conn, declared, familySchemas)
					c.Assert(planned, qt.DeepEquals, test.want)
					apply(c, conn, planned)
					c.Assert(planAgainst(c, conn, declared, familySchemas), qt.HasLen, 0)
					c.Assert(familiesOf(c, conn), qt.DeepEquals, test.wantRead)
					c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM "+table+" WHERE body = 'one'u"), qt.Equals, int64(1))
				})
			}
		})
	}
}

// A rebuild made for another change -- here a column's type -- writes into
// the new table every family the old one holds and each setting the
// declaration does not state, as a cluster's table profile would have given
// them: the default family's compression and a family the declaration never
// names, set by hand on the old table, read back from the new one, and the
// family the declaration names keeps the pool it held.
func TestYDBColumnFamilies_ARebuildKeepsWhatTheTableHolds(t *testing.T) {
	typeChange := withField("n", func(f *schemamodel.Field) { f.Type = "BIGINT" })
	inCold := func(db *schemamodel.Database) {
		db.Tables[0].YDBColumnFamilies = []ast.YDBColumnFamilySpec{{Name: "cold", Columns: []string{"label"}}}
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			cleanRebuild(c, conn)
			c.Cleanup(func() { cleanRebuild(c, conn) })
			seedRebuildItems(c, conn, rebuildItems(inCold), twoRows)
			apply(c, conn, []string{"ALTER TABLE `" + rebuildSchema + "/items` ALTER FAMILY `default` SET COMPRESSION 'lz4', " +
				"ADD FAMILY `extra` (COMPRESSION = 'lz4'), ALTER FAMILY `cold` SET DATA '" + familyPool + "'"})
			after := rebuildItems(inCold, typeChange)

			file, err := rebuildPlan(c, conn, after, true)
			c.Assert(err, qt.IsNil)
			c.Assert(file, qt.Contains, "    FAMILY `cold` (DATA = 'hdd', COMPRESSION = 'off'),\n"+
				"    FAMILY `default` (COMPRESSION = 'lz4'),\n"+
				"    FAMILY `extra` (COMPRESSION = 'lz4')\n")
			c.Assert(rebuildMigrator(c, conn, file, nil).MigrateUp(c.Context()), qt.IsNil)

			c.Assert(planAgainst(c, conn, after, rebuildSchemas), qt.HasLen, 0)
			c.Assert(tableNamed(c, readScoped(c, conn, rebuildSchemas), rebuildSchema, "items").YDBColumnFamilies, qt.DeepEquals,
				[]ast.YDBColumnFamilySpec{
					{Name: "cold", Data: familyPool, Compression: "off", Columns: []string{"label"}},
					{Name: "default", Compression: "lz4"},
					{Name: "extra", Compression: "lz4"},
				})
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+rebuildSchema+"/items`"), qt.Equals, int64(2))
		})
	}
}

// keepInMemory asks the table service to keep a table's family in memory
// with keep_in_memory, which YQL has no spelling for (`Unknown table setting:
// KEEP_IN_MEMORY`), and returns the operation's status and issues.
func keepInMemory(c *qt.C, line ydbLine, table, family string) (Ydb.StatusIds_StatusCode, string) {
	c.Helper()
	ctx := c.Context()
	driver, err := ydbsdk.Open(ctx, dbtarget.DriverDSN(c, line.engine))
	c.Assert(err, qt.IsNil)
	defer func() { _ = driver.Close(context.Background()) }()
	client := Ydb_Table_V1.NewTableServiceClient(ydbsdk.GRPCConn(driver))
	session, err := client.CreateSession(ctx, &Ydb_Table.CreateSessionRequest{})
	c.Assert(err, qt.IsNil)
	var created Ydb_Table.CreateSessionResult
	c.Assert(session.GetOperation().GetResult().UnmarshalTo(&created), qt.IsNil)
	defer func() {
		_, _ = client.DeleteSession(context.Background(), &Ydb_Table.DeleteSessionRequest{SessionId: created.GetSessionId()})
	}()
	altered, err := client.AlterTable(ctx, &Ydb_Table.AlterTableRequest{
		SessionId: created.GetSessionId(),
		Path:      path.Join(driver.Name(), table),
		AlterColumnFamilies: []*Ydb_Table.ColumnFamily{
			{Name: family, KeepInMemory: Ydb.FeatureFlag_ENABLED},
		},
	})
	c.Assert(err, qt.IsNil)
	return altered.GetOperation().GetStatus(), fmt.Sprint(altered.GetOperation().GetIssues())
}

// keep_in_memory is the one family setting the table service could set that
// YQL has no spelling for, and with default flags YDB refuses it on both
// certified lines. A table profile's `column_cache` sets it for every new
// table; the reader keeps it, no statement Ptah writes changes it, and a
// rebuild, whose CREATE TABLE cannot say it, is refused (measured with such a
// profile on 25.1.4.7 and 26.2.1.14).
func TestYDBColumnFamilies_KeepInMemoryIsRefusedWithDefaultFlags(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, familySchemas)
			c.Cleanup(func() { dropTables(c, conn, familySchemas) })
			apply(c, conn, planAgainst(c, conn, familyDocs([]ast.YDBColumnFamilySpec{{Name: "cold", Columns: []string{"body"}}}),
				familySchemas))

			status, issues := keepInMemory(c, line, familySchema+"/docs", "cold")

			c.Assert(status, qt.Equals, Ydb.StatusIds_BAD_REQUEST)
			c.Assert(issues, qt.Contains, "Setting keep_in_memory to ENABLED is not allowed")
			c.Assert(familiesOf(c, conn), qt.DeepEquals, []ast.YDBColumnFamilySpec{
				{Name: "cold", Compression: "off", Columns: []string{"body"}},
				{Name: "default", Compression: "off"},
			})
		})
	}
}
