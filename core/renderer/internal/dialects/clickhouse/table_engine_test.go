package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

// clickHouseTable builds a minimal table carrying one ENGINE option. The
// primary key is there so the MergeTree family finds an ORDER BY and the
// render fails, when it fails, on the engine rather than on a missing sort key.
func clickHouseTable(engine string) *ast.CreateTableNode {
	return &ast.CreateTableNode{
		Name:    "events",
		Options: map[string]string{"ENGINE": engine},
		Columns: []*ast.ColumnNode{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR(255)"},
		},
	}
}

// TestRenderSQL_RefusesAMySQLFamilyEngine_FailurePath measures the defect in
// stokaro/ptah#3002: the MySQL-family ENGINE value reaches this target through
// the option key both families share, and rendering it produces syntactically
// valid ClickHouse the server refuses with `Code: 56 ... Unknown table engine`,
// losing the whole CREATE TABLE.
//
// Measured on ClickHouse 25.8.33.6: `CREATE TABLE events (id Int32, name
// String) ENGINE = InnoDB` is refused and system.tables holds no such table.
func TestRenderSQL_RefusesAMySQLFamilyEngine_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		engine string
	}{
		{name: "the MySQL default", engine: "InnoDB"},
		{name: "a different case", engine: "innodb"},
		{name: "MyISAM", engine: "MyISAM"},
		{name: "the MariaDB default", engine: "Aria"},
		{name: "a cluster engine", engine: "NDBCLUSTER"},
		{name: "a MariaDB storage plugin", engine: "SPIDER"},
		// A ClickHouse engine takes arguments, so an engine value carrying
		// them is the shape this target expects and the base name still has
		// to be read out of it.
		{name: "parameterized", engine: "Aria(PAGE_CHECKSUM=1)"},
		{name: "surrounded by space", engine: "  InnoDB  "},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(platform.ClickHouse, clickHouseTable(test.engine))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches,
				`(?s).*table "events" declares engine .*MySQL-family storage engine.*`)
			// The refusal names both routes to a working declaration, because
			// which one an author has depends on where the schema came from.
			c.Assert(err.Error(), qt.Contains, "platform.clickhouse.engine")
			c.Assert(err.Error(), qt.Contains, "ENGINE clause of a SQL source")
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRenderSQL_KeepsTheEnginesClickHouseAlsoHas_HappyPath is the control for
// the list the refusal reads, and the reason it is a behavior test rather than
// an assertion about a map: these three names appear in both families, and
// measured against ClickHouse 25.8.33.6 system.table_engines reports Memory,
// Merge and S3. A list that swept them in would refuse a declaration this
// server executes, and only a render can show that it does not.
func TestRenderSQL_KeepsTheEnginesClickHouseAlsoHas_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		engine string
		want   string
	}{
		{name: "Memory", engine: "Memory", want: "ENGINE = Memory"},
		{name: "Merge", engine: "Merge", want: "ENGINE = Merge"},
		{name: "S3", engine: "S3", want: "ENGINE = S3"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(platform.ClickHouse, clickHouseTable(test.engine))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestRenderSQL_KeepsAClickHouseEngine_HappyPath pins the half of the fix that
// is easy to break: the refusal must not reach a value authored for this
// target, whether it arrived from a platform.clickhouse.engine override or
// from the ENGINE clause of a SQL source, and whether or not it carries
// arguments. ClickHouse's engine set is open-ended, so nothing here may depend
// on a name being known.
func TestRenderSQL_KeepsAClickHouseEngine_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		engine string
		want   string
	}{
		{name: "the default family", engine: "MergeTree", want: "ENGINE = MergeTree"},
		{name: "a family member", engine: "ReplacingMergeTree", want: "ENGINE = ReplacingMergeTree"},
		{name: "parameterized", engine: "ReplacingMergeTree(ver)", want: "ENGINE = ReplacingMergeTree(ver)"},
		{name: "a log engine", engine: "TinyLog", want: "ENGINE = TinyLog"},
		// A name no release has yet. The refusal is a closed list of names
		// that will never become ClickHouse engines, so an engine this
		// repository has never heard of has to render.
		{name: "an engine this list cannot know", engine: "SomeFutureEngine", want: "ENGINE = SomeFutureEngine"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(platform.ClickHouse, clickHouseTable(test.engine))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestRenderSQL_MySQLFamilyEngineIsRefusedOnlyHere_HappyPath is the control
// that keeps the fix inside ClickHouse. The same declaration renders on the
// families the engine belongs to, so a refusal that leaked into the shared
// path would be visible here rather than only in a MySQL user's output.
func TestRenderSQL_MySQLFamilyEngineIsRefusedOnlyHere_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(test.dialect, clickHouseTable("InnoDB"))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "ENGINE=InnoDB")
		})
	}
}
