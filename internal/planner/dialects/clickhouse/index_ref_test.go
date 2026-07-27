package clickhouse_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/internal/planner/dialects/clickhouse"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanner_IndexRefs_RendersDuplicateNamesOnExactTables(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_shared", TableName: "events"},
			{Name: "idx_shared", TableName: "metrics"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_shared", TableName: "events"},
			{Name: "idx_shared", TableName: "archive"},
		},
	}
	generated := &goschema.Database{Indexes: []goschema.Index{
		{Name: "idx_shared", TableName: "metrics", Fields: []string{"metric_id"}, Type: "minmax"},
		{Name: "idx_shared", TableName: "events", Fields: []string{"event_id"}, Type: "minmax"},
	}}

	nodes, err := clickhouse.New().GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)

	c.Assert(nodes, qt.HasLen, 4)
	replacementDrop, ok := nodes[0].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(replacementDrop.Table, qt.Equals, "events")
	replacementCreate, ok := nodes[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(replacementCreate.Table, qt.Equals, "events")
	c.Assert(replacementCreate.Columns, qt.DeepEquals, []string{"event_id"})
	metricsCreate, ok := nodes[2].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(metricsCreate.Table, qt.Equals, "metrics")
	c.Assert(metricsCreate.Columns, qt.DeepEquals, []string{"metric_id"})
	archiveDrop, ok := nodes[3].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(archiveDrop.Table, qt.Equals, "archive")

	sql, err := renderer.RenderSQL(platform.ClickHouse, nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ALTER TABLE `metrics` ADD INDEX `idx_shared`")
	c.Assert(sql, qt.Contains, "ALTER TABLE `archive` DROP INDEX `idx_shared`;")
	eventsDropPosition := strings.Index(sql, "ALTER TABLE `events` DROP INDEX `idx_shared`;")
	eventsCreatePosition := strings.Index(sql, "ALTER TABLE `events` ADD INDEX `idx_shared`")
	c.Assert(eventsDropPosition >= 0, qt.IsTrue)
	c.Assert(eventsCreatePosition >= 0, qt.IsTrue)
	c.Assert(eventsDropPosition < eventsCreatePosition, qt.IsTrue)
}
