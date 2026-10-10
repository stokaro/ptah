package generator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// A rollback must restore the physical hint on both an ordinary index and a
// unique index with a constraint catalog row. The MySQL owner plans both
// directions, and each asks for the table copy that makes MySQL store the hint.
func TestPlanBidirectionalSchemaDiff_IndexBlockSize(t *testing.T) {
	c := qt.New(t)
	plan, diff := blockSizePlan(c,
		"CREATE TABLE t (id int NOT NULL, a int, b int, PRIMARY KEY(id), KEY k(a) KEY_BLOCK_SIZE=8, UNIQUE KEY u(b) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED",
		"CREATE TABLE t (id int NOT NULL, a int, b int, PRIMARY KEY(id), KEY k(a) KEY_BLOCK_SIZE=4, UNIQUE KEY u(b) KEY_BLOCK_SIZE=4) ROW_FORMAT=COMPRESSED")
	forward, err := builtin.RenderSQLWithCapabilities("mysql", capability.MySQL84(), plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(forward, qt.Contains, "ALTER TABLE `t` DROP INDEX `k`, ADD INDEX `k` (`a`) KEY_BLOCK_SIZE=4, ALGORITHM=COPY;")
	c.Assert(forward, qt.Contains, "ALTER TABLE `t` DROP INDEX `u`, ADD UNIQUE INDEX `u` (`b`) KEY_BLOCK_SIZE=4, ALGORITHM=COPY;")
	reverse, err := builtin.RenderSQLWithCapabilities("mysql", capability.MySQL84(), plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(reverse, qt.Contains, "ALTER TABLE `t` DROP INDEX `k`, ADD INDEX `k` (`a`) KEY_BLOCK_SIZE=8, ALGORITHM=COPY;")
	c.Assert(reverse, qt.Contains, "ALTER TABLE `t` DROP INDEX `u`, ADD UNIQUE INDEX `u` (`b`) KEY_BLOCK_SIZE=8, ALGORITHM=COPY;")
	online, err := planner.GenerateSchemaDiffSQLWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, "mysql", planner.Options{OnlineAlter: true},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(online, qt.Contains, "ALGORITHM=COPY, LOCK=NONE")
}

// A rollback must restore the primary key's hint and comment, which the
// common plan replaces the key for.
func TestPlanBidirectionalSchemaDiff_PrimaryKeyBlockSize(t *testing.T) {
	c := qt.New(t)
	plan, _ := blockSizePlan(c,
		"CREATE TABLE t (id int NOT NULL, a int, PRIMARY KEY(id) KEY_BLOCK_SIZE=8 COMMENT 'old') ROW_FORMAT=COMPRESSED",
		"CREATE TABLE t (id int NOT NULL, a int, PRIMARY KEY(id) KEY_BLOCK_SIZE=4 COMMENT 'new') ROW_FORMAT=COMPRESSED")
	sql, err := builtin.RenderSQLWithCapabilities("mysql", capability.MySQL84(), plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "DROP PRIMARY KEY, ADD PRIMARY KEY (`id`) KEY_BLOCK_SIZE=8 COMMENT 'old'")
}

// A table whose plan changes a key constraint beside an index's block size
// is planned both ways. MySQL has no projection of the key's effects on the
// table's indexes, so the rollback has no capture of the table the forward
// plan leaves; the owner plans it from the declaration, as the reversed change
// states the hint the forward plan wrote.
func TestPlanBidirectionalSchemaDiff_IndexBlockSizeBesideAKeyChange(t *testing.T) {
	c := qt.New(t)
	plan, _ := blockSizePlan(c,
		"CREATE TABLE t (id int NOT NULL, a int, PRIMARY KEY(id) COMMENT 'old', KEY k(a) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED",
		"CREATE TABLE t (id int NOT NULL, a int, PRIMARY KEY(id) COMMENT 'new', KEY k(a) KEY_BLOCK_SIZE=4) ROW_FORMAT=COMPRESSED")
	forward, err := builtin.RenderSQLWithCapabilities("mysql", capability.MySQL84(), plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(forward, qt.Contains, "DROP PRIMARY KEY, ADD PRIMARY KEY (`id`) COMMENT 'new'")
	c.Assert(forward, qt.Contains, "ALTER TABLE `t` DROP INDEX `k`, ADD INDEX `k` (`a`) KEY_BLOCK_SIZE=4, ALGORITHM=COPY;")
	reverse, err := builtin.RenderSQLWithCapabilities("mysql", capability.MySQL84(), plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(reverse, qt.Contains, "DROP PRIMARY KEY, ADD PRIMARY KEY (`id`) COMMENT 'old'")
	c.Assert(reverse, qt.Contains, "ALTER TABLE `t` DROP INDEX `k`, ADD INDEX `k` (`a`) KEY_BLOCK_SIZE=8, ALGORITHM=COPY;")
}

// blockSizePlan plans the change from the table before declares to the one
// after declares, both ways, as a MySQL 8.4 migration.
func blockSizePlan(c *qt.C, before, after string) (*generator.BidirectionalSchemaPlan, *difftypes.SchemaDiff) {
	c.Helper()
	desired, current := blockSizeSchemas(c, before, after)
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), &desired, current, "mysql", must.Must(builtin.New())))
	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: must.Must(builtin.New()), Diff: diff, DesiredSchema: &desired, CurrentSchema: current, Dialect: "mysql", Capabilities: capability.MySQL84()})
	c.Assert(err, qt.IsNil)
	return plan, diff
}

// blockSizeSchemas reads both declarations and stands the first for the
// database, with the constraint row MySQL reports for a unique index.
func blockSizeSchemas(c *qt.C, before, after string) (schemamodel.Database, *catalog.Database) {
	c.Helper()
	prior, _, err := sqlschema.Read([]byte(before), "mysql")
	c.Assert(err, qt.IsNil)
	desired, _, err := sqlschema.Read([]byte(after), "mysql")
	c.Assert(err, qt.IsNil)
	current := must.Must(goschematodb.ToDBSchema(c.Context(), &prior, "mysql", must.Must(builtin.New())))
	current.Constraints = append(current.Constraints, catalog.Constraint{TableName: "t", Name: "u", Type: "UNIQUE", ColumnNames: []string{"b"}})
	return desired, current
}

// Rebuilding a primary key for its comment must preserve its access method.
func TestPlanBidirectionalSchemaDiff_PrimaryKeyMethodAndComment(t *testing.T) {
	c := qt.New(t)
	before, _, err := sqlschema.Read([]byte("CREATE TABLE t (id int NOT NULL, PRIMARY KEY USING HASH(id) COMMENT 'old') ENGINE=MEMORY"), "mysql")
	c.Assert(err, qt.IsNil)
	after, _, err := sqlschema.Read([]byte("CREATE TABLE t (id int NOT NULL, PRIMARY KEY USING HASH(id) COMMENT 'new') ENGINE=MEMORY"), "mysql")
	c.Assert(err, qt.IsNil)
	current := must.Must(goschematodb.ToDBSchema(t.Context(),
		&before, "mysql", must.Must(builtin.New()),
	))
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(),
		&after, current, "mysql", must.Must(builtin.New()),
	))
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: must.Must(builtin.New()), Diff: diff, DesiredSchema: &after, CurrentSchema: current, Dialect: "mysql", Capabilities: capability.MySQL84()})
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities("mysql", capability.MySQL84(), plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "PRIMARY KEY (`id`) USING HASH COMMENT 'old'")
}
