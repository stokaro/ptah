package generator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// A rollback must restore the physical hint on both an ordinary index and a
// unique index with a constraint catalog row, as well as the primary key.
func TestPlanBidirectionalSchemaDiff_IndexBlockSize(t *testing.T) {
	c := qt.New(t)
	before, _, err := sqlschema.Read([]byte("CREATE TABLE t (id int NOT NULL, a int, b int, PRIMARY KEY(id) KEY_BLOCK_SIZE=8 COMMENT 'old', KEY k(a) KEY_BLOCK_SIZE=8, UNIQUE KEY u(b) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED"), "mysql")
	c.Assert(err, qt.IsNil)
	after, _, err := sqlschema.Read([]byte("CREATE TABLE t (id int NOT NULL, a int, b int, PRIMARY KEY(id) KEY_BLOCK_SIZE=4 COMMENT 'new', KEY k(a) KEY_BLOCK_SIZE=4, UNIQUE KEY u(b) KEY_BLOCK_SIZE=4) ROW_FORMAT=COMPRESSED"), "mysql")
	c.Assert(err, qt.IsNil)
	current := must.Must(goschematodb.ToDBSchema(t.Context(),
		&before, "mysql", must.Must(builtin.New()),
	))
	current.Constraints = append(current.Constraints, catalog.Constraint{TableName: "t", Name: "u", Type: "UNIQUE", ColumnNames: []string{"b"}})
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(),
		&after, current, "mysql", must.Must(builtin.New()),
	))
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: must.Must(builtin.New()), Diff: diff, DesiredSchema: &after, CurrentSchema: current, Dialect: "mysql", Capabilities: capability.MySQL84()})
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities("mysql", capability.MySQL84(), plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ADD INDEX `k` (`a`) KEY_BLOCK_SIZE=8, ALGORITHM=COPY")
	c.Assert(sql, qt.Contains, "ADD UNIQUE INDEX `u` (`b`) KEY_BLOCK_SIZE=8, ALGORITHM=COPY")
	c.Assert(sql, qt.Contains, "DROP PRIMARY KEY, ADD PRIMARY KEY (`id`) KEY_BLOCK_SIZE=8 COMMENT 'old'")
	online, err := planner.GenerateSchemaDiffSQLWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, "mysql", planner.Options{OnlineAlter: true},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(online, qt.Contains, "ALGORITHM=COPY, LOCK=NONE")
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
