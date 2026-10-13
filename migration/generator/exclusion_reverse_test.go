package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

func TestExclusionRollbackRetainsPredicateDeferralAndComment(t *testing.T) {
	c := qt.New(t)
	runtime := builtintest.Runtime()
	desired, err := goschema.ParseSource(runtime.Annotations(), "schema.go", `package entities
//ptah:schema:table name="rooms" schema="app"
//ptah:schema:exclude name="no_overlap" using="btree" elements="room WITH =" condition="room > 0" deferrable initially="deferred" comment="Room schedule"
type Room struct {
 //ptah:schema:field name="room" type="INTEGER"
 Room int
}
`)
	c.Assert(err, qt.IsNil)
	current, err := goschematodb.ToDBSchema(t.Context(), &desired, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	desired.FeatureObjects = schemaext.Objects{}
	diff, err := schemadiff.CompareWithDialect(t.Context(), &desired, current, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].FeatureChanges, qt.HasLen, 1)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: &desired, CurrentSchema: current, Dialect: "postgres",
		Capabilities: capability.Postgres18(),
	})
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities("postgres", capability.Postgres18(), plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ADD CONSTRAINT "no_overlap" EXCLUDE USING btree (room WITH =) WHERE (room > 0) DEFERRABLE INITIALLY DEFERRED`)
	c.Assert(sql, qt.Contains, `COMMENT ON CONSTRAINT "no_overlap" ON "app"."rooms" IS 'Room schedule'`)
}
