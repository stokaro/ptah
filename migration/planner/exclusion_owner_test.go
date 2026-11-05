package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/annotation"
	"ptah.run/core/difftypes"
	"ptah.run/core/goschema"
	"ptah.run/core/planext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// The source resolves an omitted table attribute through its Go struct.
// Comparison and planning retain the owner change beside a common CHECK.
func TestExclusionOwner_ExistingTablePlan(t *testing.T) {
	for _, test := range []struct {
		name, attributes, extra, want string
		common                        int
	}{
		{name: "predicate and implicit table", attributes: `condition="room > 0"`,
			want: `EXCLUDE USING btree (room WITH =) WHERE (room > 0)`},
		{name: "explicit table", attributes: `table="rooms"`, want: `EXCLUDE USING btree (room WITH =)`},
		{name: "common and owned constraints", extra: `//ptah:schema:constraint name="positive_room" type="CHECK" check="room > 0"` + "\n",
			want: `EXCLUDE USING btree (room WITH =)`, common: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := builtintest.Runtime()
			desired, err := goschema.ParseSource(runtime.Annotations(), "schema.go", `package entities
//ptah:schema:table name="rooms"
//ptah:schema:exclude name="no_overlap" using="btree" elements="room WITH =" `+test.attributes+"\n"+test.extra+`type Room struct {
 //ptah:schema:field name="room" type="INTEGER"
 Room int
}
`)
			c.Assert(err, qt.IsNil)
			c.Assert(desired.FeatureObjects.Len(), qt.Equals, 1)
			current, err := goschematodb.ToDBSchema(t.Context(), &desired, "postgres", runtime)
			c.Assert(err, qt.IsNil)
			current.FeatureObjects = schemaext.Objects{}
			current.Constraints = nil
			diff, err := schemadiff.CompareWithDialect(t.Context(), &desired, current, "postgres", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.ConstraintsAdded, qt.HasLen, test.common)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].FeatureChanges, qt.HasLen, 1)
			statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, "postgres")
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, `ALTER TABLE "rooms" ADD CONSTRAINT "no_overlap" `+test.want)
			c.Assert(statements, qt.HasLen, test.common+1)
		})
	}
}

// parsedRooms is a rooms table with a field for each of columns, under the
// annotations prefix adds to the struct.
func parsedRooms(c *qt.C, annotations annotation.Set, prefix string, columns ...string) schemamodel.Database {
	c.Helper()
	var source strings.Builder
	source.WriteString("package entities\n//ptah:schema:table name=\"rooms\"\n" + prefix + "type Room struct {\n")
	for _, column := range columns {
		source.WriteString(" //ptah:schema:field name=\"" + column + "\" type=\"INTEGER\"\n " + strings.ToUpper(column) + " int\n")
	}
	source.WriteString("}\n")
	parsed, err := goschema.ParseSource(annotations, "schema.go", source.String())
	c.Assert(err, qt.IsNil)
	return parsed
}

// TestExclusionOwner_DropLeavesAheadOfTheColumnItCovers removes an exclusion
// constraint with the column it covers and a table. The server drops the
// constraint with that column, so the plan names the constraint first. The drop
// takes no object a later statement names, so in the exact style none of them
// gains a guard.
func TestExclusionOwner_DropLeavesAheadOfTheColumnItCovers(t *testing.T) {
	c := qt.New(t)
	runtime := builtintest.Runtime()
	held := parsedRooms(c, runtime.Annotations(), `//ptah:schema:exclude name="no_overlap" using="btree" elements="room WITH ="`+"\n", "id", "room")
	current, err := goschematodb.ToDBSchema(t.Context(), &held, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	current.Tables = append(current.Tables, catalog.Table{Name: "gone", Schema: "public", Type: "BASE TABLE"})
	desired := parsedRooms(c, runtime.Annotations(), "", "id")

	diff, err := schemadiff.CompareWithDialect(t.Context(), &desired, current, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, "postgres",
		planext.Options{Capabilities: capability.ForDialect("postgres"), Statements: planext.StatementStyleExact})
	c.Assert(err, qt.IsNil)

	c.Assert(statements, qt.DeepEquals, []string{
		`ALTER TABLE "rooms" DROP CONSTRAINT "no_overlap"`,
		`ALTER TABLE "rooms" DROP COLUMN "room"`,
		`DROP TABLE "public"."gone"`,
	})
}

func TestExclusionOwner_PlannerRefusesUnsupportedTargets(t *testing.T) {
	for _, target := range []string{"mysql", "oracle", "sqlite"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "rooms", StructName: "Room"}},
				Fields: []schemamodel.Field{{StructName: "Room", Name: "room", Type: "INTEGER"}},
			}
			objects, err := schemaext.NewObjects(schemaext.Object{
				Ref:   pgtable.ExclusionRef("rooms", "no_overlap", identifier.ForDialect("postgres")),
				Value: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "btree", Elements: "room WITH ="}},
			})
			c.Assert(err, qt.IsNil)
			desired.FeatureObjects = objects
			diff := &difftypes.SchemaDiff{TablesAdded: difftypes.TableCreationsFor(desired, identifier.ForDialect("postgres"), "rooms")}
			c.Assert(diff.TablesAdded[0].OwnedObjects.Len(), qt.Equals, 1)
			nodes, err := planner.GenerateSchemaDiffAST(t.Context(), builtintest.Runtime(), diff, target)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, "(?s).*EXCLUDE.*")
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// Creating a table must carry its owned constraints through the migration
// planner, as well as through direct schema rendering.
func TestExclusionOwner_CreatePlanAddsNamedConstraint(t *testing.T) {
	c := qt.New(t)
	runtime := builtintest.Runtime()
	desired, err := goschema.ParseSource(runtime.Annotations(), "schema.go", `package entities
//ptah:schema:table name="rooms" schema="app"
//ptah:schema:exclude name="no_overlap" using="btree" elements="room WITH =" comment="Room schedule"
type Room struct {
 //ptah:schema:field name="room" type="INTEGER"
 Room int
}
`)
	c.Assert(err, qt.IsNil)
	policy, err := pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("app", "rooms", "visible"), pgpolicy.DesiredPolicy{Using: new("room > 0")})
	c.Assert(err, qt.IsNil)
	desired.FeatureObjects, err = desired.FeatureObjects.With(policy)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(c.Context(), &desired, &catalog.Database{}, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesAdded, qt.HasLen, 1)
	c.Assert(diff.TablesAdded[0].OwnedObjects.Len(), qt.Equals, 2)
	statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), runtime, diff, "postgres")
	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `CONSTRAINT "no_overlap" EXCLUDE USING btree (room WITH =)`)
	c.Assert(sql, qt.Contains, "ADD CONSTRAINT")
	c.Assert(sql, qt.Contains, `CREATE POLICY "visible" ON "app"."rooms"`)
	c.Assert(sql, qt.Contains, `COMMENT ON CONSTRAINT "no_overlap" ON "app"."rooms" IS 'Room schedule'`)
}
