package pgtablerender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/dialect/postgres/pgtablerender"
)

func TestExclusionSQL_PreservesDefinition(t *testing.T) {
	c := qt.New(t)
	value := &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =, during WITH &&", WhereCondition: "active", Deferrable: true, Initially: "deferred"}}
	clause, err := pgtablerender.ExclusionSQL(`"No.Overlap"`, value)
	c.Assert(err, qt.IsNil)
	c.Assert(clause, qt.Equals, `CONSTRAINT "No.Overlap" EXCLUDE USING gist (room WITH =, during WITH &&) WHERE (active) DEFERRABLE INITIALLY DEFERRED`)
	anonymous, err := pgtablerender.ExclusionSQL("", value)
	c.Assert(err, qt.IsNil)
	c.Assert(anonymous, qt.Equals, `EXCLUDE USING gist (room WITH =, during WITH &&) WHERE (active) DEFERRABLE INITIALLY DEFERRED`)
}

func TestCreateTableExclusions_QuotedParent(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	for _, ref := range []objectidentity.ID{
		builder.ConstraintParts("App.Space", "Rooms.Dot", "no_overlap"),
		builder.Constraint(`"App.Space"."Rooms.Dot"`, "no_overlap"),
	} {
		t.Run(ref.Parent.Source, func(t *testing.T) {
			c := qt.New(t)
			ref.Kind = objectidentity.Kind(pgtable.ExclusionKind)
			objects := must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref,
				Value: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "btree", Elements: "room WITH ="}}}))
			statements, err := pgtablerender.CreateTableExclusionStatements("postgres", capability.Postgres18(), `"App.Space"."Rooms.Dot"`, objects)
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{`ALTER TABLE "App.Space"."Rooms.Dot" ADD CONSTRAINT "no_overlap" EXCLUDE USING btree (room WITH =);`})
		})
	}
}

func TestExclusionRendering_Changes(t *testing.T) {
	for _, test := range []struct {
		name   string
		change pgtable.ExclusionChange
		want   []string
	}{
		{"create", pgtable.ExclusionChange{After: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "btree", Elements: "room WITH ="}}}, []string{`ALTER TABLE "app"."rooms" ADD CONSTRAINT "no_overlap" EXCLUDE USING btree (room WITH =);`}},
		{"drop", pgtable.ExclusionChange{Before: &pgtable.ObservedExclusion{UsingMethod: "btree", Elements: "room WITH ="}}, []string{`ALTER TABLE "app"."rooms" DROP CONSTRAINT "no_overlap";`}},
		{"comment", pgtable.ExclusionChange{Before: &pgtable.ObservedExclusion{UsingMethod: "btree", Elements: "room WITH ="}, After: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "btree", Elements: "room WITH =", Comment: "Room's schedule"}}, CommentOnly: true}, []string{`COMMENT ON CONSTRAINT "no_overlap" ON "app"."rooms" IS 'Room''s schedule';`}},
		{"clear comment", pgtable.ExclusionChange{Before: &pgtable.ObservedExclusion{UsingMethod: "btree", Elements: "room WITH =", Comment: "Room schedule"}, After: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "btree", Elements: "room WITH ="}}, CommentOnly: true}, []string{`COMMENT ON CONSTRAINT "no_overlap" ON "app"."rooms" IS NULL;`}},
		{"replace and comment", pgtable.ExclusionChange{Before: &pgtable.ObservedExclusion{UsingMethod: "btree", Elements: "room WITH ="}, After: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "during WITH &&", Comment: "Room schedule"}}}, []string{`ALTER TABLE "app"."rooms" DROP CONSTRAINT "no_overlap";`, `ALTER TABLE "app"."rooms" ADD CONSTRAINT "no_overlap" EXCLUDE USING gist (during WITH &&);`, `COMMENT ON CONSTRAINT "no_overlap" ON "app"."rooms" IS 'Room schedule';`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			handlers := must.Must(renderer.NewExtensions(pgtablerender.ExclusionHandlers()...))
			context := renderer.ExtensionContext{Target: "postgres", Capabilities: capability.Postgres18(), Parent: &ast.AlterTableNode{Name: "app.rooms"}}
			statements, err := handlers.Render(context, ast.AlterExtension, &pgtable.AlterExclusion{Name: "no_overlap", Change: test.change})
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

func TestExclusionRendering_UnsupportedTarget(t *testing.T) {
	for _, target := range []string{"mysql", "cockroachdb", "yugabytedb", "ydb", "unknown"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			handlers := must.Must(renderer.NewExtensions(pgtablerender.ExclusionHandlers()...))
			context := renderer.ExtensionContext{Target: target, Capabilities: capability.Postgres18(), Parent: &ast.AlterTableNode{Name: "rooms"}}
			statements, err := handlers.Render(context, ast.AlterExtension, &pgtable.AlterExclusion{Name: "no_overlap", Change: pgtable.ExclusionChange{After: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "btree", Elements: "room WITH ="}}}})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, ".*EXCLUDE.*")
			c.Assert(statements, qt.IsNil)
		})
	}
}
