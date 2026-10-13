package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
)

func ownedExclusionSchema() *schemamodel.Database {
	value := &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "btree", Elements: "room WITH =", Comment: "Room schedule", Deferrable: true, Initially: "deferred"}}
	ref := pgtable.ExclusionRef("rooms", "no_overlap", identifier.ForDialect("postgres"))
	return &schemamodel.Database{Tables: []schemamodel.Table{{Name: "rooms", StructName: "Room"}}, Fields: []schemamodel.Field{{Name: "room", Type: "INTEGER", StructName: "Room"}}, FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: value})), FeatureCoverage: must.Must(pgtable.ExclusionCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))}
}

func TestOwnedExclusion_CreateTableAddsNamedConstraint(t *testing.T) {
	c := qt.New(t)
	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(ownedExclusionSchema(), "postgres", capability.Postgres18())
	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `CREATE TABLE "rooms" (`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "no_overlap" EXCLUDE USING btree (room WITH =) DEFERRABLE INITIALLY DEFERRED`)
	c.Assert(sql, qt.Contains, "ADD CONSTRAINT")
	c.Assert(sql, qt.Contains, `COMMENT ON CONSTRAINT "no_overlap" ON "rooms" IS 'Room schedule';`)
}

func TestOwnedExclusion_AlterOperation(t *testing.T) {
	c := qt.New(t)
	node := &ast.AlterTableNode{Name: "rooms", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: &pgtable.AlterExclusion{Name: "no_overlap", Change: pgtable.ExclusionChange{After: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "btree", Elements: "room WITH ="}}}}}}}
	sql, err := builtin.RenderSQLWithCapabilities("postgres", capability.Postgres18(), node)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "rooms" ADD CONSTRAINT "no_overlap" EXCLUDE USING btree (room WITH =);`)
}

func TestOwnedExclusion_AlongsideCommonConstraints(t *testing.T) {
	c := qt.New(t)
	desired := ownedExclusionSchema()
	desired.Constraints = []schemamodel.Constraint{
		{StructName: "Room", Table: "rooms", Name: "positive_room", Type: "CHECK", CheckExpression: "room > 0"},
		{StructName: "Room", Table: "rooms", Name: "unique_room", Type: "UNIQUE", Columns: []string{"room"}},
	}
	statements, err := builtin.GetOrderedCreateStatements(desired, "postgres")
	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `CONSTRAINT "positive_room" CHECK (room > 0)`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "unique_room" UNIQUE ("room")`)
	c.Assert(sql, qt.Contains, `ADD CONSTRAINT "no_overlap" EXCLUDE USING btree (room WITH =)`)
}

func TestOwnedExclusion_UnsupportedTargets(t *testing.T) {
	for _, target := range []string{"mysql", "clickhouse", "cockroachdb", "yugabytedb", "sqlite"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			sql, err := builtin.GetOrderedCreateStatementsWithCapabilities(ownedExclusionSchema(), target, capability.ForDialect(target))
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, "(?s).*EXCLUDE.*")
			c.Assert(sql, qt.IsNil)
		})
	}
}

// Equivalent definitions still declare distinct objects. Each identity and its
// comment must survive both whole-schema lowering and direct AST rendering.
func TestOwnedExclusion_EquivalentNamedDeclarations(t *testing.T) {
	c := qt.New(t)
	desired := ownedExclusionSchema()
	first, found, err := desired.FeatureObjects.Get(pgtable.ExclusionRef("rooms", "no_overlap", identifier.ForDialect("postgres")))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	second := first.Value.(*pgtable.DesiredExclusion)
	second.Comment = "Second constraint"
	desired.FeatureObjects, err = desired.FeatureObjects.With(schemaext.Object{Ref: pgtable.ExclusionRef("rooms", "second", identifier.ForDialect("postgres")), Value: second})
	c.Assert(err, qt.IsNil)
	rendered, err := builtin.GetOrderedCreateStatementsWithCapabilities(desired, "postgres", capability.Postgres18())
	c.Assert(err, qt.IsNil)
	sql := strings.Join(rendered, "\n")
	c.Assert(strings.Count(sql, "ADD CONSTRAINT"), qt.Equals, 2)
	c.Assert(sql, qt.Contains, `COMMENT ON CONSTRAINT "no_overlap" ON "rooms" IS 'Room schedule';`)
	c.Assert(sql, qt.Contains, `COMMENT ON CONSTRAINT "second" ON "rooms" IS 'Second constraint';`)
	raw := &ast.CreateTableNode{Name: "rooms", Columns: []*ast.ColumnNode{{Name: "room", Type: "INTEGER"}}, OwnedObjects: desired.FeatureObjects}
	sql, err = builtin.RenderSQLWithCapabilities("postgres", capability.Postgres18(), raw)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(sql, "ADD CONSTRAINT"), qt.Equals, 2)
	c.Assert(sql, qt.Contains, `COMMENT ON CONSTRAINT "second" ON "rooms" IS 'Second constraint';`)
}
