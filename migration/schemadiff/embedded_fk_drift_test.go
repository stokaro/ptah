package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/planner/dialects/mysql"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// reverseConstraintDiff mirrors the constraint half of the generator's down
// migration: what the up migration ADDED is what the down migration must
// REMOVE, preserving the table-qualified info so the planner targets the right
// table. (The generator's reverseSchemaDiffWithSchema is unexported; this
// reproduces only the constraint reversal exercised here.)
func reverseConstraintDiff(up *difftypes.SchemaDiff) *difftypes.SchemaDiff {
	down := &difftypes.SchemaDiff{
		ConstraintsRemoved: append([]string(nil), up.ConstraintsAdded...),
	}
	for _, add := range up.ConstraintsAddedWithTables {
		down.ConstraintsRemovedWithTables = append(down.ConstraintsRemovedWithTables, difftypes.ConstraintRemovalInfo{
			Name:      add.Name,
			TableName: add.TableName,
			Type:      add.Type,
		})
	}
	return down
}

// ownableMixinSchema builds a generated schema with an "Ownable" embedded
// inline mixin that carries two `foreign=` fields (tenant_id and
// created_by_user_id, each with an explicit shared foreign_key_name). The mixin
// is embedded inline into the named host tables, so each host materializes the
// same columns + the same FK names.
func ownableMixinSchema(hostTables ...string) *goschema.Database {
	db := &goschema.Database{
		Fields: []goschema.Field{
			{
				StructName:     "Ownable",
				Name:           "tenant_id",
				Type:           "TEXT",
				Foreign:        "tenants(id)",
				ForeignKeyName: "fk_entity_tenant",
			},
			{
				StructName:     "Ownable",
				Name:           "created_by_user_id",
				Type:           "TEXT",
				Foreign:        "users(id)",
				ForeignKeyName: "fk_entity_created_by",
			},
		},
	}
	for _, ht := range hostTables {
		// Derive a struct name from the table name for the test (e.g. locations -> Locations).
		structName := strings.ToUpper(ht[:1]) + ht[1:]
		db.Tables = append(db.Tables, goschema.Table{StructName: structName, Name: ht})
		db.Fields = append(db.Fields, goschema.Field{StructName: structName, Name: "id", Type: "TEXT", Primary: true})
		db.EmbeddedFields = append(db.EmbeddedFields, goschema.EmbeddedField{
			StructName: structName, Mode: "inline", EmbeddedTypeName: "Ownable",
		})
	}
	return db
}

// ownableMixinColumnsOnlyDB builds the introspected DB for the given host
// tables with the mixin columns present but the FKs missing (so they surface as
// additions).
func ownableMixinColumnsOnlyDB(hostTables ...string) *types.DBSchema {
	db := &types.DBSchema{}
	for _, ht := range hostTables {
		db.Tables = append(db.Tables, ownableHostTable(ht))
	}
	return db
}

// ownableMixinConvergedDB builds the introspected DB for the given host tables
// with both the mixin columns and the tenant/created_by FKs already present
// (NO ACTION), i.e. the schema is converged.
func ownableMixinConvergedDB(hostTables ...string) *types.DBSchema {
	db := ownableMixinColumnsOnlyDB(hostTables...)
	for _, ht := range hostTables {
		db.Constraints = append(db.Constraints,
			types.DBConstraint{Name: "fk_entity_tenant", TableName: ht, Type: "FOREIGN KEY", ColumnName: "tenant_id", ForeignTable: new("tenants"), ForeignColumn: new("id"), DeleteRule: new("NO ACTION"), UpdateRule: new("NO ACTION")},
			types.DBConstraint{Name: "fk_entity_created_by", TableName: ht, Type: "FOREIGN KEY", ColumnName: "created_by_user_id", ForeignTable: new("users"), ForeignColumn: new("id"), DeleteRule: new("NO ACTION"), UpdateRule: new("NO ACTION")},
		)
	}
	return db
}

// ownableMixinSchemaWithTenantOnDelete builds the same Ownable mixin schema as
// ownableMixinSchema but stamps the given ON DELETE action onto the shared
// tenant FK (fk_entity_tenant) on every host. Used to drive a multi-host FK
// action change (issue #197 MODIFY path): the generated action differs from the
// converged NO ACTION database, so the comparator routes fk_entity_tenant into
// ConstraintsAdded + ConstraintsRemoved for each host table at once.
func ownableMixinSchemaWithTenantOnDelete(onDelete string, hostTables ...string) *goschema.Database {
	db := ownableMixinSchema(hostTables...)
	for i := range db.Fields {
		if db.Fields[i].StructName == "Ownable" && db.Fields[i].Name == "tenant_id" {
			db.Fields[i].OnDelete = onDelete
		}
	}
	return db
}

func ownableHostTable(name string) types.DBTable {
	return types.DBTable{
		Name: name,
		Columns: []types.DBColumn{
			{Name: "id", DataType: "text", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "tenant_id", DataType: "text", IsNullable: "NO"},
			{Name: "created_by_user_id", DataType: "text", IsNullable: "NO"},
		},
	}
}

// TestEmbeddedInlineMixinFK_NeverTargetsStructName is the end-to-end acceptance
// test for issue #197 through the postgres planner + renderer. When an embedded
// inline-relation mixin contributes `foreign=` fields to several host tables,
// the generated migration must add one FK per real host table (correct table
// name, no duplication) and never emit `ALTER TABLE <MixinStruct> ...`.
func TestEmbeddedInlineMixinFK_NeverTargetsStructName(t *testing.T) {
	hosts := []string{"locations", "areas", "commodities"}

	t.Run("missing mixin FKs are added once per real host table, never the struct", func(t *testing.T) {
		c := qt.New(t)

		gen := ownableMixinSchema(hosts...)
		diff := schemadiff.Compare(gen, ownableMixinColumnsOnlyDB(hosts...))
		c.Assert(diff.HasChanges(), qt.IsTrue)

		nodes := postgres.New().GenerateMigrationAST(diff, gen)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		// Never the mixin struct name.
		c.Assert(strings.Contains(sql, "ALTER TABLE Ownable"), qt.IsFalse,
			qt.Commentf("must not target the mixin struct name, got:\n%s", sql))

		// One correctly-targeted ADD per host table per FK, no duplication.
		for _, h := range hosts {
			tenantStmt := "ALTER TABLE " + h + " ADD CONSTRAINT fk_entity_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id)"
			createdByStmt := "ALTER TABLE " + h + " ADD CONSTRAINT fk_entity_created_by FOREIGN KEY (created_by_user_id) REFERENCES users(id)"
			c.Assert(strings.Count(sql, tenantStmt), qt.Equals, 1, qt.Commentf("host %s tenant FK, sql:\n%s", h, sql))
			c.Assert(strings.Count(sql, createdByStmt), qt.Equals, 1, qt.Commentf("host %s created_by FK, sql:\n%s", h, sql))
		}
	})

	t.Run("down migration drops each FK from its real host table", func(t *testing.T) {
		c := qt.New(t)

		gen := ownableMixinSchema(hosts...)
		diff := schemadiff.Compare(gen, ownableMixinColumnsOnlyDB(hosts...))

		// Reverse the diff the way the generator does for the down migration.
		downDiff := reverseConstraintDiff(diff)
		nodes := postgres.New().GenerateMigrationAST(downDiff, gen)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		c.Assert(strings.Contains(sql, "Ownable"), qt.IsFalse,
			qt.Commentf("down must not reference the mixin struct, got:\n%s", sql))
		for _, h := range hosts {
			c.Assert(strings.Contains(sql, "ALTER TABLE "+h+" DROP CONSTRAINT IF EXISTS fk_entity_tenant"), qt.IsTrue,
				qt.Commentf("down must drop tenant FK from %s, got:\n%s", h, sql))
			c.Assert(strings.Contains(sql, "ALTER TABLE "+h+" DROP CONSTRAINT IF EXISTS fk_entity_created_by"), qt.IsTrue,
				qt.Commentf("down must drop created_by FK from %s, got:\n%s", h, sql))
		}
	})

	t.Run("converged mixin FKs are a no-op (no churn)", func(t *testing.T) {
		c := qt.New(t)

		gen := ownableMixinSchema(hosts...)
		diff := schemadiff.Compare(gen, ownableMixinConvergedDB(hosts...))
		c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("added=%v removed=%v", diff.ConstraintsAdded, diff.ConstraintsRemoved))
	})
}

// TestEmbeddedInlineMixinFK_MultiHostActionDrift is the regression test for the
// MODIFY (drop-before-add) half of issue #197. When a mixin-shared FK name
// (fk_entity_tenant) has an on_delete change on >=2 host tables in the same
// diff, the planner must emit a DROP of the OLD constraint for EACH host paired
// with each re-ADD. A single name-keyed drop (Postgres: the information_schema
// LIMIT 1 DO block; MySQL: the last-host-wins collapse) drops the constraint
// from only one host, so the 2nd+ host's ADD CONSTRAINT collides with the
// still-present same-named constraint ("already exists" / "Duplicate foreign
// key constraint name"). This pins one DROP per host, ordered before its ADD.
func TestEmbeddedInlineMixinFK_MultiHostActionDrift(t *testing.T) {
	hosts := []string{"locations", "areas", "commodities"}

	t.Run("postgres emits a table-qualified DROP per host before each ADD", func(t *testing.T) {
		c := qt.New(t)

		// Generated tenant FK = ON DELETE CASCADE; DB = converged NO ACTION.
		gen := ownableMixinSchemaWithTenantOnDelete("CASCADE", hosts...)
		diff := schemadiff.Compare(gen, ownableMixinConvergedDB(hosts...))
		c.Assert(diff.HasChanges(), qt.IsTrue)
		// The same FK name is added + removed once per host (a modification).
		c.Assert(countName(diff.ConstraintsAdded, "fk_entity_tenant"), qt.Equals, len(hosts))
		c.Assert(countName(diff.ConstraintsRemoved, "fk_entity_tenant"), qt.Equals, len(hosts))

		nodes := postgres.New().GenerateMigrationAST(diff, gen)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		for _, h := range hosts {
			dropStmt := "ALTER TABLE " + h + " DROP CONSTRAINT IF EXISTS fk_entity_tenant;"
			addStmt := "ALTER TABLE " + h + " ADD CONSTRAINT fk_entity_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE;"

			// Exactly one table-qualified DROP and one ADD for this host.
			c.Assert(strings.Count(sql, dropStmt), qt.Equals, 1,
				qt.Commentf("host %s: expected one table-qualified DROP, got:\n%s", h, sql))
			c.Assert(strings.Count(sql, addStmt), qt.Equals, 1,
				qt.Commentf("host %s: expected one ADD with ON DELETE CASCADE, got:\n%s", h, sql))

			// The DROP must precede the matching ADD so the re-add can't collide.
			dropIdx := strings.Index(sql, dropStmt)
			addIdx := strings.Index(sql, addStmt)
			c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
				qt.Commentf("host %s: DROP must precede ADD; drop@%d add@%d\n%s", h, dropIdx, addIdx, sql))
		}

		// Idempotency: once the database carries CASCADE, regenerating is empty.
		converged := schemadiff.Compare(gen, ownableMixinConvergedTenantOnDelete("CASCADE", hosts...))
		noopNodes := postgres.New().GenerateMigrationAST(converged, gen)
		noopSQL, err := renderer.RenderSQL("postgres", noopNodes...)
		c.Assert(err, qt.IsNil)
		c.Assert(strings.Contains(noopSQL, "fk_entity_tenant"), qt.IsFalse,
			qt.Commentf("converged tenant FK must produce no churn, got:\n%s", noopSQL))
	})

	t.Run("mysql emits a DROP FOREIGN KEY per host before each ADD", func(t *testing.T) {
		c := qt.New(t)

		gen := ownableMixinSchemaWithTenantOnDelete("CASCADE", hosts...)
		diff := schemadiff.CompareWithDialect(gen, ownableMixinConvergedDB(hosts...), "mysql")
		c.Assert(diff.HasChanges(), qt.IsTrue)

		nodes := mysql.New().GenerateMigrationAST(diff, gen)
		sql, err := renderer.RenderSQL("mysql", nodes...)
		c.Assert(err, qt.IsNil)

		for _, h := range hosts {
			dropStmt := "ALTER TABLE " + h + " DROP FOREIGN KEY fk_entity_tenant;"
			addStmt := "ALTER TABLE " + h + " ADD CONSTRAINT fk_entity_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE;"

			c.Assert(strings.Count(sql, dropStmt), qt.Equals, 1,
				qt.Commentf("host %s: expected one DROP FOREIGN KEY, got:\n%s", h, sql))
			c.Assert(strings.Count(sql, addStmt), qt.Equals, 1,
				qt.Commentf("host %s: expected one ADD with ON DELETE CASCADE, got:\n%s", h, sql))

			dropIdx := strings.Index(sql, dropStmt)
			addIdx := strings.Index(sql, addStmt)
			c.Assert(dropIdx >= 0 && addIdx >= 0 && dropIdx < addIdx, qt.IsTrue,
				qt.Commentf("host %s: DROP must precede ADD; drop@%d add@%d\n%s", h, dropIdx, addIdx, sql))
		}
	})
}

// TestEmbeddedInlineMixinFK_MixedModifyAndAdd_NoPhantomDrop covers the MIXED
// case for the same mixin-shared FK name: on some host tables the FK already
// exists with a different action (a MODIFY → drop-before-add) while on another
// host the FK is missing entirely (a pure ADD). The pure-add host must NOT get a
// DROP — there is nothing to drop there. Postgres' multi-host drop decision is
// keyed on the actual (table, name) removal set, so a host that contributes only
// an addition (no removal) emits no `ALTER TABLE <host> DROP CONSTRAINT IF EXISTS`
// noise.
func TestEmbeddedInlineMixinFK_MixedModifyAndAdd_NoPhantomDrop(t *testing.T) {
	c := qt.New(t)

	modifyHosts := []string{"locations", "areas"}
	addHost := "commodities"
	allHosts := append(append([]string(nil), modifyHosts...), addHost)

	// DB: locations+areas have the tenant FK at NO ACTION (so CASCADE = modify);
	// commodities has the column but NO FK (so CASCADE = pure add).
	dbSchema := ownableMixinColumnsOnlyDB(allHosts...)
	for _, h := range modifyHosts {
		dbSchema.Constraints = append(dbSchema.Constraints,
			types.DBConstraint{Name: "fk_entity_tenant", TableName: h, Type: "FOREIGN KEY", ColumnName: "tenant_id", ForeignTable: new("tenants"), ForeignColumn: new("id"), DeleteRule: new("NO ACTION"), UpdateRule: new("NO ACTION")},
			types.DBConstraint{Name: "fk_entity_created_by", TableName: h, Type: "FOREIGN KEY", ColumnName: "created_by_user_id", ForeignTable: new("users"), ForeignColumn: new("id"), DeleteRule: new("NO ACTION"), UpdateRule: new("NO ACTION")},
		)
	}
	// commodities also needs the created_by FK present so only tenant differs;
	// otherwise created_by would surface as an extra pure-add and muddy the test.
	dbSchema.Constraints = append(dbSchema.Constraints,
		types.DBConstraint{Name: "fk_entity_created_by", TableName: addHost, Type: "FOREIGN KEY", ColumnName: "created_by_user_id", ForeignTable: new("users"), ForeignColumn: new("id"), DeleteRule: new("NO ACTION"), UpdateRule: new("NO ACTION")},
	)

	gen := ownableMixinSchemaWithTenantOnDelete("CASCADE", allHosts...)
	diff := schemadiff.Compare(gen, dbSchema)
	c.Assert(diff.HasChanges(), qt.IsTrue)

	nodes := postgres.New().GenerateMigrationAST(diff, gen)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	// Each modify host gets a table-qualified DROP before its re-ADD.
	for _, h := range modifyHosts {
		c.Assert(strings.Count(sql, "ALTER TABLE "+h+" DROP CONSTRAINT IF EXISTS fk_entity_tenant;"), qt.Equals, 1,
			qt.Commentf("modify host %s must drop the old tenant FK once, got:\n%s", h, sql))
	}
	// The pure-add host gets the ADD but NO phantom DROP.
	c.Assert(strings.Contains(sql, "ALTER TABLE "+addHost+" ADD CONSTRAINT fk_entity_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE"), qt.IsTrue,
		qt.Commentf("pure-add host %s must add the tenant FK, got:\n%s", addHost, sql))
	c.Assert(strings.Contains(sql, "ALTER TABLE "+addHost+" DROP CONSTRAINT IF EXISTS fk_entity_tenant"), qt.IsFalse,
		qt.Commentf("pure-add host %s must NOT emit a phantom DROP, got:\n%s", addHost, sql))
}

// ownableMixinConvergedTenantOnDelete is ownableMixinConvergedDB but with the
// tenant FK already carrying the given delete rule, used to prove idempotency
// of the multi-host modify (after apply, the database agrees with generated).
func ownableMixinConvergedTenantOnDelete(deleteRule string, hostTables ...string) *types.DBSchema {
	db := ownableMixinColumnsOnlyDB(hostTables...)
	for _, ht := range hostTables {
		db.Constraints = append(db.Constraints,
			types.DBConstraint{Name: "fk_entity_tenant", TableName: ht, Type: "FOREIGN KEY", ColumnName: "tenant_id", ForeignTable: new("tenants"), ForeignColumn: new("id"), DeleteRule: new(deleteRule), UpdateRule: new("NO ACTION")},
			types.DBConstraint{Name: "fk_entity_created_by", TableName: ht, Type: "FOREIGN KEY", ColumnName: "created_by_user_id", ForeignTable: new("users"), ForeignColumn: new("id"), DeleteRule: new("NO ACTION"), UpdateRule: new("NO ACTION")},
		)
	}
	return db
}

func countName(names []string, want string) int {
	n := 0
	for _, name := range names {
		if name == want {
			n++
		}
	}
	return n
}
