package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/goschematodb"
	"go.5x5.cz/ptah/migration/schemadiff"
)

func TestToDBSchema_PreservesExtendedSchemaObjects(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Schemas: []goschema.Schema{{Name: "app", Comment: "Application"}},
		Tables:  []goschema.Table{{StructName: "User", Name: "users", Schema: "app"}},
		Fields:  []goschema.Field{{StructName: "User", Name: "id", Type: "bigint"}},
		Extensions: []goschema.Extension{{
			Name:   "pgcrypto",
			Schema: "app",
		}},
		Sequences: []goschema.Sequence{{
			Name:      "order_seq",
			Schema:    "app",
			AsType:    "bigint",
			Increment: new(int64(2)),
		}},
		Domains: []goschema.Domain{{
			Name:     "email",
			Schema:   "app",
			BaseType: "text",
			NotNull:  true,
		}},
		CompositeTypes: []goschema.CompositeType{{
			Name:   "address",
			Schema: "app",
			Fields: []goschema.CompositeTypeField{{Name: "city", Type: "text"}},
		}},
		Ranges: []goschema.Range{{
			Name:    "price_range",
			Schema:  "app",
			Subtype: "numeric",
		}},
		RLSEnabledTables: []goschema.RLSEnabledTable{{
			StructName: "User",
			Table:      "app.users",
		}},
		Grants: []goschema.Grant{{
			Role:       "app_user",
			Privileges: []string{"USAGE"},
			OnSequence: "app.order_seq",
		}},
	}
	goschema.Finalize(db)

	got := goschematodb.ToDBSchema(db, platform.Postgres)

	c.Assert(got.Schemas, qt.HasLen, 1)
	c.Assert(got.Schemas[0].Comment, qt.Equals, "Application")
	c.Assert(got.Tables[0].RLSEnabled, qt.IsTrue)
	c.Assert(got.Extensions, qt.DeepEquals, []dbschematypes.DBExtension{{Name: "pgcrypto", Schema: "app"}})
	c.Assert(got.Sequences, qt.HasLen, 1)
	c.Assert(got.Sequences[0].Increment, qt.DeepEquals, new(int64(2)))
	c.Assert(got.Domains, qt.HasLen, 1)
	c.Assert(got.Domains[0].NotNull, qt.IsTrue)
	c.Assert(got.Composites, qt.HasLen, 1)
	c.Assert(got.Composites[0].Fields, qt.DeepEquals, []dbschematypes.DBCompositeField{{
		Name: "city",
		Type: "text",
	}})
	c.Assert(got.Ranges, qt.HasLen, 1)
	c.Assert(got.Grants, qt.HasLen, 1)
	c.Assert(got.Grants[0].ObjectType, qt.Equals, "SEQUENCE")
	c.Assert(got.Grants[0].Schema, qt.Equals, "app")
	c.Assert(got.Grants[0].ObjectName, qt.Equals, "order_seq")
	diff := schemadiff.CompareWithDialect(db, got, platform.Postgres)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("extended HCL objects should not churn: %#v", diff))
}

func TestToDBSchema_FieldLevelConstraintsStayIdempotent(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Post", Name: "posts"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName: "User",
				Name:       "status",
				Type:       "TEXT",
				Check:      "status IN ('active', 'disabled')",
				CheckName:  "users_status_check",
			},
			{StructName: "Post", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName:     "Post",
				Name:           "user_id",
				Type:           "INTEGER",
				Foreign:        "users(id)",
				ForeignKeyName: "posts_user_id_fkey",
				OnDelete:       "CASCADE",
			},
		},
	}

	current := goschematodb.ToDBSchema(db, platform.Postgres)
	diff := schemadiff.CompareWithDialect(db, current, platform.Postgres)

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("field-level CHECK/FK should not produce a file-to-file churn diff: %#v", diff))
}

func TestToDBSchema_ExplicitConstraintOverridesFieldLevelConstraintWithSameName(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{{
			StructName: "User",
			Name:       "status",
			Type:       "TEXT",
			Check:      "status <> ''",
			CheckName:  "users_status_check",
		}},
		Constraints: []goschema.Constraint{{
			StructName:      "User",
			Name:            "users_status_check",
			Type:            "CHECK",
			Table:           "users",
			CheckExpression: "status IN ('active', 'disabled')",
		}},
	}

	got := goschematodb.ToDBSchema(db, platform.Postgres)

	c.Assert(got.Constraints, qt.HasLen, 1)
	c.Assert(*got.Constraints[0].CheckClause, qt.Equals, "status IN ('active', 'disabled')")
}

func TestToDBSchema_PreservesStructuralObjectIdentities(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Qualified", Schema: "tenant", Name: "data", PrimaryKey: []string{"qualified_id"}},
			{StructName: "Literal", Name: "tenant.data", PrimaryKey: []string{"literal_id"}},
		},
		Fields: []goschema.Field{
			{StructName: "Qualified", Name: "qualified_id", Type: "INTEGER"},
			{StructName: "Qualified", Name: "id", Type: "INTEGER"},
			{StructName: "Literal", Name: "literal_id", Type: "INTEGER"},
			{StructName: "Literal", Name: "id", Type: "INTEGER"},
		},
		Indexes: []goschema.Index{
			{StructName: "Literal", Name: "literal_lookup", TableName: `"tenant.data"`, Fields: []string{"id"}},
			{StructName: "Qualified", Name: "qualified_lookup", TableName: "tenant.data", Fields: []string{"id"}},
		},
		Constraints: []goschema.Constraint{{
			StructName:    "Literal",
			Name:          "literal_to_qualified_fk",
			Type:          "FOREIGN KEY",
			Table:         `"tenant.data"`,
			Columns:       []string{"id"},
			ForeignTable:  "tenant.data",
			ForeignColumn: "id",
		}},
		Views: []goschema.View{
			{Name: `"tenant.data"`, Body: "SELECT 'literal'"},
			{Name: "tenant.data", Body: "SELECT 'qualified'"},
		},
		MaterializedViews: []goschema.MaterializedView{
			{Name: `"tenant.data"`, Body: "SELECT 'literal'"},
			{Name: "tenant.data", Body: "SELECT 'qualified'"},
		},
		Triggers: []goschema.Trigger{
			{Name: "literal_trigger", Table: `"tenant.data"`, Timing: "AFTER", Event: "INSERT", Body: "SELECT 1"},
			{Name: "qualified_trigger", Table: "tenant.data", Timing: "AFTER", Event: "INSERT", Body: "SELECT 1"},
		},
		Grants: []goschema.Grant{
			{Role: "app", Privileges: []string{"SELECT"}, OnTable: `"tenant.data"`},
			{Role: "app", Privileges: []string{"SELECT"}, OnTable: "tenant.data"},
		},
	}
	goschema.Finalize(db)

	got := goschematodb.ToDBSchema(db, platform.Postgres)

	c.Assert(got.Tables[0].Columns[0].IsPrimaryKey, qt.IsTrue)
	c.Assert(got.Tables[0].Columns[1].IsPrimaryKey, qt.IsFalse)
	c.Assert(got.Tables[1].Columns[0].IsPrimaryKey, qt.IsTrue)
	c.Assert(got.Tables[1].Columns[1].IsPrimaryKey, qt.IsFalse)
	c.Assert(got.Indexes[0].QualifiedTableName(), qt.Equals, `"tenant.data"`)
	c.Assert(got.Indexes[1].QualifiedTableName(), qt.Equals, "tenant.data")
	c.Assert(got.Constraints, qt.HasLen, 3)
	c.Assert(got.Constraints[2].QualifiedTableName(), qt.Equals, `"tenant.data"`)
	c.Assert(got.Constraints[2].ForeignSchema, qt.Equals, "tenant")
	c.Assert(*got.Constraints[2].ForeignTable, qt.Equals, "data")
	c.Assert(got.Views[0].QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(got.Views[1].QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(got.MatViews[0].QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(got.MatViews[1].QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(got.Triggers[0].QualifiedTable(), qt.Equals, `"tenant.data"`)
	c.Assert(got.Triggers[1].QualifiedTable(), qt.Equals, "tenant.data")
	c.Assert(got.Grants[0].QualifiedTarget(), qt.Equals, `"tenant.data"`)
	c.Assert(got.Grants[1].QualifiedTarget(), qt.Equals, "tenant.data")

	diff := schemadiff.CompareWithDialect(db, got, platform.Postgres)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

// TestToDBSchema_PreservesFunctionIdentities pins the local-schema conversion
// used as the current side of schema diff. A qualified function must keep its
// schema separate from its name, while a quoted literal dot remains part of
// the name rather than becoming a qualification boundary.
func TestToDBSchema_PreservesFunctionIdentities(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{Functions: []goschema.Function{
		{Name: `"tenant.data"`, Parameters: "literal INTEGER", Returns: "integer", Language: "sql", Body: "SELECT 1"},
		{Name: "tenant.data", Parameters: "value TEXT", Returns: "integer", Language: "sql", Body: "SELECT 2"},
	}}
	goschema.Finalize(db)

	got := goschematodb.ToDBSchema(db, platform.Postgres)

	c.Assert(got.Functions, qt.HasLen, 2)
	c.Assert(got.Functions[0].Name, qt.Equals, "tenant.data")
	c.Assert(got.Functions[0].Schema, qt.Equals, "")
	c.Assert(got.Functions[0].QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(got.Functions[0].Parameters, qt.Equals, "literal integer")
	c.Assert(got.Functions[1].Name, qt.Equals, "data")
	c.Assert(got.Functions[1].Schema, qt.Equals, "tenant")
	c.Assert(got.Functions[1].QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(got.Functions[1].Parameters, qt.Equals, "value text")

	diff := schemadiff.CompareWithDialect(db, got, platform.Postgres)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}
