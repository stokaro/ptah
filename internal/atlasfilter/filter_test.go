package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlasfilter"
)

func TestExcludeDatabase_RemovesTableAndDependentObjects(t *testing.T) {
	c := qt.New(t)
	schema := filterFixtureSchema()

	got, err := atlasfilter.ExcludeDatabase(schema, []string{"auth.audit_log"})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"auth.users"})
	c.Assert(indexNames(got.Indexes), qt.DeepEquals, []string{"users_email_key"})
	c.Assert(constraintNames(got.Constraints), qt.DeepEquals, []string{"users_email_key"})
	c.Assert(triggerNames(got.Triggers), qt.DeepEquals, []string{"users_updated_at"})
	c.Assert(policyNames(got.RLSPolicies), qt.DeepEquals, []string{"users_policy"})
	c.Assert(grantTargets(got.Grants), qt.DeepEquals, []string{"auth.users"})
	c.Assert(tableNames(schema.Tables), qt.DeepEquals, []string{"auth.users", "auth.audit_log"})
}

func TestExcludeDatabase_TypeSelectorRemovesOnlyIndexes(t *testing.T) {
	c := qt.New(t)
	schema := filterFixtureSchema()

	got, err := atlasfilter.ExcludeDatabase(schema, []string{"auth.users.*[type=index]"})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"auth.users", "auth.audit_log"})
	c.Assert(indexNames(got.Indexes), qt.DeepEquals, []string{"audit_log_created_at_idx"})
	c.Assert(constraintNames(got.Constraints), qt.DeepEquals, []string{"users_email_key", "audit_log_user_fk"})
}

func TestExcludeDatabase_TableSubtypeSelector(t *testing.T) {
	c := qt.New(t)
	schema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Schema: "public", Name: "users", Type: "TABLE"},
			{Schema: "public", Name: "users_archive", Type: "FOREIGN TABLE"},
		},
	}

	got, err := atlasfilter.ExcludeDatabase(schema, []string{"public.*[type=foreign_table]"})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"public.users"})
}

func TestExcludeDatabase_ColumnFilterRemovesDependentObjects(t *testing.T) {
	c := qt.New(t)
	schema := filterFixtureSchema()

	got, err := atlasfilter.ExcludeDatabase(schema, []string{"auth.users.email[type=column]"})

	c.Assert(err, qt.IsNil)
	c.Assert(columnNames(got.Tables[0].Columns), qt.DeepEquals, []string{"id"})
	c.Assert(indexNames(got.Indexes), qt.DeepEquals, []string{"audit_log_created_at_idx"})
	c.Assert(constraintNames(got.Constraints), qt.DeepEquals, []string{"audit_log_user_fk"})
}

func TestExcludeDatabase_ReferencedColumnFilterRemovesForeignKeys(t *testing.T) {
	c := qt.New(t)
	foreignTable := "users"
	foreignColumn := "email"
	schema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{
				Schema: "auth",
				Name:   "users",
				Columns: []dbschematypes.DBColumn{
					{Name: "id"},
					{Name: "email"},
				},
			},
			{
				Schema: "billing",
				Name:   "invoices",
				Columns: []dbschematypes.DBColumn{
					{Name: "id"},
					{Name: "user_email"},
				},
			},
		},
		Constraints: []dbschematypes.DBConstraint{
			{
				Schema:        "billing",
				TableName:     "invoices",
				Name:          "invoices_user_email_fk",
				Type:          "FOREIGN KEY",
				ColumnNames:   []string{"user_email"},
				ForeignSchema: "auth",
				ForeignTable:  &foreignTable,
				ForeignColumn: &foreignColumn,
			},
		},
	}

	got, err := atlasfilter.ExcludeDatabase(schema, []string{"auth.users.email[type=column]"})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"auth.users", "billing.invoices"})
	c.Assert(columnNames(got.Tables[0].Columns), qt.DeepEquals, []string{"id"})
	c.Assert(constraintNames(got.Constraints), qt.DeepEquals, []string{})
}

func TestExcludeDatabase_CrossSchemaDependenciesStayIsolated(t *testing.T) {
	c := qt.New(t)
	schema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Schema: "auth", Name: "users"},
			{Schema: "billing", Name: "users"},
		},
		Indexes: []dbschematypes.DBIndex{
			{Schema: "auth", TableName: "users", Name: "auth_users_email_idx", Columns: []string{"email"}},
			{Schema: "billing", TableName: "users", Name: "billing_users_email_idx", Columns: []string{"email"}},
		},
		Triggers: []dbschematypes.DBTrigger{
			{Schema: "auth", Table: "users", Name: "auth_users_updated_at"},
			{Schema: "billing", Table: "users", Name: "billing_users_updated_at"},
		},
		Grants: []dbschematypes.DBGrant{
			{ObjectType: "TABLE", Schema: "auth", ObjectName: "users"},
			{ObjectType: "TABLE", Schema: "billing", ObjectName: "users"},
		},
	}

	got, err := atlasfilter.ExcludeDatabase(schema, []string{"auth.users"})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"billing.users"})
	c.Assert(indexNames(got.Indexes), qt.DeepEquals, []string{"billing_users_email_idx"})
	c.Assert(triggerNames(got.Triggers), qt.DeepEquals, []string{"billing_users_updated_at"})
	c.Assert(grantTargets(got.Grants), qt.DeepEquals, []string{"billing.users"})
}

func TestExcludeDatabase_ViewFilterRemovesDependentGrants(t *testing.T) {
	c := qt.New(t)
	schema := &dbschematypes.DBSchema{
		Views: []dbschematypes.DBView{
			{Schema: "public", Name: "active_users"},
		},
		Grants: []dbschematypes.DBGrant{
			{ObjectType: "TABLE", Schema: "public", ObjectName: "active_users"},
		},
	}

	got, err := atlasfilter.ExcludeDatabase(schema, []string{"public.active_users[type=view]"})

	c.Assert(err, qt.IsNil)
	c.Assert(viewNames(got.Views), qt.DeepEquals, []string{})
	c.Assert(grantTargets(got.Grants), qt.DeepEquals, []string{})
}

func TestExcludeDatabase_CommaSeparatedPatterns(t *testing.T) {
	c := qt.New(t)
	schema := filterFixtureSchema()

	got, err := atlasfilter.ExcludeDatabase(schema, []string{"auth.users.email[type=column],auth.audit_log"})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"auth.users"})
	c.Assert(columnNames(got.Tables[0].Columns), qt.DeepEquals, []string{"id"})
	c.Assert(indexNames(got.Indexes), qt.DeepEquals, []string{})
	c.Assert(constraintNames(got.Constraints), qt.DeepEquals, []string{})
}

func TestExcludeDatabase_InvalidGlob(t *testing.T) {
	c := qt.New(t)

	got, err := atlasfilter.ExcludeDatabase(filterFixtureSchema(), []string{"["})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `invalid Atlas exclude glob "["`)
	c.Assert(got, qt.IsNil)
}

func TestExcludeDatabase_EmptyTypeSelector(t *testing.T) {
	c := qt.New(t)

	got, err := atlasfilter.ExcludeDatabase(filterFixtureSchema(), []string{"*[type=]"})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `empty Atlas exclude type selector "type="`)
	c.Assert(got, qt.IsNil)
}

func TestExcludeDatabase_UnsupportedSelector(t *testing.T) {
	c := qt.New(t)

	got, err := atlasfilter.ExcludeDatabase(filterFixtureSchema(), []string{"auth.users[owner=app].version"})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `unsupported Atlas exclude selector "owner=app"`)
	c.Assert(got, qt.IsNil)
}

func TestExcludeDatabase_ExtensionVersionFieldSelector(t *testing.T) {
	c := qt.New(t)
	schema := &dbschematypes.DBSchema{
		Extensions: []dbschematypes.DBExtension{
			{Name: "pg_trgm", Schema: "public", Version: "1.6"},
			{Name: "citext", Schema: "public", Version: "1.6"},
		},
	}

	got, err := atlasfilter.ExcludeDatabase(schema, []string{"pg_trgm[type=extension].version"})

	c.Assert(err, qt.IsNil)
	c.Assert(extensionVersions(got.Extensions), qt.DeepEquals, []string{"pg_trgm:", "citext:1.6"})
	c.Assert(extensionVersions(schema.Extensions), qt.DeepEquals, []string{"pg_trgm:1.6", "citext:1.6"})
}

func TestExcludeDatabase_UnsupportedFieldSelector(t *testing.T) {
	c := qt.New(t)

	got, err := atlasfilter.ExcludeDatabase(filterFixtureSchema(), []string{"*[type=table].version"})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `unsupported Atlas exclude field selector ".version"`)
	c.Assert(got, qt.IsNil)
}

func TestExcludeGenerated_RemovesTableAndDependentObjects(t *testing.T) {
	c := qt.New(t)
	schema := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "auth", Name: "users", Engine: "InnoDB", PrimaryKey: []string{"id"}},
			{StructName: "AuditLog", Schema: "auth", Name: "audit_log"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER"},
			{StructName: "User", Name: "email", Type: "TEXT"},
			{StructName: "AuditLog", Name: "id", Type: "INTEGER"},
			{StructName: "AuditLog", Name: "user_id", Type: "INTEGER", Foreign: "users(id)"},
		},
		Indexes: []goschema.Index{
			{StructName: "User", Name: "users_email_key", Fields: []string{"email"}},
			{StructName: "AuditLog", Name: "audit_log_user_idx", Fields: []string{"user_id"}},
		},
		Constraints: []goschema.Constraint{
			{StructName: "AuditLog", Name: "audit_log_user_fk", Type: "FOREIGN KEY", Columns: []string{"user_id"}, ForeignTable: "users", ForeignColumn: "id"},
		},
		Triggers: []goschema.Trigger{
			{StructName: "User", Table: "users", Name: "users_updated_at"},
			{StructName: "AuditLog", Table: "audit_log", Name: "audit_log_updated_at"},
		},
		Grants: []goschema.Grant{
			{Role: "app", Privileges: []string{"SELECT"}, OnTable: "auth.users"},
			{Role: "app", Privileges: []string{"SELECT"}, OnTable: "auth.audit_log"},
		},
	}

	got, err := atlasfilter.ExcludeGenerated(schema, []string{"auth.audit_log"})

	c.Assert(err, qt.IsNil)
	c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"auth.users"})
	c.Assert(got.Tables[0].Engine, qt.Equals, "InnoDB")
	c.Assert(generatedFieldNames(got.Fields), qt.DeepEquals, []string{"User.id", "User.email"})
	c.Assert(generatedIndexNames(got.Indexes), qt.DeepEquals, []string{"users_email_key"})
	c.Assert(generatedConstraintNames(got.Constraints), qt.DeepEquals, []string{})
	c.Assert(generatedTriggerNames(got.Triggers), qt.DeepEquals, []string{"users_updated_at"})
	c.Assert(generatedGrantTargets(got.Grants), qt.DeepEquals, []string{"auth.users"})
	c.Assert(generatedTableNames(schema.Tables), qt.DeepEquals, []string{"auth.users", "auth.audit_log"})
}

func TestExcludeGenerated_ColumnFilterRemovesDependentObjects(t *testing.T) {
	c := qt.New(t)
	schema := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "auth", Name: "users", PrimaryKey: []string{"id", "email"}},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER"},
			{StructName: "User", Name: "email", Type: "TEXT"},
		},
		Indexes: []goschema.Index{
			{StructName: "User", Name: "users_email_key", Fields: []string{"email"}},
		},
		Constraints: []goschema.Constraint{
			{StructName: "User", Name: "users_email_check", Type: "CHECK", Columns: []string{"email"}},
		},
	}

	got, err := atlasfilter.ExcludeGenerated(schema, []string{"auth.users.email[type=column]"})

	c.Assert(err, qt.IsNil)
	c.Assert(generatedFieldNames(got.Fields), qt.DeepEquals, []string{"User.id"})
	c.Assert(got.Tables[0].PrimaryKey, qt.DeepEquals, []string{"id"})
	c.Assert(generatedIndexNames(got.Indexes), qt.DeepEquals, []string{})
	c.Assert(generatedConstraintNames(got.Constraints), qt.DeepEquals, []string{})
}

func TestExcludeGenerated_ReferencedColumnFilterRemovesFieldForeignKey(t *testing.T) {
	c := qt.New(t)
	schema := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "auth", Name: "users"},
			{StructName: "Invoice", Schema: "billing", Name: "invoices"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER"},
			{StructName: "User", Name: "email", Type: "TEXT"},
			{StructName: "Invoice", Name: "id", Type: "INTEGER"},
			{StructName: "Invoice", Name: "user_email", Type: "TEXT", Foreign: "auth.users(email)"},
		},
	}

	got, err := atlasfilter.ExcludeGenerated(schema, []string{"auth.users.email[type=column]"})

	c.Assert(err, qt.IsNil)
	c.Assert(generatedFieldNames(got.Fields), qt.DeepEquals, []string{"User.id", "Invoice.id", "Invoice.user_email"})
	c.Assert(got.Fields[2].Foreign, qt.Equals, "")
}

func TestExcludeGenerated_PreservesSelfReferencingForeignKeyMetadata(t *testing.T) {
	c := qt.New(t)
	schema := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Node", Name: "nodes"},
			{StructName: "AuditLog", Name: "audit_log"},
		},
		Fields: []goschema.Field{
			{StructName: "Node", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Node", Name: "parent_id", Type: "INTEGER", Foreign: "nodes(id)", ForeignKeyName: "nodes_parent_fk"},
			{StructName: "AuditLog", Name: "id", Type: "INTEGER", Primary: true},
		},
	}

	got, err := atlasfilter.ExcludeGenerated(schema, []string{"audit_log"})

	c.Assert(err, qt.IsNil)
	c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"nodes"})
	c.Assert(got.SelfReferencingForeignKeys["nodes"], qt.HasLen, 1)
	c.Assert(got.SelfReferencingForeignKeys["nodes"][0].FieldName, qt.Equals, "parent_id")
	c.Assert(got.SelfReferencingForeignKeys["nodes"][0].ForeignKeyName, qt.Equals, "nodes_parent_fk")
}

func filterFixtureSchema() *dbschematypes.DBSchema {
	foreignTable := "users"
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{
				Schema: "auth",
				Name:   "users",
				Columns: []dbschematypes.DBColumn{
					{Name: "id"},
					{Name: "email"},
				},
			},
			{
				Schema: "auth",
				Name:   "audit_log",
				Columns: []dbschematypes.DBColumn{
					{Name: "id"},
					{Name: "user_id"},
					{Name: "created_at"},
				},
			},
		},
		Indexes: []dbschematypes.DBIndex{
			{Schema: "auth", TableName: "users", Name: "users_email_key", Columns: []string{"email"}},
			{Schema: "auth", TableName: "audit_log", Name: "audit_log_created_at_idx", Columns: []string{"created_at"}},
		},
		Constraints: []dbschematypes.DBConstraint{
			{Schema: "auth", TableName: "users", Name: "users_email_key", Type: "UNIQUE", ColumnNames: []string{"email"}},
			{Schema: "auth", TableName: "audit_log", Name: "audit_log_user_fk", Type: "FOREIGN KEY", ColumnNames: []string{"user_id"}, ForeignSchema: "auth", ForeignTable: &foreignTable},
		},
		Triggers: []dbschematypes.DBTrigger{
			{Schema: "auth", Table: "users", Name: "users_updated_at"},
			{Schema: "auth", Table: "audit_log", Name: "audit_log_updated_at"},
		},
		RLSPolicies: []dbschematypes.DBRLSPolicy{
			{Table: "auth.users", Name: "users_policy"},
			{Table: "auth.audit_log", Name: "audit_log_policy"},
		},
		Grants: []dbschematypes.DBGrant{
			{ObjectType: "TABLE", Schema: "auth", ObjectName: "users"},
			{ObjectType: "TABLE", Schema: "auth", ObjectName: "audit_log"},
		},
	}
}

func tableNames(tables []dbschematypes.DBTable) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.QualifiedName())
	}
	return names
}

func generatedTableNames(tables []goschema.Table) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.QualifiedName())
	}
	return names
}

func generatedFieldNames(fields []goschema.Field) []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.StructName+"."+field.Name)
	}
	return names
}

func generatedIndexNames(indexes []goschema.Index) []string {
	names := make([]string, 0, len(indexes))
	for _, index := range indexes {
		names = append(names, index.Name)
	}
	return names
}

func generatedConstraintNames(constraints []goschema.Constraint) []string {
	names := make([]string, 0, len(constraints))
	for _, constraint := range constraints {
		names = append(names, constraint.Name)
	}
	return names
}

func generatedTriggerNames(triggers []goschema.Trigger) []string {
	names := make([]string, 0, len(triggers))
	for _, trigger := range triggers {
		names = append(names, trigger.Name)
	}
	return names
}

func generatedGrantTargets(grants []goschema.Grant) []string {
	names := make([]string, 0, len(grants))
	for _, grant := range grants {
		names = append(names, grant.OnTable)
	}
	return names
}

func columnNames(columns []dbschematypes.DBColumn) []string {
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name)
	}
	return names
}

func indexNames(indexes []dbschematypes.DBIndex) []string {
	names := make([]string, 0, len(indexes))
	for _, index := range indexes {
		names = append(names, index.Name)
	}
	return names
}

func constraintNames(constraints []dbschematypes.DBConstraint) []string {
	names := make([]string, 0, len(constraints))
	for _, constraint := range constraints {
		names = append(names, constraint.Name)
	}
	return names
}

func triggerNames(triggers []dbschematypes.DBTrigger) []string {
	names := make([]string, 0, len(triggers))
	for _, trigger := range triggers {
		names = append(names, trigger.Name)
	}
	return names
}

func policyNames(policies []dbschematypes.DBRLSPolicy) []string {
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.Name)
	}
	return names
}

func viewNames(views []dbschematypes.DBView) []string {
	names := make([]string, 0, len(views))
	for _, view := range views {
		names = append(names, view.QualifiedName())
	}
	return names
}

func extensionVersions(extensions []dbschematypes.DBExtension) []string {
	names := make([]string, 0, len(extensions))
	for _, extension := range extensions {
		names = append(names, extension.Name+":"+extension.Version)
	}
	return names
}

func grantTargets(grants []dbschematypes.DBGrant) []string {
	names := make([]string, 0, len(grants))
	for _, grant := range grants {
		names = append(names, grant.QualifiedTarget())
	}
	return names
}
