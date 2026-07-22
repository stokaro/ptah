package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

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
