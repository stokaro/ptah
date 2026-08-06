package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

func TestScopePositive(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		scope atlasfilter.Scope
		want  bool
	}{
		{name: "zero scope", scope: atlasfilter.Scope{}, want: false},
		{name: "exclude only", scope: atlasfilter.Scope{Exclude: []string{"users"}}, want: false},
		{name: "blank values", scope: atlasfilter.Scope{Schemas: []string{" ", ","}, Include: []string{"  "}}, want: false},
		{name: "schema scope", scope: atlasfilter.Scope{Schemas: []string{"public"}}, want: true},
		{name: "include selector", scope: atlasfilter.Scope{Include: []string{"users"}}, want: true},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(test.scope.Positive(), qt.Equals, test.want)
		})
	}
}

func TestValidateIncludeSelectors_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		values []string
	}{
		{name: "empty", values: nil},
		{name: "bare glob", values: []string{"users"}},
		{name: "wildcard", values: []string{"users_*"}},
		{name: "qualified", values: []string{"public.users"}},
		{name: "comma separated", values: []string{"users,groups"}},
		{name: "type selector", values: []string{"users[type=table]"}},
		{name: "type union", values: []string{"*[type=view|materialized_view]"}},
		{name: "blank entries", values: []string{" ", ","}},
		{name: "wildcard schema qualifier", values: []string{"*.users"}},
		{name: "qualified type selector", values: []string{"public.users[type=table]"}},
		// A dotted identifier is quoted in the qualified candidate
		// (`main."my.table"`), so the selector that matches it carries two dot
		// characters but only one separator. These two spellings really do
		// select such a table.
		{name: "qualified dotted identifier", values: []string{`main."my.table"`}},
		{name: "wildcard schema dotted identifier", values: []string{`*."my.table"`}},
		// tableref.Canonical emits double quotes and never these forms, and
		// path.Match reads `[my.table]` as a character class rather than a
		// quoted identifier, so neither spelling selects a table named
		// "my.table". They are kept because both were arms of the deleted
		// shape check and each must stay parseable.
		{name: "backtick dotted identifier", values: []string{"main.`my.table`"}},
		{name: "bracket dotted identifier", values: []string{"main.[my.table]"}},
		// path.Match reads "\." as a literal dot, so this selects the single
		// bare name "a.b.c".
		{name: "escaped dots", values: []string{`a\.b\.c`}},
		// The bare spelling of the same name. The deleted shape check refused
		// it as "child depth" even though a table can be literally named
		// a.b.c, and a shape check cannot tell the two apart. Whether it
		// selects anything is now decided by the projection, which is the only
		// place the answer exists.
		{name: "bare dotted name", values: []string{"a.b.c"}},
		{name: "bare dotted wildcard", values: []string{"main.t1.*"}},
		{name: "bare dotted name with type selector", values: []string{"main.t1.*[type=table]"}},
		{name: "bare dotted name past a quoted identifier", values: []string{`main."my.table".id`}},
		{name: "bare dotted name past a bracketed identifier", values: []string{"main.[ab].id"}},
		{name: "bare dotted name past a backticked identifier", values: []string{"main.`ab`.id"}},
		{name: "bare dotted name in a comma separated list", values: []string{"users,main.t1.id"}},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(atlasfilter.ValidateIncludeSelectors(test.values), qt.IsNil)
		})
	}
}

func TestValidateIncludeSelectors_FailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		values  []string
		wantErr string
	}{
		{
			name:    "invalid glob",
			values:  []string{"[x"},
			wantErr: `invalid Atlas include glob "\[x": syntax error in pattern`,
		},
		{
			name:    "column type",
			values:  []string{"users.email[type=column]"},
			wantErr: `unsupported Atlas include selector "users\.email\[type=column\]": column resources ride along with their parent and cannot be included on their own`,
		},
		{
			name:    "index type",
			values:  []string{"*[type=index]"},
			wantErr: `unsupported Atlas include selector "\*\[type=index\]": index resources ride along with their parent and cannot be included on their own`,
		},
		{
			name:    "grant type",
			values:  []string{"*[type=grant]"},
			wantErr: `unsupported Atlas include selector "\*\[type=grant\]": grant resources ride along with their parent and cannot be included on their own`,
		},
		{
			name:    "schema type",
			values:  []string{"public[type=schema]"},
			wantErr: `unsupported Atlas include selector "public\[type=schema\]": use --schema to select schemas`,
		},
		{
			name:    "unknown type",
			values:  []string{"*[type=widget]"},
			wantErr: `unsupported Atlas include resource type "widget" in selector "\*\[type=widget\]"`,
		},
		{
			name:    "field selector",
			values:  []string{"*[type=extension].version"},
			wantErr: `unsupported Atlas include field selector "\.version"`,
		},
		{
			name:    "selector-like suffix",
			values:  []string{"users[foo=bar]"},
			wantErr: `unsupported Atlas include selector "foo=bar"`,
		},
		{
			name:    "empty type selector",
			values:  []string{"*[type=]"},
			wantErr: `empty Atlas include type selector "type="`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(atlasfilter.ValidateIncludeSelectors(test.values), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestExcludeSelectorsReachChildResources pins that exclusion keeps parsing
// the child-resource spellings it legitimately reaches. This asserts parsing
// only, and only at the pre-connect layer, which does not yet know the schema
// the patterns are relative to. Whether a spelling then survives the filter is
// a separate question of depth: "table.child" names a child of the connection's
// schema, while "schema.table.child" is one part too deep for that scope and is
// refused there — see
// TestExcludeDatabaseWithDefaultSchema_RefusesPatternsDeeperThanTheScope.
func TestExcludeSelectorsReachChildResources(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		values []string
	}{
		{name: "table child", values: []string{"users.email"}},
		{name: "qualified table child", values: []string{"public.users.email"}},
		{name: "qualified table child wildcard", values: []string{"public.users.*"}},
		{name: "qualified child with type selector", values: []string{"public.users.*[type=column]"}},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(atlasfilter.ValidateExcludeSelectors(test.values), qt.IsNil)
		})
	}
}

// scopeGeneratedFixture models two schemas with a rich set of dependent
// objects: app.users carries an index, a trigger, an RLS policy, a grant, an
// enum-typed column, and an owned sequence; app.audit_log references
// app.users via a foreign key; billing.invoices lives in another schema.
func scopeGeneratedFixture() *goschema.Database {
	return &goschema.Database{
		Schemas: []goschema.Schema{{Name: "app"}, {Name: "billing"}},
		Tables: []goschema.Table{
			{StructName: "User", Schema: "app", Name: "users"},
			{StructName: "AuditLog", Schema: "app", Name: "audit_log"},
			{StructName: "Invoice", Schema: "billing", Name: "invoices"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER"},
			{StructName: "User", Name: "status", Type: "user_status"},
			{StructName: "AuditLog", Name: "id", Type: "INTEGER"},
			{StructName: "AuditLog", Name: "user_id", Type: "INTEGER", Foreign: "users(id)"},
			{StructName: "Invoice", Name: "id", Type: "INTEGER"},
		},
		Indexes: []goschema.Index{
			{StructName: "User", Name: "users_status_idx", Fields: []string{"status"}},
			{StructName: "AuditLog", Name: "audit_log_user_idx", Fields: []string{"user_id"}},
		},
		Enums: []goschema.Enum{
			{Name: "user_status", Values: []string{"active", "inactive"}},
			{Name: "invoice_status", Values: []string{"open", "paid"}},
		},
		Sequences: []goschema.Sequence{
			{StructName: "UserSeq", Schema: "app", Name: "user_number_seq", OwnedBy: "users.id"},
			{StructName: "InvoiceSeq", Schema: "billing", Name: "invoice_number_seq"},
		},
		Triggers: []goschema.Trigger{
			{StructName: "User", Table: "users", Name: "users_updated_at", Body: "NEW.updated_at = NOW(); RETURN NEW;"},
		},
		RLSPolicies: []goschema.RLSPolicy{
			{StructName: "User", Table: "users", Name: "users_policy"},
		},
		RLSEnabledTables: []goschema.RLSEnabledTable{
			{StructName: "User", Table: "users"},
		},
		Roles: []goschema.Role{
			{Name: "app_user"},
			{Name: "billing_user"},
		},
		Grants: []goschema.Grant{
			{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "users"},
			{Role: "billing_user", Privileges: []string{"SELECT"}, OnTable: "invoices"},
		},
	}
}

func TestScopeGenerated_SchemaUniverse(t *testing.T) {
	c := qt.New(t)
	schema := scopeGeneratedFixture()

	got, err := atlasfilter.ScopeGenerated(schema, atlasfilter.Scope{
		Schemas:       []string{"billing"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"billing.invoices"})
	c.Assert(generatedFieldNames(got.Fields), qt.DeepEquals, []string{"Invoice.id"})
	c.Assert(generatedSequenceNames(got.Sequences), qt.DeepEquals, []string{"billing.invoice_number_seq"})
	c.Assert(generatedSchemaNames(got.Schemas), qt.DeepEquals, []string{"billing"})
	// Finalize qualifies grant targets with the owning table's schema.
	c.Assert(generatedGrantTargets(got.Grants), qt.DeepEquals, []string{"billing.invoices"})
	c.Assert(generatedTableNames(schema.Tables), qt.HasLen, 3)
}

func TestScopeGenerated_IncludeTableRideAlongs(t *testing.T) {
	c := qt.New(t)
	schema := scopeGeneratedFixture()

	got, err := atlasfilter.ScopeGenerated(schema, atlasfilter.Scope{
		Include:       []string{"users"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"app.users"})
	c.Assert(generatedFieldNames(got.Fields), qt.DeepEquals, []string{"User.id", "User.status"})
	c.Assert(generatedIndexNames(got.Indexes), qt.DeepEquals, []string{"users_status_idx"})
	c.Assert(generatedTriggerNames(got.Triggers), qt.DeepEquals, []string{"users_updated_at"})
	c.Assert(got.RLSPolicies, qt.HasLen, 1)
	c.Assert(got.RLSEnabledTables, qt.HasLen, 1)
	// Finalize qualifies grant targets with the owning table's schema.
	c.Assert(generatedGrantTargets(got.Grants), qt.DeepEquals, []string{"app.users"})
	// Support objects ride along: the enum used by users.status, the sequence
	// owned by users, the role named by the kept grant, and the owning schema.
	c.Assert(generatedEnumNames(got.Enums), qt.DeepEquals, []string{"user_status"})
	c.Assert(generatedSequenceNames(got.Sequences), qt.DeepEquals, []string{"app.user_number_seq"})
	c.Assert(generatedRoleNames(got.Roles), qt.DeepEquals, []string{"app_user"})
	c.Assert(generatedSchemaNames(got.Schemas), qt.DeepEquals, []string{"app"})
}

func TestScopeGenerated_IncludeSelectsInsideSchemaUniverse(t *testing.T) {
	c := qt.New(t)

	c.Run("selector inside the universe matches", func(c *qt.C) {
		got, err := atlasfilter.ScopeGenerated(scopeGeneratedFixture(), atlasfilter.Scope{
			Schemas:       []string{"app"},
			Include:       []string{"users"},
			DefaultSchema: "public",
		})
		c.Assert(err, qt.IsNil)
		c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"app.users"})
	})

	c.Run("selector outside the universe matches nothing", func(c *qt.C) {
		got, err := atlasfilter.ScopeGenerated(scopeGeneratedFixture(), atlasfilter.Scope{
			Schemas:       []string{"app"},
			Include:       []string{"invoices"},
			DefaultSchema: "public",
		})
		// The projection is empty and says so. billing.invoices exists in the
		// fixture but sits outside the schema universe, so the selector is
		// evaluated against nothing and never counts as a match.
		c.Assert(err, qt.ErrorMatches, `the --include selection matched no objects: "invoices"`)
		c.Assert(got.Tables, qt.HasLen, 0)
	})
}

func TestScopeGenerated_UnionIsDeterministic(t *testing.T) {
	c := qt.New(t)

	first, err := atlasfilter.ScopeGenerated(scopeGeneratedFixture(), atlasfilter.Scope{
		Include:       []string{"users", "audit_log"},
		DefaultSchema: "public",
	})
	c.Assert(err, qt.IsNil)
	second, err := atlasfilter.ScopeGenerated(scopeGeneratedFixture(), atlasfilter.Scope{
		Include:       []string{"audit_log,users"},
		DefaultSchema: "public",
	})
	c.Assert(err, qt.IsNil)

	c.Assert(first, qt.DeepEquals, second)
	c.Assert(generatedTableNames(first.Tables), qt.DeepEquals, []string{"app.users", "app.audit_log"})
}

func TestScopeGenerated_ExcludeSubtractsFromSelection(t *testing.T) {
	c := qt.New(t)

	got, err := atlasfilter.ScopeGenerated(scopeGeneratedFixture(), atlasfilter.Scope{
		Include:       []string{"users", "invoices"},
		Exclude:       []string{"invoices"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"app.users"})
}

func TestScopeGenerated_EmptyMatchProjectsNothing(t *testing.T) {
	c := qt.New(t)

	got, err := atlasfilter.ScopeGenerated(scopeGeneratedFixture(), atlasfilter.Scope{
		Include:       []string{"does_not_exist"},
		DefaultSchema: "public",
	})

	// The empty projection is still returned, and it is still empty in every
	// object kind — callers that tolerate an empty selection keep using it.
	c.Assert(err, qt.ErrorMatches, `the --include selection matched no objects: "does_not_exist"`)
	c.Assert(got.Tables, qt.HasLen, 0)
	c.Assert(got.Fields, qt.HasLen, 0)
	c.Assert(got.Enums, qt.HasLen, 0)
	c.Assert(got.Sequences, qt.HasLen, 0)
	c.Assert(got.Grants, qt.HasLen, 0)
	c.Assert(got.Roles, qt.HasLen, 0)
	c.Assert(got.Schemas, qt.HasLen, 0)
}

func TestScopeGenerated_NonPositiveDelegatesToExclude(t *testing.T) {
	c := qt.New(t)

	scoped, err := atlasfilter.ScopeGenerated(scopeGeneratedFixture(), atlasfilter.Scope{
		Exclude: []string{"app.audit_log"},
	})
	c.Assert(err, qt.IsNil)
	excluded, err := atlasfilter.ExcludeGenerated(scopeGeneratedFixture(), []string{"app.audit_log"})
	c.Assert(err, qt.IsNil)

	c.Assert(scoped, qt.DeepEquals, excluded)
}

func TestScopeGenerated_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("foreign key to unselected table", func(c *qt.C) {
		got, err := atlasfilter.ScopeGenerated(scopeGeneratedFixture(), atlasfilter.Scope{
			Include:       []string{"audit_log"},
			DefaultSchema: "public",
		})
		c.Assert(got, qt.IsNil)
		var crossScope *atlasfilter.CrossScopeError
		c.Assert(err, qt.ErrorAs, &crossScope)
		c.Assert(crossScope.Diagnostics, qt.DeepEquals, []string{
			`table "app.audit_log" depends on table "app.users" via a foreign key, but "app.users" is not selected`,
		})
	})

	c.Run("constraint foreign key to unselected table", func(c *qt.C) {
		schema := &goschema.Database{
			Tables: []goschema.Table{
				{StructName: "Order", Name: "orders"},
				{StructName: "User", Name: "users"},
			},
			Fields: []goschema.Field{
				{StructName: "Order", Name: "id", Type: "INTEGER"},
				{StructName: "User", Name: "id", Type: "INTEGER"},
			},
			Constraints: []goschema.Constraint{
				{StructName: "Order", Name: "orders_user_fk", Type: "FOREIGN KEY", Columns: []string{"user_id"}, ForeignTable: "users", ForeignColumn: "id"},
			},
		}
		_, err := atlasfilter.ScopeGenerated(schema, atlasfilter.Scope{Include: []string{"orders"}})
		var crossScope *atlasfilter.CrossScopeError
		c.Assert(err, qt.ErrorAs, &crossScope)
		c.Assert(crossScope.Diagnostics, qt.DeepEquals, []string{
			`table "orders" depends on table "users" via a foreign key, but "users" is not selected`,
		})
	})

	c.Run("function depending on unselected function", func(c *qt.C) {
		schema := &goschema.Database{
			Functions: []goschema.Function{
				{Name: "outer_fn"},
				{Name: "inner_fn"},
			},
			FunctionDependencies: map[string][]string{
				"outer_fn": {"inner_fn"},
			},
		}
		_, err := atlasfilter.ScopeGenerated(schema, atlasfilter.Scope{Include: []string{"outer_fn"}})
		var crossScope *atlasfilter.CrossScopeError
		c.Assert(err, qt.ErrorAs, &crossScope)
		c.Assert(crossScope.Diagnostics, qt.DeepEquals, []string{
			`function "outer_fn" depends on function "inner_fn", but "inner_fn" is not selected`,
		})
	})

	c.Run("view referencing unselected table", func(c *qt.C) {
		schema := &goschema.Database{
			Tables: []goschema.Table{{StructName: "User", Name: "users"}},
			Fields: []goschema.Field{{StructName: "User", Name: "id", Type: "INTEGER"}},
			Views:  []goschema.View{{Name: "active_users", Body: "SELECT * FROM users WHERE active"}},
		}
		_, err := atlasfilter.ScopeGenerated(schema, atlasfilter.Scope{Include: []string{"active_users"}})
		var crossScope *atlasfilter.CrossScopeError
		c.Assert(err, qt.ErrorAs, &crossScope)
		c.Assert(crossScope.Diagnostics, qt.DeepEquals, []string{
			`view "active_users" references "users", but "users" is not selected`,
		})
	})

	c.Run("excluded enum still used by selected table", func(c *qt.C) {
		_, err := atlasfilter.ScopeGenerated(scopeGeneratedFixture(), atlasfilter.Scope{
			Include:       []string{"users"},
			Exclude:       []string{"user_status"},
			DefaultSchema: "public",
		})
		var crossScope *atlasfilter.CrossScopeError
		c.Assert(err, qt.ErrorAs, &crossScope)
		c.Assert(crossScope.Diagnostics, qt.DeepEquals, []string{
			`selected tables use enum "public.user_status", but "public.user_status" is not selected`,
		})
	})
}

// scopeDatabaseFixture mirrors scopeGeneratedFixture for the introspected
// side.
func scopeDatabaseFixture() *dbschematypes.DBSchema {
	usersTable := "users"
	return &dbschematypes.DBSchema{
		Schemas: []dbschematypes.DBSchemaInfo{{Name: "app"}, {Name: "billing"}},
		Tables: []dbschematypes.DBTable{
			{
				Schema: "app",
				Name:   "users",
				Columns: []dbschematypes.DBColumn{
					{Name: "id", DataType: "integer"},
					{Name: "status", DataType: "USER-DEFINED", UDTName: "user_status"},
				},
			},
			{
				Schema: "app",
				Name:   "audit_log",
				Columns: []dbschematypes.DBColumn{
					{Name: "id", DataType: "integer"},
					{Name: "user_id", DataType: "integer"},
				},
			},
			{
				Schema: "billing",
				Name:   "invoices",
				Columns: []dbschematypes.DBColumn{
					{Name: "id", DataType: "integer"},
				},
			},
		},
		Enums: []dbschematypes.DBEnum{
			{Name: "user_status", Values: []string{"active", "inactive"}},
			{Name: "invoice_status", Values: []string{"open", "paid"}},
		},
		Indexes: []dbschematypes.DBIndex{
			{Schema: "app", TableName: "users", Name: "users_status_idx", Columns: []string{"status"}},
			{Schema: "app", TableName: "audit_log", Name: "audit_log_user_idx", Columns: []string{"user_id"}},
		},
		Constraints: []dbschematypes.DBConstraint{
			{Schema: "app", TableName: "audit_log", Name: "audit_log_user_fk", Type: "FOREIGN KEY", ColumnNames: []string{"user_id"}, ForeignSchema: "app", ForeignTable: &usersTable},
		},
		Sequences: []dbschematypes.DBSequence{
			{Schema: "app", Name: "user_number_seq", OwnedBy: "users.id"},
			{Schema: "billing", Name: "invoice_number_seq"},
		},
		Triggers: []dbschematypes.DBTrigger{
			{Schema: "app", Table: "users", Name: "users_updated_at"},
		},
		RLSPolicies: []dbschematypes.DBRLSPolicy{
			{Table: "app.users", Name: "users_policy"},
		},
		Roles: []dbschematypes.DBRole{
			{Name: "app_user"},
			{Name: "billing_user"},
		},
		Grants: []dbschematypes.DBGrant{
			{Role: "app_user", ObjectType: "TABLE", Schema: "app", ObjectName: "users"},
			{Role: "billing_user", ObjectType: "TABLE", Schema: "billing", ObjectName: "invoices"},
		},
	}
}

func TestScopeDatabase_SchemaUniverse(t *testing.T) {
	c := qt.New(t)
	schema := scopeDatabaseFixture()

	got, err := atlasfilter.ScopeDatabase(schema, atlasfilter.Scope{
		Schemas:       []string{"billing"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"billing.invoices"})
	c.Assert(databaseSequenceNames(got.Sequences), qt.DeepEquals, []string{"billing.invoice_number_seq"})
	c.Assert(databaseSchemaNames(got.Schemas), qt.DeepEquals, []string{"billing"})
	c.Assert(grantTargets(got.Grants), qt.DeepEquals, []string{"billing.invoices"})
	c.Assert(tableNames(schema.Tables), qt.HasLen, 3)
}

func TestScopeDatabase_IncludeTableRideAlongs(t *testing.T) {
	c := qt.New(t)

	got, err := atlasfilter.ScopeDatabase(scopeDatabaseFixture(), atlasfilter.Scope{
		Include:       []string{"users"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(got.Tables), qt.DeepEquals, []string{"app.users"})
	c.Assert(indexNames(got.Indexes), qt.DeepEquals, []string{"users_status_idx"})
	c.Assert(triggerNames(got.Triggers), qt.DeepEquals, []string{"users_updated_at"})
	c.Assert(policyNames(got.RLSPolicies), qt.DeepEquals, []string{"users_policy"})
	c.Assert(grantTargets(got.Grants), qt.DeepEquals, []string{"app.users"})
	c.Assert(databaseEnumNames(got.Enums), qt.DeepEquals, []string{"user_status"})
	c.Assert(databaseSequenceNames(got.Sequences), qt.DeepEquals, []string{"app.user_number_seq"})
	c.Assert(databaseRoleNames(got.Roles), qt.DeepEquals, []string{"app_user"})
	c.Assert(databaseSchemaNames(got.Schemas), qt.DeepEquals, []string{"app"})
}

func TestScopeDatabase_EmptyMatchProjectsNothing(t *testing.T) {
	c := qt.New(t)

	got, err := atlasfilter.ScopeDatabase(scopeDatabaseFixture(), atlasfilter.Scope{
		Include:       []string{"does_not_exist"},
		DefaultSchema: "public",
	})

	c.Assert(err, qt.ErrorMatches, `the --include selection matched no objects: "does_not_exist"`)
	c.Assert(got.Tables, qt.HasLen, 0)
	c.Assert(got.Enums, qt.HasLen, 0)
	c.Assert(got.Sequences, qt.HasLen, 0)
	c.Assert(got.Grants, qt.HasLen, 0)
	c.Assert(got.Roles, qt.HasLen, 0)
	c.Assert(got.Schemas, qt.HasLen, 0)
}

func TestScopeDatabase_NonPositiveDelegatesToExclude(t *testing.T) {
	c := qt.New(t)

	scoped, err := atlasfilter.ScopeDatabase(scopeDatabaseFixture(), atlasfilter.Scope{
		Exclude: []string{"app.audit_log"},
	})
	c.Assert(err, qt.IsNil)
	excluded, err := atlasfilter.ExcludeDatabase(scopeDatabaseFixture(), []string{"app.audit_log"})
	c.Assert(err, qt.IsNil)

	c.Assert(scoped, qt.DeepEquals, excluded)
}

func TestScopeDatabase_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("foreign key to unselected table", func(c *qt.C) {
		got, err := atlasfilter.ScopeDatabase(scopeDatabaseFixture(), atlasfilter.Scope{
			Include:       []string{"audit_log"},
			DefaultSchema: "public",
		})
		c.Assert(got, qt.IsNil)
		var crossScope *atlasfilter.CrossScopeError
		c.Assert(err, qt.ErrorAs, &crossScope)
		c.Assert(crossScope.Diagnostics, qt.DeepEquals, []string{
			`table "app.audit_log" depends on table "app.users" via a foreign key, but "app.users" is not selected`,
		})
	})

	c.Run("excluded enum still used by selected table", func(c *qt.C) {
		_, err := atlasfilter.ScopeDatabase(scopeDatabaseFixture(), atlasfilter.Scope{
			Include:       []string{"users"},
			Exclude:       []string{"user_status"},
			DefaultSchema: "public",
		})
		var crossScope *atlasfilter.CrossScopeError
		c.Assert(err, qt.ErrorAs, &crossScope)
		c.Assert(crossScope.Diagnostics, qt.DeepEquals, []string{
			`selected tables use enum "public.user_status", but "public.user_status" is not selected`,
		})
	})
}

func generatedSchemaNames(schemas []goschema.Schema) []string {
	names := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		names = append(names, schema.Name)
	}
	return names
}

func generatedEnumNames(enums []goschema.Enum) []string {
	names := make([]string, 0, len(enums))
	for _, enum := range enums {
		names = append(names, enum.Name)
	}
	return names
}

func generatedSequenceNames(sequences []goschema.Sequence) []string {
	names := make([]string, 0, len(sequences))
	for _, sequence := range sequences {
		names = append(names, sequence.QualifiedName())
	}
	return names
}

func generatedRoleNames(roles []goschema.Role) []string {
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}
	return names
}

func databaseSchemaNames(schemas []dbschematypes.DBSchemaInfo) []string {
	names := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		names = append(names, schema.Name)
	}
	return names
}

func databaseEnumNames(enums []dbschematypes.DBEnum) []string {
	names := make([]string, 0, len(enums))
	for _, enum := range enums {
		names = append(names, enum.Name)
	}
	return names
}

func databaseSequenceNames(sequences []dbschematypes.DBSequence) []string {
	names := make([]string, 0, len(sequences))
	for _, sequence := range sequences {
		names = append(names, sequence.QualifiedName())
	}
	return names
}

func databaseRoleNames(roles []dbschematypes.DBRole) []string {
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}
	return names
}
