package shadow

// White-box testing required: these tests exercise shadow replay stages and
// deterministic mismatch collection, neither of which is observable on its own
// through the exported verification API. Public propagation is covered
// separately in verify_test.go.

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestDescribeDiffMissingColumn(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{
			{
				TableName:    "users",
				ColumnsAdded: difftypes.ColumnChanges{{Name: "email"}, {Name: "name"}},
			},
		},
	}

	c.Assert(describeDiff(diff), qt.Equals, "missing column users.email")
	mismatches := collectMismatches(diff)
	c.Assert(mismatches, qt.DeepEquals, []Mismatch{
		{
			Kind:    "missing_column",
			Object:  "users.email",
			Table:   "users",
			Column:  "email",
			Message: "missing column users.email",
		},
		{
			Kind:    "missing_column",
			Object:  "users.name",
			Table:   "users",
			Column:  "name",
			Message: "missing column users.name",
		},
	})
	c.Assert((&VerificationError{Result: VerificationResult{
		Stage:      "schema-match",
		Mismatches: mismatches,
	}}).Error(), qt.Equals, "shadow check failed: missing column users.email")
}

func TestDescribeChangesIsDeterministic(t *testing.T) {
	c := qt.New(t)

	got := describeChanges(map[string]string{
		"nullable": "true -> false",
		"type":     "text -> varchar",
	})

	c.Assert(got, qt.Equals, "nullable true -> false, type text -> varchar")
}

func TestCollectMismatchesCoversEverySchemaDiffCategory(t *testing.T) {
	c := qt.New(t)
	changes := map[string]string{"definition": "old -> new"}
	diff := &difftypes.SchemaDiff{
		TablesAdded:   difftypes.TableChanges{{Name: "missing_table"}},
		TablesRemoved: []string{"extra_table"},
		TablesModified: []difftypes.TableDiff{{
			TableName:          "changed_table",
			ColumnsAdded:       difftypes.ColumnChanges{{Name: "missing_column"}},
			ColumnsRemoved:     difftypes.ColumnChanges{{Name: "extra_column"}},
			ColumnsModified:    []difftypes.ColumnDiff{{ColumnName: "changed_column", Changes: changes}},
			ConstraintsAdded:   []string{"missing_table_constraint"},
			ConstraintsRemoved: []string{"extra_table_constraint"},
		}},
		EnumsAdded:   difftypes.EnumChanges{{Name: "missing_enum"}},
		EnumsRemoved: difftypes.EnumChanges{{Name: "extra_enum"}},
		EnumsModified: []difftypes.EnumDiff{{
			EnumName:      "changed_enum",
			ValuesAdded:   []string{"missing_value"},
			ValuesRemoved: []string{"extra_value"},
		}},
		IndexesAdded:      difftypes.IndexChanges{{Index: schemamodel.Index{Name: "missing_index", Fields: []string{"email"}}, TableName: "users"}},
		IndexesRemoved:    []difftypes.IndexRef{{TableName: "users", Name: "extra_index"}},
		ExtensionsAdded:   difftypes.ExtensionChanges{{Name: "missing_extension"}},
		ExtensionsRemoved: difftypes.ExtensionChanges{{Name: "extra_extension"}},
		ExtensionsModified: []difftypes.ExtensionDiff{{
			Name: "changed_extension", FromSchema: "public", ToSchema: "extensions",
		}},
		FunctionsAdded:            difftypes.FunctionChanges{{Function: schemamodel.Function{Name: "missing_function"}}},
		FunctionsRemoved:          difftypes.FunctionChanges{{Function: schemamodel.Function{Name: "extra_function"}}},
		FunctionsModified:         []difftypes.FunctionDiff{{FunctionName: "changed_function", Changes: changes}},
		SequencesAdded:            difftypes.SequenceChanges{{Name: "missing_sequence"}},
		SequencesRemoved:          difftypes.SequenceChanges{{Name: "extra_sequence"}},
		SequencesModified:         []difftypes.SequenceDiff{{SequenceName: "changed_sequence", Changes: changes}},
		DomainsAdded:              difftypes.DomainChanges{{Name: "missing_domain"}},
		DomainsRemoved:            difftypes.DomainChanges{{Name: "extra_domain"}},
		DomainsModified:           []difftypes.DomainDiff{{DomainName: "changed_domain", Changes: changes}},
		CompositeTypesAdded:       difftypes.CompositeTypeChanges{{Name: "missing_composite"}},
		CompositeTypesRemoved:     difftypes.CompositeTypeChanges{{Name: "extra_composite"}},
		CompositeTypesModified:    []difftypes.CompositeTypeDiff{{TypeName: "changed_composite", Changes: changes}},
		RangesAdded:               difftypes.RangeChanges{{Name: "missing_range"}},
		RangesRemoved:             difftypes.RangeChanges{{Name: "extra_range"}},
		ViewsAdded:                difftypes.ViewChanges{{Name: "missing_view"}},
		ViewsRemoved:              difftypes.ViewChanges{{Name: "extra_view"}},
		ViewsModified:             []difftypes.ViewDiff{{ViewName: "changed_view", Changes: changes}},
		MaterializedViewsAdded:    difftypes.MaterializedViewChanges{{Name: "missing_materialized_view"}},
		MaterializedViewsRemoved:  difftypes.MaterializedViewChanges{{Name: "extra_materialized_view"}},
		MaterializedViewsModified: []difftypes.MaterializedViewDiff{{ViewName: "changed_materialized_view", Changes: changes}},
		TriggersAdded:             []difftypes.TriggerRef{{TableName: "users", TriggerName: "missing_trigger"}},
		TriggersRemoved:           []difftypes.TriggerRef{{TableName: "users", TriggerName: "extra_trigger"}},
		TriggersModified:          []difftypes.TriggerDiff{{TableName: "users", TriggerName: "changed_trigger", Changes: changes}},
		RLSPoliciesAdded:          []difftypes.RLSPolicyRef{{TableName: "users", PolicyName: "missing_policy"}},
		RLSPoliciesRemoved:        []difftypes.RLSPolicyRef{{TableName: "users", PolicyName: "extra_policy"}},
		RLSPoliciesModified:       []difftypes.RLSPolicyDiff{{TableName: "users", PolicyName: "changed_policy", Changes: changes}},
		RLSEnabledTablesAdded:     difftypes.RLSEnabledTableChanges{{Table: "missing_rls_table"}},
		RLSEnabledTablesRemoved:   difftypes.RLSEnabledTableChanges{{Table: "extra_rls_table"}},
		RolesAdded:                difftypes.RoleChanges{{Name: "missing_role"}},
		RolesRemoved:              difftypes.RoleChanges{{Name: "extra_role"}},
		RolesModified:             []difftypes.RoleDiff{{RoleName: "changed_role", Changes: changes}},
		GrantsAdded:               []difftypes.GrantRef{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users"}},
		GrantsRemoved:             []difftypes.GrantRef{{Role: "app", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "users"}},
		GrantOptionsAdded:         []difftypes.GrantRef{{Role: "app", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "users"}},
		GrantOptionsRevoked:       []difftypes.GrantRef{{Role: "app", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "users"}},
		ConstraintsAdded: []difftypes.ConstraintAdditionInfo{{
			Name:      "missing_global_constraint",
			TableName: "accounts",
		}},
		ConstraintsRemoved: []difftypes.ConstraintRemovalInfo{{
			Name:      "extra_global_constraint",
			TableName: "accounts",
		}},
	}

	mismatches := collectMismatches(diff)
	c.Assert(mismatchKinds(mismatches), qt.DeepEquals, []string{
		"missing_table",
		"extra_table",
		"missing_column",
		"missing_constraint",
		"column_mismatch",
		"extra_column",
		"extra_constraint",
		"missing_enum",
		"extra_enum",
		"missing_enum_value",
		"extra_enum_value",
		"missing_index",
		"extra_index",
		"missing_extension",
		"extra_extension",
		"extension_mismatch",
		"missing_function",
		"extra_function",
		"function_mismatch",
		"missing_sequence",
		"extra_sequence",
		"sequence_mismatch",
		"missing_domain",
		"extra_domain",
		"domain_mismatch",
		"missing_composite_type",
		"extra_composite_type",
		"composite_type_mismatch",
		"missing_range",
		"extra_range",
		"missing_view",
		"extra_view",
		"view_mismatch",
		"missing_materialized_view",
		"extra_materialized_view",
		"materialized_view_mismatch",
		"missing_trigger",
		"extra_trigger",
		"trigger_mismatch",
		"missing_rls_policy",
		"extra_rls_policy",
		"rls_policy_mismatch",
		"missing_rls_enablement",
		"extra_rls_enablement",
		"missing_role",
		"extra_role",
		"role_mismatch",
		"missing_grant",
		"extra_grant",
		"missing_grant_option",
		"extra_grant_option",
		"missing_constraint",
		"extra_constraint",
	})
	c.Assert(mismatches[len(mismatches)-2].Object, qt.Equals, "accounts.missing_global_constraint")
	c.Assert(mismatches[len(mismatches)-2].Table, qt.Equals, "accounts")
	c.Assert(mismatches[len(mismatches)-1].Object, qt.Equals, "accounts.extra_global_constraint")
	c.Assert(mismatches[len(mismatches)-1].Table, qt.Equals, "accounts")
	c.Assert(mismatches[15], qt.DeepEquals, Mismatch{
		Kind:    "extension_mismatch",
		Object:  "changed_extension",
		Changes: map[string]string{"schema": "public -> extensions"},
		Message: "extension mismatch changed_extension: schema public -> extensions",
	})
}

func mismatchKinds(mismatches []Mismatch) []string {
	kinds := make([]string, len(mismatches))
	for index, mismatch := range mismatches {
		kinds[index] = mismatch.Kind
	}
	return kinds
}

// TestCollectMismatchesNamesTheTableOwningAnRLSPolicy pins the shape of
// the two policy-reference mismatches, not only their kinds. Mismatch is
// serialized into the shadow verification report, so a reader of that JSON is
// told which policy is missing and which table it belongs to. Reporting a bare
// name could not distinguish two tables that each carry a policy called
// tenant_isolation, which PostgreSQL permits (stokaro/ptah#1276).
//
// Ordering is part of the contract too: the refs are sorted by table first, so
// alpha_orders leads zeta_orders regardless of the order the comparison put
// them in.
func TestCollectMismatchesNamesTheTableOwningAnRLSPolicy(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		RLSPoliciesAdded: []difftypes.RLSPolicyRef{
			{TableName: "zeta_orders", PolicyName: "tenant_isolation"},
			{TableName: "alpha_orders", PolicyName: "tenant_isolation"},
		},
		RLSPoliciesRemoved: []difftypes.RLSPolicyRef{
			{TableName: "zeta_orders", PolicyName: "legacy_isolation"},
		},
	}

	c.Assert(collectMismatches(diff), qt.DeepEquals, []Mismatch{
		{
			Kind:    "missing_rls_policy",
			Table:   "alpha_orders",
			Object:  "alpha_orders.tenant_isolation",
			Message: "missing RLS policy alpha_orders.tenant_isolation",
		},
		{
			Kind:    "missing_rls_policy",
			Table:   "zeta_orders",
			Object:  "zeta_orders.tenant_isolation",
			Message: "missing RLS policy zeta_orders.tenant_isolation",
		},
		{
			Kind:    "extra_rls_policy",
			Table:   "zeta_orders",
			Object:  "zeta_orders.legacy_isolation",
			Message: "extra RLS policy zeta_orders.legacy_isolation",
		},
	})
}

func TestVerifyShadowMigrationConnectErrorIsStructured(t *testing.T) {
	c := qt.New(t)

	err := VerifyMigration(t.Context(), MigrationVerifyOptions{
		ShadowDatabaseURL: "not-a-dsn",
		Dialect:           "postgres",
	})

	var shadowErr *VerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result.Stage, qt.Equals, "connect")
	c.Assert(shadowErr.Result.Mismatches, qt.HasLen, 1)
	c.Assert(shadowErr.Result.Mismatches[0].Kind, qt.Equals, "connect_error")
	c.Assert(shadowErr.Err, qt.IsNotNil)
	c.Assert(err, qt.ErrorMatches, `shadow check failed: connect to shadow database: invalid database URL: missing scheme`)
}

func TestVerifyMigration_ReplayHonorsCallerCancellation(t *testing.T) {
	c := qt.New(t)
	target, err := dbschema.ConnectToDatabase(
		t.Context(),
		"sqlite://"+filepath.Join(t.TempDir(), "target.db"),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	err = VerifyMigration(ctx, MigrationVerifyOptions{
		ShadowDatabaseURL: "sqlite://" + filepath.Join(t.TempDir(), "shadow.db"),
		TargetConnection:  target,
		Dialect:           platform.SQLite,
		Candidates: []Candidate{{
			Version: 1,
			Name:    "long replay",
			UpSQL: `WITH RECURSIVE counter(value) AS (
				VALUES (0)
				UNION ALL
				SELECT value + 1 FROM counter WHERE value < 100000000
			) SELECT sum(value) FROM counter;`,
			DownSQL: "SELECT 1;",
		}},
	})

	var shadowErr *VerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result.Stage, qt.Equals, "replay")
	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
}
