package generator

// White-box testing required: these tests exercise shadow replay stages,
// deterministic mismatch collection, and migration-version helpers that are
// not observable independently through the exported generation API. Public
// propagation is covered separately in shadow_external_test.go.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestDescribeShadowDiffMissingColumn(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{
			{
				TableName:    "users",
				ColumnsAdded: []string{"email", "name"},
			},
		},
	}

	c.Assert(describeShadowDiff(diff), qt.Equals, "missing column users.email")
	mismatches := collectShadowMismatches(diff)
	c.Assert(mismatches, qt.DeepEquals, []ShadowMismatch{
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
	c.Assert((&ShadowVerificationError{Result: ShadowVerificationResult{
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

func TestCollectShadowMismatchesCoversEverySchemaDiffCategory(t *testing.T) {
	c := qt.New(t)
	changes := map[string]string{"definition": "old -> new"}
	diff := &types.SchemaDiff{
		TablesAdded:   []string{"missing_table"},
		TablesRemoved: []string{"extra_table"},
		TablesModified: []types.TableDiff{{
			TableName:          "changed_table",
			ColumnsAdded:       []string{"missing_column"},
			ColumnsRemoved:     []string{"extra_column"},
			ColumnsModified:    []types.ColumnDiff{{ColumnName: "changed_column", Changes: changes}},
			ConstraintsAdded:   []string{"missing_table_constraint"},
			ConstraintsRemoved: []string{"extra_table_constraint"},
		}},
		EnumsAdded:   []string{"missing_enum"},
		EnumsRemoved: []string{"extra_enum"},
		EnumsModified: []types.EnumDiff{{
			EnumName:      "changed_enum",
			ValuesAdded:   []string{"missing_value"},
			ValuesRemoved: []string{"extra_value"},
		}},
		IndexesAdded:              []types.IndexRef{{TableName: "users", Name: "missing_index"}},
		IndexesRemoved:            []types.IndexRef{{TableName: "users", Name: "extra_index"}},
		ExtensionsAdded:           []string{"missing_extension"},
		ExtensionsRemoved:         []string{"extra_extension"},
		FunctionsAdded:            []string{"missing_function"},
		FunctionsRemoved:          []string{"extra_function"},
		FunctionsModified:         []types.FunctionDiff{{FunctionName: "changed_function", Changes: changes}},
		SequencesAdded:            []string{"missing_sequence"},
		SequencesRemoved:          []string{"extra_sequence"},
		SequencesModified:         []types.SequenceDiff{{SequenceName: "changed_sequence", Changes: changes}},
		DomainsAdded:              []string{"missing_domain"},
		DomainsRemoved:            []string{"extra_domain"},
		DomainsModified:           []types.DomainDiff{{DomainName: "changed_domain", Changes: changes}},
		CompositeTypesAdded:       []string{"missing_composite"},
		CompositeTypesRemoved:     []string{"extra_composite"},
		CompositeTypesModified:    []types.CompositeTypeDiff{{TypeName: "changed_composite", Changes: changes}},
		RangesAdded:               []string{"missing_range"},
		RangesRemoved:             []string{"extra_range"},
		ViewsAdded:                []string{"missing_view"},
		ViewsRemoved:              []string{"extra_view"},
		ViewsModified:             []types.ViewDiff{{ViewName: "changed_view", Changes: changes}},
		MaterializedViewsAdded:    []string{"missing_materialized_view"},
		MaterializedViewsRemoved:  []string{"extra_materialized_view"},
		MaterializedViewsModified: []types.MaterializedViewDiff{{ViewName: "changed_materialized_view", Changes: changes}},
		TriggersAdded:             []types.TriggerRef{{TableName: "users", TriggerName: "missing_trigger"}},
		TriggersRemoved:           []types.TriggerRef{{TableName: "users", TriggerName: "extra_trigger"}},
		TriggersModified:          []types.TriggerDiff{{TableName: "users", TriggerName: "changed_trigger", Changes: changes}},
		RLSPoliciesAdded:          []types.RLSPolicyRef{{TableName: "users", PolicyName: "missing_policy"}},
		RLSPoliciesRemoved:        []types.RLSPolicyRef{{TableName: "users", PolicyName: "extra_policy"}},
		RLSPoliciesModified:       []types.RLSPolicyDiff{{TableName: "users", PolicyName: "changed_policy", Changes: changes}},
		RLSEnabledTablesAdded:     []string{"missing_rls_table"},
		RLSEnabledTablesRemoved:   []string{"extra_rls_table"},
		RolesAdded:                []string{"missing_role"},
		RolesRemoved:              []string{"extra_role"},
		RolesModified:             []types.RoleDiff{{RoleName: "changed_role", Changes: changes}},
		GrantsAdded:               []types.GrantRef{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users"}},
		GrantsRemoved:             []types.GrantRef{{Role: "app", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "users"}},
		GrantOptionsAdded:         []types.GrantRef{{Role: "app", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "users"}},
		GrantOptionsRevoked:       []types.GrantRef{{Role: "app", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "users"}},
		ConstraintsAdded:          []string{"missing_global_constraint"},
		ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
			Name:      "missing_global_constraint",
			TableName: "accounts",
		}},
		ConstraintsRemoved: []string{"extra_global_constraint"},
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{{
			Name:      "extra_global_constraint",
			TableName: "accounts",
		}},
	}

	mismatches := collectShadowMismatches(diff)
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
}

func mismatchKinds(mismatches []ShadowMismatch) []string {
	kinds := make([]string, len(mismatches))
	for index, mismatch := range mismatches {
		kinds[index] = mismatch.Kind
	}
	return kinds
}

// TestCollectShadowMismatchesNamesTheTableOwningAnRLSPolicy pins the shape of
// the two policy-reference mismatches, not only their kinds. ShadowMismatch is
// serialized into the shadow verification report, so a reader of that JSON is
// told which policy is missing and which table it belongs to. Reporting a bare
// name could not distinguish two tables that each carry a policy called
// tenant_isolation, which PostgreSQL permits (stokaro/ptah#1276).
//
// Ordering is part of the contract too: the refs are sorted by table first, so
// alpha_orders leads zeta_orders regardless of the order the comparison put
// them in.
func TestCollectShadowMismatchesNamesTheTableOwningAnRLSPolicy(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		RLSPoliciesAdded: []types.RLSPolicyRef{
			{TableName: "zeta_orders", PolicyName: "tenant_isolation"},
			{TableName: "alpha_orders", PolicyName: "tenant_isolation"},
		},
		RLSPoliciesRemoved: []types.RLSPolicyRef{
			{TableName: "zeta_orders", PolicyName: "legacy_isolation"},
		},
	}

	c.Assert(collectShadowMismatches(diff), qt.DeepEquals, []ShadowMismatch{
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

func TestNextAvailableMigrationVersionChecksUpAndDownFiles(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, migrator.GenerateMigrationFileName(100, "add_email", "down")), []byte("SELECT 1;"), 0600)
	c.Assert(err, qt.IsNil)
	err = os.WriteFile(filepath.Join(dir, migrator.GenerateMigrationFileName(105, "future", "up")), []byte("SELECT 1;"), 0600)
	c.Assert(err, qt.IsNil)

	writer, err := bindPlannedMigrationDir("", dir)
	c.Assert(err, qt.IsNil)
	defer func() { _ = writer.Close() }()

	version, err := nextAvailableMigrationVersion(writer, 100, "add_email")

	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(106))
}

func TestLoadPriorMigrationsMissingDir(t *testing.T) {
	c := qt.New(t)

	migrations, err := loadPriorMigrations(filepath.Join(t.TempDir(), "missing"))

	c.Assert(err, qt.IsNil)
	c.Assert(migrations, qt.HasLen, 0)
}

func TestVerifyShadowMigrationConnectErrorIsStructured(t *testing.T) {
	c := qt.New(t)

	err := verifyShadowMigration(t.Context(), shadowMigrationOptions{
		DatabaseURL: "not-a-dsn",
		Dialect:     "postgres",
	})

	var shadowErr *ShadowVerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result.Stage, qt.Equals, "connect")
	c.Assert(shadowErr.Result.Mismatches, qt.HasLen, 1)
	c.Assert(shadowErr.Result.Mismatches[0].Kind, qt.Equals, "connect_error")
	c.Assert(shadowErr.Err, qt.IsNotNil)
	c.Assert(err, qt.ErrorMatches, `shadow check failed: connect to shadow database: invalid database URL: missing scheme`)
}

func TestVerifyShadowMigration_ReplayHonorsCallerCancellation(t *testing.T) {
	c := qt.New(t)
	target, err := dbschema.ConnectToDatabase(
		t.Context(),
		"sqlite://"+filepath.Join(t.TempDir(), "target.db"),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	err = verifyShadowMigration(ctx, shadowMigrationOptions{
		DatabaseURL:      "sqlite://" + filepath.Join(t.TempDir(), "shadow.db"),
		TargetConnection: target,
		Dialect:          platform.SQLite,
		Candidates: []shadowCandidate{{
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

	var shadowErr *ShadowVerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result.Stage, qt.Equals, "replay")
	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
}
