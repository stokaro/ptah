package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// policySchema is one table keyed on id whose policy reads column c of the
// given type.
func policySchema(columnType string, policy *ast.RowDeletionPolicySpec) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}, RowDeletionPolicy: policy}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", Name: "c", Type: columnType, Nullable: true},
		},
	}
}

// TestRender_RowDeletionPolicy_FailurePath refuses a row deletion policy on
// every target without row_deletion_policy, through the whole-schema render
// and a change on a table that exists alike. Without the refusal the MySQL,
// MariaDB, SQLite, SQL Server, Oracle and ClickHouse renderers write the table
// without the policy, and the server keeps every row the declaration said to
// delete.
func TestRender_RowDeletionPolicy_FailurePath(t *testing.T) {
	policy := &ast.RowDeletionPolicySpec{Column: "c", Interval: "P30D"}
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{dialect: platform.Postgres, caps: capability.Postgres18()},
		{dialect: platform.MySQL, caps: capability.MySQL84()},
		{dialect: platform.MariaDB, caps: capability.MariaDB1011()},
		{dialect: platform.SQLite, caps: capability.SQLite3()},
		{dialect: platform.SQLServer, caps: capability.SQLServer2022()},
		{dialect: platform.Oracle, caps: capability.Oracle23()},
		{dialect: platform.ClickHouse, caps: capability.ClickHouse24()},
		{dialect: platform.YDB, caps: capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false).
			With(capability.RowDeletionPolicy, false)},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
				policySchema("TIMESTAMP", policy), test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, `table "t" declares a row deletion policy, which requires target capability `+
				`row_deletion_policy, unavailable on this \w+ target`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)

			change := &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{
				&ast.SetRowDeletionPolicyOperation{Column: "c", Interval: "P30D"},
			}}
			sql, err := renderer.RenderSQLWithCapabilities(test.dialect, test.caps, change)
			c.Assert(err, qt.ErrorMatches, `table "t" declares a row deletion policy, which requires target capability `+
				`row_deletion_policy, .*`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRender_RowDeletionPolicyOnAnIntegerColumn_FailurePath refuses a policy
// naming the unit an integer column counts in on a target that has policies
// and not that form: Spanner's clause reads a timestamp column only.
func TestRender_RowDeletionPolicyOnAnIntegerColumn_FailurePath(t *testing.T) {
	policy := &ast.RowDeletionPolicySpec{Column: "c", Interval: "30 days", Unit: "SECONDS"}
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{dialect: platform.Spanner, caps: capability.SpannerPostgres()},
		{dialect: platform.YDB, caps: capability.YDB251().With(capability.RowDeletionPolicyEpochColumn, false)},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
				policySchema("BIGINT", policy), test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, `table "t" declares a row deletion policy on an integer column counting SECONDS, `+
				`which requires target capability row_deletion_policy_epoch_column, unavailable on this \w+ target`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRender_RowDeletionPolicy_HappyPath renders the policy on the two targets
// that have it, each in its own spelling.
func TestRender_RowDeletionPolicy_HappyPath(t *testing.T) {
	tests := []struct {
		dialect string
		caps    capability.Capabilities
		policy  *ast.RowDeletionPolicySpec
		want    string
	}{
		{
			dialect: platform.Spanner, caps: capability.SpannerPostgres(),
			policy: &ast.RowDeletionPolicySpec{Column: "c", Interval: "30 days"},
			want:   `TTL INTERVAL '30 days' ON "c"`,
		},
		{
			dialect: platform.YDB, caps: capability.YDB251(),
			policy: &ast.RowDeletionPolicySpec{Column: "c", Interval: "P30D"},
			want:   "WITH (TTL = Interval(\"P30D\") ON `c`)",
		},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
				policySchema("TIMESTAMP", test.policy), test.dialect, test.caps)
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0], qt.Contains, test.want)
		})
	}
}
