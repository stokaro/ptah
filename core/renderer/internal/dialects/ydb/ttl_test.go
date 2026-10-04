package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/ydb"
)

// withPolicy builds a table keyed on id with one more column, of the given
// type, and the row deletion policy policy.
func withPolicy(columnType string, policy *ast.RowDeletionPolicySpec) *ast.CreateTableNode {
	return &ast.CreateTableNode{
		Name:              "t",
		Columns:           []*ast.ColumnNode{ast.NewColumn("id", "BIGINT").SetPrimary(), {Name: "c", Type: columnType, Nullable: true}},
		RowDeletionPolicy: policy,
	}
}

// TestRender_TTL_HappyPath pins how a row deletion policy is written: as the
// TTL setting of CREATE TABLE's WITH clause, and on a table that exists as SET
// (TTL = ...) or RESET (TTL). Each statement was applied to local-ydb 26.2.1.14
// and 25.1.4.7 and read back through DescribeTable.
func TestRender_TTL_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "a date column on 25.1",
			caps: capability.YDB251(),
			node: withPolicy("TIMESTAMP", &ast.RowDeletionPolicySpec{Column: "c", Interval: "P30D"}),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `c` Timestamp,\n" +
				"    PRIMARY KEY (`id`)\n" +
				") WITH (TTL = Interval(\"P30D\") ON `c`);\n",
		},
		{
			name: "a 64-bit date column on 26.2",
			caps: capability.YDB262(),
			node: withPolicy("TIMESTAMP", &ast.RowDeletionPolicySpec{Column: "c", Interval: "PT12H"}),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `c` Timestamp64,\n" +
				"    PRIMARY KEY (`id`)\n" +
				") WITH (TTL = Interval(\"PT12H\") ON `c`);\n",
		},
		{
			name: "an integer column with its unit",
			caps: capability.YDB262(),
			node: withPolicy("BIGINT UNSIGNED", &ast.RowDeletionPolicySpec{Column: "c", Interval: "PT1H", Unit: "milliseconds"}),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `c` Uint64,\n" +
				"    PRIMARY KEY (`id`)\n" +
				") WITH (TTL = Interval(\"PT1H\") ON `c` AS MILLISECONDS);\n",
		},
		{
			name: "the WITH clause before the table's own SQL",
			caps: capability.YDB262(),
			node: func() *ast.CreateTableNode {
				table := withPolicy("DATE", &ast.RowDeletionPolicySpec{Column: "c", Interval: "P1W"})
				table.CustomSQL = "-- note"
				return table
			}(),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `c` Date32,\n" +
				"    PRIMARY KEY (`id`)\n" +
				") WITH (TTL = Interval(\"P1W\") ON `c`) -- note;\n",
		},
		{
			name: "a policy put on a table that exists",
			caps: capability.YDB251(),
			node: alter(&ast.SetRowDeletionPolicyOperation{Column: "ts", Interval: "P2D"}),
			want: "ALTER TABLE `t` SET (TTL = Interval(\"P2D\") ON `ts`);\n",
		},
		{
			name: "a policy replaced, which is the same statement",
			caps: capability.YDB262(),
			node: alter(&ast.SetRowDeletionPolicyOperation{Column: "e", Interval: "PT30M", Unit: "SECONDS", Replace: true}),
			want: "ALTER TABLE `t` SET (TTL = Interval(\"PT30M\") ON `e` AS SECONDS);\n",
		},
		{
			name: "a policy removed",
			caps: capability.YDB251(),
			node: &ast.AlterTableNode{Name: "dir.t", Operations: []ast.AlterOperation{&ast.DropRowDeletionPolicyOperation{}}},
			want: "ALTER TABLE `dir/t` RESET (TTL);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRender_TTL_FailurePath refuses a policy YDB would refuse, or would keep
// as something other than what was declared.
func TestRender_TTL_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		node    ast.Node
		wantErr string
	}{
		{
			name:    "an interval in Spanner's spelling",
			caps:    capability.YDB262(),
			node:    withPolicy("TIMESTAMP", &ast.RowDeletionPolicySpec{Column: "c", Interval: "30 days"}),
			wantErr: `the row deletion policy of table "t": interval "30 days" is not an ISO 8601 duration YDB takes .*`,
		},
		{
			name: "a fraction of a second",
			caps: capability.YDB262(),
			node: withPolicy("TIMESTAMP", &ast.RowDeletionPolicySpec{Column: "c", Interval: "PT1.5S"}),
			wantErr: `the row deletion policy of table "t": interval "PT1.5S" has a fraction of a second, ` +
				`and YDB keeps whole seconds only \(PT1.5S reads back as PT1S\)`,
		},
		{
			name:    "a column the table does not declare",
			caps:    capability.YDB262(),
			node:    withPolicy("TIMESTAMP", &ast.RowDeletionPolicySpec{Column: "missing", Interval: "P1D"}),
			wantErr: `the row deletion policy of table "t": it reads column "missing", which the table does not declare .*`,
		},
		{
			name: "a signed integer column",
			caps: capability.YDB262(),
			node: withPolicy("BIGINT", &ast.RowDeletionPolicySpec{Column: "c", Interval: "P1D", Unit: "SECONDS"}),
			wantErr: `the row deletion policy of table "t": column "c" is Int64, and YDB reads a TTL from a Date, .*` +
				"\\(`Unsupported column type`\\)",
		},
		{
			name:    "an integer column without its unit",
			caps:    capability.YDB262(),
			node:    withPolicy("BIGINT UNSIGNED", &ast.RowDeletionPolicySpec{Column: "c", Interval: "P1D"}),
			wantErr: `the row deletion policy of table "t": column "c" is Uint64, an integer type, and YDB needs the unit .*`,
		},
		{
			name:    "a date column with a unit",
			caps:    capability.YDB251(),
			node:    withPolicy("TIMESTAMP", &ast.RowDeletionPolicySpec{Column: "c", Interval: "P1D", Unit: "SECONDS"}),
			wantErr: `the row deletion policy of table "t": column "c" is Timestamp, a date type, and YDB takes no unit .*`,
		},
		{
			name:    "a unit YQL does not have",
			caps:    capability.YDB262(),
			node:    alter(&ast.SetRowDeletionPolicyOperation{Column: "e", Interval: "P1D", Unit: "DAYS"}),
			wantErr: `the row deletion policy of table "t": unit "DAYS" is not one YDB takes: .*`,
		},
		{
			name:    "a policy that names no column",
			caps:    capability.YDB262(),
			node:    alter(&ast.SetRowDeletionPolicyOperation{Interval: "P1D"}),
			wantErr: `table "t": the row deletion policy it sets names no column or no interval`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestRender_TTL_RefusedByCapability_FailurePath refuses a policy by the key
// a target lacks: the policy itself, and an integer column's unit.
func TestRender_TTL_RefusedByCapability_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		node    ast.Node
		wantKey capability.Capability
		wantErr string
	}{
		{
			name:    "a policy at CREATE TABLE",
			caps:    capability.YDB262().With(capability.RowDeletionPolicy, false),
			node:    withPolicy("TIMESTAMP", &ast.RowDeletionPolicySpec{Column: "c", Interval: "P1D"}),
			wantKey: capability.RowDeletionPolicy,
			wantErr: `the row deletion policy of table "t", which requires target capability row_deletion_policy, .*`,
		},
		{
			name:    "a policy put on a table",
			caps:    capability.YDB262().With(capability.RowDeletionPolicy, false),
			node:    alter(&ast.SetRowDeletionPolicyOperation{Column: "ts", Interval: "P1D"}),
			wantKey: capability.RowDeletionPolicy,
			wantErr: `the row deletion policy of table "t", which requires target capability row_deletion_policy, .*`,
		},
		{
			name:    "a policy removed",
			caps:    capability.YDB262().With(capability.RowDeletionPolicy, false),
			node:    alter(&ast.DropRowDeletionPolicyOperation{}),
			wantKey: capability.RowDeletionPolicy,
			wantErr: `removing the row deletion policy of table "t", which requires target capability row_deletion_policy, .*`,
		},
		{
			name:    "an integer column's unit",
			caps:    capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false),
			node:    withPolicy("BIGINT UNSIGNED", &ast.RowDeletionPolicySpec{Column: "c", Interval: "P1D", Unit: "SECONDS"}),
			wantKey: capability.RowDeletionPolicyEpochColumn,
			wantErr: `the row deletion policy of table "t" reads an integer column counting SECONDS, which requires ` +
				`target capability row_deletion_policy_epoch_column, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			var refusal *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(refusal.Feature, qt.Equals, string(test.wantKey))
			c.Assert(got, qt.Equals, "")
		})
	}
}
