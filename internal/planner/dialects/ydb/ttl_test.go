package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// ttlChange is a modification of items whose only change is its TTL.
func ttlChange(declaration difftypes.TableDeclaration, desired, current *ast.RowDeletionPolicySpec) *difftypes.SchemaDiff {
	declaration.Table.RowDeletionPolicy = desired
	return modified(difftypes.TableDiff{
		TableName: "items", Desired: declaration,
		RowDeletionPolicyChange: &difftypes.RowDeletionPolicyChange{Desired: desired, Current: current},
	})
}

// TestGenerateMigrationAST_TTL_HappyPath pins the statement each TTL change
// becomes: SET (TTL = ...) whether the table had a TTL or not, and RESET
// (TTL) to remove one. Each was applied on local-ydb 26.2.1.14 and 25.1.4.7
// and read back.
func TestGenerateMigrationAST_TTL_HappyPath(t *testing.T) {
	declaration := itemsDeclaration(field("ts", "TIMESTAMP", true), field("e", "BIGINT UNSIGNED", true))
	tests := []struct {
		name string
		caps capability.Capabilities
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "a TTL put on a table",
			caps: capability.YDB251(),
			diff: ttlChange(declaration, &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"}, nil),
			want: "ALTER TABLE `items` SET (TTL = Interval(\"P30D\") ON `ts`);\n",
		},
		{
			name: "a TTL moved to an integer column",
			caps: capability.YDB262(),
			diff: ttlChange(declaration,
				&ast.RowDeletionPolicySpec{Column: "e", Interval: "PT1H", Unit: "SECONDS"},
				&ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"}),
			want: "ALTER TABLE `items` SET (TTL = Interval(\"PT1H\") ON `e` AS SECONDS);\n",
		},
		{
			name: "a TTL removed",
			caps: capability.YDB262(),
			diff: ttlChange(declaration, nil, &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"}),
			want: "ALTER TABLE `items` RESET (TTL);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(render(c, test.caps, test.diff), qt.Equals, test.want)
		})
	}
}

// TestGenerateMigrationAST_TTL_Order pins where a TTL change goes among a
// table's changes: after the column it reads is added, and before the column
// it read is dropped, which YDB refuses while a TTL reads it (`Can't drop TTL
// column: 'ts', disable TTL first`, measured on 26.2.1.14 and 25.1.4.7). The
// same order, applied one statement per query on both lines, ends with the
// declared table.
func TestGenerateMigrationAST_TTL_Order(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "a TTL reset before its column is dropped",
			diff: modified(difftypes.TableDiff{
				TableName: "items", Desired: itemsDeclaration(field("label", "TEXT", true)),
				ColumnsRemoved: difftypes.ColumnChanges{field("ts", "TIMESTAMP", true)},
				RowDeletionPolicyChange: &difftypes.RowDeletionPolicyChange{
					Current: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P1D"},
				},
			}),
			want: "ALTER TABLE `items` RESET (TTL);\n" +
				"ALTER TABLE `items` DROP COLUMN `ts`;\n",
		},
		{
			name: "a TTL moved to a new column, and its old column dropped",
			diff: modified(difftypes.TableDiff{
				TableName: "items", Desired: func() difftypes.TableDeclaration {
					declaration := itemsDeclaration(field("expires_at", "TIMESTAMP", true))
					declaration.Table.RowDeletionPolicy = &ast.RowDeletionPolicySpec{Column: "expires_at", Interval: "P7D"}
					return declaration
				}(),
				ColumnsAdded:   difftypes.ColumnChanges{field("expires_at", "TIMESTAMP", true)},
				ColumnsRemoved: difftypes.ColumnChanges{field("ts", "TIMESTAMP", true)},
				RowDeletionPolicyChange: &difftypes.RowDeletionPolicyChange{
					Desired: &ast.RowDeletionPolicySpec{Column: "expires_at", Interval: "P7D"},
					Current: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P1D"},
				},
			}),
			want: "ALTER TABLE `items` ADD COLUMN `expires_at` Timestamp;\n" +
				"ALTER TABLE `items` SET (TTL = Interval(\"P7D\") ON `expires_at`);\n" +
				"ALTER TABLE `items` DROP COLUMN `ts`;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(render(c, capability.YDB251(), test.diff), qt.Equals, test.want)
		})
	}
}

// TestGenerateMigrationAST_TTL_FailurePath refuses, before any node is
// returned, a TTL change the target cannot make or YDB would refuse.
func TestGenerateMigrationAST_TTL_FailurePath(t *testing.T) {
	declaration := itemsDeclaration(field("ts", "TIMESTAMP", true), field("n", "BIGINT", true),
		field("e", "BIGINT UNSIGNED", true))
	runInterval := ttlChange(declaration, &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P2D"},
		&ast.RowDeletionPolicySpec{Column: "ts", Interval: "P1D"})
	runInterval.CurrentNotDescribed = coverage.Set{}.With(coverage.Object{Kind: coverage.TTL, Name: "items"})
	tests := []struct {
		name    string
		caps    capability.Capabilities
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{
			name:    "a TTL on a target without row deletion policies",
			caps:    capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false).With(capability.RowDeletionPolicy, false),
			diff:    ttlChange(declaration, &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P1D"}, nil),
			wantErr: `the row deletion policy of table "items", which requires target capability row_deletion_policy, .*`,
		},
		{
			name:    "a TTL removed on a target without row deletion policies",
			caps:    capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false).With(capability.RowDeletionPolicy, false),
			diff:    ttlChange(declaration, nil, &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P1D"}),
			wantErr: `the row deletion policy of table "items", which requires target capability row_deletion_policy, .*`,
		},
		{
			name: "an integer column's unit without the key",
			caps: capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false),
			diff: ttlChange(declaration, &ast.RowDeletionPolicySpec{Column: "e", Interval: "P1D", Unit: "SECONDS"}, nil),
			wantErr: `the row deletion policy of table "items" reads an integer column counting SECONDS, which requires ` +
				`target capability row_deletion_policy_epoch_column, .*`,
		},
		{
			name:    "a fraction of a second",
			caps:    capability.YDB262(),
			diff:    ttlChange(declaration, &ast.RowDeletionPolicySpec{Column: "ts", Interval: "PT0.5S"}, nil),
			wantErr: `the row deletion policy of table "items": interval "PT0.5S" has a fraction of a second, .*`,
		},
		{
			name: "a signed integer column",
			caps: capability.YDB262(),
			diff: ttlChange(declaration, &ast.RowDeletionPolicySpec{Column: "n", Interval: "P1D", Unit: "SECONDS"}, nil),
			wantErr: `the row deletion policy of table "items": column "n" is Int64, and YDB reads a TTL from a Date, .*` +
				"\\(`Unsupported column type`\\)",
		},
		{
			name:    "an integer column without its unit",
			caps:    capability.YDB262(),
			diff:    ttlChange(declaration, &ast.RowDeletionPolicySpec{Column: "e", Interval: "P1D"}, nil),
			wantErr: `the row deletion policy of table "items": column "e" is Uint64, an integer type, and YDB needs the unit .*`,
		},
		{
			name:    "a column the table does not declare",
			caps:    capability.YDB262(),
			diff:    ttlChange(declaration, &ast.RowDeletionPolicySpec{Column: "gone", Interval: "P1D"}, nil),
			wantErr: `the row deletion policy of table "items": it reads column "gone", which the table does not declare .*`,
		},
		{
			name: "a TTL whose run interval SET would reset",
			caps: capability.YDB262(),
			diff: runInterval,
			wantErr: `the row deletion policy of table "items": the table's TTL carries a run interval or a tiering policy ` +
				"Ptah does not model, and SET \\(TTL = \\.\\.\\.\\) resets it to YDB's default\\. .*",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(test.diff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// A TTL removed from a table whose run interval the read recorded takes the
// run interval with it, which is what the declaration asks for.
func TestGenerateMigrationAST_TTL_RemovedWithItsRunInterval(t *testing.T) {
	c := qt.New(t)
	diff := ttlChange(itemsDeclaration(field("ts", "TIMESTAMP", true)), nil, &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P1D"})
	diff.CurrentNotDescribed = coverage.Set{}.With(coverage.Object{Kind: coverage.TTL, Name: "items"})

	c.Assert(render(c, capability.YDB262(), diff), qt.Equals, "ALTER TABLE `items` RESET (TTL);\n")
}

// A rebuilt table gets the declared TTL in its new CREATE TABLE, and a TTL
// change on it travels with the table rather than as a statement of its own.
func TestGenerateMigrationAST_TableRebuild_CarriesTheTTL(t *testing.T) {
	c := qt.New(t)
	declaration := appItems(field("label", "TEXT", true), field("n", "BIGINT", true), field("ts", "TIMESTAMP", true))
	declaration.Table.RowDeletionPolicy = &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"}
	diff := modified(difftypes.TableDiff{
		TableName: "app.items", Desired: declaration,
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
		RowDeletionPolicyChange: &difftypes.RowDeletionPolicyChange{
			Desired: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"},
			Current: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P1D"},
		},
	})

	got := renderRebuild(c, capability.YDB251(), diff)

	c.Assert(got, qt.Contains, "    INDEX `items_label` GLOBAL SYNC ON (`label`)\n) WITH (TTL = Interval(\"P30D\") ON `ts`, "+
		heldDefaultSettings+");\n")
	c.Assert(got, qt.Not(qt.Contains), "SET (TTL")
	c.Assert(got, qt.Not(qt.Contains), "RESET (TTL)")
}

// A rebuilt table writes its TTL into the new CREATE TABLE, and on a target
// without row deletion policies the plan is refused before any node is
// returned rather than at render time.
func TestGenerateMigrationAST_TableRebuild_TTLWithoutTheKey(t *testing.T) {
	c := qt.New(t)
	declaration := appItems(field("label", "TEXT", true), field("n", "BIGINT", true), field("ts", "TIMESTAMP", true))
	declaration.Table.RowDeletionPolicy = &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"}
	diff := modified(difftypes.TableDiff{
		TableName: "app.items", Desired: declaration,
		ColumnsModified:         []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
		RowDeletionPolicyChange: &difftypes.RowDeletionPolicyChange{Desired: declaration.Table.RowDeletionPolicy},
	})
	caps := capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false).With(capability.RowDeletionPolicy, false)

	nodes, err := ydb.NewWithCapabilities(caps).WithTableRebuild(true).GenerateMigrationAST(diff)

	c.Assert(err, qt.ErrorMatches, `the row deletion policy of table "app.items", which requires target capability row_deletion_policy, .*`)
	c.Assert(nodes, qt.IsNil)
}

// TestGenerateMigrationAST_NewTableTTL writes a new table's TTL inside its
// CREATE TABLE.
func TestGenerateMigrationAST_NewTableTTL(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{
			Name: "events",
			Table: schemamodel.Table{StructName: "E", Name: "events",
				RowDeletionPolicy: &ast.RowDeletionPolicySpec{Column: "expires", Interval: "PT1H", Unit: "MILLISECONDS"}},
			Fields: []schemamodel.Field{
				{StructName: "E", Name: "id", Type: "BIGINT", Primary: true},
				{StructName: "E", Name: "expires", Type: "BIGINT UNSIGNED", Nullable: true},
			},
		}},
	}

	c.Assert(render(c, capability.YDB262(), diff), qt.Equals, "CREATE TABLE `events` (\n"+
		"    `id` Int64 NOT NULL,\n"+
		"    `expires` Uint64,\n"+
		"    PRIMARY KEY (`id`)\n"+
		") WITH (TTL = Interval(\"PT1H\") ON `expires` AS MILLISECONDS);\n")
}

// Dropping the column a TTL the plan keeps reads is refused before any node is
// returned, as YDB refuses it (`Can't drop TTL column`). A declaration that
// names a dropped column is refused when it is validated; this is the TTL a
// declaration does not describe and keeps from the database.
func TestGenerateMigrationAST_TTL_ColumnDroppedUnderAKeptTTL(t *testing.T) {
	c := qt.New(t)
	declaration := itemsDeclaration(field("label", "TEXT", true))
	declaration.Table.RowDeletionPolicy = &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P1D"}
	diff := modified(difftypes.TableDiff{
		TableName: "items", Desired: declaration,
		ColumnsRemoved: difftypes.ColumnChanges{field("ts", "TIMESTAMP", true)},
	})

	nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(diff)

	c.Assert(err, qt.ErrorMatches, `dropping column "ts" of table "items": the table's TTL reads it, and YDB refuses `+
		"to drop the column a TTL reads \\(`Can't drop TTL column`\\); .*")
	c.Assert(nodes, qt.IsNil)
}

// A rebuild is not refused for a TTL the current side cannot spell: an HCL
// document standing for the current state records that it has no spelling for
// a TTL, which says nothing about the table carrying one. A record the read of
// a database made is still refused; see
// TestGenerateMigrationAST_TableRebuild_FailurePath.
func TestGenerateMigrationAST_TableRebuild_FormatLimitIsNotASetting(t *testing.T) {
	c := qt.New(t)
	diff := notDescribing(modified(difftypes.TableDiff{TableName: "app.items",
		Desired:         appItems(field("label", "TEXT", true), field("n", "BIGINT", true)),
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}}}),
		coverage.Object{Kind: coverage.TTL, Reason: coverage.Unsupported, Provenance: coverage.DerivedFromFact})

	c.Assert(renderRebuild(c, capability.YDB262(), diff), qt.Contains, "RENAME TO `app/items`;\n")
}
