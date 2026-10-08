package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// serialField is a Serial key column of struct S declared with typ, start
// and increment, the way the annotation parser builds one: identity settings
// make it BY DEFAULT and auto-incrementing.
func serialField(typ, start, increment string) schemamodel.Field {
	return schemamodel.Field{
		StructName: "S", Name: "id", Type: typ, Primary: true, AutoInc: true,
		IdentityGeneration: "BY_DEFAULT", IdentityStart: start, IdentityIncrement: increment,
	}
}

// createdOrders is a diff that creates table app.orders around column id.
func createdOrders(id schemamodel.Field) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		CurrentDatabasePath: "/local",
		TablesAdded: difftypes.TableChanges{{
			Name:   "app.orders",
			Table:  schemamodel.Table{StructName: "S", Name: "orders", Schema: "app"},
			Fields: []schemamodel.Field{id, {StructName: "S", Name: "note", Type: "TEXT", Nullable: true}},
		}},
	}
}

// changedItems is a diff whose only change is the start or the increment of
// the sequence of items.id, which the comparison records under these keys.
func changedItems(id schemamodel.Field, restart string, changes map[string]string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		CurrentDatabasePath: "/local",
		TablesModified: []difftypes.TableDiff{{
			TableName: "items",
			Desired:   schemacapture.TableDeclaration{Table: schemamodel.Table{StructName: "S", Name: "items"}, Fields: []schemamodel.Field{id}},
			ColumnsModified: []difftypes.ColumnDiff{{
				ColumnName: "id", Changes: changes, Desired: id, CurrentSequenceRestart: restart,
			}},
		}},
	}
}

// TestGenerateMigrationAST_SerialSequence_HappyPath pins where a plan writes a
// Serial's ALTER SEQUENCE and what it says.
//
// A new table's statement follows its CREATE TABLE, takes the settings off the
// column, and restarts the sequence at a start other than 1, because START
// alone leaves the next value at 1 even on a sequence that never issued one
// (measured on 25.1.4.7 and 26.2.1.14). An increment alone needs no restart.
// A table that exists is altered without a restart, which could land the next
// value on a key a row holds, and the statement names both settings whatever
// changed.
func TestGenerateMigrationAST_SerialSequence_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "a new table with a start and an increment",
			diff: createdOrders(serialField("BIGSERIAL", "100", "5")),
			want: "CREATE TABLE `app/orders` (\n    `id` BigSerial NOT NULL,\n    `note` Utf8,\n    PRIMARY KEY (`id`)\n);\n" +
				"ALTER SEQUENCE `/local/app/orders/_serial_column_id` START WITH 100 INCREMENT BY 5 RESTART WITH 100;\n",
		},
		{
			name: "a new table with an increment alone",
			diff: createdOrders(serialField("BIGINT", "", "5")),
			want: "CREATE TABLE `app/orders` (\n    `id` BigSerial NOT NULL,\n    `note` Utf8,\n    PRIMARY KEY (`id`)\n);\n" +
				"ALTER SEQUENCE `/local/app/orders/_serial_column_id` START WITH 1 INCREMENT BY 5;\n",
		},
		{
			name: "a new table whose Serial declares the defaults",
			diff: createdOrders(serialField("BIGSERIAL", "1", "1")),
			want: "CREATE TABLE `app/orders` (\n    `id` BigSerial NOT NULL,\n    `note` Utf8,\n    PRIMARY KEY (`id`)\n);\n",
		},
		{
			name: "a table that exists, whose increment changed",
			diff: changedItems(serialField("BIGSERIAL", "100", "10"), "", map[string]string{"identity_increment": "5 -> 10"}),
			want: "ALTER SEQUENCE `/local/items/_serial_column_id` START WITH 100 INCREMENT BY 10;\n",
		},
		{
			name: "a narrow Serial that exists, moved back to the defaults",
			diff: changedItems(serialField("SERIAL", "", ""), "", map[string]string{"identity_start": "11 -> 1"}),
			want: "ALTER SEQUENCE `/local/items/_serial_column_id` START WITH 1 INCREMENT BY 1;\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(render(c, capability.YDB262(), test.diff), qt.Equals, test.want)
		})
	}
}

// TestGenerateMigrationAST_SerialSequence_FailurePath refuses, before any node,
// each change to a Serial's sequence a plan cannot make safely.
func TestGenerateMigrationAST_SerialSequence_FailurePath(t *testing.T) {
	withoutPath := createdOrders(serialField("BIGSERIAL", "100", ""))
	withoutPath.CurrentDatabasePath = ""
	// A reversal whose prior schema did not describe the column carries no
	// declaration of it.
	withoutDeclaration := changedItems(serialField("BIGSERIAL", "", ""), "", map[string]string{"identity_start": "100 -> 1"})
	withoutDeclaration.TablesModified[0].ColumnsModified[0].Desired = schemamodel.Field{}
	for _, test := range []struct {
		name    string
		caps    capability.Capabilities
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{
			name: "a 32-bit Serial given a start, whose ALTER would widen the sequence past the column",
			caps: capability.YDB262(),
			diff: createdOrders(serialField("SERIAL", "100", "")),
			wantErr: `giving the sequence of column "id" of table "app.orders" a start or an increment \(an ALTER SEQUENCE raises ` +
				`the maximum of a Serial's sequence from 2147483647 to the Int64 maximum, .*\), which requires target ` +
				`capability serial_sequence_keeps_range, unavailable on this ydb target`,
		},
		{
			name: "a 16-bit Serial of a table that exists, given an increment",
			caps: capability.YDB262(),
			diff: changedItems(serialField("SMALLSERIAL", "", "2"), "", map[string]string{"identity_increment": "1 -> 2"}),
			wantErr: `giving the sequence of column "id" of table "items" a start or an increment \(an ALTER SEQUENCE raises ` +
				`the maximum of a SmallSerial's sequence from 32767 .*\), which requires target capability ` +
				`serial_sequence_keeps_range, unavailable on this ydb target`,
		},
		{
			name: "a sequence the database restarted, whose restart the ALTER would replay",
			caps: capability.YDB262(),
			diff: changedItems(serialField("BIGSERIAL", "100", "10"), "100", map[string]string{"identity_increment": "5 -> 10"}),
			wantErr: `the sequence of column "id" of table "items": the sequence was restarted at 100, and YDB replays ` +
				`that restart on every later ALTER SEQUENCE, .*`,
		},
		{
			name: "a plan made against no read of a database",
			caps: capability.YDB262(),
			diff: withoutPath,
			wantErr: `the sequence of column "id" of table "app.orders": giving it start 100 and increment 1 takes an ` +
				`ALTER SEQUENCE, which names the sequence only by its absolute path, .*`,
		},
		{
			name:    "a start YDB refuses",
			caps:    capability.YDB262(),
			diff:    createdOrders(serialField("BIGSERIAL", "0", "")),
			wantErr: `the sequence of column "id" of table "app.orders": identity_start 0: .*`,
		},
		{
			name: "a target without the key",
			caps: capability.YDB262().With(capability.SerialSequenceOptions, false).
				With(capability.SerialSequenceKeepsRange, false),
			diff: createdOrders(serialField("BIGSERIAL", "100", "")),
			wantErr: `giving the sequence of column "id" of table "app.orders" a start or an increment, which requires ` +
				`target capability serial_sequence_options, unavailable on this ydb target`,
		},
		{
			name: "a change to a column the declaration does not make a Serial",
			caps: capability.YDB262(),
			diff: changedItems(schemamodel.Field{StructName: "S", Name: "id", Type: "BIGINT", Primary: true},
				"", map[string]string{"identity_start": "100 -> 1"}),
			wantErr: `the sequence of column "id" of table "items": the column is not a Serial column, .*`,
		},
		{
			name:    "a change whose declaration the plan does not carry",
			caps:    capability.YDB262(),
			diff:    withoutDeclaration,
			wantErr: `the sequence of column "id" of table "items": the plan carries no declaration of the column .*`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				test.diff,
			)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestGenerateMigrationAST_SerialSequenceReportsNothingDropped renders a plan
// that creates a table with a Serial's start and increment while reporting
// what the renderer could not carry. The plan writes both in its ALTER
// SEQUENCE, so the CREATE TABLE it hands the renderer carries neither, and
// nothing is reported dropped; a render of the declaration alone reports both.
func TestGenerateMigrationAST_SerialSequenceReportsNothingDropped(t *testing.T) {
	c := qt.New(t)
	nodes, err := ydb.NewWithCapabilities(capability.YDB262()).
		GenerateMigrationAST(
			context.Background(), must.Must(builtin.New()),
			createdOrders(serialField("BIGSERIAL", "100", "5")),
		)
	c.Assert(err, qt.IsNil)

	sql, omissions, err := builtin.RenderSQLReportingOmissions("ydb", capability.YDB262(), nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "START WITH 100 INCREMENT BY 5 RESTART WITH 100")
	c.Assert(omissions, qt.HasLen, 0)
}
