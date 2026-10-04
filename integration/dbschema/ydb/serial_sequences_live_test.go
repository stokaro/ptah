//go:build integration

package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// serialSchema is the directory the Serial sequence tests write into.
const serialSchema = "ptah_ydb_serials"

var serialSchemas = []string{serialSchema}

// serialDeclaration is three tables whose keys are Serial columns: orders is
// a 64-bit Serial that starts at 100 and steps by ordersIncrement, counters
// one that steps by countersIncrement from 1, and plain a 32-bit Serial that
// starts at plainStart, which is empty for a Serial nobody gave a setting.
func serialDeclaration(ordersIncrement, countersIncrement, plainStart string) *schemamodel.Database {
	key := func(structName, typ, start, increment string) schemamodel.Field {
		field := schemamodel.Field{StructName: structName, Name: "id", Type: typ, Primary: true, AutoInc: true}
		if start != "" || increment != "" {
			field.IdentityGeneration = "BY_DEFAULT"
			field.IdentityStart, field.IdentityIncrement = start, increment
		}
		return field
	}
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Order", Name: "orders", Schema: serialSchema},
			{StructName: "Counter", Name: "counters", Schema: serialSchema},
			{StructName: "Plain", Name: "plain", Schema: serialSchema},
		},
		Fields: []schemamodel.Field{
			key("Order", "BIGSERIAL", "100", ordersIncrement),
			{StructName: "Order", Name: "note", Type: "TEXT", Nullable: true},
			key("Counter", "BIGINT", "", countersIncrement),
			{StructName: "Counter", Name: "note", Type: "TEXT", Nullable: true},
			key("Plain", "SERIAL", plainStart, ""),
			{StructName: "Plain", Name: "note", Type: "TEXT", Nullable: true},
		},
	}
	schemamodel.Finalize(db)
	return db
}

// serialIDs inserts one row per note into table of the serial directory,
// naming no id, and returns every id the table holds in order.
func serialIDs(c *qt.C, conn *dbschema.DatabaseConnection, table string, notes ...string) []int64 {
	c.Helper()
	for _, note := range notes {
		apply(c, conn, []string{"INSERT INTO `" + serialSchema + "/" + table + "` (note) VALUES ('" + note + "'u)"})
	}
	rows, err := conn.QueryContext(c.Context(), "SELECT id FROM `"+serialSchema+"/"+table+"` ORDER BY id")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = rows.Close() })
	var ids []int64
	for rows.Next() {
		var id int64
		c.Assert(rows.Scan(&id), qt.IsNil)
		ids = append(ids, id)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return ids
}

// serialColumn returns column id of table in the serial directory as the
// reader describes it.
func serialColumn(c *qt.C, live *catalog.Database, table string) catalog.Column {
	c.Helper()
	for _, described := range live.Tables {
		for _, column := range described.Columns {
			if described.Name == table && column.Name == "id" {
				return column
			}
		}
	}
	c.Fatalf("no column id of table %s in %+v", table, live.Tables)
	return catalog.Column{}
}

// TestYDBSerialSequence_RoundTrip creates Serial columns with a start and an
// increment, reads both back, and plans nothing after; the same declaration
// applied again plans nothing too. The rows the tables then take come from the
// declared sequences, which is the measurement the read-back alone cannot
// make: a START without a restart is read back as declared while the first
// row still takes 1. Changing an increment on a table that holds rows is one
// ALTER SEQUENCE without a restart, and the next row follows on from the last
// one rather than landing on a key a row holds.
func TestYDBSerialSequence_RoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, serialSchemas)
			c.Cleanup(func() { dropTables(c, conn, serialSchemas) })
			database := readScoped(c, conn, serialSchemas).DatabasePath
			c.Assert(database, qt.Not(qt.Equals), "")
			sequence := func(table string) string {
				return "`" + database + "/" + serialSchema + "/" + table + "/_serial_column_id`"
			}

			declared := serialDeclaration("5", "3", "")
			first := planAgainst(c, conn, declared, serialSchemas)
			c.Assert(first, qt.DeepEquals, []string{
				"CREATE TABLE `ptah_ydb_serials/counters` (\n    `id` BigSerial NOT NULL,\n    `note` Utf8,\n    PRIMARY KEY (`id`)\n)",
				"ALTER SEQUENCE " + sequence("counters") + " START WITH 1 INCREMENT BY 3",
				"CREATE TABLE `ptah_ydb_serials/orders` (\n    `id` BigSerial NOT NULL,\n    `note` Utf8,\n    PRIMARY KEY (`id`)\n)",
				"ALTER SEQUENCE " + sequence("orders") + " START WITH 100 INCREMENT BY 5 RESTART WITH 100",
				"CREATE TABLE `ptah_ydb_serials/plain` (\n    `id` Serial NOT NULL,\n    `note` Utf8,\n    PRIMARY KEY (`id`)\n)",
			})
			apply(c, conn, first)
			c.Assert(planAgainst(c, conn, declared, serialSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, serialSchemas))
			c.Assert(planAgainst(c, conn, declared, serialSchemas), qt.HasLen, 0)

			c.Assert(serialIDs(c, conn, "orders", "a", "b"), qt.DeepEquals, []int64{100, 105})
			c.Assert(serialIDs(c, conn, "counters", "a", "b"), qt.DeepEquals, []int64{1, 4})
			live := readScoped(c, conn, serialSchemas)
			orders := serialColumn(c, live, "orders")
			c.Assert([]string{orders.IdentityStart, orders.IdentityIncrement, orders.SequenceRestart},
				qt.DeepEquals, []string{"100", "5", "100"})
			counters := serialColumn(c, live, "counters")
			c.Assert([]string{counters.IdentityStart, counters.IdentityIncrement, counters.SequenceRestart},
				qt.DeepEquals, []string{"", "3", ""})
			plain := serialColumn(c, live, "plain")
			c.Assert([]string{plain.IdentityStart, plain.IdentityIncrement, plain.SequenceRestart},
				qt.DeepEquals, []string{"", "", ""})

			declared = serialDeclaration("5", "7", "")
			change := planAgainst(c, conn, declared, serialSchemas)
			c.Assert(change, qt.DeepEquals, []string{
				"ALTER SEQUENCE " + sequence("counters") + " START WITH 1 INCREMENT BY 7",
			})
			apply(c, conn, change)
			c.Assert(planAgainst(c, conn, declared, serialSchemas), qt.HasLen, 0)
			// The sequence had computed 7 as its next value before the ALTER,
			// and steps by 7 from there.
			c.Assert(serialIDs(c, conn, "counters", "c", "d"), qt.DeepEquals, []int64{1, 4, 7, 14})
		})
	}
}

// TestYDBSerialSequence_RefusesWhatTheServerWouldBreak plans the two changes
// a plan refuses, against the server state that makes each unsafe, and then
// makes the change by hand to show the server does what the refusal says.
//
// A sequence some RESTART moved has that restart replayed by every later ALTER
// SEQUENCE, so the next insert takes a key a row already holds. A Serial's or
// SmallSerial's ALTER SEQUENCE raises its maximum to the Int64 maximum, so the
// value after the column's maximum is stored as a negative number.
func TestYDBSerialSequence_RefusesWhatTheServerWouldBreak(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, serialSchemas)
			c.Cleanup(func() { dropTables(c, conn, serialSchemas) })
			database := readScoped(c, conn, serialSchemas).DatabasePath
			apply(c, conn, planAgainst(c, conn, serialDeclaration("5", "3", ""), serialSchemas))
			c.Assert(serialIDs(c, conn, "orders", "a", "b"), qt.DeepEquals, []int64{100, 105})

			restarted := serialDeclaration("10", "3", "")
			c.Assert(planErrorAgainst(c, conn, restarted), qt.ErrorMatches,
				`.*the sequence of column "id" of table "ptah_ydb_serials.orders": the sequence was restarted at 100, `+
					`and YDB replays that restart on every later ALTER SEQUENCE.*`)
			apply(c, conn, []string{"ALTER SEQUENCE `" + database + "/ptah_ydb_serials/orders/_serial_column_id` INCREMENT BY 5"})
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"INSERT INTO `ptah_ydb_serials/orders` (note) VALUES ('c'u)"), qt.ErrorMatches, `(?s).*Conflict with existing key.*`)

			narrow := serialDeclaration("5", "3", "100")
			c.Assert(planErrorAgainst(c, conn, narrow), qt.ErrorMatches,
				`.*giving the sequence of column "id" of table "ptah_ydb_serials.plain" a start or an increment `+
					`\(an ALTER SEQUENCE raises the maximum of a Serial's sequence from 2147483647 .*\), which requires `+
					`target capability serial_sequence_keeps_range, unavailable on this ydb target`)
			apply(c, conn, []string{
				"ALTER SEQUENCE `" + database + "/ptah_ydb_serials/plain/_serial_column_id` RESTART WITH 2147483647",
			})
			c.Assert(serialIDs(c, conn, "plain", "a", "b"), qt.DeepEquals, []int64{-2147483648, 2147483647})
		})
	}
}

// planErrorAgainst plans the declaration against the serial directory and
// returns the planner's refusal, asserting it is one.
func planErrorAgainst(c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database) error {
	c.Helper()
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabaseInfo(declared, readScoped(c, conn, serialSchemas), info, nil)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		diff, info.Dialect, planner.Options{Capabilities: info.Capabilities},
	)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(statements, qt.IsNil)
	return err
}
