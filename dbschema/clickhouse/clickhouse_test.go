package clickhouse

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema/types"
)

// TestClickHouseWriter_SchemaWriterInterface is a compile-time guard that
// *Writer continues to satisfy types.SchemaWriter as that interface evolves
// (e.g., the parameterized ExecuteSQL signature added in #130/#177). If the
// interface drifts, this fails to compile rather than at integration-test
// time. Mirrors the postgres / mysql convention.
func TestClickHouseWriter_SchemaWriterInterface(t *testing.T) {
	c := qt.New(t)
	writer := NewClickHouseWriter(nil, "default")
	var _ types.SchemaWriter = writer
	c.Assert(writer, qt.IsNotNil)
}

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "simple identifier", in: "users", want: "`users`"},
		{name: "empty", in: "", want: "``"},
		{name: "mixed case preserved", in: "MyTable", want: "`MyTable`"},
		{name: "embedded backtick doubled", in: "weird`name", want: "`weird``name`"},
		{name: "multiple embedded backticks", in: "a`b`c", want: "`a``b``c`"},
		{name: "name with space and semicolon", in: "t; DROP TABLE x; --", want: "`t; DROP TABLE x; --`"},
		{name: "injection attempt via backtick", in: "t` SYNC; DROP TABLE y; --", want: "`t`` SYNC; DROP TABLE y; --`"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(quoteIdent(tc.in), qt.Equals, tc.want)
		})
	}
}

// TestWriter_DryRun_NoDBRequired verifies that the dry-run path of
// ExecuteSQL works without an underlying *sql.DB. This locks in the
// observable contract that ExecuteSQL never touches the DB when dry-run
// is enabled — important because the migrator relies on this to safely
// preview migrations even when no live ClickHouse is reachable.
func TestWriter_DryRun_NoDBRequired(t *testing.T) {
	c := qt.New(t)
	w := NewClickHouseWriter(nil, "default")
	w.SetDryRun(true)
	c.Assert(w.IsDryRun(), qt.IsTrue)

	// ExecuteSQL must not touch w.db when dry-run is on; passing args
	// also verifies the new (ctx, sql, args...) signature compiles end to
	// end.
	err := w.ExecuteSQL(t.Context(), "SELECT ?", 42)
	c.Assert(err, qt.IsNil)

	// Begin/Commit/Rollback are documented no-ops on ClickHouse; the
	// dry-run path simply logs.
	c.Assert(w.BeginTransaction(), qt.IsNil)
	c.Assert(w.CommitTransaction(), qt.IsNil)
	c.Assert(w.RollbackTransaction(), qt.IsNil)

	// DropAllTables dry-run prints a stub table list and emits no DDL,
	// so it must succeed without a live DB.
	c.Assert(w.DropAllTables(), qt.IsNil)
}
