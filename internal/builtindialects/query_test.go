package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestDescriptors_QueryAgreesWithTheRetiredSwitches pins each built-in
// engine's Query and Scripts sections against the answers of the switches
// core/query and core/sqlutil kept before they read the descriptor: the row
// limit, the bare-OFFSET literal, the joins refused, the upsert spelling and
// its refusal, the UPDATE refusal, the capability-decided constructs, the GO
// batch separator and the engines whose scripts carry no delimiter directive.
// The rows are keyed by the constants those switches compared with, so a
// descriptor that drifts from the switch it replaced fails here by name.
func TestDescriptors_QueryAgreesWithTheRetiredSwitches(t *testing.T) {
	const (
		mysqlNoLimit  = "18446744073709551615"
		sqliteNoLimit = "-1"
	)
	onConflict := descriptor.Query{Upsert: descriptor.UpsertOnConflict}
	onDuplicateKey := descriptor.Query{
		UnsupportedJoins: []descriptor.Join{descriptor.JoinFull},
		OffsetOnlyLimit:  mysqlNoLimit,
		Upsert:           descriptor.UpsertOnDuplicateKey,
	}
	delimiters := descriptor.Scripts{ClientDelimiters: true}
	want := map[string]struct {
		query   descriptor.Query
		scripts descriptor.Scripts
	}{
		platform.Postgres:    {onConflict, delimiters},
		platform.CockroachDB: {onConflict, delimiters},
		platform.YugabyteDB:  {onConflict, delimiters},
		platform.Spanner:     {onConflict, delimiters},
		platform.MySQL:       {onDuplicateKey, delimiters},
		platform.MariaDB:     {onDuplicateKey, delimiters},
		platform.SQLite: {descriptor.Query{
			UnsupportedJoins: []descriptor.Join{descriptor.JoinRight, descriptor.JoinFull},
			OffsetOnlyLimit:  sqliteNoLimit,
			Upsert:           descriptor.UpsertOnConflict,
		}, delimiters},
		platform.ClickHouse: {descriptor.Query{
			UpsertRefusal: "ClickHouse has no upsert statement: a ReplacingMergeTree deduplicates in the background " +
				"rather than at insert time, which is a table design and not a clause on this INSERT",
			UpdateRefusal: "UPDATE is not a portable statement on ClickHouse: a plain UPDATE runs only on a table " +
				"with the materialized _block_number column, which a statement cannot declare; use " +
				"ALTER TABLE … UPDATE, or enable enable_block_number_column on the table",
		}, delimiters},
		platform.SQLServer: {descriptor.Query{
			OffsetFetch:  true,
			FetchOrderBy: "(SELECT NULL)",
			UpsertRefusal: "SQL Server has no ON CONFLICT: it expresses an upsert as MERGE, " +
				"a different statement with its own source, join condition and match clauses",
		}, descriptor.Scripts{BatchSeparator: "GO", ClientDelimiters: true}},
		platform.Oracle: {descriptor.Query{OffsetFetch: true}, delimiters},
		platform.YDB: {descriptor.Query{
			ChecksCapabilities: true,
			UnsignedBounds:     true,
			Upsert:             descriptor.UpsertInto,
			UpsertRefusal: "YDB has no ON CONFLICT: UpsertInto writes each row over the row with the same " +
				"primary key, and the server refuses INSERT OR IGNORE",
		}, descriptor.Scripts{}},
	}

	described := builtindialects.Descriptors()

	c := qt.New(t)
	c.Assert(described, qt.HasLen, len(want))
	for _, dialect := range described {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			expected, found := want[dialect.Name]
			c.Assert(found, qt.IsTrue)
			c.Assert(dialect.Query, qt.DeepEquals, expected.query)
			// Bodies is internal/sqlcompound's, which
			// TestDescriptors_BodiesAgreeWithTheSwitchTheyReplace pins.
			scripts := dialect.Scripts
			scripts.Bodies = ""
			c.Assert(scripts, qt.DeepEquals, expected.scripts)
		})
	}
}
