//go:build integration

package ydb_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/atlasretry"
	"ptah.run/migration/migrator"
)

const connectionSchema = "ptah_ydb_connection"

var connectionSchemas = []string{connectionSchema}

// The connection reports the server it reached: YDB, the version Version()
// answers, the capabilities of the line that version is on, and the database
// root as the schema an unqualified name means. The capabilities are the
// line's preset with the keys the contour's feature flags turn on, so a
// target that reaches a server on another line -- an
// address the server advertises through discovery that leads to the other
// server, say -- fails here rather than letting the rest of the package
// measure that line under this one's name.
func TestYDBConnection_DescribesTheServer(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)

			var version string
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT Version()").Scan(&version), qt.IsNil)

			info := conn.Info()
			c.Assert(info.Dialect, qt.Equals, platform.YDB)
			c.Assert(info.Version, qt.Equals, version)
			c.Assert(info.Schema, qt.Equals, "")
			c.Assert(info.IdentifierSemantics.DefaultSchema, qt.Equals, "")
			c.Assert(info.Capabilities, qt.DeepEquals, line.capabilities())
		})
	}
}

// A `?` Ptah writes is a YQL named parameter after Rebind, and the connection
// binds a positional argument to it. A Go int is bound as Int64: the SDK binds
// one as Int32 and wraps 5000000000 to 705032704, and the round trip through
// the server is what shows the value survived. A `?` inside a literal stays
// text.
func TestYDBConnection_BindsPositionalArguments(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)

			var wide int64
			var literal []byte
			var widened string
			c.Assert(conn.QueryRowContext(c.Context(),
				sqlutil.Rebind(platform.YDB, `SELECT ? AS wide, '?' AS literal, FormatType(TypeOf(?)) AS widened`),
				5000000000, 7,
			).Scan(&wide, &literal, &widened), qt.IsNil)

			c.Assert(wide, qt.Equals, int64(5000000000))
			c.Assert(string(literal), qt.Equals, "?")
			c.Assert(widened, qt.Equals, "Int64")
		})
	}
}

// liveID and liveCount are named integers, which the SDK reads by their kind.
type (
	liveID    int
	liveCount uint
)

// Every shape a Go int or uint reaches the connection in is bound at 64 bits.
// The SDK binds an int or a uint as a 32-bit value, so without the widening
// 5000000000 would reach the server as 705032704; the type the server reports
// and the value it reads back are what show it did not.
func TestYDBConnection_BindsEachIntegerShapeAt64Bits(t *testing.T) {
	const wide = 5000000000
	tests := []struct {
		name      string
		arg       any
		valueExpr string
		wantType  string
	}{
		{name: "uint", arg: uint(wide), valueExpr: "CAST(? AS Utf8)", wantType: "Uint64"},
		{name: "a pointer to uint", arg: new(uint(wide)), valueExpr: "CAST(? AS Utf8)", wantType: "Optional<Uint64>"},
		{name: "a slice of uint", arg: []uint{wide}, valueExpr: "CAST(ListHead(?) AS Utf8)", wantType: "List<Uint64>"},
		{name: "a nullable uint", arg: sql.Null[uint]{V: wide, Valid: true}, valueExpr: "CAST(? AS Utf8)",
			wantType: "Optional<Uint64>"},
		{name: "a named uint", arg: liveCount(wide), valueExpr: "CAST(? AS Utf8)", wantType: "Uint64"},
		{name: "a pointer to int", arg: new(int(wide)), valueExpr: "CAST(? AS Utf8)", wantType: "Optional<Int64>"},
		{name: "a slice of int", arg: []int{wide}, valueExpr: "CAST(ListHead(?) AS Utf8)", wantType: "List<Int64>"},
		{name: "a nullable int", arg: sql.Null[int]{V: wide, Valid: true}, valueExpr: "CAST(? AS Utf8)",
			wantType: "Optional<Int64>"},
		{name: "a named int", arg: liveID(wide), valueExpr: "CAST(? AS Utf8)", wantType: "Int64"},
	}

	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					conn := openYDB(c, line)

					var bound string
					var value sql.NullString
					err := conn.QueryRowContext(c.Context(),
						sqlutil.Rebind(platform.YDB, "SELECT FormatType(TypeOf(?)) AS bound, "+test.valueExpr+" AS value"),
						test.arg, test.arg,
					).Scan(&bound, &value)

					c.Assert(err, qt.IsNil)
					c.Assert(bound, qt.Equals, test.wantType)
					c.Assert(value, qt.Equals, sql.NullString{String: "5000000000", Valid: true})
				})
			}
		})
	}
}

// An Int64 argument bound to a narrower column is refused by the server
// rather than converted, and the same value written with the column's own
// width is accepted. The connection binds a value without knowing the column
// it lands in, so it widens a Go int to 64 bits, where a value too wide for a
// column fails loudly instead of wrapping, and a caller writing a narrow column
// passes the narrow Go type. The data diff, declared rows and seeds know the
// column and write its own type; TestYDBDataDiff_RoundTrips writes a Go int
// into an Int32 column, and TestYDBDeclaredRows_RefuseAValueTheColumnCannotHold
// refuses one the column cannot hold.
func TestYDBConnection_RefusesAWideArgumentForANarrowColumn(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, connectionSchemas)
			c.Cleanup(func() { dropTables(c, conn, connectionSchemas) })
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"CREATE TABLE `ptah_ydb_connection/narrow` (`id` Int64 NOT NULL, `small` Int32, PRIMARY KEY (`id`))"), qt.IsNil)
			insert := sqlutil.Rebind(platform.YDB, "UPSERT INTO `ptah_ydb_connection/narrow` (`id`, `small`) VALUES (?, ?)")

			_, wideErr := conn.ExecContext(c.Context(), insert, 1, 5)
			_, narrowErr := conn.ExecContext(c.Context(), insert, 2, int32(5))

			c.Assert(wideErr, qt.ErrorMatches, `(?s).*Failed to convert 'small': Int64 to Optional<Int32>.*`)
			c.Assert(narrowErr, qt.IsNil)
			var stored int32
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT `small` FROM `ptah_ydb_connection/narrow` WHERE `id` = 2").Scan(&stored), qt.IsNil)
			c.Assert(stored, qt.Equals, int32(5))
		})
	}
}

// An isolated read session is a snapshot read-only transaction on YDB: a read
// runs, and a write inside it is refused by the server.
func TestYDBConnection_IsolatedReadSessionIsReadOnly(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, connectionSchemas)
			c.Cleanup(func() { dropTables(c, conn, connectionSchemas) })
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"CREATE TABLE `ptah_ydb_connection/guarded` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))"), qt.IsNil)

			var rows int64
			readErr := conn.WithIsolatedQuerySession(c.Context(), &sql.TxOptions{ReadOnly: true},
				func(queryer dbschema.IsolatedQueryer) error {
					return queryOne(c.Context(), queryer, "SELECT COUNT(*) FROM `ptah_ydb_connection/guarded`", &rows)
				})
			writeErr := conn.WithIsolatedQuerySession(c.Context(), &sql.TxOptions{ReadOnly: true},
				func(queryer dbschema.IsolatedQueryer) error {
					return queryOne(c.Context(), queryer, "UPSERT INTO `ptah_ydb_connection/guarded` (`id`) VALUES (1l)")
				})

			c.Assert(readErr, qt.IsNil)
			c.Assert(rows, qt.Equals, int64(0))
			// The failed write ended the transaction on the server, so the rollback
			// that follows finds none; that is not reported over the refusal.
			c.Assert(writeErr, qt.ErrorMatches, `(?s).*can't be performed in read only transaction.*`)
			c.Assert(writeErr.Error(), qt.Not(qt.Contains), "roll back transaction")
		})
	}
}

// db verify evaluates its assertions in the isolated session above. One that
// holds is verified, and one the server cannot run is reported as errored
// rather than ending the run.
func TestYDBConnection_VerifiesChecks(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)

			report, err := migrator.VerifyChecks(c.Context(), conn, []migrator.Check{
				{Name: "holds", Assert: "SELECT 1 = 1"},
				{Name: "errors", Assert: "SELECT COUNT(*) = 0 FROM `ptah_ydb_connection/no_such_table`"},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(report.Results, qt.HasLen, 2)
			c.Assert(report.Results[0].Status, qt.Equals, migrator.VerifyStatusVerified)
			c.Assert(report.Results[1].Status, qt.Equals, migrator.VerifyStatusErrored)
		})
	}
}

// A transaction whose read another transaction changed before it committed is
// aborted (`Transaction locks invalidated`), and the error the driver returns
// is the one the retry reads as safe to run again.
func TestYDBConnection_AbortedTransactionIsRetryable(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, connectionSchemas)
			c.Cleanup(func() { dropTables(c, conn, connectionSchemas) })
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"CREATE TABLE `ptah_ydb_connection/contended` (`id` Int64 NOT NULL, `v` Int64, PRIMARY KEY (`id`))"), qt.IsNil)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"UPSERT INTO `ptah_ydb_connection/contended` (`id`, `v`) VALUES (1l, 1l)"), qt.IsNil)

			tx, err := conn.BeginTx(c.Context(), &sql.TxOptions{Isolation: sql.LevelSerializable})
			c.Assert(err, qt.IsNil)
			var v int64
			c.Assert(tx.QueryRowContext(c.Context(),
				"SELECT `v` FROM `ptah_ydb_connection/contended` WHERE `id` = 1").Scan(&v), qt.IsNil)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"UPDATE `ptah_ydb_connection/contended` SET `v` = 2l WHERE `id` = 1"), qt.IsNil)

			// Where the abort arrives depends on the line: 26.2.1.14 answers it at
			// the commit, and 25.1.4.7 at the write, saying the transaction `has
			// deferred effects, but locks are broken`. Either way it is one error.
			_, writeErr := tx.ExecContext(c.Context(), "UPDATE `ptah_ydb_connection/contended` SET `v` = 10l WHERE `id` = 1")
			aborted := errors.Join(writeErr, tx.Commit())

			c.Assert(aborted, qt.ErrorMatches, `(?s).*Transaction locks invalidated.*`)
			c.Assert(atlasretry.IsRetryable(aborted), qt.IsTrue)
		})
	}
}

// The writer judges a statement by its status. YDB answers CREATE TABLE IF NOT
// EXISTS on an existing table, and ADD INDEX on a table holding rows, with
// issue text and a success status, and both succeed. Two DDL statements in
// one text run as two queries, so the second sees the column the first added.
func TestYDBWriter_StatementsSucceedByStatus(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, connectionSchemas)
			c.Cleanup(func() { dropTables(c, conn, connectionSchemas) })
			create := "CREATE TABLE IF NOT EXISTS `ptah_ydb_connection/statused` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))"
			c.Assert(conn.Writer().ExecuteSQL(c.Context(), create), qt.IsNil)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"UPSERT INTO `ptah_ydb_connection/statused` (`id`) VALUES (1l), (2l)"), qt.IsNil)

			c.Assert(conn.Writer().ExecuteSQL(c.Context(), create), qt.IsNil)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"ALTER TABLE `ptah_ydb_connection/statused` ADD COLUMN `v` Utf8;\n"+
					"ALTER TABLE `ptah_ydb_connection/statused` ADD INDEX `by_v` GLOBAL SYNC ON (`v`);"), qt.IsNil)

			live := readScoped(c, conn, connectionSchemas)
			c.Assert(columnNamesOf(tableNamed(c, live, connectionSchema, "statused")), qt.DeepEquals, []string{"id", "v"})
			c.Assert(indexNamesOf(live), qt.DeepEquals, []string{"by_v"})
		})
	}
}

// A table setting Ptah does not model is recorded by the read: a TTL run
// interval, which only the SDK and the CLI write. Column tables are modeled
// alongside row tables. A view is described, with
// the query the server stores, and the table's TTL, column family and
// partitioning are read as its row deletion policy and its YDB settings.
func TestYDBReader_RecordsWhatItDoesNotModel(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, connectionSchemas)
			dropObjects(c, conn)
			c.Cleanup(func() {
				dropObjects(c, conn)
				dropTables(c, conn, connectionSchemas)
			})
			for _, statement := range []string{
				"CREATE TABLE `ptah_ydb_connection/base` (`id` Int64 NOT NULL, `ts` Timestamp, PRIMARY KEY (`id`), " +
					"FAMILY default (COMPRESSION = \"lz4\")) WITH (TTL = Interval('P1D') ON `ts`, AUTO_PARTITIONING_BY_LOAD = ENABLED)",
				"CREATE VIEW `ptah_ydb_connection/v` WITH (security_invoker = TRUE) AS SELECT 1 AS a",
				"CREATE TABLE `ptah_ydb_connection/olap` (`id` Int64 NOT NULL, PRIMARY KEY (`id`)) " +
					"PARTITION BY HASH(`id`) WITH (STORE = COLUMN)",
			} {
				c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil, qt.Commentf("execute: %s", statement))
			}
			setRunInterval(c, line, connectionSchema+"/base", "ts", 86400, 1800)

			live := readScoped(c, conn, connectionSchemas)

			c.Assert(tableNames(live), qt.DeepEquals, []string{"ptah_ydb_connection|base", "ptah_ydb_connection|olap"})
			c.Assert(live.Views, qt.DeepEquals, []catalog.View{{Name: "v", Schema: "ptah_ydb_connection", Body: "SELECT 1 AS a"}})
			c.Assert(live.NotDescribed.Describes(coverage.View, "ptah_ydb_connection.v"), qt.IsTrue)
			c.Assert(live.NotDescribed.Describes(coverage.ColumnTable, "ptah_ydb_connection.olap"), qt.IsTrue)
			c.Assert(live.NotDescribed.Describes(coverage.TTL, "ptah_ydb_connection.base"), qt.IsFalse)
			c.Assert(heldFamilies(c, tableNamed(c, live, connectionSchema, "base")), qt.DeepEquals,
				[]ydbschema.ColumnFamily{{Name: "default", Compression: "lz4"}})
			c.Assert(live.NotDescribed.Describes(coverage.TableOption, "ptah_ydb_connection.base"), qt.IsTrue)
			c.Assert(observedTTL(c, tableNamed(c, live, connectionSchema, "base")), qt.DeepEquals,
				&ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P1D"}, RunIntervalSeconds: 1800})
			c.Assert(heldSettings(c, tableNamed(c, live, connectionSchema, "base")), qt.DeepEquals,
				&ydbschema.TablePartitioning{ByLoad: new(true)})
		})
	}
}

// A vector index is read as the kind it is, with the settings the server
// built it with, rather than as a plain global index, which is how
// ydb-go-sdk's own description reads it. 26.2 builds one by default; 25.1
// keeps vector indexes behind the EnableVectorIndex feature flag and answers
// `Vector index support is disabled` without it, so go-integration-tests.yml
// starts the 25.1 server with the flag on.
func TestYDBReader_ReadsAVectorIndex(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, connectionSchemas)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"CREATE TABLE `ptah_ydb_connection/vectors` (`id` Int64 NOT NULL, `emb` String, PRIMARY KEY (`id`), "+
					"INDEX `by_emb` GLOBAL USING vector_kmeans_tree ON (`emb`) "+
					"WITH (distance=cosine, vector_type=\"float\", vector_dimension=3, levels=1, clusters=2))"), qt.IsNil)
			c.Cleanup(func() {
				c.Assert(conn.Writer().ExecuteSQL(context.Background(), "DROP TABLE `ptah_ydb_connection/vectors`"), qt.IsNil)
			})

			live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, connectionSchemas)

			c.Assert(err, qt.IsNil)
			c.Assert(indexNamed(c, live, "by_emb").Method, qt.Equals, "GLOBAL USING vector_kmeans_tree")
			c.Assert(observedVector(c, indexNamed(c, live, "by_emb")), qt.DeepEquals,
				&ydbschema.ObservedVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2})
		})
	}
}

// dropObjects drops what the unmodeled-object test creates besides tables.
func dropObjects(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	for _, statement := range []string{
		"DROP VIEW IF EXISTS `ptah_ydb_connection/v`",
		"DROP TABLE IF EXISTS `ptah_ydb_connection/olap`",
	} {
		c.Assert(conn.Writer().ExecuteSQL(context.Background(), statement), qt.IsNil, qt.Commentf("execute: %s", statement))
	}
}

// queryOne runs query in an isolated session and scans its one row into
// destinations, which may be none.
func queryOne(ctx context.Context, queryer dbschema.IsolatedQueryer, query string, destinations ...any) error {
	result, err := queryer.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer result.Close()
	for result.Next() {
		if err := result.Scan(destinations...); err != nil {
			return err
		}
	}
	return result.Err()
}
