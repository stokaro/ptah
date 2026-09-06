//go:build integration

package gonative_test

import (
	"database/sql"
	"testing"

	_ "github.com/ClickHouse/clickhouse-go/v2" // registers the ClickHouse driver for database/sql
	qt "github.com/frankban/quicktest"
	_ "github.com/microsoft/go-mssqldb" // registers the SQL Server driver for database/sql

	"ptah.run/core/query"
	"ptah.run/internal/dbtarget"
)

// The query builder's dialect matrix pins what each dialect renders. It cannot
// pin that the server takes it: a renderer test compares one string against
// another, and every cell would stay green if the SQL were wrong in a way the
// renderer and the fixture agreed on.
//
// SQL Server is where that gap was widest. Its pagination is the shape nothing
// else in the matrix has -- `ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT
// @p2 ROWS ONLY`, invented because the engine refuses OFFSET without ORDER BY
// -- and it was hand-verified once, during the change that added it, and pinned
// only against the renderer's own output ever since (stokaro/ptah#941).
//
// These execute what the matrix renders and read the rows back.

const dmlExecutionTable = "ptah_941_dml"

func dmlWhereID(id int64) query.Expression {
	return &query.Comparison{
		Left:     &query.ColumnRef{Name: "id"},
		Operator: query.OpEqual,
		Right:    &query.BoundValue{Value: id},
	}
}

// TestSQLServerRenderedDMLExecutes runs all four verbs against a live SQL
// Server, including the pagination clause the renderer writes for it.
func TestSQLServerRenderedDMLExecutes(t *testing.T) {
	dsn := requireReachableEngine(t, dbtarget.SQLServer, "sqlserver", "SQL Server")
	c := qt.New(t)

	db, err := sql.Open("sqlserver", dsn)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()

	_, err = db.Exec("IF OBJECT_ID('" + dmlExecutionTable + "', 'U') IS NOT NULL DROP TABLE " + dmlExecutionTable)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec("CREATE TABLE " + dmlExecutionTable + " (id BIGINT NOT NULL PRIMARY KEY, name NVARCHAR(50) NOT NULL)")
	c.Assert(err, qt.IsNil)
	defer func() { _, _ = db.Exec("DROP TABLE " + dmlExecutionTable) }()

	insert, insertArgs, err := query.RenderInsert(&query.InsertStatement{
		Table:   dmlExecutionTable,
		Columns: []string{"id", "name"},
		Rows: [][]query.Expression{
			{&query.BoundValue{Value: int64(1)}, &query.BoundValue{Value: "a"}},
			{&query.BoundValue{Value: int64(2)}, &query.BoundValue{Value: "b"}},
		},
	}, "sqlserver")
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(insert, insertArgs...)
	c.Assert(err, qt.IsNil, qt.Commentf("rendered INSERT: %s", insert))

	// The pagination clause is the reason this test exists. LIMIT is not a SQL
	// Server keyword, so the renderer writes an ORDER BY the caller never asked
	// for; only the server can say whether that is accepted.
	limit := int64(1)
	selectSQL, selectArgs, err := query.RenderSelect(&query.SelectStatement{
		Columns: []query.ResultColumn{{Name: "id"}, {Name: "name"}},
		From:    dmlExecutionTable,
		Where:   dmlWhereID(1),
		Limit:   &limit,
	}, "sqlserver")
	c.Assert(err, qt.IsNil)
	c.Assert(selectSQL, qt.Contains, "OFFSET", qt.Commentf("rendered SELECT: %s", selectSQL))

	rows, err := db.Query(selectSQL, selectArgs...)
	c.Assert(err, qt.IsNil, qt.Commentf("rendered SELECT: %s", selectSQL))
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var gotID int64
	var gotName string
	c.Assert(rows.Next(), qt.IsTrue, qt.Commentf("rendered SELECT returned no row: %s", selectSQL))
	c.Assert(rows.Scan(&gotID, &gotName), qt.IsNil)
	c.Assert(gotID, qt.Equals, int64(1))
	c.Assert(gotName, qt.Equals, "a")
	c.Assert(rows.Next(), qt.IsFalse, qt.Commentf("the rendered LIMIT did not bound the result"))
	c.Assert(rows.Err(), qt.IsNil)

	update, updateArgs, err := query.RenderUpdate(&query.UpdateStatement{
		Table: dmlExecutionTable,
		Set:   []query.Assignment{{Column: "name", Value: &query.BoundValue{Value: "z"}}},
		Where: dmlWhereID(1),
	}, "sqlserver")
	c.Assert(err, qt.IsNil)
	updated, err := db.Exec(update, updateArgs...)
	c.Assert(err, qt.IsNil, qt.Commentf("rendered UPDATE: %s", update))
	affected, err := updated.RowsAffected()
	c.Assert(err, qt.IsNil)
	c.Assert(affected, qt.Equals, int64(1))

	del, delArgs, err := query.RenderDelete(&query.DeleteStatement{
		Table: dmlExecutionTable,
		Where: dmlWhereID(1),
	}, "sqlserver")
	c.Assert(err, qt.IsNil)
	deleted, err := db.Exec(del, delArgs...)
	c.Assert(err, qt.IsNil, qt.Commentf("rendered DELETE: %s", del))
	affected, err = deleted.RowsAffected()
	c.Assert(err, qt.IsNil)
	c.Assert(affected, qt.Equals, int64(1))

	var remaining int
	c.Assert(db.QueryRow("SELECT COUNT(*) FROM "+dmlExecutionTable).Scan(&remaining), qt.IsNil)
	c.Assert(remaining, qt.Equals, 1)
}

// TestClickHouseRenderedDMLExecutes runs the three verbs ClickHouse takes
// portably, and pins that the fourth is refused rather than rendered into SQL
// the server would not accept.
func TestClickHouseRenderedDMLExecutes(t *testing.T) {
	dsn := requireReachableEngine(t, dbtarget.ClickHouse, "clickhouse", "ClickHouse")
	c := qt.New(t)

	db, err := sql.Open("clickhouse", dsn)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()

	_, err = db.Exec("DROP TABLE IF EXISTS " + dmlExecutionTable)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec("CREATE TABLE " + dmlExecutionTable + " (id Int64, name String) ENGINE = MergeTree ORDER BY id")
	c.Assert(err, qt.IsNil)
	defer func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + dmlExecutionTable) }()

	insert, insertArgs, err := query.RenderInsert(&query.InsertStatement{
		Table:   dmlExecutionTable,
		Columns: []string{"id", "name"},
		Rows:    [][]query.Expression{{&query.BoundValue{Value: int64(1)}, &query.BoundValue{Value: "a"}}},
	}, "clickhouse")
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(insert, insertArgs...)
	c.Assert(err, qt.IsNil, qt.Commentf("rendered INSERT: %s", insert))

	selectSQL, selectArgs, err := query.RenderSelect(&query.SelectStatement{
		Columns: []query.ResultColumn{{Name: "id"}, {Name: "name"}},
		From:    dmlExecutionTable,
		Where:   dmlWhereID(1),
	}, "clickhouse")
	c.Assert(err, qt.IsNil)
	var gotID int64
	var gotName string
	c.Assert(db.QueryRow(selectSQL, selectArgs...).Scan(&gotID, &gotName), qt.IsNil,
		qt.Commentf("rendered SELECT: %s", selectSQL))
	c.Assert(gotID, qt.Equals, int64(1))
	c.Assert(gotName, qt.Equals, "a")

	// DELETE is rendered for ClickHouse because the engine executes the
	// portable spelling -- lightweight delete has been on by default since
	// 23.3, and refusing it here was once stricter than the server.
	del, delArgs, err := query.RenderDelete(&query.DeleteStatement{
		Table: dmlExecutionTable,
		Where: dmlWhereID(1),
	}, "clickhouse")
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(del, delArgs...)
	c.Assert(err, qt.IsNil, qt.Commentf("rendered DELETE: %s", del))

	// And UPDATE is refused with the engine's reason rather than rendered. The
	// refusal belongs beside the executions: without it, deleting the arm would
	// leave every other assertion here green.
	_, _, err = query.RenderUpdate(&query.UpdateStatement{
		Table: dmlExecutionTable,
		Set:   []query.Assignment{{Column: "name", Value: &query.BoundValue{Value: "z"}}},
		Where: dmlWhereID(1),
	}, "clickhouse")
	c.Assert(err, qt.ErrorMatches, `renderer: UPDATE is not a portable statement on ClickHouse.*`)
}
