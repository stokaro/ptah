//go:build integration

package ydb_test

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/query"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/managedrows"
	"ptah.run/migration/datadiff"
)

// The directories the data tests write into. Each test owns one, so a scoped
// read sees only its tables.
const (
	dataQuerySchema    = "ptah_ydb_data/query"
	dataDiffSchema     = "ptah_ydb_data/diff"
	dataDeclaredSchema = "ptah_ydb_data/declared"
)

// ownDirectory drops every table in schema before the test and after it.
func ownDirectory(c *qt.C, conn *dbschema.DatabaseConnection, schema string) {
	c.Helper()
	dropTables(c, conn, []string{schema})
	c.Cleanup(func() { dropTables(c, conn, []string{schema}) })
}

// execRendered returns what runs a statement the query builder rendered; it
// takes the render function's three results as they are.
func execRendered(c *qt.C, conn *dbschema.DatabaseConnection) func(string, []any, error) {
	return func(text string, args []any, err error) {
		c.Helper()
		c.Assert(err, qt.IsNil)
		_, err = conn.ExecContext(c.Context(), text, args...)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", text))
	}
}

// queryRendered returns what runs a query the builder rendered and returns its
// rows, each column scanned into the Go value the driver chooses.
func queryRendered(c *qt.C, conn *dbschema.DatabaseConnection) func(string, []any, error) [][]any {
	return func(text string, args []any, err error) [][]any {
		c.Helper()
		c.Assert(err, qt.IsNil)
		got, err := scanAll(c.Context(), conn, text, args)
		c.Assert(err, qt.IsNil, qt.Commentf("query: %s", text))
		return got
	}
}

// scanAll runs a query and returns its rows, and the server's refusal as an
// error rather than a failed assertion, for the tests that compare it.
func scanAll(ctx context.Context, conn *dbschema.DatabaseConnection, text string, args []any) ([][]any, error) {
	rows, err := conn.QueryContext(ctx, text, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		targets := make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		out = append(out, values)
	}
	return out, rows.Err()
}

// readSorted reads a table through dbschema.ReadTableRows, ordered by the
// first column.
func readSorted(c *qt.C, conn *dbschema.DatabaseConnection, schema, table string, columns ...string) []map[string]any {
	c.Helper()
	rows, err := dbschema.ReadTableRows(c.Context(), conn, schema, table, columns)
	c.Assert(err, qt.IsNil)
	slices.SortFunc(rows, func(a, b map[string]any) int {
		return cmp.Compare(fmt.Sprint(a[columns[0]]), fmt.Sprint(b[columns[0]]))
	})
	return rows
}

// TestYDBQueryBuilder_RunsWhatItRenders sends the server every kind of
// statement the builder renders for YDB -- an INSERT, an UPSERT over existing
// and new rows, a join paged with LIMIT and OFFSET, an IN subquery, an
// uncorrelated EXISTS, a grouped count -- and reads each answer back.
func TestYDBQueryBuilder_RunsWhatItRenders(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			caps := conn.Info().Capabilities
			ownDirectory(c, conn, dataQuerySchema)
			users, orders := dataQuerySchema+"/users", dataQuerySchema+"/orders"
			apply(c, conn, []string{
				"CREATE TABLE `" + users + "` (id Int64 NOT NULL, name Utf8, age Int32, PRIMARY KEY (id))",
				"CREATE TABLE `" + orders + "` (id Int64 NOT NULL, user_id Int64, total Double, PRIMARY KEY (id))",
			})

			execRendered(c, conn)(query.RenderInsertWithCapabilities(query.InsertInto(users).
				Columns("id", "name", "age").Values(int64(1), "alice", int32(30)).Values(int64(2), "bob", int32(40)).
				Build(), platform.YDB, caps))
			execRendered(c, conn)(query.RenderInsertWithCapabilities(query.InsertInto(orders).
				Columns("id", "user_id", "total").
				Values(int64(10), int64(1), 5.5).Values(int64(11), int64(1), 7.0).Values(int64(12), int64(2), 1.0).
				Build(), platform.YDB, caps))
			// The UPSERT writes the name of user 1 over the stored one, keeps the age
			// it does not name, and inserts user 3.
			execRendered(c, conn)(query.RenderInsertWithCapabilities(query.UpsertInto(users).
				Columns("id", "name").Values(int64(1), "alicia").Values(int64(3), "carol").
				Build(), platform.YDB, caps))

			c.Assert(readSorted(c, conn, dataQuerySchema, "users", "name", "id", "age"), qt.DeepEquals, []map[string]any{
				{"id": int64(1), "name": "alicia", "age": int32(30)},
				{"id": int64(2), "name": "bob", "age": int32(40)},
				{"id": int64(3), "name": "carol", "age": nil},
			})
			c.Assert(queryRendered(c, conn)(query.RenderSelectWithCapabilities(query.Select().
				Columns(query.Col("u", "name"), query.Col("o", "total")).
				FromAs(users, "u").
				InnerJoin(orders, "o", query.Col("o", "user_id").EqCol(query.Col("u", "id"))).
				Where(query.Col("o", "total").Gt(2.0)).
				OrderBy(query.Col("o", "total").Desc()).
				Limit(1).Offset(1).
				Build(), platform.YDB, caps)), qt.DeepEquals, [][]any{{"alicia", 5.5}})
			c.Assert(queryRendered(c, conn)(query.RenderSelectWithCapabilities(query.Select("id").From(users).
				Where(query.InQuery("id", query.Select("user_id").From(orders))).
				OrderBy(query.Asc("id")).
				Build(), platform.YDB, caps)), qt.DeepEquals, [][]any{{int64(1)}, {int64(2)}})
			c.Assert(queryRendered(c, conn)(query.RenderSelectWithCapabilities(query.Select("id").From(users).
				Where(query.Exists(query.Select("id").From(orders).Where(query.Gt("total", 6.0)))).
				OrderBy(query.Asc("id")).
				Build(), platform.YDB, caps)), qt.DeepEquals, [][]any{{int64(1)}, {int64(2)}, {int64(3)}})
			c.Assert(queryRendered(c, conn)(query.RenderSelectWithCapabilities(query.Select().
				Columns(query.Col("o", "user_id")).
				ExprAs(query.CountStar(), "n").
				FromAs(orders, "o").
				GroupBy(query.Col("o", "user_id")).
				Having(query.Expr(query.CountStar()).Gt(uint64(1))).
				Build(), platform.YDB, caps)), qt.DeepEquals, [][]any{{int64(1), uint64(2)}})
		})
	}
}

// TestYDBQueryBuilder_ReturningFollowsTheLine holds returning_clause to the
// server. The UPDATE ... RETURNING is rendered for a line that has the key and
// sent to this one: it runs exactly where this line's preset holds the key,
// and the builder refuses it for this line exactly where it does not.
func TestYDBQueryBuilder_ReturningFollowsTheLine(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			caps := conn.Info().Capabilities
			ownDirectory(c, conn, dataQuerySchema)
			accounts := dataQuerySchema + "/accounts"
			apply(c, conn, []string{
				"CREATE TABLE `" + accounts + "` (id Int64 NOT NULL, email Utf8, plan Utf8, PRIMARY KEY (id), " +
					"INDEX accounts_email GLOBAL UNIQUE SYNC ON (email))",
			})
			inserted := queryRendered(c, conn)(query.RenderInsertWithCapabilities(query.InsertInto(accounts).
				Columns("id", "email", "plan").Values(int64(1), "a@x", "free").Returning("id").
				Build(), platform.YDB, capability.YDB262()))
			update := query.Update(accounts).Set("plan", "paid").Where(query.Eq("id", int64(1))).Returning("plan").Build()

			text, args, err := query.RenderUpdateWithCapabilities(update, platform.YDB, capability.YDB262())
			c.Assert(err, qt.IsNil)
			_, serverErr := scanAll(c.Context(), conn, text, args)
			_, _, builderErr := query.RenderUpdateWithCapabilities(update, platform.YDB, caps)

			c.Assert(inserted, qt.DeepEquals, [][]any{{int64(1)}})
			c.Assert(serverErr == nil, qt.Equals, caps.Has(capability.ReturningClause), qt.Commentf("server: %v", serverErr))
			c.Assert(builderErr == nil, qt.Equals, caps.Has(capability.ReturningClause))
		})
	}
}

// upsertNamingNoIndexColumn is what each certified line answers to an UPSERT
// that names no column a synchronous index of the table is keyed on, on a
// table with a unique index. 26.2.1.14 fails it with an internal error, which
// is a defect in the server, and 25.1.4.7 runs it. The YDB page documents
// the defect and the workaround; this entry turns red when a line stops
// answering the way the page says.
var upsertNamingNoIndexColumn = map[string]string{
	"26.2": `(?s).*INTERNAL_ERROR.*verification=!hasUniqIndex \|\| !usedIndexes\.empty\(\);` +
		`fline=kqp_opt_phy_upsert_index\.cpp:359.*`,
	"25.1": `<nil>`,
}

// An UPSERT the query builder renders on a table with a unique index runs on
// both lines when it names every column, which is the workaround the YDB page
// gives for the 26.2 defect, and the statement that omits the indexed column
// gets the answer upsertNamingNoIndexColumn records for the line. A failed
// UPSERT writes nothing.
func TestYDBQueryBuilder_UpsertOnATableWithAUniqueIndex(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			caps := conn.Info().Capabilities
			ownDirectory(c, conn, dataQuerySchema)
			accounts := dataQuerySchema + "/accounts"
			apply(c, conn, []string{
				"CREATE TABLE `" + accounts + "` (id Int64 NOT NULL, email Utf8, plan Utf8, PRIMARY KEY (id), " +
					"INDEX accounts_email GLOBAL UNIQUE SYNC ON (email))",
			})
			execRendered(c, conn)(query.RenderInsertWithCapabilities(query.UpsertInto(accounts).
				Columns("id", "email", "plan").Values(int64(1), "a@x", "free").
				Build(), platform.YDB, caps))

			text, args, err := query.RenderInsertWithCapabilities(query.UpsertInto(accounts).
				Columns("id", "plan").Values(int64(2), "trial").
				Build(), platform.YDB, caps)
			c.Assert(err, qt.IsNil)
			_, partialErr := conn.ExecContext(c.Context(), text, args...)
			afterPartial := readSorted(c, conn, dataQuerySchema, "accounts", "id", "email", "plan")
			execRendered(c, conn)(query.RenderInsertWithCapabilities(query.UpsertInto(accounts).
				Columns("id", "email", "plan").Values(int64(1), "a@x", "paid").
				Build(), platform.YDB, caps))

			c.Assert(fmt.Sprint(partialErr), qt.Matches, upsertNamingNoIndexColumn[line.name])
			c.Assert(afterPartial, qt.HasLen, map[string]int{"26.2": 1, "25.1": 2}[line.name])
			c.Assert(readSorted(c, conn, dataQuerySchema, "accounts", "id", "email", "plan")[0], qt.DeepEquals,
				map[string]any{"id": int64(1), "email": "a@x", "plan": "paid"})
		})
	}
}

// diffDeclaration is a reference table whose columns cover the YDB types a
// declared row is written into on every certified line: a declared TIMESTAMP
// and DATE land on the 64-bit types where the line has them, and DECIMAL(22,9)
// is the decimal 25.1 takes.
func diffDeclaration(schema string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Country", Name: "countries", Schema: schema}},
		Fields: []schemamodel.Field{
			{StructName: "Country", Name: "code", Type: "VARCHAR(8)", Primary: true},
			{StructName: "Country", Name: "rank", Type: "INTEGER", Nullable: true},
			{StructName: "Country", Name: "population", Type: "BIGINT", Nullable: true},
			{StructName: "Country", Name: "rate", Type: "DECIMAL(22,9)", Nullable: true},
			{StructName: "Country", Name: "joined", Type: "TIMESTAMP", Nullable: true},
			{StructName: "Country", Name: "founded", Type: "DATE", Nullable: true},
			{StructName: "Country", Name: "active", Type: "BOOLEAN", Nullable: true},
			{StructName: "Country", Name: "ratio", Type: "DOUBLE PRECISION", Nullable: true},
			{StructName: "Country", Name: "flag", Type: "BYTEA", Nullable: true},
			{StructName: "Country", Name: "meta", Type: "JSONB", Nullable: true},
			{StructName: "Country", Name: "uid", Type: "UUID", Nullable: true},
			{StructName: "Country", Name: "tiny", Type: "SMALLINT UNSIGNED", Nullable: true},
		},
	}
	schemamodel.Finalize(db)
	return db
}

var diffColumns = []string{
	"code", "rank", "population", "rate", "joined", "founded", "active", "ratio", "flag", "meta", "uid", "tiny",
}

// diffRows is a declared row set, in the Go values a declaration resolves to:
// an int for every integer column, the text a person writes for a decimal, a
// moment, a UUID and a JSON document, and a float for a double.
func diffRows(czRank int) []map[string]any {
	return []map[string]any{{
		"code": "CZ", "rank": czRank, "population": 10900000, "rate": "1.50", "joined": "2004-05-01T00:00:00Z",
		"founded": "1993-01-01", "active": true, "ratio": 0.5, "flag": "\x01\xff", "meta": `{"b": 2, "a": 1.50}`,
		"uid": "550E8400-E29B-41D4-A716-446655440000", "tiny": 7,
	}, {
		"code": "SK", "rank": 2, "population": 5400000, "rate": nil, "joined": nil, "founded": "1993-01-01",
		"active": false, "ratio": nil, "flag": nil, "meta": nil, "uid": nil, "tiny": nil,
	}}
}

// diffRowsWithDE is diffRows with a third row.
func diffRowsWithDE(czRank int) []map[string]any {
	return append(diffRows(czRank), map[string]any{
		"code": "DE", "rank": 3, "population": 84000000, "rate": 0.25, "joined": "1995-01-01T00:00:00Z",
		"founded": "1990-10-03", "active": true, "ratio": 1.25, "flag": nil, "meta": `{}`, "uid": nil, "tiny": 0,
	})
}

// compareDeclared asks managedrows for the diff that writes rows, with
// everything a migration body needs to undo it.
func compareDeclared(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	db *schemamodel.Database,
	rows []map[string]any,
) *datadiff.DataDiff {
	c.Helper()
	diff, err := managedrows.Compare(c.Context(), conn, managedrows.Request{
		Desired: db,
		Declaration: schemamodel.ManagedData{
			StructName: "Country", Table: "countries", Schema: dataDiffSchema, Keys: []string{"code"},
		},
		Rows:              rows,
		Live:              readScoped(c, conn, []string{dataDiffSchema}),
		CatalogIsComplete: true,
		Intent:            managedrows.Write,
	})
	c.Assert(err, qt.IsNil)
	return diff
}

func changes(diff *datadiff.DataDiff) [3]int {
	return [3]int{len(diff.Inserts), len(diff.Updates), len(diff.Deletes)}
}

// TestYDBDataDiff_RoundTrips writes a declared row set through the data diff,
// reads it back, and finds nothing left to write; then moves the declaration,
// applies the change and its reverse, and lands where each started.
//
// Every value is written in its column's own type, an Int32 column taking a Go
// int, and read back in the form the driver returns it. Without the typed
// literals the server refuses the INSERT, and without the canonical comparison
// the declared `1.50` never pairs with the 1.5 a Decimal reads back, so the
// second comparison plans an UPDATE again.
func TestYDBDataDiff_RoundTrips(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			ownDirectory(c, conn, dataDiffSchema)
			db := diffDeclaration(dataDiffSchema)
			apply(c, conn, planAgainst(c, conn, db, []string{dataDiffSchema}))

			first := compareDeclared(c, conn, db, diffRows(1))
			up, _, err := datadiff.RenderStatements(first, conn.Info().Dialect)
			c.Assert(err, qt.IsNil)
			apply(c, conn, up)
			written := readSorted(c, conn, dataDiffSchema, "countries", diffColumns...)
			converged := compareDeclared(c, conn, db, diffRows(1))

			moved := compareDeclared(c, conn, db, diffRowsWithDE(9))
			forward, backward, err := datadiff.RenderStatements(moved, conn.Info().Dialect)
			c.Assert(err, qt.IsNil)
			apply(c, conn, forward)
			afterForward := compareDeclared(c, conn, db, diffRowsWithDE(9))
			movedRows := readSorted(c, conn, dataDiffSchema, "countries", diffColumns...)
			apply(c, conn, backward)
			afterBackward := readSorted(c, conn, dataDiffSchema, "countries", diffColumns...)

			day := func(year int, month time.Month, d int) time.Time {
				return time.Date(year, month, d, 0, 0, 0, 0, time.UTC)
			}
			c.Assert(changes(first), qt.Equals, [3]int{2, 0, 0})
			c.Assert(written, qt.DeepEquals, []map[string]any{{
				"code": "CZ", "rank": int32(1), "population": int64(10900000), "rate": "1.5", "joined": day(2004, 5, 1),
				"founded": day(1993, 1, 1), "active": true, "ratio": 0.5, "flag": []byte{0x01, 0xff},
				"meta": `{"a":1.5,"b":2}`, "uid": "550e8400-e29b-41d4-a716-446655440000", "tiny": uint16(7),
			}, {
				"code": "SK", "rank": int32(2), "population": int64(5400000), "rate": nil, "joined": nil,
				"founded": day(1993, 1, 1), "active": false, "ratio": nil, "flag": nil, "meta": nil, "uid": nil, "tiny": nil,
			}})
			c.Assert(changes(converged), qt.Equals, [3]int{0, 0, 0})
			c.Assert(changes(moved), qt.Equals, [3]int{1, 1, 0})
			c.Assert(changes(afterForward), qt.Equals, [3]int{0, 0, 0})
			c.Assert(movedRows, qt.HasLen, 3)
			c.Assert(movedRows[1]["rate"], qt.Equals, "0.25")
			c.Assert(afterBackward, qt.DeepEquals, written)
		})
	}
}

// declaredRowsSchema is a reference table carried by a declaration, with its
// rows spelled as the YAML a person writes resolves them.
func declaredRowsSchema(population string) *schemamodel.Database {
	db := diffDeclaration(dataDeclaredSchema)
	db.ManagedData = []schemamodel.ManagedData{{
		StructName: "Country", Table: "countries", Schema: dataDeclaredSchema, Keys: []string{"code"},
		File: "countries.yaml",
		Rows: []schemamodel.ManagedRow{{
			"code":       {Tag: "str", Text: "CZ"},
			"rank":       {Tag: "int", Text: "1"},
			"population": {Tag: "int", Text: population},
			"rate":       {Tag: "str", Text: "1.50"},
			"joined":     {Tag: "timestamp", Text: "2004-05-01"},
			"founded":    {Tag: "str", Text: "1993-01-01"},
			"active":     {Tag: "bool", Text: "true"},
			"ratio":      {Tag: "float", Text: "0.5"},
			"meta":       {Tag: "str", Text: `{"b": 2, "a": 1}`},
			"uid":        {Tag: "str", Text: "550E8400-E29B-41D4-A716-446655440000"},
			"tiny":       {Tag: "int", Text: "7"},
		}},
	}}
	return db
}

// declaredRowsPlan is the plan `ptah schema apply` prepares for the declared
// directory.
func declaredRowsPlan(c *qt.C, conn *dbschema.DatabaseConnection, desired *schemamodel.Database) []string {
	c.Helper()
	plan, err := atlasschema.PlanApply(c.Context(), conn, atlasschema.ApplyOptions{
		Desired: desired, Schemas: []string{dataDeclaredSchema},
		Runtime: must.Must(builtin.New())})
	c.Assert(err, qt.IsNil)
	return plan.Statements()
}

// TestYDBDeclaredRows_ConvergeThroughSchemaApply applies the plan `ptah schema
// apply` prepares for a table and its declared rows, plans again and finds
// nothing, then changes one declared value and finds exactly the one UPDATE
// that writes it.
func TestYDBDeclaredRows_ConvergeThroughSchemaApply(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			ownDirectory(c, conn, dataDeclaredSchema)

			first := declaredRowsPlan(c, conn, declaredRowsSchema("10900000"))
			apply(c, conn, first)
			again := declaredRowsPlan(c, conn, declaredRowsSchema("10900000"))
			changed := declaredRowsPlan(c, conn, declaredRowsSchema("11000000"))
			apply(c, conn, changed)
			afterChange := declaredRowsPlan(c, conn, declaredRowsSchema("11000000"))
			stored := readSorted(c, conn, dataDeclaredSchema, "countries", "code", "rank", "population", "joined")

			c.Assert(first, qt.HasLen, 2)
			c.Assert(again, qt.HasLen, 0)
			c.Assert(changed, qt.DeepEquals, []string{
				"UPDATE `" + dataDeclaredSchema + "/countries` SET `active` = true, `founded` = " + dateLiteral(conn) +
					", `joined` = " + timestampLiteral(conn) + ", `meta` = JsonDocument('{\"a\":1,\"b\":2}'), " +
					"`population` = 11000000l, `rank` = 1, `rate` = Decimal('1.5', 22, 9), `ratio` = Double('0.5'), " +
					"`tiny` = 7us, `uid` = Uuid('550e8400-e29b-41d4-a716-446655440000') WHERE `code` = 'CZ'u;",
			})
			c.Assert(afterChange, qt.HasLen, 0)
			c.Assert(stored, qt.DeepEquals, []map[string]any{{
				"code": "CZ", "rank": int32(1), "population": int64(11000000),
				"joined": time.Date(2004, time.May, 1, 0, 0, 0, 0, time.UTC),
			}})
		})
	}
}

// dateLiteral and timestampLiteral are the literals the declared day and
// moment are written as on this line: the 64-bit types where it has them.
func dateLiteral(conn *dbschema.DatabaseConnection) string {
	if conn.Info().Capabilities.Has(capability.WideDateTimeTypes) {
		return "Date32('1993-01-01')"
	}
	return "Date('1993-01-01')"
}

func timestampLiteral(conn *dbschema.DatabaseConnection) string {
	if conn.Info().Capabilities.Has(capability.WideDateTimeTypes) {
		return "Timestamp64('2004-05-01T00:00:00Z')"
	}
	return "Timestamp('2004-05-01T00:00:00Z')"
}

// TestYDBDeclaredRows_RefuseAValueTheColumnCannotHold declares a value the live
// column cannot hold. The type comes from the table the server built, and the
// value is refused with the column and the type before any statement exists,
// rather than wrapped or left for the server to convert.
func TestYDBDeclaredRows_RefuseAValueTheColumnCannotHold(t *testing.T) {
	tests := []struct {
		name    string
		column  string
		value   any
		wantErr string
	}{
		{name: "an integer over Int32", column: "rank", value: int64(5000000000),
			wantErr: `declared row 1, column "rank": Int32 cannot hold 5000000000: it is outside the range ` +
				`-2147483648 to 2147483647`},
		{name: "a negative value for Uint16", column: "tiny", value: -1,
			wantErr: `declared row 1, column "tiny": Uint16 cannot hold -1: it is outside the range 0 to 65535`},
		{name: "a moment for a Utf8 key", column: "code", value: time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC),
			wantErr: `declared row 1, column "code": Utf8 cannot hold 2026-01-02 00:00:00 \+0000 UTC: it is a Go ` +
				`time.Time, which is not a Utf8 value`},
	}

	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			ownDirectory(c, conn, dataDiffSchema)
			db := diffDeclaration(dataDiffSchema)
			apply(c, conn, planAgainst(c, conn, db, []string{dataDiffSchema}))
			live := readScoped(c, conn, []string{dataDiffSchema})
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					rows := diffRows(1)[:1]
					rows[0][test.column] = test.value

					diff, err := managedrows.Compare(c.Context(), conn, managedrows.Request{
						Desired: db,
						Declaration: schemamodel.ManagedData{
							StructName: "Country", Table: "countries", Schema: dataDiffSchema, Keys: []string{"code"},
						},
						Rows:              rows,
						Live:              live,
						CatalogIsComplete: true,
						Intent:            managedrows.Write,
					})

					c.Assert(err, qt.ErrorMatches, `compare declared rows of .*countries: `+test.wantErr)
					c.Assert(diff, qt.IsNil)
				})
			}
		})
	}
}
