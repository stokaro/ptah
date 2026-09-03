package postgres_test

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
	"go.5x5.cz/ptah/internal/dbschema/postgres"
)

// errConnectionReset stands in for whatever the server or the driver reports
// when a result set ends before its rows do. Its identity is what these
// assertions follow, so none of them depends on the wording of a message the
// driver would have written.
var errConnectionReset = errors.New("connection reset by peer")

// enumMarker identifies the enum read among the two dozen a full schema read
// asks. It is the catalog table the query joins, so it names the read rather
// than a formatting detail of the SQL.
const enumMarker = "pg_enum"

// catalogAnswers answers a full ReadSchemaContext.
//
// Every read gets an empty result set except the two the reader cannot proceed
// without: the capability probe, asked as a single EXISTS row, and the enum
// read, which carries the one row this file's failure path truncates.
//
// truncation reshapes one read's answer so that it ends in a terminal row error
// rather than in the end of its result set. It sees the queries in the order the
// reader asks them, so the index it receives is a position in that order.
func catalogAnswers(truncation func(index int, query string, answer dbtest.QueryResult) dbtest.QueryResult, asked *[]string) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		index := len(*asked)
		*asked = append(*asked, strings.Join(strings.Fields(query), " "))

		result := dbtest.QueryResult{Columns: []string{"a", "b", "c", "d", "e", "f", "g", "h"}}
		if strings.Contains(query, "SELECT EXISTS") {
			result = dbtest.QueryResult{Columns: []string{"exists"}, Rows: [][]driver.Value{{true}}}
		}
		if strings.Contains(query, "has_table_privilege") {
			result = dbtest.QueryResult{Columns: []string{"has_table_privilege"}, Rows: [][]driver.Value{{false}}}
		}
		if strings.Contains(query, enumMarker) {
			result = dbtest.QueryResult{
				Columns: []string{"enum_name", "enum_value"},
				Rows:    [][]driver.Value{{"color", "red"}},
			}
		}
		return truncation(index, query, result), nil
	}
}

// intact answers every read to the end.
func intact(_ int, _ string, answer dbtest.QueryResult) dbtest.QueryResult {
	return answer
}

// afterItsRows breaks the read the marker names once every row it carries has
// been handed over. This is the reported shape: the rows that arrived are valid
// and the failure is observable only through Rows.Err.
func afterItsRows(marker string) func(int, string, dbtest.QueryResult) dbtest.QueryResult {
	return func(_ int, query string, answer dbtest.QueryResult) dbtest.QueryResult {
		if !strings.Contains(query, marker) {
			return answer
		}
		answer.TerminalErr = errConnectionReset
		return answer
	}
}

// beforeItsRows breaks the read at one position and delivers none of its rows.
// Truncating to nothing is what makes the property uniform across every read: a
// single-row probe whose one row DID arrive has nothing left to lose, so
// tolerating ITS terminal error is correct rather than a gap, and a property
// that had to carve out that one position would stop being a property.
func beforeItsRows(position int) func(int, string, dbtest.QueryResult) dbtest.QueryResult {
	return func(index int, _ string, answer dbtest.QueryResult) dbtest.QueryResult {
		if index != position {
			return answer
		}
		answer.Rows = nil
		answer.TerminalErr = errConnectionReset
		return answer
	}
}

func readAnswering(c *qt.C, handler dbtest.QueryHandler) (*catalog.Database, error) {
	db := dbtest.Open(c, handler)
	reader := postgres.NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())
	return reader.ReadSchemaContext(c.Context())
}

// TestReadSchemaContext_TruncatedEnumReadFailurePath is the shape
// stokaro/ptah#2720 reported. One enum row arrives, the connection then drops,
// and rows.Next() answers false for the closed result set exactly as it does
// for an exhausted one.
//
// Measured against the pre-fix reader, this read returned a *catalog.Database
// holding `color` with the single value `red` and a nil error, so an enum whose
// remaining values the server never got to send reached the caller as complete
// state. What the planner does with that is add the values it cannot see to a
// type that already has them.
//
// The assertion is on the error AND on the absent schema. Reporting an error
// while still handing back the truncated catalog would leave a caller that logs
// and carries on fingerprinting exactly the state this refuses.
func TestReadSchemaContext_TruncatedEnumReadFailurePath(t *testing.T) {
	c := qt.New(t)

	var asked []string
	schema, err := readAnswering(c, catalogAnswers(afterItsRows(enumMarker), &asked))

	c.Assert(err, qt.ErrorIs, errConnectionReset)
	c.Assert(schema, qt.IsNil)
}

// TestReadSchemaContext_IntactEnumReadHappyPath is the control for the test
// above, and it is not decoration: without it, a reader that failed the enum
// read outright -- or a fixture whose enum row never parsed -- would satisfy
// the refusal while proving nothing about terminal errors. The same handler,
// asked to break nothing, has to produce the enum the failure path truncates.
//
// The empty-schema row is the third acceptance criterion of stokaro/ptah#2720
// read back: catalog.Database.Enums carries no `omitempty`, so nil and []
// serialize as `null` and `[]` and hash differently. The terminal check must
// not reach for an empty slice on its way past.
func TestReadSchemaContext_IntactEnumReadHappyPath(t *testing.T) {
	t.Run("the enum the failure path truncates arrives whole", func(t *testing.T) {
		c := qt.New(t)

		var asked []string
		schema, err := readAnswering(c, catalogAnswers(intact, &asked))

		c.Assert(err, qt.IsNil)
		c.Assert(schema, qt.IsNotNil)
		c.Assert(schema.Enums, qt.HasLen, 1)
		c.Assert(schema.Enums[0].Name, qt.Equals, "color")
		c.Assert(schema.Enums[0].Values, qt.DeepEquals, []string{"red"})
	})

	t.Run("a schema with no enums keeps a nil slice", func(t *testing.T) {
		c := qt.New(t)

		var asked []string
		handler := func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
			result, err := catalogAnswers(intact, &asked)(query, args)
			return withoutEnumRows(query, result), err
		}
		schema, err := readAnswering(c, handler)

		c.Assert(err, qt.IsNil)
		c.Assert(schema, qt.IsNotNil)
		c.Assert(schema.Enums, qt.IsNil)
	})
}

// withoutEnumRows empties the enum read alone, leaving every other answer as it
// was.
func withoutEnumRows(query string, result dbtest.QueryResult) dbtest.QueryResult {
	if !strings.Contains(query, enumMarker) {
		return result
	}
	result.Rows = nil
	return result
}

// TestReadSchemaContext_EveryReadRefusesATerminalRowErrorFailurePath asserts
// the property rather than the instance. stokaro/ptah#2720 reported one read;
// a missing terminal check is a class, and eleven of this reader's functions
// were missing one -- twelve positions in the order below, because queryRoles
// is asked twice.
//
// Each position is broken in turn and the whole read has to fail. A read added
// later that iterates without checking Rows.Err turns this red without anyone
// having to remember the rule, which is what the reported defect needed and did
// not have.
func TestReadSchemaContext_EveryReadRefusesATerminalRowErrorFailurePath(t *testing.T) {
	c := qt.New(t)
	var order []string
	_, err := readAnswering(c, catalogAnswers(intact, &order))
	c.Assert(err, qt.IsNil)
	c.Assert(len(order) > 20, qt.IsTrue, qt.Commentf("a full read asks about two dozen queries; got %d", len(order)))

	for position, query := range order {
		t.Run(fmt.Sprintf("query %02d", position), func(t *testing.T) {
			c := qt.New(t)

			var asked []string
			schema, err := readAnswering(c, catalogAnswers(beforeItsRows(position), &asked))

			c.Assert(err, qt.ErrorIs, errConnectionReset, qt.Commentf("query: %s", truncate(query)))
			c.Assert(schema, qt.IsNil)
		})
	}
}

// truncate keeps a failure comment readable: these queries run to dozens of
// lines and the opening projection is enough to name the read.
func truncate(query string) string {
	if len(query) <= 90 {
		return query
	}
	return query[:90] + "..."
}
