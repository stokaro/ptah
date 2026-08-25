package oracle

// White-box testing required: readSequences is package-local and the two fields
// under test are filled from columns of its own query, which no exported API
// exposes on their own.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// sequenceNamed reads the fake catalog and returns the sequence named.
func sequenceNamed(c *qt.C, answer dbtest.QueryHandler, name string) (start, cache *int64) {
	c.Helper()
	db := dbtest.Open(c.TB, answer)
	reader := NewOracleReader(db.SQL, "PTAHSQ")

	sequences, err := reader.readSequences()
	c.Assert(err, qt.IsNil)

	for _, sequence := range sequences {
		if sequence.Name == name {
			return sequence.Start, sequence.Cache
		}
	}
	c.Fatalf("no sequence %q in %+v", name, sequences)
	return nil, nil
}

// TestReadSequences_ReportsTheStartCounter pins the value Oracle keeps instead
// of a START WITH.
//
// Oracle has no START WITH column. user_sequences.last_number is the next value
// the sequence will issue, and the read never asked for it, so a sequence
// created START WITH 500 was described with no start at all and replayed from 1
// -- measured on Oracle 23, the source's first nextval was 500 and the replay's
// was 1 (stokaro/ptah#2207).
func TestReadSequences_ReportsTheStartCounter(t *testing.T) {
	c := qt.New(t)

	start, _ := sequenceNamed(c, answeringSequenceCounters, "SEQ_CACHED")

	c.Assert(start, qt.IsNotNil)
	c.Assert(*start, qt.Equals, int64(500))
}

// TestReadSequences_ReportsTheCacheSize is the other half. The column was
// already read; nothing rendered it, so a declared cache was planned on every
// run and never applied.
func TestReadSequences_ReportsTheCacheSize(t *testing.T) {
	c := qt.New(t)

	_, cache := sequenceNamed(c, answeringSequenceCounters, "SEQ_CACHED")

	c.Assert(cache, qt.IsNotNil)
	c.Assert(*cache, qt.Equals, int64(42))
}

// TestReadSequences_LeavesAnUnreportedCounterUnset is the control.
//
// A catalog that answers NULL for either column must leave the pointer nil
// rather than inventing a zero: the comparator reads a nil as "not managed", and
// a zero would be a start of 0 and a cache of NOCACHE, neither of which the
// sequence has.
func TestReadSequences_LeavesAnUnreportedCounterUnset(t *testing.T) {
	c := qt.New(t)

	start, cache := sequenceNamed(c, answeringSequenceCounters, "SEQ_UNKNOWN")

	c.Assert(start, qt.IsNil)
	c.Assert(cache, qt.IsNil)
}

// answeringSequenceCounters answers what the QUERY asks for.
//
// A fake returning a fixed shape cannot tell a projection that dropped a column
// from one that kept it: the scan still gets as many values as it has
// destinations, so a mutant removing last_number would pass as killed.
func answeringSequenceCounters(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "last_number") {
		return dbtest.QueryResult{
			Columns: []string{"sequence_name", "min_value", "max_value", "increment_by", "cycle_flag", "cache_size"},
			Rows: [][]driver.Value{
				{"SEQ_CACHED", "1", "999", "7", "N", "42"},
				{"SEQ_UNKNOWN", "1", "999", "1", "N", nil},
			},
		}, nil
	}
	return dbtest.QueryResult{
		Columns: []string{
			"sequence_name", "min_value", "max_value", "increment_by",
			"cycle_flag", "cache_size", "last_number",
		},
		Rows: [][]driver.Value{
			{"SEQ_CACHED", "1", "999", "7", "N", "42", "500"},
			{"SEQ_UNKNOWN", "1", "999", "1", "N", nil, nil},
		},
	}, nil
}
