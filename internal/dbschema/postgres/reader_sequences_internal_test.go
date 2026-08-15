package postgres

// White-box testing required: readSequencesForSchema decides whether a sequence
// is standalone or a backing sequence of its owning column entirely inside the
// SQL it issues, and drops the backing ones before anything exported can see
// them. The decision has no other source, so there is nothing to assert on
// without either a live server or this fake one.
//
// The fake server follows the same rule as the column and index guards next
// door: a projection is answered from the fixture catalog only when the query
// actually reads the catalog column the answer comes from.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// pgSequenceCatalog is one sequence as PostgreSQL 17.10 reports it, reduced to
// the three facts the is_implicit decision rests on.
type pgSequenceCatalog struct {
	name string
	// ownedColumn is the column the sequence is OWNED BY, empty when it is
	// owned by nothing.
	ownedColumn string
	// ownerDefaultDrawsFromSequence is whether that column's DEFAULT calls
	// nextval on this sequence. Ownership alone does not make a sequence
	// implicit -- a lifecycle-only OWNED BY sequence is standalone (#657).
	ownerDefaultDrawsFromSequence bool
	// ownerColumnTypeCategory is pg_type.typtype for the owning column's type:
	// "b" for a base type such as integer, "d" for a domain.
	ownerColumnTypeCategory string
}

// serialSequenceCatalog is `id SERIAL PRIMARY KEY`: the sequence is owned by an
// integer column whose default draws from it, and writing that column back as
// SERIAL recreates it. It must stay implicit.
func serialSequenceCatalog() pgSequenceCatalog {
	return pgSequenceCatalog{
		name:                          "t_id_seq",
		ownedColumn:                   "id",
		ownerDefaultDrawsFromSequence: true,
		ownerColumnTypeCategory:       "b",
	}
}

// domainSequenceCatalog is the same shape over a domain column:
//
//	CREATE DOMAIN positive AS integer CHECK (VALUE > 0);
//	CREATE SEQUENCE s;
//	CREATE TABLE t (id positive DEFAULT nextval('s'));
//	ALTER SEQUENCE s OWNED BY t.id;
//
// Measured on PostgreSQL 17.10: pg_get_serial_sequence answers for this column
// and the catalog edges are indistinguishable from the SERIAL case above. They
// are not the same case. SERIAL only ever builds an integer column, so the
// column is written back as its domain with an ordinary nextval default, and
// nothing recreates the sequence that default names unless this reader reports
// it. See stokaro/ptah#1242.
func domainSequenceCatalog() pgSequenceCatalog {
	catalog := serialSequenceCatalog()
	catalog.name = "s"
	catalog.ownerColumnTypeCategory = "d"
	return catalog
}

// lifecycleSequenceCatalog is a sequence OWNED BY a column that does not draw
// from it. It is standalone on both builds, and it is here so a fix that widens
// the domain case cannot be confused with a fix that widens ownership (#657).
func lifecycleSequenceCatalog() pgSequenceCatalog {
	catalog := serialSequenceCatalog()
	catalog.name = "lifecycle_seq"
	catalog.ownerDefaultDrawsFromSequence = false
	return catalog
}

// serveSequenceQuery answers the sequence query the way PostgreSQL would, with
// one restriction: the owning column's type category is available only to a
// query that reads it. A CASE that never joins pg_type cannot tell the SERIAL
// fixture from the domain one on a real server either.
func serveSequenceQuery(catalog pgSequenceCatalog, query string) (dbtest.QueryResult, error) {
	implicit, err := sequenceIsImplicit(catalog, query)
	if err != nil {
		return dbtest.QueryResult{}, err
	}
	return dbtest.QueryResult{
		Columns: []string{
			"schema_name", "sequence_name", "data_type",
			"start_value", "increment_by", "min_value", "max_value", "cache_size", "is_cycled",
			"owned_schema", "owned_table", "owned_column", "comment", "is_implicit",
		},
		Rows: [][]driver.Value{{
			"public", catalog.name, "bigint",
			int64(1), int64(1), int64(1), int64(9223372036854775807), int64(1), false,
			"public", "t", catalog.ownedColumn, nil, implicit,
		}},
	}, nil
}

// sequenceIsImplicit models the CASE the reader issues.
//
// The projection has to name pg_type.typtype AND the domain code 'd' before the
// fixture's type category is made available to it. Naming typtype alone is not
// enough: a CASE that reads the column but branches on `IS NOT NULL` classifies
// every owned sequence as standalone, which un-hides the backing sequence of
// every SERIAL column in every database. That over-correction was applied and
// this guard stayed green against a check that only asked for "typtype".
func sequenceIsImplicit(catalog pgSequenceCatalog, query string) (bool, error) {
	projection, ok := selectListItem(query, "is_implicit", "FROM pg_sequence")
	if !ok {
		return false, fmt.Errorf("query has no projection aliased is_implicit:\n%s", query)
	}
	body := stripSQLLineComments(projection)
	asksForDomains := strings.Contains(body, "typtype") && strings.Contains(body, "'d'")
	if asksForDomains && catalog.ownerColumnTypeCategory == "d" {
		return false, nil
	}
	return catalog.ownerDefaultDrawsFromSequence, nil
}

// TestReadSequencesForSchema_DomainBackedSequenceIsStandalone pins which
// sequences survive the read.
//
// Measured end to end on PostgreSQL 17.10 with ptah-compat: before this change,
// `schema diff` against the domain fixture emitted the table with the domain
// column and no CREATE SEQUENCE, and replaying it into a fresh database failed
// at psql exit 3 with `ERROR: relation "s" does not exist`. After it, the same
// replay is exit 0 and the pinned binary v1.3.0 reports the replayed database
// synced with the source.
func TestReadSequencesForSchema_DomainBackedSequenceIsStandalone(t *testing.T) {
	tests := []struct {
		name    string
		catalog func() pgSequenceCatalog
		// wantNames is the sequences the reader reports. Empty means the
		// sequence was classified as its column's backing sequence and dropped.
		wantNames []string
	}{
		{
			name:      "sequence a domain column draws from is reported",
			catalog:   domainSequenceCatalog,
			wantNames: []string{"s"},
		},
		{
			name:      "SERIAL backing sequence stays hidden",
			catalog:   serialSequenceCatalog,
			wantNames: nil,
		},
		{
			name:      "lifecycle-only owned sequence stays reported",
			catalog:   lifecycleSequenceCatalog,
			wantNames: []string{"lifecycle_seq"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t2 *testing.T) {
			c := qt.New(t2)
			catalog := test.catalog()
			db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				return serveSequenceQuery(catalog, query)
			})

			sequences, err := NewPostgreSQLReader(db.SQL, "public").readSequencesForSchema("public")
			c.Assert(err, qt.IsNil)

			var names []string
			for _, sequence := range sequences {
				names = append(names, sequence.Name)
			}
			c.Assert(names, qt.DeepEquals, test.wantNames)
		})
	}
}
