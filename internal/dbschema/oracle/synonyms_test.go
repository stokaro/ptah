package oracle_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/synonym"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/oracle"
)

// answeringSynonyms answers the reader's ALL_SYNONYMS query with rows and
// every other query with no rows.
func answeringSynonyms(rows [][]driver.Value) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if !strings.Contains(query, "all_synonyms") {
			return dbtest.QueryResult{}, nil
		}
		return dbtest.QueryResult{Columns: []string{"synonym_name", "table_owner", "table_name", "db_link"}, Rows: rows}, nil
	}
}

// TestReader_ReadsSynonyms reads the schema's synonyms as the owner's objects,
// named without the account the read describes. A target the account owns is
// written without the account too, and one another account owns as
// owner.object, as Oracle records it. A synonym through a database link is
// recorded as unrepresentable rather than as an object: a declaration has no
// place for the link.
func TestReader_ReadsSynonyms(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringSynonyms([][]driver.Value{
		{"ORDERS_ALIAS", "APP", "ORDERS", nil},
		{"REMOTE_ALIAS", "SALES", "ORDERS", "SALES_LINK"},
		{"SALES_ALIAS", "SALES", "ORDERS", nil},
	}))

	live, err := oracle.NewOracleReader(db.SQL, "app").ReadSchema()

	c.Assert(err, qt.IsNil)
	objects, err := live.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []schemaext.Object{
		synonym.ObservedObject(synonym.ObservedSynonym{Synonym: synonym.Synonym{Name: "ORDERS_ALIAS", Target: "ORDERS"}}),
		synonym.ObservedObject(synonym.ObservedSynonym{Synonym: synonym.Synonym{Name: "SALES_ALIAS", Target: "SALES.ORDERS"}}),
	})
	remote := synonym.Synonym{Name: "REMOTE_ALIAS"}.Ref()
	c.Assert(live.FeatureCoverage.Lookup(synonym.Kind, remote).State, qt.Equals, schemaext.Unrepresentable)
	c.Assert(live.FeatureCoverage.Lookup(synonym.Kind, synonym.Synonym{Name: "OTHER"}.Ref()).State, qt.Equals, schemaext.Complete)
}
