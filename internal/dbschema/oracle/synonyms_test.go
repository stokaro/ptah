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
// with the target in owner.object form as Oracle records it, and records a
// synonym through a database link as unrepresentable rather than as an
// object: a declaration has no place for the link.
func TestReader_ReadsSynonyms(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringSynonyms([][]driver.Value{
		{"ORDERS_ALIAS", "APP", "ORDERS", nil},
		{"REMOTE_ALIAS", "SALES", "ORDERS", "SALES_LINK"},
	}))

	live, err := oracle.NewOracleReader(db.SQL, "app").ReadSchema()

	c.Assert(err, qt.IsNil)
	objects, err := live.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []schemaext.Object{synonym.ObservedObject(synonym.ObservedSynonym{
		Synonym: synonym.Synonym{Schema: "APP", Name: "ORDERS_ALIAS", Target: "APP.ORDERS"}})})
	remote := synonym.Synonym{Schema: "APP", Name: "REMOTE_ALIAS"}.Ref()
	c.Assert(live.FeatureCoverage.Lookup(synonym.Kind, remote).State, qt.Equals, schemaext.Unrepresentable)
	c.Assert(live.FeatureCoverage.Lookup(synonym.Kind, synonym.Synonym{Schema: "APP", Name: "OTHER"}.Ref()).State, qt.Equals, schemaext.Complete)
}
