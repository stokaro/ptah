package mssql_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlproperty"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mssql"
)

// answeringPropertiesThen answers the reader's extended property query with
// rows and hands every other query to next.
func answeringPropertiesThen(rows [][]driver.Value, next dbtest.QueryHandler) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, "SQL_VARIANT_PROPERTY") {
			return dbtest.QueryResult{
				Columns: []string{"schema_name", "table_name", "column_name", "name", "value", "base_type"},
				Rows:    rows,
			}, nil
		}
		return next(query, args)
	}
}

// TestReader_KeepsEveryOwnersObjects reads an extended property and a security
// policy in one pass. Each owner's objects join the read, so a read that
// recorded one owner's and then replaced them with the next one's reported a
// database without the first: an extended property that the next apply
// would then add again.
func TestReader_KeepsEveryOwnersObjects(t *testing.T) {
	c := qt.New(t)
	policies := answeringPolicies([][]driver.Value{{"rls", "dormant", false, false, false, nil, nil, nil, nil, nil}})
	db := dbtest.Open(t, answeringPropertiesThen([][]driver.Value{{"app", "", "", "ptah_flag", "on", "nvarchar"}}, policies))

	live, err := mssql.NewSQLServerReader(db.SQL, "").ReadSchema()

	c.Assert(err, qt.IsNil)
	property := mssqlproperty.Property{Schema: "app", Name: "ptah_flag"}.Ref()
	c.Assert(live.FeatureObjects.Refs(), qt.ContentEquals, []objectidentity.ID{property, mssqlschema.SecurityPolicyRef("rls", "dormant")})
	c.Assert(live.FeatureCoverage.Lookup(mssqlproperty.Kind, property).State, qt.Equals, schemaext.Complete)
	c.Assert(live.FeatureCoverage.Lookup(mssqlschema.SecurityPolicyKind, mssqlschema.SecurityPolicyRef("rls", "other")).State,
		qt.Equals, schemaext.Complete)
}
