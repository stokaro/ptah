package oracle

// White-box testing required: the index read is package-local and its
// projection is not reachable through an exported API without a live Oracle
// server. The live read is covered by integration/oracle_column_collation_live_test.go.

import (
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

// held is the collation each column of table T compares with: A holds
// BINARY_CI, B none, U the one a character column that names none reads.
var held = map[string]map[string]string{
	"T": {"A": "BINARY_CI", "B": "", "U": "USING_NLS_COMP"},
}

// An index over a column with a collation is listed under a hidden column, and
// the read has to name the column the declaration names. Without the name the
// index matches no declaration and every plan replaces it.
func TestReadIndexColumns_NamesTheColumnOfACollatedKey(t *testing.T) {
	c := qt.New(t)

	db := dbtest.Open(t, answeringIndexCatalog)
	reader := &Reader{db: db.SQL, schema: "APP"}

	parts, err := reader.readIndexColumns(t.Context(), held)

	c.Assert(err, qt.IsNil)
	names := func(index string) []string {
		var got []string
		for _, part := range parts[index] {
			got = append(got, part.Name)
		}
		return got
	}
	c.Assert(names("CX_A"), qt.DeepEquals, []string{"A"})
	c.Assert(names("CX_AB"), qt.DeepEquals, []string{"A", "B"})
	c.Assert(names("CX_B"), qt.DeepEquals, []string{"B"})
	c.Assert(names("CX_FN"), qt.DeepEquals, []string{""})
	c.Assert(parts["CX_FN"][0].Expr, qt.Equals, `UPPER("B")`)
}

// A user-written NLSSORT over a column that holds another collation lists the
// same expression as the key over a collated column. Mapping it to the column
// would read it as a plain index and hide the function from the comparison, so
// only a column that holds the sort the key applies is mapped.
func TestReadIndexColumns_KeepsAnNLSSortOverAColumnWithAnotherCollation(t *testing.T) {
	c := qt.New(t)

	db := dbtest.Open(t, answeringIndexCatalog)
	reader := &Reader{db: db.SQL, schema: "APP"}

	parts, err := reader.readIndexColumns(t.Context(), held)

	c.Assert(err, qt.IsNil)
	c.Assert(parts["NS_USER"], qt.HasLen, 1)
	c.Assert(parts["NS_USER"][0].Name, qt.Equals, "")
	c.Assert(parts["NS_USER"][0].Expr, qt.Equals, `NLSSORT("B",'nls_sort=''binary_ci''')`)
	c.Assert(parts["NS_OTHER"], qt.HasLen, 1)
	c.Assert(parts["NS_OTHER"][0].Name, qt.Equals, "")
	c.Assert(parts["NS_OTHER"][0].Expr, qt.Equals, `NLSSORT("A",'nls_sort=''BINARY_AI''')`)
}

// answeringIndexCatalog answers the key parts of six indexes, and refuses a
// projection that does not read the expression of a key part.
func answeringIndexCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if !contains(query, "all_ind_expressions") || !contains(query, "ie.column_expression") {
		return dbtest.QueryResult{}, errUnaskedExpression
	}
	sortKey := `NLSSORT("A",'nls_sort=''BINARY_CI''')`
	return dbtest.QueryResult{
		Columns: []string{"INDEX_NAME", "TABLE_NAME", "COLUMN_NAME", "DESCEND", "COLUMN_POSITION", "COLUMN_EXPRESSION"},
		Rows: [][]driver.Value{
			{"CX_A", "T", "SYS_NC00004$", "ASC", int64(1), sortKey},
			{"CX_AB", "T", "SYS_NC00004$", "ASC", int64(1), sortKey},
			{"CX_AB", "T", "B", "ASC", int64(2), nil},
			{"CX_B", "T", "B", "ASC", int64(1), nil},
			{"CX_FN", "T", "SYS_NC00006$", "ASC", int64(1), `UPPER("B")`},
			{"NS_USER", "T", "SYS_NC00007$", "ASC", int64(1), `NLSSORT("B",'nls_sort=''binary_ci''')`},
			{"NS_OTHER", "T", "SYS_NC00008$", "ASC", int64(1), `NLSSORT("A",'nls_sort=''BINARY_AI''')`},
		},
	}, nil
}

var errUnaskedExpression = errors.New("index column query does not read all_ind_expressions")
