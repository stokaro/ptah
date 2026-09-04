package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/parser"
	"go.5x5.cz/ptah/internal/sqlschema"
)

// TestToDatabase_AlterTableAddIndexReachesTheModel is the second half of
// stokaro/ptah#2778.
//
// The parser producing an AddIndexOperation is not the fix on its own: an
// operation the conversion does not model disappears exactly as the
// mis-parsed column did, and the render still exits 0 without the index. This
// asserts the joined path, which is where the defect was visible.
func TestToDatabase_AlterTableAddIndexReachesTheModel(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		operation string
		wantName  string
		wantType  string
	}{
		{name: "mysql ADD KEY", dialect: "mysql", operation: "ADD KEY k_b (b)", wantName: "k_b"},
		{name: "mysql ADD INDEX", dialect: "mysql", operation: "ADD INDEX k_b (b)", wantName: "k_b"},
		{name: "mariadb ADD KEY", dialect: "mariadb", operation: "ADD KEY k_b (b)", wantName: "k_b"},
		{
			name: "mysql ADD SPATIAL KEY", dialect: "mysql",
			operation: "ADD SPATIAL KEY s_g (g)", wantName: "s_g", wantType: "SPATIAL",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(
				"CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, g GEOMETRY NOT NULL);\n"+
					"ALTER TABLE t "+test.operation+";",
				parser.WithDialect(test.dialect),
			).Parse()
			c.Assert(err, qt.IsNil)

			database, err := sqlschema.ToDatabase(statements, test.dialect)
			c.Assert(err, qt.IsNil)

			c.Assert(database.Indexes, qt.HasLen, 1)
			c.Assert(database.Indexes[0].Name, qt.Equals, test.wantName)
			c.Assert(database.Indexes[0].Type, qt.Equals, test.wantType)
			c.Assert(database.Indexes[0].TableName, qt.Equals, "t")
		})
	}
}
