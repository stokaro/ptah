package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// TestRender_PrimaryKeyMethod writes a primary key's access method as the
// pinned community binary v1.3.0 does for a MariaDB key built USING HASH,
// measured on MariaDB 11.8.9: `type = HASH` in the primary_key block
// (stokaro/ptah#3853).
func TestRender_PrimaryKeyMethod(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}, PrimaryKeyMethod: "HASH"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "INT", Primary: true}},
	}

	result, err := atlashclrender.RenderInspected(database, "mariadb", "app")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Matches, `(?s).*primary_key \{\n    columns = \[column\.id\]\n    type\s+= HASH\n  \}.*`)
}
