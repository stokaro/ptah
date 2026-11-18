package schemavalidate_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine/builtin"
	"ptah.run/internal/schemavalidate"
	"ptah.run/internal/sqlschema"
)

// TestCollect_BareIndexReferenceToAQuotedNamesake reads Oracle SQL files that
// declare a table or a column quoted and name it bare in an index. On Oracle a
// quoted lower-case name is a different object from the bare one, so the index
// names something the file does not declare. The problem names the quoted
// declaration and the spelling to write, which the bare refusal alone does not
// point at.
func TestCollect_BareIndexReferenceToAQuotedNamesake(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "a quoted column named bare",
			sql:  "CREATE TABLE customers (\"id\" NUMBER(10) PRIMARY KEY);\nCREATE INDEX ix_c ON customers (id);\n",
			want: `names column "id", which table "customers" does not declare; ` +
				`the schema declares the column "id" quoted, which oracle keeps apart from the bare id: write "id" to name it`,
		},
		{
			name: "a quoted table named bare",
			sql:  "CREATE TABLE \"customers\" (id NUMBER(10) PRIMARY KEY);\nCREATE INDEX ix_c ON customers (id);\n",
			want: `names table "customers", which no declaration defines; ` +
				`the schema declares the table "customers" quoted, which oracle keeps apart from the bare customers: write "customers" to name it`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.Read([]byte(test.sql), "oracle")
			c.Assert(err, qt.IsNil)

			problems := must.Must(schemavalidate.Collect(c.Context(), must.Must(builtin.New()), &database, "oracle"))

			c.Assert(problems, qt.HasLen, 1)
			c.Assert(problems[0].Message, qt.Equals, test.want)
		})
	}
}
