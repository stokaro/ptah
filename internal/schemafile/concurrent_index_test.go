package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/schemafile"
)

// The parser has accepted CONCURRENTLY since it learned CREATE INDEX, and the
// conversion into the authoring model dropped it. A `.sql` desired state asking
// for PostgreSQL's non-locking build was therefore planned as a locking one,
// silently -- and on a table large enough for the request to be worth making,
// that is the difference between a migration and an outage (stokaro/ptah#1663).
//
// It is the same loss internal/sqlschema records for ClickHouse's
// GRANULARITY: a field the parser read and the conversion did not copy, found
// only by asking what came out the other end.
func TestAConcurrentIndexSurvivesTheConversion(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      bool
	}{
		{
			name:      "the source asks for a concurrent build",
			statement: "CREATE INDEX CONCURRENTLY idx_users_email ON users (email);",
			want:      true,
		},
		{
			// The control. A loader that reported every index as concurrent
			// would satisfy the row above and turn every index build into one
			// the target may refuse.
			name:      "the source asks for an ordinary build",
			statement: "CREATE INDEX idx_users_email ON users (email);",
			want:      false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(t.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(path, []byte(
				"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\n"+test.statement+"\n",
			), 0o600), qt.IsNil)

			database, err := schemafile.LoadPath(path, schemafile.Options{})

			c.Assert(err, qt.IsNil)
			c.Assert(database.Indexes, qt.HasLen, 1)
			c.Assert(database.Indexes[0].Concurrently, qt.Equals, test.want)
		})
	}
}
