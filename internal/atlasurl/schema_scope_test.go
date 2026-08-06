package atlasurl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasurl"
)

// TestSchemaScope pins which dev URLs restrict schema analysis to one schema
// (stokaro/ptah#1074 S1).
//
// The empty results are what keep the boundary honest: a URL that scopes
// nothing leaves every object under review, which is how a dialect or a URL
// form that has not been measured against the pinned community binary v1.3.0
// stays at least as strict as it is today.
func TestSchemaScope(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{
			name:   "empty URL scopes nothing",
			rawURL: "",
			want:   "",
		},
		{
			name:   "postgres without search_path scopes nothing",
			rawURL: "postgres://localhost:5432/dev?sslmode=disable",
			want:   "",
		},
		{
			name:   "postgres search_path names the reviewed schema",
			rawURL: "postgres://localhost:5432/dev?sslmode=disable&search_path=public",
			want:   "public",
		},
		{
			name:   "a non-public search_path is honored as written",
			rawURL: "postgres://localhost:5432/dev?search_path=app",
			want:   "app",
		},
		{
			name:   "the postgresql spelling is the same dialect",
			rawURL: "postgresql://localhost/dev?search_path=reporting",
			want:   "reporting",
		},
		{
			name:   "cockroachdb is in the PostgreSQL family",
			rawURL: "cockroach://localhost:26257/dev?search_path=app",
			want:   "app",
		},
		{
			name:   "yugabytedb is in the PostgreSQL family",
			rawURL: "yugabyte://localhost:5433/dev?search_path=app",
			want:   "app",
		},
		{
			// The pinned binary reads this value as one schema NAME and refuses
			// the run with `schema "public,app" was not found`, so there is no
			// scoping behavior to match and nothing is filtered.
			name:   "a comma-carrying search_path scopes nothing",
			rawURL: "postgres://localhost/dev?search_path=public,app",
			want:   "",
		},
		{
			name:   "an empty search_path value scopes nothing",
			rawURL: "postgres://localhost/dev?search_path=",
			want:   "",
		},
		{
			name:   "a whitespace-only search_path value scopes nothing",
			rawURL: "postgres://localhost/dev?search_path=%20",
			want:   "",
		},
		{
			name:   "surrounding whitespace is trimmed off the schema name",
			rawURL: "postgres://localhost/dev?search_path=%20app%20",
			want:   "app",
		},
		{
			name:   "MySQL scopes nothing even with a search_path parameter",
			rawURL: "mysql://root@localhost:3306/dev?search_path=public",
			want:   "",
		},
		{
			name:   "SQLite scopes nothing",
			rawURL: "sqlite://file?mode=memory&search_path=public",
			want:   "",
		},
		{
			name:   "an unsupported scheme scopes nothing",
			rawURL: "oracle://localhost/dev?search_path=public",
			want:   "",
		},
		{
			name:   "a docker dev URL without search_path scopes nothing",
			rawURL: "docker://postgres/16/dev",
			want:   "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			c.Assert(atlasurl.SchemaScope(test.rawURL), qt.Equals, test.want)
		})
	}
}
