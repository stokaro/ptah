package postgres

// White-box testing required: indexInvisible is unexported, and a fake server
// answers whatever definition the test hands it, so the parse is pinned here
// against the definitions CockroachDB v26.3.2 printed. The live test in
// integration/cockroachdb_invisible_index_e2e_test.go reads a real hidden and a
// real partially visible index.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// TestIndexInvisible_HappyPath reads the visibility out of what the server
// printed: NOT VISIBLE last, after any WHERE, hides the index, and a column
// named visible inside the condition does not.
func TestIndexInvisible_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		definition string
		want       bool
	}{
		{
			name:       "a hidden index",
			dialect:    platform.CockroachDB,
			definition: "CREATE INDEX kb ON public.t USING btree (b ASC) NOT VISIBLE",
			want:       true,
		},
		{
			name:       "a hidden partial index",
			dialect:    platform.CockroachDB,
			definition: "CREATE INDEX kw ON public.t USING btree (a ASC) WHERE (a > 0) NOT VISIBLE",
			want:       true,
		},
		{
			name:       "a hidden index with a payload",
			dialect:    platform.CockroachDB,
			definition: "CREATE INDEX ky ON public.t USING btree (a ASC) INCLUDE (b) NOT VISIBLE",
			want:       true,
		},
		{
			name:       "a condition on a column named visible",
			dialect:    platform.CockroachDB,
			definition: "CREATE INDEX kv1 ON public.tv USING btree (a ASC) WHERE (NOT visible)",
			want:       false,
		},
		{
			name:       "a shown index",
			dialect:    platform.CockroachDB,
			definition: "CREATE INDEX kc ON public.t USING btree (c ASC)",
			want:       false,
		},
		{
			name:       "the clause outside CockroachDB",
			dialect:    platform.Postgres,
			definition: "CREATE INDEX kb ON public.t USING btree (b) NOT VISIBLE",
			want:       false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			invisible, err := indexInvisible(test.dialect, "k", test.definition)

			c.Assert(err, qt.IsNil)
			c.Assert(invisible, qt.Equals, test.want)
		})
	}
}

// TestIndexInvisible_FailurePath refuses an index the optimizer uses for a
// share of queries: the model holds a shown or a hidden index, and either
// reading moves it on the next apply.
func TestIndexInvisible_FailurePath(t *testing.T) {
	c := qt.New(t)

	invisible, err := indexInvisible(platform.CockroachDB, "kv",
		"CREATE INDEX kv ON public.t USING btree (a ASC, b ASC) VISIBILITY 0.50")

	c.Assert(err, qt.ErrorMatches,
		`index "kv" is partially visible \(VISIBILITY 0\.50\), which Ptah does not model; make it VISIBLE or NOT VISIBLE`)
	c.Assert(invisible, qt.IsFalse)
}
