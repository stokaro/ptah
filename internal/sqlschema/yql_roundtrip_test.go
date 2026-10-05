package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/internal/sqlschema"
)

func TestParseRender(t *testing.T) {
	for _, text := range []string{
		`CREATE TABLE t (id Utf8 NOT NULL, PRIMARY KEY (id)) WITH (PARTITION_AT_KEYS = ('a b'u));`,
		`CREATE TABLE t (id Uint64 NOT NULL, part Utf8 NOT NULL, PRIMARY KEY (id, part)) WITH (PARTITION_AT_KEYS = ((10, 'a\n\'b'u), (20)));`,
		"--!syntax_v1\nCREATE TABLE t (id Int64 NOT NULL, value Utf8 DEFAULT 'active'u, PRIMARY KEY (id), INDEX i GLOBAL SYNC ON (value));",
		"CREATE TABLE t (id Uint64 NOT NULL, v String, PRIMARY KEY (id), INDEX i GLOBAL SYNC USING vector_kmeans_tree ON (v) WITH (distance = 'cosine', vector_type = 'float', vector_dimension = 3, levels = 1, clusters = 2));",
		"CREATE TABLE t (id Uint64 NOT NULL, v Utf8, PRIMARY KEY (id), INDEX i GLOBAL USING fulltext_plain ON (v) WITH (tokenizer = 'standard', use_filter_lowercase = true));",
		"CREATE TABLE t (id Uint64 NOT NULL, v Utf8, PRIMARY KEY (id), INDEX i LOCAL USING bloom_filter ON (v)) PARTITION BY HASH (id) WITH (STORE = COLUMN);",
	} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.Read([]byte(text), "ydb")
			c.Assert(err, qt.IsNil)
			rendered, err := renderer.GetOrderedCreateStatements(&database, "ydb")
			c.Assert(err, qt.IsNil)
			again, _, err := sqlschema.Read([]byte(strings.Join(rendered, "\n")), "ydb")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", rendered))
			second, err := renderer.GetOrderedCreateStatements(&again, "ydb")
			c.Assert(err, qt.IsNil)
			c.Assert(second, qt.DeepEquals, rendered)
		})
	}
}
