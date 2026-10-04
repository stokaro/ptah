package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// vectorTable is an up migration creating a table that holds a vector index.
const vectorTable = "CREATE TABLE docs (id Uint64 NOT NULL, emb String, PRIMARY KEY (id),\n" +
	"  INDEX docs_emb GLOBAL USING vector_kmeans_tree ON (emb)\n" +
	"  WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=2));\n"

// TestYDBVectorRules_ReportWhatTheLineDoesNotDo pins YD130 to a vector index
// a line does not build, inline and added, and YD131 to the rows a migration
// writes past a vector index on a line that does not take them in.
func TestYDBVectorRules_ReportWhatTheLineDoesNotDo(t *testing.T) {
	tests := []struct {
		name    string
		version string
		files   map[string]string
		want    []string
	}{
		{
			name:    "a vector index inline and added, on a line that keeps them behind a flag",
			version: "25.1.4.7",
			files: map[string]string{
				"0001_docs.up.sql": vectorTable + "ALTER TABLE docs ADD INDEX docs_emb2 GLOBAL USING vector_kmeans_tree ON (emb) " +
					"WITH (distance=euclidean, vector_type=float, vector_dimension=3, levels=1, clusters=2);\n",
			},
			want: []string{"0001_docs.up.sql:1:YD130", "0001_docs.up.sql:4:YD130"},
		},
		{
			name:    "a bit vector index on a line that does not build one",
			version: "25.4.1.15",
			files: map[string]string{
				"0001_docs.up.sql": "ALTER TABLE docs ADD INDEX docs_bits GLOBAL USING vector_kmeans_tree ON (emb) " +
					"WITH (distance=manhattan, vector_type=\"bit\", vector_dimension=8, levels=1, clusters=2);\n",
			},
			want: []string{"0001_docs.up.sql:1:YD130"},
		},
		{
			name:    "rows written after the index, in its file and a later one",
			version: "25.2.1.24",
			files: map[string]string{
				"0001_docs.up.sql": vectorTable + "UPSERT INTO docs (id, emb) VALUES (1, \"x\");\n",
				"0002_docs.up.sql": "UPDATE docs SET emb = \"y\" WHERE id = 1;\nDELETE FROM docs WHERE id = 1;\n",
			},
			want: []string{"0001_docs.up.sql:4:YD131", "0002_docs.up.sql:1:YD131", "0002_docs.up.sql:2:YD131"},
		},
		{
			name:    "rows written into a table whose vector index was added by ALTER TABLE",
			version: "25.1.4.7",
			files: map[string]string{
				"0001_docs.up.sql": "CREATE TABLE docs (id Uint64 NOT NULL, emb String, PRIMARY KEY (id));\n",
				"0002_docs.up.sql": "ALTER TABLE docs ADD INDEX docs_emb GLOBAL USING vector_kmeans_tree ON (emb) " +
					"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=2);\n" +
					"INSERT INTO docs (id, emb) VALUES (1, \"x\");\n",
			},
			// 25.1's preset keeps vector indexes off, so YD130 reports the
			// index and YD131, which judges a line that builds one, says
			// nothing about the write.
			want: []string{"0002_docs.up.sql:1:YD130"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSites(ydbLint(c, test.files, test.version)), qt.DeepEquals, test.want)
		})
	}
}

// TestYDBVectorRules_LeaveWhatTheLineDoes is the control: the same files on a
// line that builds the index and keeps it current, rows written before the
// index, and an index the migration drops before the write.
func TestYDBVectorRules_LeaveWhatTheLineDoes(t *testing.T) {
	tests := []struct {
		name    string
		version string
		files   map[string]string
	}{
		{
			name: "the newest line",
			files: map[string]string{
				"0001_docs.up.sql": vectorTable + "UPSERT INTO docs (id, emb) VALUES (1, \"x\");\n" +
					"ALTER TABLE docs ADD INDEX docs_bits GLOBAL USING vector_kmeans_tree ON (emb) " +
					"WITH (distance=manhattan, vector_type=bit, vector_dimension=8, levels=1, clusters=2);\n",
			},
		},
		{
			name:    "rows written before the index",
			version: "25.2.1.24",
			files: map[string]string{
				"0001_docs.up.sql": "CREATE TABLE docs (id Uint64 NOT NULL, emb String, PRIMARY KEY (id));\n" +
					"UPSERT INTO docs (id, emb) VALUES (1, \"x\");\n" +
					"ALTER TABLE docs ADD INDEX docs_emb GLOBAL USING vector_kmeans_tree ON (emb) " +
					"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=2);\n",
			},
		},
		{
			name:    "the index dropped before the write",
			version: "25.2.1.24",
			files: map[string]string{
				"0001_docs.up.sql": vectorTable,
				"0002_docs.up.sql": "ALTER TABLE docs DROP INDEX docs_emb;\nUPSERT INTO docs (id, emb) VALUES (1, \"x\");\n",
			},
		},
		{
			name:    "a write into a table holding a global index only",
			version: "25.2.1.24",
			files: map[string]string{
				"0001_docs.up.sql": "CREATE TABLE docs (id Uint64 NOT NULL, v Utf8, PRIMARY KEY (id), INDEX docs_v GLOBAL ON (v));\n" +
					"UPSERT INTO docs (id, v) VALUES (1, \"x\"u);\n",
			},
		},
		{
			name:    "a write into another table",
			version: "25.2.1.24",
			files: map[string]string{
				"0001_docs.up.sql": vectorTable + "UPSERT INTO notes (id) VALUES (1);\n",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSites(ydbLint(c, test.files, test.version)), qt.HasLen, 0)
		})
	}
}
