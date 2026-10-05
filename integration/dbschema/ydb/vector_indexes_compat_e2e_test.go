//go:build integration

package ydb_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestYDBVectorIndexes_ApplyBackFromInspectedHCL inspects a table holding a
// vector index through ptah-compat and applies the document back, which
// finds it synced: the document carries the index's settings, where without
// them the plan would rebuild an index it then refuses.
func TestYDBVectorIndexes_ApplyBackFromInspectedHCL(t *testing.T) {
	c := qt.New(t)
	binary := buildCompatBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropTables(c, conn, vectorSchemas)
			c.Cleanup(func() { dropTables(c, conn, vectorSchemas) })
			apply(c, conn, planAgainst(c, conn, withTitleVector(vectorDeclaration("docs_emb", 2)), vectorSchemas))

			inspected, _, inspectErr := runCompat(ctx, binary, "schema", "inspect", "--url", url, "--schema", vectorSchema)
			c.Assert(inspectErr, qt.IsNil, qt.Commentf("schema inspect:\n%s", inspected))
			c.Assert(inspected, qt.Contains, "    type = \"GLOBAL USING vector_kmeans_tree\"\n    include = [column.body]\n"+
				"    distance = \"cosine\"\n    vector_type = \"float\"\n    vector_dimension = 3\n    levels = 1\n    clusters = 2\n")
			reapplied, _, reapplyErr := runCompat(ctx, binary, "schema", "apply", "--url", url, "--schema", vectorSchema,
				"--to", "file://"+writeCompatFile(c, c.TempDir(), "inspected.hcl", inspected), "--dry-run")
			c.Assert(reapplyErr, qt.IsNil, qt.Commentf("schema apply of the inspected document:\n%s", reapplied))
			c.Assert(reapplied, qt.Equals, "Schema is synced, no changes to be made\n")
		})
	}
}
