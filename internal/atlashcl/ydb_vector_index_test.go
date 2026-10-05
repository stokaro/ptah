package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
)

// TestParse_YDBVectorIndex_FailurePath refuses a vector setting YDB would
// refuse where the document wrote it, naming the index.
func TestParse_YDBVectorIndex_FailurePath(t *testing.T) {
	c := qt.New(t)
	db, err := atlashcl.Parse([]byte(`
table "docs" {
  column "id" {
    type = Uint64
  }
  column "emb" {
    type = String
    null = true
  }
  primary_key {
    columns = [column.id]
  }
  index "docs_emb" {
    type = "GLOBAL USING vector_kmeans_tree"
    columns = [column.emb]
    distance = "inner_product"
  }
}
`), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `.*index "docs_emb": invalid distance "inner_product": write one of cosine, euclidean, manhattan`)
	c.Assert(db, qt.DeepEquals, (*schemamodel.Database)(nil))
}
