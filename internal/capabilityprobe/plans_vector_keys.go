package capabilityprobe

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withVectorKeys adds the questions whether the line builds a YDB vector
// index at all, whether the index takes in the rows written after its build,
// and whether it takes bit vectors. A YDB vector index is YDB's, which the
// YDB provider measures on its own lines, so every engine here is asked the
// statement and its refusal is the measurement, as with an index's
// partitioning; pgvector's indexes are another declaration, which no key here
// describes.
func withVectorKeys(p plan, dialect string) plan {
	spelling, ok := typeKeySpellingFor(platform.NormalizeDialect(dialect))
	if !ok {
		return p
	}
	refused := func(key capability.Capability, table, vectorType string) experiment {
		return proven(key, schemaChange{
			setup:  []string{spelling.table(table, "n "+spelling.integer)},
			change: []string{vectorIndexStatement(table, "n", vectorType, 3)},
		})
	}
	p.experiments = append(p.experiments,
		refused(capability.VectorIndexes, "vki", "float"),
		refused(capability.VectorIndexMaintainedOnWrite, "vkm", "float"),
		refused(capability.VectorBitType, "vkb", "bit"),
	)
	return p
}

// vectorIndexStatement adds to table a vector index over column, of elements
// of vectorType and dimension, in a tree of one level of two clusters.
func vectorIndexStatement(table, column, vectorType string, dimension int) string {
	return fmt.Sprintf("ALTER TABLE %s ADD INDEX %s_v GLOBAL USING vector_kmeans_tree ON (%s) "+
		"WITH (distance=cosine, vector_type=%s, vector_dimension=%d, levels=1, clusters=2)",
		table, table, column, vectorType, dimension)
}
