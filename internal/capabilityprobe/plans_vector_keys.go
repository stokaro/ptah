package capabilityprobe

import (
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// withVectorKeys adds the questions the YDB renderer, reader and planner
// decide a vector index with: whether the line builds one at all, whether the
// index takes in the rows written after its build, and whether it takes bit
// vectors.
//
// On YDB each is asked in YDB's spelling and then used: the index is read back
// through Ptah's reader, since no SQL describes one, and the write is found
// through the index or not. A YDB vector index is YDB's, so every other engine
// is asked the same statement and its refusal is the measurement, as with an
// index's partitioning; pgvector's indexes are another declaration, which no
// key here describes.
func withVectorKeys(p plan, dialect string) plan {
	normalized := platform.NormalizeDialect(dialect)
	if normalized == platform.YDB {
		p.experiments = append(p.experiments, ydbVectorExperiments()...)
		return p
	}
	spelling, ok := typeKeySpellingFor(normalized)
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

// floatVector writes a vector of float elements as the String a vector column
// stores.
func floatVector(elements ...string) string {
	return `Untag(Knn::ToBinaryStringFloat([` + strings.Join(elements, ", ") + `]), "FloatVector")`
}

// nearestTo answers, through table's vector index, the key of the row
// nearest the vector [1, 0, 0].
func nearestTo(table string) string {
	return fmt.Sprintf("SELECT CAST(id AS Int64) FROM %s VIEW %s_v "+
		"ORDER BY Knn::CosineDistance(emb, Knn::ToBinaryStringFloat([1.0f, 0.0f, 0.0f])) LIMIT 1", table, table)
}

// ydbVectorExperiments asks YDB to build a vector index over a table holding
// rows, to find a row written after the build through it, and to build one
// over bit vectors, and reads each back. The two later questions need the
// first: on a line whose flag keeps vector indexes off, an index refused for
// that reason says nothing about writes or bit vectors.
func ydbVectorExperiments() []experiment {
	t := ydbSpelling
	far := fmt.Sprintf("(1, %s), (2, %s), (3, %s), (4, %s)",
		floatVector("0.0f", "1.0f", "0.0f"), floatVector("0.0f", "0.0f", "1.0f"),
		floatVector("-1.0f", "0.0f", "0.0f"), floatVector("0.0f", "-1.0f", "0.0f"))
	vectorTable := func(table string) string { return t.table(table, "id Uint64 NOT NULL, emb String", "id") }
	maintained := proven(capability.VectorIndexMaintainedOnWrite, schemaChange{
		setup: []string{
			vectorTable("vkm"),
			"UPSERT INTO vkm (id, emb) VALUES " + far,
			vectorIndexStatement("vkm", "emb", "float", 3),
		},
		before: []check{counts("SELECT COUNT(*) FROM ("+nearestTo("vkm")+")", 1)},
		change: []string{fmt.Sprintf("UPSERT INTO vkm (id, emb) VALUES (5, %s)", floatVector("1.0f", "0.0f", "0.0f"))},
		after:  []check{counts(nearestTo("vkm"), 5)},
	})
	maintained.requires = []capability.Capability{capability.VectorIndexes}
	bits := proven(capability.VectorBitType, schemaChange{
		setup: []string{
			vectorTable("vkb"),
			`UPSERT INTO vkb (id, emb) VALUES ` +
				`(1, Untag(Knn::ToBinaryStringBit([1.0f, 0.0f, 0.0f, 0.0f, 1.0f, 0.0f, 0.0f, 0.0f]), "BitVector")), ` +
				`(2, Untag(Knn::ToBinaryStringBit([0.0f, 1.0f, 1.0f, 1.0f, 0.0f, 1.0f, 1.0f, 1.0f]), "BitVector"))`,
		},
		change: []string{vectorIndexStatement("vkb", "emb", "bit", 8)},
		after: []check{ydbDescribedIndex("vkb", "vkb_v", "a vector index over bit vectors of 8 elements",
			func(index catalog.Index) bool {
				vector, found := observedVector(index)
				return found && vector.VectorType == "bit" && vector.Dimension == 8
			})},
	})
	bits.requires = []capability.Capability{capability.VectorIndexes}
	return []experiment{
		proven(capability.VectorIndexes, schemaChange{
			setup:  []string{vectorTable("vki"), "UPSERT INTO vki (id, emb) VALUES " + far},
			change: []string{vectorIndexStatement("vki", "emb", "float", 3)},
			after: []check{ydbDescribedIndex("vki", "vki_v", "a cosine vector index over float vectors of 3 elements, "+
				"in a tree of one level of two clusters",
				func(index catalog.Index) bool {
					vector, found := observedVector(index)
					return found && vector == ydbschema.VectorSettings{
						Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2,
					}
				})},
		}),
		maintained,
		bits,
	}
}

// observedVector is the vector settings a read attached to index, the YDB
// owner's observation.
func observedVector(index catalog.Index) (ydbschema.VectorSettings, bool) {
	vector, found, err := schemaext.FacetAs[*ydbschema.ObservedVectorIndex](index.Facets, ydbschema.VectorIndexKind)
	if err != nil || !found {
		return ydbschema.VectorSettings{}, false
	}
	return vector.Settings(), true
}
