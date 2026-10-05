//go:build integration

package ydb_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/migration/schemadiff"
)

// vectorSchema is the directory the vector-index tests write into.
const vectorSchema = "ptah_ydb_vector_indexes"

var vectorSchemas = []string{vectorSchema}

// vectorDeclaration is a table of documents with a generation, a vector
// column, a body, and a vector index over the vector under the generation
// that covers the body; clusters is the width of its tree, and name its name.
func vectorDeclaration(name string, clusters uint64) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs", Schema: vectorSchema}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Doc", Name: "gen", Type: "BIGINT", Nullable: true},
			{StructName: "Doc", Name: "emb", Type: "vector(3)", Nullable: true},
			{StructName: "Doc", Name: "body", Type: "TEXT", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "Doc", Name: name, Fields: []string{"gen", "emb"},
			IncludeColumns: []string{"body"}, Type: "vector_kmeans_tree",
			Vector: &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: clusters}}},
	}
	schemamodel.Finalize(db)
	return db
}

// withTitleVector adds to a vectorDeclaration a second vector column and an
// index over it that names its metric by pgvector's operator class.
func withTitleVector(db *schemamodel.Database) *schemamodel.Database {
	db.Fields = append(db.Fields, schemamodel.Field{StructName: "Doc", Name: "title_emb", Type: "vector(3)", Nullable: true})
	db.Indexes = append(db.Indexes, schemamodel.Index{StructName: "Doc", Name: "docs_title_ip", Fields: []string{"title_emb"},
		Type: "vector_kmeans_tree", Operator: "vector_ip_ops",
		Vector: &ast.VectorIndexSpec{VectorType: "int8", Dimension: 3, Levels: 2, Clusters: 2}})
	schemamodel.Finalize(db)
	return db
}

// vectorPath is the path of a table in vectorSchema, quoted.
func vectorPath(table string) string {
	return "`" + vectorSchema + "/" + table + "`"
}

// floatVector writes a vector of three float elements as the String a vector
// column stores.
func floatVector(x, y, z float64) string {
	return fmt.Sprintf(`Untag(Knn::ToBinaryStringFloat([CAST(%g AS Float), CAST(%g AS Float), CAST(%g AS Float)]), "FloatVector")`, x, y, z)
}

// nearest asks index of table for the key of the row of generation 1 nearest
// [1, 0, 0]. A search through a prefixed index names the prefix: without it
// both lines answer `Given predicate is not suitable for used index`.
func nearest(c *qt.C, conn *dbschema.DatabaseConnection, table, index string) int64 {
	c.Helper()
	return scalar(c, conn, fmt.Sprintf("SELECT CAST(id AS Int64) FROM %s VIEW `%s` WHERE gen = 1l "+
		"ORDER BY Knn::CosineDistance(emb, Knn::ToBinaryStringFloat([1.0f, 0.0f, 0.0f])) LIMIT 1", vectorPath(table), index))
}

// TestYDBVectorIndexes_RoundTrip applies a vector index over a table holding
// rows, reads it back, and plans nothing after, applied once and twice; it
// searches through it to show the index the server built is one. Then it
// changes a setting, which YDB makes only by a drop and an add, renames the
// index, which keeps it, adds a vector column with an index in the same plan
// and drops both again, each step ending with nothing left to plan.
func TestYDBVectorIndexes_RoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, vectorSchemas)
			c.Cleanup(func() { dropTables(c, conn, vectorSchemas) })
			c.Assert(conn.Info().Capabilities.Has(capability.VectorIndexes), qt.IsTrue,
				qt.Commentf("the contour starts 25.1 with EnableVectorIndex, and the connection reads it"))

			c.Assert(conn.Writer().ExecuteSQL(c.Context(), "CREATE TABLE "+vectorPath("docs")+
				" (id Int64 NOT NULL, gen Int64, emb String, body Utf8, PRIMARY KEY (id))"), qt.IsNil)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(), "UPSERT INTO "+vectorPath("docs")+" (id, gen, emb, body) VALUES "+
				"(1l, 1l, "+floatVector(0, 1, 0)+", 'a'u), (2l, 1l, "+floatVector(0, 0, 1)+", 'b'u), "+
				"(3l, 1l, "+floatVector(0.9, 0.1, 0)+", 'c'u), (4l, 2l, "+floatVector(1, 0, 0)+", 'd'u)"), qt.IsNil)

			declared := vectorDeclaration("docs_emb", 2)
			created := planAgainst(c, conn, declared, vectorSchemas)
			c.Assert(created, qt.DeepEquals, []string{
				"ALTER TABLE " + vectorPath("docs") + " ADD INDEX `docs_emb` GLOBAL USING vector_kmeans_tree ON (`gen`, `emb`) " +
					"COVER (`body`) WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=2)",
			})
			apply(c, conn, created)
			c.Assert(planAgainst(c, conn, declared, vectorSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, vectorSchemas))
			c.Assert(planAgainst(c, conn, declared, vectorSchemas), qt.HasLen, 0)

			live := readScoped(c, conn, vectorSchemas)
			c.Assert(indexNamed(c, live, "docs_emb"), qt.DeepEquals, catalog.Index{
				Name: "docs_emb", TableName: "docs", Schema: vectorSchema, Columns: []string{"gen", "emb"},
				IncludeColumns: []string{"body"}, Method: "GLOBAL USING vector_kmeans_tree",
				Vector: &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2},
				Definition: "INDEX `docs_emb` GLOBAL USING vector_kmeans_tree ON (`gen`, `emb`) COVER (`body`) " +
					"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=2)",
			})
			c.Assert(columnNamed(c, tableNamed(c, live, vectorSchema, "docs"), "emb").DataType, qt.Equals, "String")
			c.Assert(nearest(c, conn, "docs", "docs_emb"), qt.Equals, int64(3))

			// No setting changes in place: the index is dropped and added again.
			declared = vectorDeclaration("docs_emb", 4)
			retuned := planAgainst(c, conn, declared, vectorSchemas)
			c.Assert(retuned, qt.DeepEquals, []string{
				"ALTER TABLE " + vectorPath("docs") + " DROP INDEX `docs_emb`",
				"ALTER TABLE " + vectorPath("docs") + " ADD INDEX `docs_emb` GLOBAL USING vector_kmeans_tree ON (`gen`, `emb`) " +
					"COVER (`body`) WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=4)",
			})
			apply(c, conn, retuned)
			c.Assert(planAgainst(c, conn, declared, vectorSchemas), qt.HasLen, 0)

			declared = vectorDeclaration("docs_emb_cosine", 4)
			renamed := planAgainst(c, conn, declared, vectorSchemas)
			c.Assert(renamed, qt.DeepEquals, []string{
				"ALTER TABLE " + vectorPath("docs") + " RENAME INDEX `docs_emb` TO `docs_emb_cosine`",
			})
			apply(c, conn, renamed)
			c.Assert(planAgainst(c, conn, declared, vectorSchemas), qt.HasLen, 0)
			c.Assert(nearest(c, conn, "docs", "docs_emb_cosine"), qt.Equals, int64(3))

			// A vector column and its index in one plan: the column first.
			declared = withTitleVector(vectorDeclaration("docs_emb_cosine", 4))
			added := planAgainst(c, conn, declared, vectorSchemas)
			c.Assert(added, qt.DeepEquals, []string{
				"ALTER TABLE " + vectorPath("docs") + " ADD COLUMN `title_emb` String",
				"ALTER TABLE " + vectorPath("docs") + " ADD INDEX `docs_title_ip` GLOBAL USING vector_kmeans_tree ON (`title_emb`) " +
					"WITH (similarity=inner_product, vector_type=int8, vector_dimension=3, levels=2, clusters=2)",
			})
			apply(c, conn, added)
			c.Assert(planAgainst(c, conn, declared, vectorSchemas), qt.HasLen, 0)

			// And both gone again: the index first.
			declared = vectorDeclaration("docs_emb_cosine", 4)
			dropped := planAgainst(c, conn, declared, vectorSchemas)
			c.Assert(dropped, qt.DeepEquals, []string{
				"ALTER TABLE " + vectorPath("docs") + " DROP INDEX `docs_title_ip`",
				"ALTER TABLE " + vectorPath("docs") + " DROP COLUMN `title_emb`",
			})
			apply(c, conn, dropped)
			c.Assert(planAgainst(c, conn, declared, vectorSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBVectorIndexes_CreatedWithTheTable writes a new table's vector index
// inside its CREATE TABLE, where every setting goes, and reads it back.
func TestYDBVectorIndexes_CreatedWithTheTable(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, vectorSchemas)
			c.Cleanup(func() { dropTables(c, conn, vectorSchemas) })

			declared := withTitleVector(vectorDeclaration("docs_emb", 2))
			created := planAgainst(c, conn, declared, vectorSchemas)
			c.Assert(created, qt.HasLen, 1)
			apply(c, conn, created)
			c.Assert(planAgainst(c, conn, declared, vectorSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, vectorSchemas))
			c.Assert(planAgainst(c, conn, declared, vectorSchemas), qt.HasLen, 0)

			live := readScoped(c, conn, vectorSchemas)
			c.Assert(indexNamed(c, live, "docs_title_ip").Vector, qt.DeepEquals,
				&ast.VectorIndexSpec{Similarity: "inner_product", VectorType: "int8", Dimension: 3, Levels: 2, Clusters: 2})
		})
	}
}

// TestYDBVectorIndexes_FollowTheirKeys holds the two keys a line answers
// differently to the server: a row written after the build is found through
// the index exactly where vector_index_maintained_on_write holds, and a plan
// for an index over bit vectors builds where vector_bit_type holds and is
// refused by name where it does not.
func TestYDBVectorIndexes_FollowTheirKeys(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, vectorSchemas)
			c.Cleanup(func() { dropTables(c, conn, vectorSchemas) })
			caps := conn.Info().Capabilities

			// The index is built over rows, the way a vector index is built
			// over a table in use, and a row is written after it.
			c.Assert(conn.Writer().ExecuteSQL(c.Context(), "CREATE TABLE "+vectorPath("docs")+
				" (id Int64 NOT NULL, gen Int64, emb String, body Utf8, PRIMARY KEY (id))"), qt.IsNil)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(), "UPSERT INTO "+vectorPath("docs")+" (id, gen, emb) VALUES "+
				"(1l, 1l, "+floatVector(0, 1, 0)+"), (2l, 1l, "+floatVector(0, 0, 1)+"), (3l, 1l, "+floatVector(-1, 0, 0)+")"),
				qt.IsNil)
			declared := vectorDeclaration("docs_emb", 2)
			apply(c, conn, planAgainst(c, conn, declared, vectorSchemas))
			c.Assert(conn.Writer().ExecuteSQL(c.Context(), "UPSERT INTO "+vectorPath("docs")+" (id, gen, emb) VALUES "+
				"(5l, 1l, "+floatVector(1, 0, 0)+")"), qt.IsNil)
			c.Assert(nearest(c, conn, "docs", "docs_emb") == 5, qt.Equals, caps.Has(capability.VectorIndexMaintainedOnWrite))

			// The vector column declared as bytes, so the bit index's
			// dimension is the index's alone.
			bits := vectorDeclaration("docs_emb", 2)
			bits.Fields[2].Type = "BYTEA"
			bits.Indexes[0].Vector = &ast.VectorIndexSpec{Distance: "manhattan", VectorType: "bit", Dimension: 8, Levels: 1, Clusters: 2}
			withoutBits := conn.Info()
			withoutBits.Capabilities = caps.With(capability.VectorBitType, false)
			diff, err := schemadiff.CompareWithDatabaseInfo(bits, readScoped(c, conn, vectorSchemas), withoutBits, nil)
			c.Assert(err, qt.ErrorMatches, `.*index "docs_emb" stores bit vectors, which requires target capability vector_bit_type, .*`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(diff, qt.IsNil)
			// The server's own answer, which the key names: the index built
			// over the rows above, or refused.
			built := conn.Writer().ExecuteSQL(c.Context(), "ALTER TABLE "+vectorPath("docs")+" ADD INDEX `docs_bits` "+
				"GLOBAL USING vector_kmeans_tree ON (emb) WITH (distance=manhattan, vector_type=bit, vector_dimension=8, "+
				"levels=1, clusters=2)")
			c.Assert(built == nil, qt.Equals, caps.Has(capability.VectorBitType), qt.Commentf("%v", built))
		})
	}
}
