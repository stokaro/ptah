//go:build integration

package ydb_test

import (
	qt "github.com/frankban/quicktest"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"testing"
)

// fullTextDeclaration uses the shared WITH option map for analyzer settings.
func fullTextDeclaration(tokenizer string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables:  []schemamodel.Table{{StructName: "Doc", Name: "docs", Schema: "ptah_ydb_fulltext"}},
		Fields:  []schemamodel.Field{{StructName: "Doc", Name: "id", Type: "Uint64", Primary: true}, {StructName: "Doc", Name: "body", Type: "TEXT", Nullable: true}},
		Indexes: []schemamodel.Index{{StructName: "Doc", Name: "ft", Fields: []string{"body"}, Type: "fulltext_relevance", StorageParams: map[string]string{"tokenizer": tokenizer, "use_filter_lowercase": "true"}}},
	}
	schemamodel.Finalize(db)
	return db
}

// The newest certified line creates, reads, compares and rebuilds a full-text
// index with its flag enabled. Reapplying the same declaration plans nothing.
func TestYDBFullText_RoundTrip(t *testing.T) {
	c := qt.New(t)
	line := lineNamed(c, "26.2")
	setClusterFlags(c, line, clusterFlag{yaml: "enable_fulltext_index", page: "EnableFulltextIndex", on: true})
	conn := openYDB(c, line)
	schemas := []string{"ptah_ydb_fulltext"}
	dropTables(c, conn, schemas)
	c.Cleanup(func() { dropTables(c, conn, schemas) })
	c.Assert(conn.Info().Capabilities.Has(capability.FullTextIndexes), qt.IsTrue)
	declared := fullTextDeclaration("standard")
	apply(c, conn, planAgainst(c, conn, declared, schemas))
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
	apply(c, conn, planAgainst(c, conn, declared, schemas))
	live := readScoped(c, conn, schemas)
	c.Assert(indexNamed(c, live, "ft").StorageParams, qt.DeepEquals, declared.Indexes[0].StorageParams)
	changed := fullTextDeclaration("keyword")
	planned := planAgainst(c, conn, changed, schemas)
	c.Assert(planned, qt.HasLen, 2)
	apply(c, conn, planned)
	c.Assert(planAgainst(c, conn, changed, schemas), qt.HasLen, 0)
	plain := fullTextDeclaration("keyword")
	plain.Indexes[0].Type = "fulltext_plain"
	plain.Indexes[0].StorageParams["use_filter_stopwords"] = "false"
	planned = planAgainst(c, conn, plain, schemas)
	c.Assert(planned, qt.HasLen, 2)
	apply(c, conn, planned)
	c.Assert(planAgainst(c, conn, plain, schemas), qt.HasLen, 0)
	c.Assert(indexNamed(c, readScoped(c, conn, schemas), "ft").StorageParams, qt.DeepEquals, plain.Indexes[0].StorageParams)
}

// 25.1 predates both the full-text grammar and its feature flag.
func TestYDBFullText_OlderLineRefuses(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "25.1"))
	c.Assert(conn.Info().Capabilities.Has(capability.FullTextIndexes), qt.IsFalse)
	err := conn.Writer().ExecuteSQL(c.Context(), "CREATE TABLE ptah_ydb_fulltext_absent (id Uint64 NOT NULL, body Utf8, PRIMARY KEY (id), INDEX ft GLOBAL USING fulltext_relevance ON (body) WITH (tokenizer=standard))")
	c.Assert(err, qt.ErrorMatches, `(?s).*FULLTEXT_RELEVANCE index subtype is not supported.*`)
}
