package embedpg

import "strings"

// qualify renders a relation with its schema when it has one.
//
// This is the one answer to "where does this object live", and it is one
// function rather than five because the alternative was measured. Five copies
// of this body sat beside the target, the source, the searcher, the outbox and
// the index, agreeing with each other; retirement read the table name out of
// the registry, where no schema had ever been recorded, and interpolated it
// bare. So `ALTER TABLE "articles" DROP COLUMN "embedding"` resolved through
// search_path and destroyed the columns of a live generation in another schema,
// at exit 0, reporting the named one as gone (stokaro/ptah#2629).
//
// An empty schema is not the same as a missing one: it means the specification
// named none, so the server's search_path decides, which is what an author who
// wrote no schema asked for. Every caller that HAS a schema has to pass it,
// which is why this takes the two parts rather than a spec -- a spec-shaped
// helper is what let the registry path quietly have neither.
func qualify(schema, table string) string {
	if trimmed := strings.TrimSpace(schema); trimmed != "" {
		return quoteIdentifier(trimmed) + "." + quoteIdentifier(table)
	}
	return quoteIdentifier(table)
}
