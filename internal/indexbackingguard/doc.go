// Package indexbackingguard hosts the test that keeps one answer to
// constraint-backed index ownership from becoming two again.
//
// Whether a reported index is the physical backing of a constraint -- created
// and dropped through that constraint rather than on its own -- is asked by two
// trees for two different purposes. [go.5x5.cz/ptah/migration/schemadiff] asks
// it to decide which reported index a comparison must ignore.
// [go.5x5.cz/ptah/internal/convert/dbschematogo] asks it to decide which of the
// two representations describes the object. Both uses are legitimate and both
// stay where they are; what must not be duplicated is the EVIDENCE, and
// [go.5x5.cz/ptah/internal/indexbacking] is where it now lives.
//
// It was duplicated, and the two copies had drifted. The comparator decided per
// dialect and from the index's own structure; the converter decided from name
// equality with no dialect at all. They compensated on almost every shape a
// reader produces -- which is what made the divergence invisible -- and on one
// shape they did not: a SQLite table with a NAMED unique constraint, where
// pragma index_list reports `sqlite_autoindex_<table>_N` and the constraint
// keeps the name the DDL gave it. No name match, no suppression, and
// `ptah db read` described one physical object twice. Replaying that
// description failed, because SQLite refuses to create a name reserved for its
// own use (stokaro/ptah#2894).
//
// # What this guard checks, and what it cannot
//
// It checks that both trees still CALL the shared decision. That is exact: a
// package either contains the call or it does not, and no heuristic decides
// what counts as an ownership resolver. The regression it refuses is the
// realistic one -- a tree inlining the rule again while "simplifying" -- which
// is how the two copies came to exist in the first place.
//
// It cannot see a SECOND rule added beside the shared call. Nothing structural
// distinguishes one, which is why stokaro/ptah#2606 records that a
// "no second ownership resolver" gate needs a heuristic and this repository's
// rule is that a gate checks exactly. The property that catches a second rule
// is behavioral rather than structural, and it already exists:
// `migration/schemadiff`'s ownership round trip converts a catalog to a model,
// compares that model against the catalog it came from, and requires that no
// object is added or removed. Two rules that disagree fail it.
//
// The two are a pair, and neither is the other. This guard refuses the REMOVAL
// of the shared decision; the round trip refuses DISAGREEMENT between whatever
// decisions exist. A guard whose reach is not written down is read as covering
// everything, so it is written down here.
//
// The package carries no runtime code of its own.
package indexbackingguard
