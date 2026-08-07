package atlas

import (
	"fmt"
	"net/url"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
)

// resolveWritingVerbDirFormat resolves the migration directory layout the one
// verb that WRITES into the directory — `migrate diff` — was pointed at, from
// the two spellings that can select one: the `--dir` URL query and
// `--dir-format`.
//
// It is [atlasmigrate.ResolveApplyDirFormat], which is the same resolution
// every verb that READS a directory already runs (resolveAtlasMigrateSource in
// migrate_integrity.go). `migrate diff` used to carry a second rule of its own
// — `strings.ToLower(strings.TrimSpace(--dir-format)) != "atlas"` — and that
// rule diverged from the community binary in both directions. Measured against
// the pinned community binary v1.3.0 on 2026-08-07, on a hashed native Atlas
// directory:
//
//	--dir-format ATLAS                                CE 1 unknown dir format "ATLAS"  ptah 0, WROTE
//	--dir-format Atlas                                CE 1                             ptah 0, WROTE
//	--dir-format ' atlas '                            CE 1                             ptah 0, WROTE
//	--dir 'd?format=atlas' --dir-format golang-migrate CE 0, WROTE                      ptah 1
//	--dir 'd?format='      --dir-format golang-migrate CE 0, WROTE                      ptah 1
//
// The first three rows are the forbidden direction — exit 0 where the community
// binary exits 1, on the one compat verb that mutates the directory — and the
// coercion is what produced them: only the verbatim value `atlas` selects that
// layout there. The last two are the query-outranks-the-flag precedence the
// same binary applies wherever both spellings are present.
//
// The rows that must NOT move, same fixture and same binary:
//
//	--dir-format golang-migrate                       CE 1                             ptah 1
//	--dir 'd?nonsense=1' --dir-format golang-migrate  CE 1                             ptah 1
//	--dir-format atlas                                CE 0, WROTE                      ptah 0, WROTE
//	--dir-format ''                                   CE 0, WROTE                      ptah 0, WROTE
//	no --dir-format at all                            CE 0, WROTE                      ptah 0, WROTE
//
// The `?nonsense=1` row is the one that separates this from "any query at all
// disables the flag": an ignored key selects no layout, so `--dir-format` still
// decides and the refusal stands.
func resolveWritingVerbDirFormat(
	configured string,
	query url.Values,
) (atlasmigrateimport.Format, error) {
	return atlasmigrate.ResolveApplyDirFormat(configured, query)
}

// atlasDirFormatSpelling names the flag a rejected migration-directory format
// value came from, so the diagnostic blames what the operator actually typed.
//
// A `?format=` query is the only thing that can carry a format value other than
// the configured one, so it is the only thing that can be blamed for a rejected
// one. A query carrying only ignored keys selects nothing, and the blame stays
// on --dir-format.
func atlasDirFormatSpelling(query url.Values) string {
	if atlasmigrate.DirFormatFromQuery(query) {
		return "--dir"
	}
	return "--dir-format"
}

// checkWritingVerbDirQuery refuses a foreign layout that the `--dir` QUERY
// named, for the one verb that WRITES into the directory: `migrate diff`.
//
// `migrate new` used to share this refusal. It no longer calls here at all —
// it grew a converted path in stokaro/ptah#845 and honors `?format=goose`
// today, exit 0 with files in that layout, matching the community binary.
//
// Writing a foreign layout is a different problem from reading one. The reading
// verbs — hash, validate, lint, status, set — convert a foreign layout in
// memory and report on it, which is what #992, #1002 and #1133 built. Measured
// against the pinned community binary v1.3.0 on 2026-08-07, `migrate diff` in a
// foreign layout writes reverse SQL as well as forward SQL, in five different
// shapes: golang-migrate and flyway put it in a second file, goose and dbmate
// put it under a directive in the same file, and liquibase interleaves one
// `--rollback:` line per forward statement. Ptah's `migrate diff` plans forward
// statements only, so honoring the query here would write a directory whose
// rollback half is missing or empty. Refusing is the strict side: it never
// exits 0 where the community binary exits 1. stokaro/ptah#1013 tracks the gap.
//
// The refusal is limited to the query spelling deliberately. `--dir-format`
// naming a foreign layout is refused too, but AFTER the atlas.sum gate, because
// that is where the community binary refuses it: on an unhashed directory
// `migrate diff demo --dir file://d --dir-format goose` prints the checksum
// error there, not a format complaint, while `--dir-format ATLAS` — a value it
// cannot parse at all — prints `unknown dir format` ahead of the gate.
//
// An empty `?format=` value selects the native Atlas layout and passes, which
// is what the community binary does with it too.
func checkWritingVerbDirQuery(format atlasmigrateimport.Format, query url.Values) error {
	if atlasmigrate.ReadsNativeAtlasDir(format) || !atlasmigrate.DirFormatFromQuery(query) {
		return nil
	}
	return fmt.Errorf(
		"Atlas accepts ?format=%s, but Ptah does not implement that directory format for this command yet",
		format,
	)
}
