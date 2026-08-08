package atlas

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"

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

// dirQueryStrictEnvVar turns the report below into a refusal.
//
// WHY AN ENVIRONMENT VARIABLE AND NOT A FLAG. The pinned community binary
// v1.3.0 registers no flag for this on any migrate verb, so registering one
// here would put a non-Atlas flag on the compatibility surface and break the
// conformance `cli-surface` tier, which asserts flag parity against that
// binary. An environment variable is invisible to the help surface, which is
// why it is the sanctioned spelling for a capability the community binary has
// no spelling for at all (precedent: PTAH_ALLOW_EXTERNAL_SCHEMA,
// PTAH_SKIP_CHECKS).
//
// WHY THE CAPABILITY IS EXPOSED AT ALL. Ptah refused every `--dir` query on
// every verb until stokaro/ptah#1087 and #1135 relaxed it, on the eight verbs
// that read the query, to match — `checkpoint`, `down`, `edit`, `rebase`, `rm`
// and `test` register `--dir` and still refuse one, which is tracked in
// stokaro/ptah#1013 and is why this doc says "the verbs this variable governs"
// rather than "every verb". That refusal caught something real on its way out:
// a misspelled key such as `?fromat=goose` selects nothing on either binary, so
// the directory is read in the native Atlas layout while the operator believes
// it is being read as Goose. Reaching parity means the default can no longer fail that run —
// but it does not mean the check has to be deleted, only moved off the default
// path. The report below keeps the information on every run; this variable
// keeps the refusal available to a pipeline that wants a typo to stop it.
//
// An invalid value is a hard error rather than a silent false, for the reason
// PTAH_SKIP_CHECKS states: a typo in a CI environment file must not read as
// "off" to the tool while the operator believes it is on. That promise is only
// kept if the value is READ on every run of the verbs this variable governs,
// which is why [reportIgnoredDirQuery] resolves it before it looks at the query
// keys. Resolving it after the key check made `PTAH_STRICT_DIR_QUERY=nope` exit
// 0 in silence on every invocation carrying no ignored key — the whole of a
// healthy pipeline, and the only runs a CI environment file is read on until
// someone makes the very typo the variable is set to catch. PTAH_SKIP_CHECKS
// resolves unconditionally on every `migrate apply`; this now matches it.
const dirQueryStrictEnvVar = "PTAH_STRICT_DIR_QUERY"

// atlasDirQueryStrictFromEnv resolves whether an ignored `--dir` query key is
// refused rather than reported.
func atlasDirQueryStrictFromEnv() (bool, error) {
	value, ok := os.LookupEnv(dirQueryStrictEnvVar)
	if !ok || value == "" {
		return false, nil
	}
	strict, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid boolean value %q for %s", value, dirQueryStrictEnvVar)
	}
	return strict, nil
}

// reportIgnoredDirQuery names the `--dir` URL query keys the run took no
// meaning from, on every verb that reads that query.
//
// It changes no exit code and writes nothing to stdout. Measured on the pinned
// community binary v1.3.0, `--dir 'file://m?nonsense=1'` exits 0 on all eight
// verbs that register --dir and reads the directory exactly as no query at all
// does; Ptah does the same, and this is the note beside it rather than a
// difference in what happens. The note goes to stderr because stdout carries
// the machine-readable `--format` document on several of these verbs and the
// community binary emits no field for this, so putting it there would invent
// one and corrupt a caller's parse.
//
// Reporting rather than dropping in silence is the half of the compatibility
// policy that survives matching: the default is what the community binary
// accepts, and what that default leaves out is said out loud. The key an
// operator typed is the only evidence that they expected it to do something —
// `?fromat=goose` reads the directory as native Atlas on both binaries, and a
// run that says nothing about it is a run that lets the misspelling look like
// the layout they asked for.
//
// The refusal the note offers instead lives behind [dirQueryStrictEnvVar]; see
// there for why it is an environment variable. Its value is resolved BEFORE the
// keys are examined, so an unparsable one fails every run that reads a `--dir`
// URL rather than only the runs already carrying an ignored key. That ordering
// is the whole of the "an invalid value is not a silent off" promise: the runs
// it would otherwise let pass are the correct ones, which is every run a CI
// environment file is read on until the typo it was set to catch finally
// happens.
//
// WHERE CALLERS PUT IT. Right after the format value is resolved, and therefore
// before the atlas.sum gate. Two rules, and only two:
//
//   - it follows the format resolution, so a run refused for `?format=totally-bogus`
//     prints that refusal alone. Both diagnostics are about the same query, and
//     the one that decides the exit code is the one to print;
//   - it precedes everything else, including the integrity gate, so the eight
//     verbs report it at the same point. It describes how the URL was read
//     rather than how the run ended, and a directory that then fails its
//     checksum was still read with that key dropped.
func reportIgnoredDirQuery(out io.Writer, verb string, query url.Values) error {
	strict, err := atlasDirQueryStrictFromEnv()
	if err != nil {
		return fmt.Errorf("atlas migrate %s --dir: %w", verb, err)
	}
	keys := atlasmigrate.IgnoredDirQueryKeys(query)
	if len(keys) == 0 {
		return nil
	}
	if strict {
		return fmt.Errorf(
			"atlas migrate %s --dir: unrecognized migration directory URL query %s:"+
				" only ?%s= selects the directory layout, and %s is set",
			verb, describeDirQueryKeys(keys), atlasmigrate.DirFormatQueryKey, dirQueryStrictEnvVar,
		)
	}
	fmt.Fprintf(
		out,
		"note: atlas migrate %s --dir: ignoring migration directory URL query %s."+
			" Only ?%s= selects the directory layout. Set %s=1 to refuse an unrecognized key instead.\n",
		verb, describeDirQueryKeys(keys), atlasmigrate.DirFormatQueryKey, dirQueryStrictEnvVar,
	)
	return nil
}

// describeDirQueryKeys renders the ignored keys as the noun and the quoted list
// both messages above share, so the report and the refusal cannot name a
// different set.
func describeDirQueryKeys(keys []string) string {
	quoted := make([]string, 0, len(keys))
	for _, key := range keys {
		quoted = append(quoted, strconv.Quote(key))
	}
	noun := "keys"
	if len(keys) == 1 {
		noun = "key"
	}
	return noun + " " + strings.Join(quoted, ", ")
}
