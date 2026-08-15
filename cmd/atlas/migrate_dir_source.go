package atlas

import (
	"io"
	"io/fs"
	"net/url"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/fsnapshot"
)

// This file holds the shared way a read-only compat verb turns a `--dir`
// migration directory into the filesystem it interprets: resolve the layout
// from both spellings that can carry it, capture the source, let the verb's
// integrity gate run over what atlas.sum covers for THAT layout, and only then
// convert.
//
// `migrate status` and `migrate set` came first (stokaro/ptah#1002), and
// `migrate lint` joined them (stokaro/ptah#1013 section 1). It is deliberately
// verb-neutral for the reason #974 recorded about the integrity gate: a rule
// that lives inside one verb is a rule the next verb reading the same
// directories quietly does not have.
//
// The write verbs -- `migrate new` and `migrate diff` -- are NOT here, and the
// reason has changed twice, so it is worth stating the current one. It used to
// be the missing atlas.sum gate: those two wrote a migration file and a fresh
// sum over a directory nothing had verified, so honoring a `?format=` there
// would have traded one divergence for a worse one. That gate landed in
// stokaro/ptah#1086, and with it `migrate new` grew its own converted path --
// it honors `?format=goose` today, exit 0, files in that layout, matching the
// community binary.
//
// `migrate diff` is out for a reason that no longer has anything to do with
// refusing a layout. It honors `?format=goose` too since stokaro/ptah#1013 --
// exit 0, files in that layout, atlas.sum over that layout's covered set. What
// keeps it out of THIS file is that the verbs here READ a directory, and
// reading one is a conversion in memory; `migrate diff` also WRITES one, so it
// resolves the same value through [resolveWritingVerbDirFormat] and then
// carries it into the writer, which converts, names, composes and hashes per
// layout. Both spellings and both resolvers land on
// [atlasmigrate.ResolveApplyDirFormat], so the two paths cannot drift on which
// spelling wins or on which values are accepted.

// resolveAtlasVerbDirFormat resolves the directory layout a compat verb reads,
// from both spellings that can carry it, and blames the one that did.
//
// It is [atlasmigrate.ResolveApplyDirFormat], the resolver `migrate apply`
// uses, so the verbs that read a converted directory cannot drift on which
// spelling wins or on which values are accepted. Two things follow from that,
// both measured against the pinned community binary v1.3.0:
//
//   - The `?format=` query outranks `--dir-format`, in both directions.
//     `--dir 'file://gm?format=golang-migrate' --dir-format atlas` and
//     `--dir 'file://m?format=atlas' --dir-format golang-migrate` each exit 0
//     reading the layout the QUERY names.
//   - Values are matched verbatim. `--dir-format ATLAS`, `--dir-format ' atlas '`
//     and `?format=FLYWAY` each exit 1 with `unknown dir format`. The
//     lower-and-trim normalization [atlasMigrateDirFormatValue] applies made
//     `migrate lint --dir-format ATLAS` exit 0 on a clean directory where the
//     community binary exits 1 -- a divergence in the direction parity must
//     never take.
//
// An unrecognized query key is dropped rather than refused, inside
// ResolveApplyDirFormat, so `?nonsense=1` reads the directory exactly as no
// query at all does (stokaro/ptah#1013 section 2). Dropped is not the same as
// unsaid: [reportIgnoredDirQuery] names it on out, without changing what the
// run does.
//
// out is where that note goes; pass the command's stderr. A caller validating
// only --dir-format passes a nil query and nothing is written.
//
// The returned error already names the command and the flag, because only this
// function knows which of the two spellings carried the rejected value.
func resolveAtlasVerbDirFormat(
	out io.Writer,
	verb string,
	configured string,
	query url.Values,
) (atlasmigrateimport.Format, error) {
	format, err := atlasmigrate.ResolveApplyDirFormat(configured, query)
	if err == nil {
		// Inside the accepted branch, so a run refused for `?format=totally-bogus`
		// prints that refusal alone; see [reportIgnoredDirQuery].
		if reportErr := reportIgnoredDirQuery(out, verb, query); reportErr != nil {
			return "", reportErr
		}
		return format, nil
	}
	// A ?format= query is the only thing that can carry a format value other
	// than the configured one, so it is the only thing that can be blamed for a
	// rejected one. A query holding only ignored keys selects nothing, and the
	// blame stays on --dir-format.
	//
	// [atlasDirFormatError] builds that semantic wrapper and adapts the
	// DISPLAYED text of a rejected format value to the community binary's. It
	// is shared with every other verb that resolves a directory layout; see
	// migrate_dir_format_error.go for why the adaptation belongs on the refusal
	// rather than in one verb's wrapper.
	return "", atlasDirFormatError(verb, atlasDirFormatSpelling(query), err)
}

// atlasDirCapture is a migration directory a read-only verb has read but not
// yet interpreted: the bytes its integrity gate verifies, and -- for a directory
// laid out in a foreign tool's convention -- the covered source set that gets
// converted once the gate passes.
//
// The two-step shape is the point. Integrity is verified BEFORE the source
// layout is parsed, because that is where the community binary verifies it: an
// unhashed Goose directory whose .sql carries no `-- +goose Up` directive is
// refused with a checksum error, never with a conversion error. Converting
// first and gating the result would report the wrong failure, and would gate a
// filesystem rebuilt in memory that carries no integrity file by construction
// (#973).
type atlasDirCapture struct {
	format atlasmigrateimport.Format
	// source is the directory as read. It is what the gate verifies for a
	// native Atlas directory.
	source fs.FS
	// captured is the covered source set for a foreign layout, empty for a
	// native one. It is both what the gate verifies and what gets converted, so
	// the bytes that were checked are the bytes that get interpreted rather
	// than two reads of the same directory that happen to agree.
	captured fsnapshot.Snapshot
}

// captureAtlasDirSource reads a migration directory in the shape format
// requires. A native Atlas directory is kept as-is, so the native gate keeps
// seeing every file in it, including the ones it warns are not covered by
// atlas.sum.
func captureAtlasDirSource(
	source fs.FS,
	format atlasmigrateimport.Format,
) (atlasDirCapture, error) {
	if atlasmigrate.ReadsNativeAtlasDir(format) {
		return atlasDirCapture{format: format, source: source}, nil
	}
	captured, err := atlasmigrate.CaptureApplySource(source, format)
	if err != nil {
		return atlasDirCapture{}, err
	}
	return atlasDirCapture{format: format, source: captured, captured: captured}, nil
}

// gateFS returns the filesystem the integrity gate verifies.
func (c atlasDirCapture) gateFS() fs.FS {
	return c.source
}

// coveredNames returns the file names atlas.sum covers for the resolved layout,
// in the order Atlas hashes them, for a caller whose gate takes the set instead
// of rediscovering it -- `migrate lint`, whose integrity outcome is report
// content rather than a refusal.
//
// It is [atlasmigrateimport.SumFileNames], the same rule `migrate hash` writes
// from and [verifyCoveredAtlasDirChecksum] verifies against, so lint refuses
// exactly the directories those two refuse.
func (c atlasDirCapture) coveredNames() ([]string, error) {
	return atlasmigrateimport.SumFileNames(c.gateFS(), c.format)
}

// versionTokens returns the two-way dictionary between the versions an operator
// can name and the versions this build executes, for a directory whose layout
// has a version space of its own. It is empty for every layout but Flyway.
//
// It reads the GATE filesystem, which for a foreign layout is the same
// immutable snapshot [atlasDirCapture.migrationFS] converts, so a token can
// never belong to a different read of the directory than the migration it
// names.
//
// Call it only after the gate has passed, for the same reason migrationFS says
// so: it interprets the source layout.
func (c atlasDirCapture) versionTokens() (flywayVersionTokens, error) {
	covered, err := atlasmigrateimport.FlywayCoveredSourceVersions(c.gateFS(), c.format)
	if err != nil {
		return flywayVersionTokens{}, err
	}
	return newFlywayVersionTokens(covered), nil
}

// sourceVersions returns the complete Flyway identity mapping, including
// versions a surviving baseline has squashed from the converted filesystem.
// Those retired entries interpret existing history and preserve the linearity
// high-water mark; they never materialize a migration in migrationFS.
func (c atlasDirCapture) sourceVersions() (map[int64]string, error) {
	return atlasmigrateimport.FlywaySourceVersions(c.gateFS(), c.format)
}

// migrationFS returns the filesystem the verb interprets as Atlas migrations.
// A foreign layout is rebuilt in memory as up-only Atlas migrations, which is
// the same conversion `migrate apply` executes -- so the versions a status
// report names, a `migrate set` writes and a lint analyzes are the versions an
// apply of the same directory records.
//
// Call it only after the gate has passed.
func (c atlasDirCapture) migrationFS(display string) (fs.FS, error) {
	if atlasmigrate.ReadsNativeAtlasDir(c.format) {
		return c.source, nil
	}
	return atlasmigrate.ConvertApplySource(c.captured, display, c.format)
}
