package atlas

import (
	"fmt"
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
// The write verbs -- `migrate new` and `migrate diff` -- are NOT here, and that
// is the current state of stokaro/ptah#1013 rather than an oversight. Every verb
// in this file verifies atlas.sum before anything reads the directory; those two
// run no such gate and they write a migration file and a fresh sum. Routing them
// through here would let a `?format=` produce a write over a directory nothing
// verified, which is a worse divergence than the refusal it replaces. The gate
// is stokaro/ptah#1086, and the relaxation lands with it.

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
// query at all does (stokaro/ptah#1013 section 2).
//
// The returned error already names the command and the flag, because only this
// function knows which of the two spellings carried the rejected value.
func resolveAtlasVerbDirFormat(
	verb string,
	configured string,
	query url.Values,
) (atlasmigrateimport.Format, error) {
	format, err := atlasmigrate.ResolveApplyDirFormat(configured, query)
	if err == nil {
		return format, nil
	}
	// A ?format= query is the only thing that can carry a format value other
	// than the configured one, so it is the only thing that can be blamed for a
	// rejected one. A query holding only ignored keys selects nothing, and the
	// blame stays on --dir-format.
	spelling := "--dir-format"
	if atlasmigrate.DirFormatFromQuery(query) {
		spelling = "--dir"
	}
	return "", fmt.Errorf("atlas migrate %s %s: %w", verb, spelling, err)
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
