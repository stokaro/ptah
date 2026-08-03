package atlas

import (
	"fmt"
	"io/fs"
	"net/url"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/fsnapshot"
)

// resolveAtlasRevisionDirFormat resolves the directory layout a revision-table
// verb — `migrate status` and `migrate set` — reads, from both spellings that
// can carry it, and blames the one that did.
//
// It is [atlasmigrate.ResolveApplyDirFormat], the resolver `migrate apply`
// uses, so the verbs that read a converted directory cannot drift on which
// spelling wins or on which values are accepted. That matters for case in
// particular: the community binary matches the value verbatim, and measured on
// v1.3.0 `--dir-format ATLAS`, `--dir-format ' atlas '` and `?format=FLYWAY`
// each exit 1 with `unknown dir format`. The lower-and-trim normalization these
// two verbs used to apply accepted all three.
//
// The returned error already names the command and the flag, because only this
// function knows which of the two spellings carried the rejected value.
func resolveAtlasRevisionDirFormat(
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

// atlasRevisionCapture is a migration directory a revision-table verb has read
// but not yet interpreted: the bytes its integrity gate verifies, and — for a
// directory laid out in a foreign tool's convention — the covered source set
// that gets converted once the gate passes.
//
// The two-step shape is the point. Integrity is verified BEFORE the source
// layout is parsed, because that is where the community binary verifies it: an
// unhashed Goose directory whose .sql carries no `-- +goose Up` directive is
// refused with a checksum error, never with a conversion error. Converting
// first and gating the result would report the wrong failure, and would gate a
// filesystem rebuilt in memory that carries no integrity file by construction
// (#973).
type atlasRevisionCapture struct {
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

// captureAtlasRevisionSource reads a migration directory in the shape format
// requires. A native Atlas directory is kept as-is, so the native gate keeps
// seeing every file in it, including the ones it warns are not covered by
// atlas.sum.
func captureAtlasRevisionSource(
	source fs.FS,
	format atlasmigrateimport.Format,
) (atlasRevisionCapture, error) {
	if atlasmigrate.ReadsNativeAtlasDir(format) {
		return atlasRevisionCapture{format: format, source: source}, nil
	}
	captured, err := atlasmigrate.CaptureApplySource(source, format)
	if err != nil {
		return atlasRevisionCapture{}, err
	}
	return atlasRevisionCapture{format: format, source: captured, captured: captured}, nil
}

// gateFS returns the filesystem the integrity gate verifies.
func (c atlasRevisionCapture) gateFS() fs.FS {
	return c.source
}

// migrationFS returns the filesystem the verb interprets as Atlas migrations.
// A foreign layout is rebuilt in memory as up-only Atlas migrations, which is
// the same conversion `migrate apply` executes — so the versions a status
// report names and a `migrate set` writes are the versions an apply of the same
// directory records.
//
// Call it only after the gate has passed.
func (c atlasRevisionCapture) migrationFS(display string) (fs.FS, error) {
	if atlasmigrate.ReadsNativeAtlasDir(c.format) {
		return c.source, nil
	}
	return atlasmigrate.ConvertApplySource(c.captured, display, c.format)
}
