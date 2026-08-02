package atlasmigrate

import (
	"fmt"
	"io/fs"
	"maps"
	"net/url"
	"slices"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/fsnapshot"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
)

// ResolveApplySourceForFormat returns the immutable filesystem the Atlas apply
// migrator should read for an already-resolved directory format. The native
// Atlas format preserves atlas.sum and down migrations. Every other supported
// Atlas OSS format (golang-migrate, goose, flyway, liquibase, dbmate) is
// converted in memory to Atlas single-file, up-only migrations, so applying it
// executes only the source tool's up SQL and never its down or rollback
// section.
//
// Formats Ptah cannot execute yet (such as Flyway repeatable migrations) and
// malformed layouts return an error. This never opens a pathname or database
// connection, so callers can capture a rooted source and reject a bad layout
// before mutating the target database.
//
// Callers resolve the format once with [ResolveApplyDirFormat] and pass the
// same value to everything that branches on it — the apply-time checksum gate
// selects the covered file set from it — so the filesystem that gets executed
// and the format the gate reasons about are one decision rather than two
// computations that happen to agree (#970).
//
// A caller that gates integrity must not use this: it converts, and integrity
// is verified before conversion. Use [CaptureApplySource], gate, then
// [ConvertApplySource] (#973).
func ResolveApplySourceForFormat(
	source fs.FS,
	display string,
	format atlasmigrateimport.Format,
) (fsnapshot.Snapshot, error) {
	captured, err := CaptureApplySource(source, format)
	if err != nil {
		return fsnapshot.Snapshot{}, err
	}
	return ConvertApplySource(captured, display, format)
}

// CaptureApplySource captures the immutable source snapshot for a migration
// directory read as format. It is the first half of
// [ResolveApplySourceForFormat], split out so a caller can verify the source
// directory's integrity before its layout is parsed.
//
// The split is not cosmetic. Atlas CE checks atlas.sum before it reads the
// source format at all: an unhashed Goose directory whose .sql carries no
// `-- +goose Up` directive is refused with "checksum file not found", and a
// hashed one tampered until it no longer parses is refused with "checksum
// mismatch" — never with a conversion error. Converting first and gating the
// result would report the wrong failure for both, and would gate a filesystem
// rebuilt in memory that carries no integrity file by construction.
//
// The returned snapshot holds every file the format's integrity file covers
// (see [atlasmigrateimport.CaptureFS]), so verification and execution read one
// double-captured set of bytes rather than two reads of the same directory.
func CaptureApplySource(
	source fs.FS,
	format atlasmigrateimport.Format,
) (fsnapshot.Snapshot, error) {
	if source == nil {
		return fsnapshot.Snapshot{}, fmt.Errorf("migration directory filesystem is required")
	}
	snapshot, err := atlasmigrateimport.CaptureFS(source, format)
	if err != nil {
		return fsnapshot.Snapshot{}, fmt.Errorf("capture migration directory: %w", err)
	}
	return snapshot, nil
}

// ConvertApplySource converts an already-captured source snapshot into the
// filesystem the Atlas apply migrator executes. It is the second half of
// [ResolveApplySourceForFormat] and reads only the captured bytes, never the
// original directory.
//
// A native Atlas directory is executed as captured, keeping atlas.sum and its
// down migrations. Every other format is rebuilt as up-only Atlas migrations.
func ConvertApplySource(
	captured fsnapshot.Snapshot,
	display string,
	format atlasmigrateimport.Format,
) (fsnapshot.Snapshot, error) {
	if format == atlasmigrateimport.FormatAtlas {
		return captured, nil
	}
	loaded, err := atlasmigrateimport.LoadFS(captured, display, format)
	if err != nil {
		return fsnapshot.Snapshot{}, err
	}
	converted, err := migrationsnapshot.Capture(loaded.FS())
	if err != nil {
		return fsnapshot.Snapshot{}, fmt.Errorf("capture converted migration directory: %w", err)
	}
	return converted, nil
}

// ReadsNativeAtlasDir reports whether format selects a native Atlas directory
// rather than an external-tool directory converted in memory. Only a native
// Atlas directory is read as-is, keeping whatever atlas.sum it carries; every
// other format is rebuilt as up-only Atlas migrations with no integrity file.
//
// It does not report whether integrity is enforced. Both kinds of directory
// are gated — the source directory carries atlas.sum in either case — they
// differ only in which files that sum covers (#973).
func ReadsNativeAtlasDir(format atlasmigrateimport.Format) bool {
	return format == atlasmigrateimport.FormatAtlas
}

// ResolveApplyDirFormat resolves the migration directory format an apply reads:
// the ?format= URL query value when present (an empty value selects the native
// Atlas format), otherwise the configured project format.
func ResolveApplyDirFormat(configured string, query url.Values) (atlasmigrateimport.Format, error) {
	queryFormat, found, err := applyDirURLFormat(query)
	if err != nil {
		return "", err
	}
	value := configured
	if found {
		value = queryFormat
	}
	return parseApplyDirFormat(value)
}

// parseApplyDirFormat maps a directory format string to a supported format. It
// is deliberately strict: values are matched verbatim (case-sensitive, no
// trimming) so a typo such as "ATLAS" or " atlas " is rejected rather than
// silently coerced. An empty value selects the native Atlas format.
func parseApplyDirFormat(value string) (atlasmigrateimport.Format, error) {
	switch atlasmigrateimport.Format(value) {
	case "", atlasmigrateimport.FormatAtlas:
		return atlasmigrateimport.FormatAtlas, nil
	case atlasmigrateimport.FormatGolangMigrate,
		atlasmigrateimport.FormatGoose,
		atlasmigrateimport.FormatFlyway,
		atlasmigrateimport.FormatLiquibase,
		atlasmigrateimport.FormatDBMate:
		return atlasmigrateimport.Format(value), nil
	default:
		return "", fmt.Errorf(
			"unknown Atlas migration directory format %q: expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate",
			value,
		)
	}
}

func applyDirURLFormat(query url.Values) (string, bool, error) {
	for _, key := range slices.Sorted(maps.Keys(query)) {
		if key != "format" {
			return "", false, fmt.Errorf("unsupported migration directory URL query parameter %q", key)
		}
	}
	formats := query["format"]
	if len(formats) > 1 {
		return "", false, fmt.Errorf("migration directory URL contains multiple format parameters")
	}
	if len(formats) == 1 {
		return formats[0], true, nil
	}
	return "", false, nil
}
