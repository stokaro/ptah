package atlasmigrate

import (
	"fmt"
	"io/fs"
	"net/url"
	"slices"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/fsnapshot"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
)

// DirFormatQueryKey is the one migration-directory URL query key that selects
// anything. It is named rather than spelled twice so that "which keys mean
// something here" has a single answer, which is what
// [IgnoredDirQueryKeys] reports the complement of.
const DirFormatQueryKey = "format"

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
// Atlas format), otherwise the configured project format. Query keys other than
// format are ignored — see [applyDirURLFormat] for what that does and does not
// widen.
func ResolveApplyDirFormat(configured string, query url.Values) (atlasmigrateimport.Format, error) {
	value := configured
	if queryFormat, found := applyDirURLFormat(query); found {
		value = queryFormat
	}
	return parseApplyDirFormat(value)
}

// DirFormatFromQuery reports whether a migration directory URL query selects the
// directory's format, so a caller can name the spelling — --dir or --dir-format
// — that carried a value [ResolveApplyDirFormat] rejected. A query holding only
// ignored keys selects nothing, so the blame stays on --dir-format.
func DirFormatFromQuery(query url.Values) bool {
	_, found := applyDirURLFormat(query)
	return found
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

// applyDirURLFormat reads the format selection out of a migration directory
// URL query. Only the `format` key carries meaning; the return reports the
// selected value and whether the key was present at all, because a present but
// EMPTY value selects the native Atlas format and outranks the configured one.
//
// Two rules here look lax and are not. Both were measured against the pinned
// community binary v1.3.0 on a directory holding 1_init.sql, V1__x.sql and
// U1__undo.sql — a fixture whose covered set differs per format (atlas 3 files,
// flyway 1, golang-migrate 0), so each row is separable rather than three
// formats agreeing by accident (stokaro/ptah#990):
//
//   - A key other than `format` is IGNORED, not refused. `?format=flyway&other=1`
//     exits 0 covering exactly V1__x.sql, so the foreign key is dropped while the
//     format is still honored; `?other=1` alone exits 0 covering all three, i.e.
//     it reads as the native Atlas format rather than as an error.
//   - A repeated `format` takes the FIRST value. `?format=flyway&format=goose`
//     covers V1__x.sql and `?format=goose&format=flyway` covers all three, so the
//     winner tracks position and not the format name.
//
// Loosening this widens what the apply-time integrity gate (stokaro/ptah#973)
// accepts, since the gate selects its covered file set from the format resolved
// here. What it admits is one more spelling of the same three formats — the
// value itself still goes through the strict, verbatim [parseApplyDirFormat],
// so `?format=totally-bogus&x=1` is still refused. No directory becomes
// ungated, and no file set becomes uncovered.
//
// A semicolon in the query stays refused, one step earlier, out of
// url.ParseQuery inside atlasargs.ParseLocalDir. That is a deliberate, recorded
// divergence: measured, `?format=flyway;x=1` exits 0 on the community binary but
// silently drops the WHOLE pair and reads the directory as native Atlas — a
// semicolon costs you the format you asked for, covering three files where you
// asked for one. Refusing is the safe side of that, so it is kept
// (stokaro/ptah#990 item 6).
func applyDirURLFormat(query url.Values) (string, bool) {
	formats := query[DirFormatQueryKey]
	if len(formats) > 0 {
		return formats[0], true
	}
	return "", false
}

// IgnoredDirQueryKeys returns the migration-directory URL query keys the run
// takes no meaning from, sorted, each listed once.
//
// It is the complement of [DirFormatQueryKey], which is the only key
// [applyDirURLFormat] reads. Dropping the rest is parity — measured on the
// pinned community binary v1.3.0, `--dir 'file://m?nonsense=1'` exits 0 on all
// eight verbs that register --dir and reads the directory exactly as no query
// at all does — but dropping them SILENTLY is not something that parity
// requires, and a caller that can name them can say so instead.
//
// A repeated `format` is deliberately absent from the result. That key did
// select the layout; what a second occurrence loses is a VALUE, under the
// first-one-wins rule [applyDirURLFormat] documents and stokaro/ptah#990
// measured. Listing the key here would report the opposite of what happened.
func IgnoredDirQueryKeys(query url.Values) []string {
	keys := make([]string, 0, len(query))
	for key := range query {
		if key == DirFormatQueryKey {
			continue
		}
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
