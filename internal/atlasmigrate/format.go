package atlasmigrate

import (
	"fmt"
	"io/fs"
	"maps"
	"net/url"
	"slices"

	"github.com/stokaro/ptah/internal/atlasmigrateimport"
	"github.com/stokaro/ptah/internal/fsnapshot"
	"github.com/stokaro/ptah/internal/migrationsnapshot"
)

// ResolveApplySource validates the requested migration directory format and
// returns the immutable filesystem the Atlas apply migrator should read. The
// native Atlas format preserves atlas.sum and down migrations. Every other
// supported Atlas OSS format (golang-migrate, goose, flyway, liquibase, dbmate)
// is converted in memory to Atlas single-file, up-only migrations, so applying
// it executes only the source tool's up SQL and never its down or rollback
// section.
//
// Unknown formats, unsupported URL query parameters, and formats Ptah cannot
// execute yet (such as Flyway repeatable migrations) return an error.
// ResolveApplySource never opens a pathname or database connection, so callers
// can capture a rooted source and reject a bad format or malformed layout
// before mutating the target database. The URL ?format= query value, when
// present, overrides the configured project format; an empty query value
// selects the native Atlas format.
func ResolveApplySource(
	source fs.FS,
	display string,
	configured string,
	query url.Values,
) (fsnapshot.Snapshot, error) {
	format, err := resolveApplyDirFormat(configured, query)
	if err != nil {
		return fsnapshot.Snapshot{}, err
	}
	if source == nil {
		return fsnapshot.Snapshot{}, fmt.Errorf("migration directory filesystem is required")
	}
	snapshot, err := atlasmigrateimport.CaptureFS(source, format)
	if err != nil {
		return fsnapshot.Snapshot{}, fmt.Errorf("capture migration directory: %w", err)
	}
	if format == atlasmigrateimport.FormatAtlas {
		return snapshot, nil
	}
	loaded, err := atlasmigrateimport.LoadFS(snapshot, display, format)
	if err != nil {
		return fsnapshot.Snapshot{}, err
	}
	converted, err := migrationsnapshot.Capture(loaded.FS())
	if err != nil {
		return fsnapshot.Snapshot{}, fmt.Errorf("capture converted migration directory: %w", err)
	}
	return converted, nil
}

// ApplyReadsNativeAtlasDir reports whether an apply of configured/query reads a
// native Atlas directory rather than an external-tool directory converted in
// memory. It resolves the format exactly the way [ResolveApplySource] does, so
// the two can never disagree about what a given --dir selects.
//
// Only a native Atlas directory carries atlas.sum: every converted format is
// rebuilt as up-only Atlas migrations with no integrity file, so the apply-time
// checksum gate applies to the native format alone (#970).
func ApplyReadsNativeAtlasDir(configured string, query url.Values) (bool, error) {
	format, err := resolveApplyDirFormat(configured, query)
	if err != nil {
		return false, err
	}
	return format == atlasmigrateimport.FormatAtlas, nil
}

func resolveApplyDirFormat(configured string, query url.Values) (atlasmigrateimport.Format, error) {
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
