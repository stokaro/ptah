package atlasmigrate

import (
	"fmt"
	"io/fs"
	"maps"
	"net/url"
	"os"
	"slices"

	"github.com/stokaro/ptah/internal/atlasmigrateimport"
)

// ResolveApplyDir validates the requested migration directory format and returns
// the filesystem the Atlas apply migrator should read. The native Atlas format
// is read from disk unchanged, preserving atlas.sum verification and down
// migrations. Every other supported Atlas OSS format (golang-migrate, goose,
// flyway, liquibase, dbmate) is read and converted in memory to Atlas
// single-file, up-only migrations, so applying it executes only the source
// tool's up SQL and never its down or rollback section.
//
// Unknown formats, unsupported URL query parameters, and formats Ptah cannot
// execute yet (such as Flyway repeatable migrations) return an error.
// ResolveApplyDir never opens a database connection, so callers can reject a
// bad format or a malformed layout before mutating the target database. The URL
// ?format= query value, when present, overrides the configured project format;
// an empty query value selects the native Atlas format.
func ResolveApplyDir(dir, configured string, query url.Values) (fs.FS, error) {
	format, err := resolveApplyDirFormat(configured, query)
	if err != nil {
		return nil, err
	}
	if format == atlasmigrateimport.FormatAtlas {
		return os.DirFS(dir), nil
	}
	loaded, err := atlasmigrateimport.LoadDir(dir, format)
	if err != nil {
		return nil, err
	}
	return loaded.FS(), nil
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
