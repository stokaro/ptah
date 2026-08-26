package migrationfile

import (
	"fmt"
	"strings"
)

// DirFormat selects how a filesystem migration directory is parsed.
type DirFormat string

const (
	// DirFormatAuto prefers Ptah files when present and otherwise
	// falls back to Atlas single-file migrations.
	DirFormatAuto DirFormat = "auto"
	// DirFormatPtah parses NNNNNNNNNN_description.(up|down).sql pairs.
	DirFormatPtah DirFormat = "ptah"
	// DirFormatAtlas parses Atlas version.sql or version_description.sql files.
	DirFormatAtlas DirFormat = "atlas"
)

// ParseDirFormat normalizes a migration directory format value.
func ParseDirFormat(value string) (DirFormat, error) {
	switch DirFormat(strings.ToLower(strings.TrimSpace(value))) {
	case "", DirFormatAuto:
		return DirFormatAuto, nil
	case DirFormatPtah:
		return DirFormatPtah, nil
	case DirFormatAtlas:
		return DirFormatAtlas, nil
	default:
		return "", fmt.Errorf("unknown migration directory format %q: expected auto, ptah, or atlas", value)
	}
}
