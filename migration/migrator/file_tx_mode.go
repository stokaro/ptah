package migrator

import (
	"fmt"
	"strings"
)

// MigrationFileTxMode controls transaction handling for one migration file.
type MigrationFileTxMode string

const (
	// MigrationFileTxModeUnspecified indicates that no file directive was set.
	MigrationFileTxModeUnspecified MigrationFileTxMode = ""
	// MigrationFileTxModeFile runs the migration file in its own transaction.
	MigrationFileTxModeFile MigrationFileTxMode = "file"
	// MigrationFileTxModeNone runs the migration file without a transaction.
	MigrationFileTxModeNone MigrationFileTxMode = "none"
)

const atlasFileTxModeKey = "atlas:txmode"

// ParsedMigrationUp is the executable up-direction view of a plain SQL or
// Atlas txtar migration file.
type ParsedMigrationUp struct {
	// SQL is the plain file or the migration.sql section of a txtar file.
	SQL string
	// TxMode is the explicit transaction mode found in SQL.
	TxMode MigrationFileTxMode
	// SourceLineOffset maps line numbers in SQL back to the source file.
	SourceLineOffset int
}

// ParseMigrationUp returns the executable up-direction view of plain SQL or
// Atlas txtar migration content. An unspecified transaction mode means the
// caller's global mode applies.
func ParseMigrationUp(filename, sql string) (ParsedMigrationUp, error) {
	parsed, isTxtar, err := parseAtlasTxtarSQL(filename, sql)
	if err != nil {
		return ParsedMigrationUp{}, err
	}
	result := ParsedMigrationUp{SQL: sql}
	if isTxtar {
		filename += "#" + atlasTxtarMigrationSection
		result.SQL = parsed.migrationSQL
		result.SourceLineOffset = parsed.migrationLineOffset
	}

	txMode := parseMigrationFileTxMode(filename, result.SQL)
	if txMode.err != nil {
		if txMode.source == migrationFileTxModeSourceAtlas {
			return ParsedMigrationUp{}, txMode.err
		}
		return ParsedMigrationUp{}, fmt.Errorf("invalid migration directives in %s: %w", filename, txMode.err)
	}
	result.TxMode = txMode.mode
	return result, nil
}

func parseAtlasFileTxMode(filename, sql string) (MigrationFileTxMode, bool, error) {
	if !strings.HasPrefix(sql, "--") {
		return MigrationFileTxModeUnspecified, false, nil
	}

	var values []string
	headerComplete := false
	for sql != "" {
		line, rest, hasNewline := strings.Cut(sql, "\n")
		if strings.TrimSpace(line) == "" {
			headerComplete = true
			break
		}
		if !hasNewline || !strings.HasPrefix(line, "--") {
			return MigrationFileTxModeUnspecified, false, nil
		}

		for search := line; ; {
			_, after, found := strings.Cut(search, atlasFileTxModeKey)
			if !found {
				break
			}
			value := ""
			if strings.HasPrefix(after, " ") {
				value = strings.TrimSpace(after[1:])
			}
			values = append(values, value)
			search = after
		}
		sql = rest
	}

	if !headerComplete {
		return MigrationFileTxModeUnspecified, false, nil
	}
	if len(values) == 0 {
		return MigrationFileTxModeUnspecified, false, nil
	}
	if len(values) > 1 {
		return MigrationFileTxModeUnspecified, true, fmt.Errorf(
			"multiple txmode values found in file %q: %q", filename, values,
		)
	}

	mode := MigrationFileTxMode(values[0])
	switch mode {
	case MigrationFileTxModeFile, MigrationFileTxModeNone:
		return mode, true, nil
	case "all":
		return MigrationFileTxModeUnspecified, true, fmt.Errorf(
			"txmode %q is not allowed in file directive %q. Use %q instead",
			mode,
			filename,
			MigrationFileTxModeFile,
		)
	default:
		return MigrationFileTxModeUnspecified, true, fmt.Errorf(
			"unknown txmode %q found in file directive %q", mode, filename,
		)
	}
}
