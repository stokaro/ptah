package migrationfile

import (
	"fmt"
)

// ParsedUp is the executable up-direction view of one migration's content:
// for a plain SQL file the whole file, for an Atlas txtar archive its
// migration.sql section. [ParseUp] and [ParseUpForAnalysis] produce it.
type ParsedUp struct {
	// SQL is the plain file or the migration.sql section of a txtar file.
	SQL string
	// TxMode is the explicit transaction mode the file's directives select.
	// [FileTxModeUnspecified] means no directive set one, and the caller's
	// global mode applies.
	TxMode FileTxMode
	// SourceLineOffset maps line numbers in SQL back to the source file: line
	// n of SQL is line n+SourceLineOffset of the file. It is 0 for a plain
	// file.
	SourceLineOffset int
}

// ParseUp returns the executable up-direction view of plain SQL or
// Atlas txtar migration content. An unspecified transaction mode means the
// caller's global mode applies.
func ParseUp(filename, sql string) (ParsedUp, error) {
	parsed, isTxtar, err := ParseAtlasTxtar(filename, sql)
	if err != nil {
		return ParsedUp{}, err
	}
	result := ParsedUp{SQL: sql}
	if isTxtar {
		filename += "#" + AtlasTxtarMigrationSection
		result.SQL = parsed.MigrationSQL
		result.SourceLineOffset = parsed.MigrationLineOffset
	}

	txMode := ParseFileTxMode(filename, result.SQL)
	if txMode.Err != nil {
		if txMode.Source == FileTxModeSourceAtlas {
			return ParsedUp{}, txMode.Err
		}
		return ParsedUp{}, fmt.Errorf("invalid migration directives in %s: %w", filename, txMode.Err)
	}
	result.TxMode = txMode.Mode
	return result, nil
}

// ParseUpForAnalysis is [ParseUp] for read-only analysis,
// where an unrecognized transaction mode is not a reason to refuse the file.
//
// `migrate lint` reads a directory to report on it and executes nothing, so a
// directive that only decides how the file would be APPLIED cannot make the
// analysis wrong. Refusing there also diverges: measured on the pinned
// community binary, `migrate lint` over a directory carrying
// `-- atlas:txmode unknown` exits 0 and analyzes normally, while refusing it
// left a user unable to lint a directory they could still apply-check.
//
// Only the transaction mode is forgiven. A malformed file, a bad txtar
// envelope, or anything else [ParseUp] rejects is still an error, and
// the apply path keeps using [ParseUp] unchanged -- executing a file
// whose transaction mode we could not read is a different question with a
// different answer.
func ParseUpForAnalysis(filename, sql string) (ParsedUp, error) {
	parsed, err := ParseUp(filename, sql)
	if err == nil {
		return parsed, nil
	}
	stripped, txErr := parseUpIgnoringTxMode(filename, sql)
	if txErr != nil {
		return ParsedUp{}, err
	}
	return stripped, nil
}

// parseUpIgnoringTxMode repeats [ParseUp] without consulting
// the transaction-mode directive, so a caller can tell a txmode complaint apart
// from a file it genuinely cannot read.
func parseUpIgnoringTxMode(filename, sql string) (ParsedUp, error) {
	parsed, isTxtar, err := ParseAtlasTxtar(filename, sql)
	if err != nil {
		return ParsedUp{}, err
	}
	result := ParsedUp{SQL: sql}
	if isTxtar {
		result.SQL = parsed.MigrationSQL
		result.SourceLineOffset = parsed.MigrationLineOffset
	}
	return result, nil
}
