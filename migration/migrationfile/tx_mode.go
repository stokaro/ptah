package migrationfile

import (
	"fmt"
	"strings"
)

// FileTxMode controls transaction handling for one migration file.
type FileTxMode string

const (
	// FileTxModeUnspecified indicates that no file directive was set.
	FileTxModeUnspecified FileTxMode = ""
	// FileTxModeFile runs the migration file in its own transaction.
	FileTxModeFile FileTxMode = "file"
	// FileTxModeNone runs the migration file without a transaction.
	FileTxModeNone FileTxMode = "none"
)

const atlasFileTxModeKey = "atlas:txmode"

// AtlasTxModeDirectiveError reports an invalid or conflicting Atlas
// transaction-mode file directive.
type AtlasTxModeDirectiveError struct {
	message string
}

func (e *AtlasTxModeDirectiveError) Error() string {
	return e.message
}

func newAtlasTxModeDirectiveError(format string, args ...any) *AtlasTxModeDirectiveError {
	return &AtlasTxModeDirectiveError{message: fmt.Sprintf(format, args...)}
}

// NewAtlasTxModeDirectiveError returns an [AtlasTxModeDirectiveError] carrying
// message. The migrator's Atlas directive resolution uses it to refuse a
// combination of global and file transaction modes with the same error identity
// this package's own file parsing raises.
func NewAtlasTxModeDirectiveError(message string) *AtlasTxModeDirectiveError {
	return &AtlasTxModeDirectiveError{message: message}
}

// FileTxModeSource names the directive family a parsed transaction mode came
// from. The zero value means no directive set a mode.
type FileTxModeSource uint8

const (
	// FileTxModeSourcePtah marks a mode set by `-- +ptah no_transaction`.
	FileTxModeSourcePtah FileTxModeSource = iota + 1
	// FileTxModeSourceAtlas marks a mode set by `-- atlas:txmode`.
	FileTxModeSourceAtlas
)

// ParsedFileTxMode is the outcome of reading a file's transaction-mode
// directives: the mode, which directive family set it, and the error a
// malformed or misplaced directive raised. Mode and Source are meaningful only
// when Err is nil.
type ParsedFileTxMode struct {
	Mode   FileTxMode
	Source FileTxModeSource
	Err    error
}

// ParseFileTxMode reads the transaction mode a migration file's directives
// select, across both directive families, with no target dialect resolved.
// Directives only one dialect's string rules would expose stay unread; see
// [ParseFileTxModeForDialect].
func ParseFileTxMode(filename, sql string) ParsedFileTxMode {
	return ParseFileTxModeForDialect(filename, sql, "")
}

// ParseFileTxModeForDialect is [ParseFileTxMode] with the target dialect's
// string, comment, and statement rules deciding what is a directive and where
// the directive header ends. Pass an empty dialect only when no target dialect
// is available.
func ParseFileTxModeForDialect(filename, sql, dialect string) ParsedFileTxMode {
	directives := parseDirectivesConservatively(sql)
	if dialect != "" {
		directives = ParseDirectivesForDialect(sql, dialect)
	}
	parsed := parseFileTxModeWithDirectives(filename, sql, directives)
	if parsed.Err != nil {
		return parsed
	}
	// A directive the region does not honor still has to be well formed. The
	// mode above came from the header; this is the separate question of whether
	// a recognized directive elsewhere in the file carries a value nobody can
	// read. See [misplacedDirectiveError].
	if err := misplacedDirectiveError(sql, dialect); err != nil {
		return ParsedFileTxMode{Source: FileTxModeSourcePtah, Err: err}
	}
	return parsed
}

func parseFileTxModeWithDirectives(
	filename,
	sql string,
	directives map[string]string,
) ParsedFileTxMode {
	atlasMode, hasAtlasMode, atlasErr := parseAtlasFileTxMode(filename, sql)
	if atlasErr != nil {
		return ParsedFileTxMode{Source: FileTxModeSourceAtlas, Err: atlasErr}
	}

	noTransaction, err := parseNoTransactionDirective(directives)
	if err != nil {
		return ParsedFileTxMode{
			Source: FileTxModeSourcePtah,
			Err:    err,
		}
	}
	if noTransaction {
		return ParsedFileTxMode{
			Mode:   FileTxModeNone,
			Source: FileTxModeSourcePtah,
		}
	}
	if hasAtlasMode {
		return ParsedFileTxMode{
			Mode:   atlasMode,
			Source: FileTxModeSourceAtlas,
		}
	}
	return ParsedFileTxMode{}
}

func parseAtlasFileTxMode(filename, sql string) (FileTxMode, bool, error) {
	if !strings.HasPrefix(sql, "--") {
		return FileTxModeUnspecified, false, nil
	}

	var values []string
	headerComplete := false
	for sql != "" {
		line, rest, hasNewline := strings.Cut(sql, "\n")
		// The directive header ends at the first line that is not a comment.
		// A blank line ends it, and so does the first statement: requiring a
		// blank line means `-- atlas:txmode none` immediately above the
		// statement it applies to is silently dropped, and the statement then
		// runs inside a transaction it asked to stay out of.
		//
		// Measured on the pinned community binary: with the blank line the
		// directive is honored, without it the same file fails with
		// `CREATE INDEX CONCURRENTLY cannot run inside a transaction block`.
		// Ptah honored both until it was changed to follow that shape, and
		// honoring both is the behavior restored here -- a directive the author
		// wrote and we can read is not made meaningless by the line after it.
		if strings.TrimSpace(line) == "" || !strings.HasPrefix(line, "--") {
			headerComplete = true
			break
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
		if !hasNewline {
			// The file is nothing but its header, with no trailing newline.
			headerComplete = true
			break
		}
		sql = rest
	}

	if !headerComplete {
		return FileTxModeUnspecified, false, nil
	}
	if len(values) == 0 {
		return FileTxModeUnspecified, false, nil
	}
	if len(values) > 1 {
		return FileTxModeUnspecified, true, newAtlasTxModeDirectiveError(
			"multiple txmode values found in file %q: %q", filename, values,
		)
	}

	mode := FileTxMode(values[0])
	switch mode {
	case FileTxModeFile, FileTxModeNone:
		return mode, true, nil
	case "all":
		return FileTxModeUnspecified, true, newAtlasTxModeDirectiveError(
			"txmode %q is not allowed in file directive %q. Use %q instead",
			mode,
			filename,
			FileTxModeFile,
		)
	default:
		return FileTxModeUnspecified, true, newAtlasTxModeDirectiveError(
			"unknown txmode %q found in file directive %q", mode, filename,
		)
	}
}
