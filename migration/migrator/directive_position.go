package migrator

import (
	"fmt"
	"iter"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/dialectlexer"
	"go.5x5.cz/ptah/internal/lexer"
	"go.5x5.cz/ptah/internal/ptahdirective"
)

// A migration file's directives are significant only BEFORE its first
// executable SQL statement. Both directive families answer to that one rule.
//
// They did not always. Measured on `ptah-compat migrate apply --tx-mode all`
// over three SQLite directories differing only in where the directive sits,
// exit codes read directly:
//
//	-- +ptah no_transaction  in the header      exit 1, refused
//	-- +ptah no_transaction  after the SQL      exit 1, refused    <- honored
//	no directive at all                         exit 0
//	-- atlas:txmode none     in the header      exit 1, refused
//	-- atlas:txmode none     after the SQL      exit 0, applied    <- dropped
//
// The pinned community binary answers exit 1 and exit 0 to those last two, so
// the `atlas:` half was already correct on ACCEPTANCE and wrong only in being
// silent about it. The `+ptah` half honored a directive written below the
// statements it claims to govern, which is the half that changes execution
// semantics from a position nobody reads as a header.
//
// Two further facts made the divergence hard to see. Inside the `+ptah` family
// the rule was already inconsistent: `parseMigrationTimeoutDirectives` stopped
// at the first executable line while [ParseFileDirectives] scanned the whole
// file, so `-- +ptah lock_timeout=5s` and `-- +ptah no_transaction` written on
// the same misplaced line had different fates. And a dropped directive produced
// no output at all -- the operator writes `txmode none`, sees exit 0, and
// believes it took effect.
//
// So: one region, computed once by [directiveHeaderLength], and anything
// recognizable that falls outside it is reported rather than dropped. The
// `atlas:` family keeps its own, stricter acceptance INSIDE that region --
// Atlas reads only the unbroken comment block that starts at byte 0, which
// [atlasDirectiveHeaderLength] describes -- because compatibility pins it there
// and because a directive Atlas would not honor must not be honored here.

// directiveRegion returns the part of sql in which a directive is significant
// for dialect.
func directiveRegion(sql, dialect string) string {
	return sql[:directiveHeaderLength(sql, dialect)]
}

// directiveHeaderLength returns the byte length of sql's directive header: the
// prefix that precedes the first executable SQL line.
//
// A line is part of the header when it is blank or a line comment; the header
// ends at the first line that is neither. Leading whitespace before `--` is
// allowed, and a blank line inside the header does not end it -- both were
// already true of the timeout directives, and neither changes which statement a
// directive governs.
//
// The scan is line-based on purpose, and that is safe in front of the
// lexer-driven scans that consume the result: a region built only from blank
// lines and line comments cannot open a string literal, so truncating to it can
// never split a token. A file that opens with a block comment has a zero-length
// header, which is the conservative answer.
func directiveHeaderLength(sql, dialect string) int {
	offset := 0
	for rest := sql; rest != ""; {
		line, tail, hasNewline := strings.Cut(rest, "\n")
		if !directiveHeaderLine(line, dialect) {
			return offset
		}
		offset += len(line)
		if hasNewline {
			offset++
		}
		rest = tail
	}
	return len(sql)
}

// directiveHeaderLine reports whether line is blank or an ordinary line
// comment in dialect. MySQL and MariaDB accept # comments and require
// whitespace after --; asking the shared lexer keeps this boundary identical
// to the parser that will execute the file.
func directiveHeaderLine(line, dialect string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return true
	}
	if strings.HasPrefix(trimmed, "#") {
		normalized := platform.NormalizeDialect(dialect)
		if normalized != platform.MySQL && normalized != platform.MariaDB {
			return false
		}
	} else if !strings.HasPrefix(trimmed, "--") {
		return false
	}
	token := lexer.NewLexerWithOptions(trimmed, dialectlexer.Options(dialect)).NextToken()
	return token.Type == lexer.TokenComment
}

// atlasDirectiveHeaderLength returns the byte length of the block Atlas reads a
// `-- atlas:` file directive from: the unbroken run of line comments that
// begins at the first byte of the file, each starting in column 0.
//
// It is deliberately stricter than [directiveHeaderLength]. Measured on the
// pinned community binary with `--tx-mode all` over one-statement SQLite
// directories, `-- atlas:txmode none` is honored on line 1 and on line 2 below
// another comment (exit 1 both times), and dropped when a blank line precedes
// it, when it is indented, when a blank line separates it from an earlier
// comment, and when it follows the statement (exit 0 all four times).
// [parseAtlasFileTxMode] already matched that, and this function exists so the
// diagnosis can name the same boundary the parser uses instead of guessing at
// one.
func atlasDirectiveHeaderLength(sql string) int {
	if !strings.HasPrefix(sql, "--") {
		return 0
	}
	offset := 0
	for rest := sql; rest != ""; {
		line, tail, hasNewline := strings.Cut(rest, "\n")
		if strings.TrimSpace(line) == "" || !strings.HasPrefix(line, "--") {
			return offset
		}
		offset += len(line)
		if hasNewline {
			offset++
		}
		rest = tail
	}
	return len(sql)
}

// misplacedDirective is one directive line the migrator recognized but did not
// honor, because it sits outside the region where its family is significant.
type misplacedDirective struct {
	// line is the 1-based line number in the migration file.
	line int
	// text is the source line, trimmed.
	text string
	// remedy names what the operator can do about it.
	remedy string
	// err is set when the directive's key is one the merged parser recognizes
	// and its VALUE is one no parser can read. Position and value are separate
	// facts about a line, and a malformed value stays an error wherever the
	// line sits -- see [misplacedDirectiveError].
	err error
}

// validateRecognizedDirectives reports the first recognized `-- +ptah` key
// whose value cannot be read.
//
// Only keys some parser consumes are checked. A `key=value` pair nobody reads
// has no grammar to be wrong against, and a bare token that is not
// no_transaction is not a directive at all -- treating `-- +ptah TODO revisit`
// as a malformed directive would refuse ordinary comments this tree has always
// accepted below a statement.
//
// The bool and duration rules are not restated here; the two functions that own
// them are called, so a change to either reaches this check.
func validateRecognizedDirectives(directives map[string]string) error {
	if _, err := parseNoTransactionDirective(directives); err != nil {
		return err
	}
	for _, key := range []string{"lock_timeout", "lock-timeout", "statement_timeout", "statement-timeout"} {
		value, ok := directives[key]
		if !ok {
			continue
		}
		if _, err := parsePositiveDuration(value); err != nil {
			return fmt.Errorf("invalid +ptah %s value: %w", key, err)
		}
	}
	return nil
}

// misplacedDirectiveError returns the first malformed-value error among the
// directives sql places outside the region where they are significant.
//
// This is the half of the position rule that is NOT a warning, and the reason
// is that position and value are independent facts. Demoting a typo to a
// position warning lets two failures mask each other: the operator is told the
// line is in the wrong place, moves it into the header, and only then learns
// the value was nonsense all along.
//
// The `atlas:` family deliberately gets no equivalent. Measured on the pinned
// community binary, `-- atlas:txmode bogus` in the header exits 1 and the same
// line below the statement exits 0, so refusing it here would exit non-zero
// where the binary accepts, which the compatibility policy forbids by default.
// That divergence in SEVERITY between the families is real, measured, and loud
// on both sides: the `atlas:` line is still reported, just not fatal.
func misplacedDirectiveError(sql, dialect string) error {
	for _, misplaced := range misplacedDirectives(sql, dialect) {
		if misplaced.err == nil {
			continue
		}
		return fmt.Errorf("%w (on line %d, below the first SQL statement, where it would not have been honored)",
			misplaced.err, misplaced.line)
	}
	return nil
}

// misplacedDirectiveMarkers yields the `-- +ptah` markers to judge, using the
// same marker set the transaction mode itself was decided from.
//
// With no target dialect the scan must be conservative, because a marker only
// one dialect's string rules expose is not yet known to be a directive. Asking
// a different question here than [parseMigrationFileTxModeForDialect] asked
// would let load time refuse a file the target dialect reads as string content.
func misplacedDirectiveMarkers(sql, dialect string) iter.Seq[ptahdirective.Marker] {
	if dialect == "" {
		return ptahdirective.ConservativeMarkers(sql)
	}
	return ptahdirective.Markers(sql, dialectlexer.Options(dialect))
}

// misplacedDirectives returns every directive line in sql that a reader would
// take for a directive and the migrator did not honor.
//
// This is the whole of "diagnosed, not dropped". A directive that is present
// but positionally inert is the dangerous case in both families: nothing fails,
// the run exits 0, and the operator believes the file is running the way they
// wrote it.
//
// Two shapes are deliberately NOT reported. A trailing comment after a
// statement carries no directive in either family -- that is documented, not
// accidental, and warning about every comment that mentions a directive name
// would bury the real finding. And a `-- +ptah check` line is significant
// wherever it appears: checks are an ordered list that always runs before the
// first body statement, so its position never decided anything.
func misplacedDirectives(sql, dialect string) []misplacedDirective {
	options := dialectlexer.Options(dialect)
	var found []misplacedDirective

	headerLength := directiveHeaderLength(sql, dialect)
	for marker := range misplacedDirectiveMarkers(sql, dialect) {
		if marker.Start < headerLength {
			continue
		}
		directives := parseFileDirectives(slices.Values([]string{marker.Body}))
		err := misplacedDirectiveValueError(directives, marker.Body)
		if len(directives) == 0 && err == nil {
			continue // a marker every directive parser would have ignored
		}
		found = append(found, misplacedDirective{
			line:   lineNumberAt(sql, marker.Start),
			text:   directiveLineAt(sql, marker.Start),
			remedy: "move it above the first SQL statement",
			err:    err,
		})
	}

	atlasHeaderLength := atlasDirectiveHeaderLength(sql)
	for comment := range ptahdirective.LineComments(sql, options) {
		if comment.Start < atlasHeaderLength {
			continue
		}
		if !strings.Contains(comment.Text, atlasFileTxModeKey) {
			continue
		}
		found = append(found, misplacedDirective{
			line:   lineNumberAt(sql, comment.Start),
			text:   directiveLineAt(sql, comment.Start),
			remedy: "move it into the unbroken comment block that starts on line 1",
		})
	}

	slices.SortFunc(found, func(a, b misplacedDirective) int { return a.line - b.line })
	return found
}

// misplacedDirectiveValueError reports the first value a misplaced `-- +ptah`
// line carries that no parser could read.
//
// Both halves run on every line, and that is the point. The bare-timeout half
// used to run only when the merged parser had extracted nothing at all, so
// whether `-- +ptah no_transaction lock_timeout` was refused depended on a
// SEPARATE field on the same line being parsable -- while the identical line
// written in the header was refused by [parseTimeoutDirectiveFields] either
// way. Position and value are independent facts, and so are the fields of one
// directive line.
func misplacedDirectiveValueError(directives map[string]string, body string) error {
	if err := validateRecognizedDirectives(directives); err != nil {
		return err
	}
	return validateMisplacedBareTimeout(body)
}

// validateMisplacedBareTimeout reports a timeout key written with no value.
//
// The header parser refuses one through [parseTimeoutDirectiveFields], where a
// bare field that is not no_transaction has no reading at all. The merged
// directive map is the looser of the two -- it drops such a token -- so a line
// below the statement needs this scan to reach the same verdict.
//
// An ordered `-- +ptah check` body is exempt for the reason every other parser
// in this package exempts it: its quoted arguments are ParseChecks' grammar,
// not field-split key=value pairs, and the header parser skips it whole. Field-
// splitting one here would refuse below the statement exactly what the header
// accepts, which is the asymmetry this function exists to close.
func validateMisplacedBareTimeout(body string) error {
	if isCheckDirectiveBody(body) {
		return nil
	}
	for field := range strings.FieldsSeq(body) {
		switch field {
		case "lock_timeout", "lock-timeout", "statement_timeout", "statement-timeout":
			return fmt.Errorf("invalid +ptah directive %q", field)
		}
	}
	return nil
}

// lineNumberAt returns the 1-based line number of the byte at offset.
func lineNumberAt(sql string, offset int) int {
	return strings.Count(sql[:offset], "\n") + 1
}

// directiveLineAt returns the trimmed physical line containing offset, so the
// diagnosis quotes what the operator wrote rather than a reconstruction of it.
func directiveLineAt(sql string, offset int) string {
	start := strings.LastIndexByte(sql[:offset], '\n') + 1
	line := sql[start:]
	if end := strings.IndexByte(line, '\n'); end >= 0 {
		line = line[:end]
	}
	return strings.TrimSpace(line)
}
