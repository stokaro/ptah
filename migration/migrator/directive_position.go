package migrator

import (
	"fmt"
	"iter"
	"slices"
	"strings"

	"go.5x5.cz/ptah/internal/dialectlexer"
	"go.5x5.cz/ptah/internal/envbool"
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

// directivesAnywhereEnvVar restores the scope the merged `-- +ptah` directive
// map had before the position rule was unified: significant anywhere in the
// file, including below the statements it governs.
//
// It exists because that scope is behavior this tree shipped, and narrowing it
// silently would break a directory that works today. It restores exactly what
// was there and nothing more, so the timeout keys stay header-scoped under it:
// they were never file-wide, and widening them here would be adding a
// capability behind a variable whose job is to keep an old one.
//
// The variable does NOT touch the `atlas:` spelling, which stays exactly what
// the pinned community binary does in every mode -- an operator restoring their
// own directive convention must not also move ptah-compat off Atlas parity.
const directivesAnywhereEnvVar = "PTAH_DIRECTIVES_ANYWHERE"

// directivesAnywhere is the declaration of the variable, made once, in the
// package that owns it. See [go.5x5.cz/ptah/internal/envbool].
//
// It is [go.5x5.cz/ptah/internal/envbool.Gated]. `-- +ptah` is a Ptah-only
// directive family the pinned community binary has no reading of at all, so
// widening where one is significant selects behavior that binary does not have.
// Strict mode already refuses a migration carrying a `+ptah` directive
// wherever it sits, and refusing the variable too is what makes the refusal
// legible: the operator learns the setting is out of scope for the run instead
// of finding out from a migration that happens to contain one.
var directivesAnywhere = envbool.New(directivesAnywhereEnvVar, false, envbool.Gated)

// directiveScope selects the region of a migration file in which a `-- +ptah`
// directive is significant.
type directiveScope uint8

const (
	// directiveScopeHeader honors directives only before the first executable
	// SQL statement. It is the default and the rule both families share.
	directiveScopeHeader directiveScope = iota
	// directiveScopeFile honors directives anywhere in the file. It is reached
	// only through directivesAnywhereEnvVar.
	directiveScopeFile
)

// resolveDirectiveScope reads the opt-in.
//
// Unset keeps the header rule and a valid false spelling keeps it too; an empty
// or unparsable value is a configuration error rather than a silent fall back
// to the default, because an operator who wrote the variable to keep their
// files working must not be told nothing when the value is a typo.
func resolveDirectiveScope() (directiveScope, error) {
	anywhere, err := directivesAnywhere.Resolve()
	if err != nil {
		return directiveScopeHeader, err
	}
	if anywhere {
		return directiveScopeFile, nil
	}
	return directiveScopeHeader, nil
}

// directiveRegion returns the part of sql in which a directive is significant
// under scope.
func directiveRegion(sql string, scope directiveScope) string {
	if scope == directiveScopeFile {
		return sql
	}
	return sql[:directiveHeaderLength(sql)]
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
func directiveHeaderLength(sql string) int {
	offset := 0
	for rest := sql; rest != ""; {
		line, tail, hasNewline := strings.Cut(rest, "\n")
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "--") {
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
// the value was nonsense all along. Worse, the verdict would depend on
// PTAH_DIRECTIVES_ANYWHERE -- the same file accepted in one mode and refused in
// the other -- when whether `maybe` is a boolean is a property of the file.
//
// The `atlas:` family deliberately gets no equivalent. Measured on the pinned
// community binary, `-- atlas:txmode bogus` in the header exits 1 and the same
// line below the statement exits 0, so refusing it here would exit non-zero
// where the binary accepts, which the compatibility policy forbids by default.
// That divergence in SEVERITY between the families is real, measured, and loud
// on both sides: the `atlas:` line is still reported, just not fatal.
func misplacedDirectiveError(sql, dialect string, scope directiveScope) error {
	for _, misplaced := range misplacedDirectives(sql, dialect, scope) {
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
func misplacedDirectives(sql, dialect string, scope directiveScope) []misplacedDirective {
	options := dialectlexer.Options(dialect)
	var found []misplacedDirective

	if scope == directiveScopeHeader {
		headerLength := directiveHeaderLength(sql)
		for marker := range misplacedDirectiveMarkers(sql, dialect) {
			if marker.Start < headerLength {
				continue
			}
			directives := parseFileDirectives(slices.Values([]string{marker.Body}))
			if len(directives) == 0 {
				continue // a marker the merged parser would have ignored anyway
			}
			found = append(found, misplacedDirective{
				line:   lineNumberAt(sql, marker.Start),
				text:   directiveLineAt(sql, marker.Start),
				remedy: "move it above the first SQL statement, or set " + directivesAnywhereEnvVar + "=1",
				err:    validateRecognizedDirectives(directives),
			})
		}
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
