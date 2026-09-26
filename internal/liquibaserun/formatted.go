package liquibaserun

import (
	"fmt"
	"regexp"
	"strings"
)

// FormattedChangelog is a formatted-SQL changelog split the way Liquibase's
// parser splits it: the lines before the first changeset, then each changeset
// with its body and its rollback. [ReadFormattedSQL] builds it.
type FormattedChangelog struct {
	// Preamble holds the lines before the first `--changeset`, as written.
	Preamble []string
	// Changesets holds the changesets in the order they appear.
	Changesets []FormattedChangeset
}

// FormattedChangeset is one changeset of a formatted-SQL changelog.
type FormattedChangeset struct {
	// Marker is the `--changeset` line as written.
	Marker string
	// Args is what follows the `--changeset` keyword: the author:id and the
	// attributes.
	Args string
	// Body holds the changeset's lines as written and in order -- its SQL, its
	// comments and its preconditions -- without its rollback.
	Body []string
	// Rollback is what the changeset's `--rollback` lines and
	// `/* liquibase rollback */` blocks spell.
	Rollback Rollback
}

// Name is the changeset's author:id as the marker writes it.
func (c FormattedChangeset) Name() string {
	return changesetName(c.Args)
}

// The patterns below are Liquibase's own (AbstractFormattedChangeLogParser),
// matched against a line whose ASCII letters are lowered, which is what Java's
// CASE_INSENSITIVE flag compares: ASCII letters only. javaSpace is Java's \s
// and javaDot is Java's `.`, which stops at every Java line terminator. Go's
// own classes differ from both, and (?i) folds letters outside ASCII.
const (
	javaSpace = `[ \t\n\x0B\f\r]`
	javaDot   = `[^\n\r\x{85}\x{2028}\x{2029}]`
)

var (
	// rollbackLineRE is a `--rollback <sql>` line. The one blank after the
	// keyword is part of it: `--rollback;` and `--rollback<tab>` are SQL
	// comments to Liquibase, not rollbacks.
	rollbackLineRE = regexp.MustCompile(`^` + javaSpace + `*--` + javaSpace + `*rollback (` + javaDot + `*)$`)
	// rollbackOneDashRE is `-rollback <sql>` with one dash, which Liquibase
	// refuses rather than reads as SQL.
	rollbackOneDashRE = regexp.MustCompile(`^` + javaSpace + `*-` + javaSpace + `*rollback` + javaSpace + javaDot + `*$`)
	// rollbackBlockStartRE opens a `/* liquibase rollback` block. Nothing may
	// follow the keyword on its line, so `/* liquibase rollback */` and
	// `/* liquibase rollback DROP TABLE t; */` are SQL comments to Liquibase.
	rollbackBlockStartRE = regexp.MustCompile(`^` + javaSpace + `*/\*` + javaSpace + `*liquibase` + javaSpace +
		`*rollback` + javaSpace + `*$`)
	// rollbackBlockEndRE closes the block: the first line that ends in `*/`,
	// blanks after it aside, whatever comes before it on the line.
	rollbackBlockEndRE = regexp.MustCompile(`^` + javaDot + `*` + javaSpace + `*\*/` + javaSpace + `*$`)
)

// ReadFormattedSQL splits a formatted-SQL changelog the way Liquibase's parser
// reads it. Every Ptah reader of formatted SQL takes its lines from here, so a
// line means the same to each of them.
//
// Lines are split on a line feed, and a line's trailing carriage return is
// ignored when matching and kept in what is returned.
//
// An `--ignoreLines` directive and the lines it skips are left out, wherever
// they stand. `--ignoreLines:start` skips every line up to the next
// `--ignoreLines:end`, and to the end of the file when there is none, and
// `--ignoreLines:<n>` skips the next n lines. The directive must fill its line,
// its name matches in any case, and start and end match only in lower case. Any
// other word, such as `START`, is an error, as it is to Liquibase, and so are
// the spellings Liquibase refuses: `-ignoreLines:` with one dash, and
// `--ignore:`.
//
// Inside a changeset, a `--rollback <sql>` line and a `/* liquibase rollback`
// block go to the changeset's [Rollback] rather than its body. The block runs
// to the first line that ends in `*/`, and Liquibase reads the lines inside it
// as they are: an `--ignoreLines` directive or a `--changeset` marker there is
// part of the rollback. A block no line closes is an error, as it is to
// Liquibase, and so is `-rollback` with one dash. Before the first changeset
// Liquibase reads no rollback, and such a line is part of the preamble.
func ReadFormattedSQL(file, content string) (FormattedChangelog, error) {
	lines := strings.Split(content, "\n")
	var read FormattedChangelog
	var current *FormattedChangeset
	for number := 0; number < len(lines); number++ {
		line := strings.TrimSuffix(lines[number], "\r")
		last, ignored, err := ignoredThrough(file, lines, number)
		switch {
		case err != nil:
			return FormattedChangelog{}, err
		case ignored:
			number = last
			continue
		}
		if args, ok := ChangesetArgs(line); ok {
			read.Changesets = append(read.Changesets, FormattedChangeset{Marker: lines[number], Args: args})
			current = &read.Changesets[len(read.Changesets)-1]
			continue
		}
		if current == nil {
			read.Preamble = append(read.Preamble, lines[number])
			continue
		}
		lowered := asciiLower(line)
		if match := rollbackLineRE.FindStringSubmatchIndex(lowered); match != nil {
			current.Rollback.addLine(line[match[2]:match[3]])
			continue
		}
		if rollbackOneDashRE.MatchString(lowered) {
			return FormattedChangelog{}, fmt.Errorf("liquibase changelog %q line %d: %q is not a directive Liquibase "+
				"reads, and Liquibase refuses it -- write --rollback <SQL>", file, number+1, line)
		}
		if rollbackBlockStartRE.MatchString(lowered) {
			end, closed := current.Rollback.addBlock(lines, number)
			if !closed {
				return FormattedChangelog{}, fmt.Errorf("liquibase changeset %s in %q opens a /* liquibase rollback "+
					"block that no line closes, and Liquibase refuses the changelog -- end the block with a line "+
					"that ends in */", changesetName(current.Args), file)
			}
			number = end
			continue
		}
		current.Body = append(current.Body, lines[number])
	}
	return read, nil
}

// addBlock adds the `/* liquibase rollback` block that opens at lines[start],
// the way Liquibase's parser reads it (extractMultiLineRollBack): each line
// inside the block is added as it is, and the line that closes it adds what
// comes before its `*/` unless that is blank. Liquibase puts nothing between
// them, not even a line break. It returns the index of the closing line, and
// false when no line closes the block.
func (r *Rollback) addBlock(lines []string, start int) (end int, closed bool) {
	for number := start + 1; number < len(lines); number++ {
		line := strings.TrimSuffix(lines[number], "\r")
		if !rollbackBlockEndRE.MatchString(line) {
			r.addPiece(line)
			continue
		}
		before := strings.TrimRight(line, " \t\n\x0B\f\r")
		before = strings.TrimSuffix(before, "*/")
		if !javaBlank(before) {
			r.addPiece(before)
		}
		return number, true
	}
	return 0, false
}

// javaBlank reports what Apache Commons StringUtils.isWhitespace reports, which
// is how Liquibase decides that the text before a block's `*/` adds nothing:
// true for an empty string, and for one made of Java whitespace only.
func javaBlank(text string) bool {
	for _, r := range text {
		if !javaWhitespace(r) {
			return false
		}
	}
	return true
}

// javaWhitespace is Java's Character.isWhitespace: a Unicode space, line or
// paragraph separator other than the no-break spaces, or one of the controls
// U+0009 to U+000D and U+001C to U+001F.
func javaWhitespace(r rune) bool {
	switch {
	case r >= '\t' && r <= '\r', r >= 0x1C && r <= 0x1F, r == ' ':
		return true
	case r == 0xA0, r == 0x2007, r == 0x202F:
		return false
	case r == 0x1680, r >= 0x2000 && r <= 0x200A, r == 0x2028, r == 0x2029, r == 0x205F, r == 0x3000:
		return true
	default:
		return false
	}
}

// asciiLower lowers the ASCII letters of text and leaves every other byte
// where it is, so an index into the result is an index into text.
func asciiLower(text string) string {
	lowered := []byte(text)
	for index, b := range lowered {
		if b >= 'A' && b <= 'Z' {
			lowered[index] = b + 'a' - 'A'
		}
	}
	return string(lowered)
}
