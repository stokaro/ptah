// Package migrationfailure explains the statement or the commit a failed
// migration run stopped at, for a person who asked to see the database
// server's own account of it: the migration file the statement came from,
// where the error falls in the statement and in the file, and what the server
// said beyond its code (see internal/serverdiag).
//
// The run document's failure record, [migrator.RunFailure], identifies the same
// statement and carries codes alone, so a caller that must not publish data,
// such as the Kubernetes operator, can read it. What this package builds can
// quote row values and is never part of a default output: `ptah migrations up`
// and `ptah migrations down` add it only under --server-details, and
// `ptah-compat migrate apply` only under PTAH_SERVER_DETAILS.
package migrationfailure

import (
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"ptah.run/internal/serverdiag"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// Options says what [New] may read besides the run's error.
type Options struct {
	// Source is the migration directory as its author wrote it. It is what
	// maps the server's position to a line of the file; nil maps none.
	Source fs.FS
	// Renamed maps a file the migrator ran to the file of Source it was
	// converted from, for a directory converted before the run. A file it
	// does not name is the file of the same name in Source.
	Renamed map[string]string
	// Redact takes the secrets the command must not show out of a text the
	// server answered with, such as the passwords of the addresses it opened.
	// It runs before the size bound. nil takes out only what the writer's own
	// error takes out.
	Redact func(string) string
}

// Details is the local account of the statement or the commit a failed run
// stopped at.
type Details struct {
	// Failure identifies the statement or the commit, as the run document's
	// failure record does. The document prints that record itself, so the
	// JSON form leaves it out.
	Failure migrator.RunFailure `json:"-"`
	// File is the migration file the failed statement came from, relative to
	// the migration directory and named as the directory holds it: an Atlas
	// txtar file is named without the section the statement came from. It is
	// empty for a commit and for a migration that was not read from a file.
	File string `json:"file,omitempty"`
	// Location is where the server's position falls, and nil when the server
	// gave none or the position cannot be placed reliably.
	Location *Location `json:"location,omitempty"`
	// Server is what the server said, and nil when no server answered, as
	// after a lost connection or a client-side timeout, and on an engine whose
	// driver reports none of it.
	Server *serverdiag.Answer `json:"server,omitempty"`
}

// Location is where the server's error position falls. Lines and columns are
// 1-based, and a column counts characters, as the server's position does.
type Location struct {
	// Line and Column place the position in the statement as it was sent.
	Line   int `json:"line"`
	Column int `json:"column"`
	// FileLine and FileColumn place it in the migration file. They are zero
	// unless the statement's text appears in the file exactly once, unchanged:
	// a statement a template rendered, a directive rewrote or a comment was
	// taken out of has no place in the file that can be named. A statement of
	// an Atlas txtar section has none either, since the section is not the
	// file.
	FileLine   int `json:"file_line,omitempty"`
	FileColumn int `json:"file_column,omitempty"`
}

// New reads the details of the statement or the commit err stopped at, and
// returns nil when it stopped at neither, as [migrator.FailureCauseOf] decides.
//
// A position is placed in the statement only when the writer sent the
// statement as the migrator reports it (see [serverdiag.Rewritten]) and the
// position falls inside it. A commit has no statement, so its details carry
// no file and no location.
func New(err error, opts Options) *Details {
	cause, ok := migrator.FailureCauseOf(err)
	if !ok {
		return nil
	}
	file, section := sourceFile(cause.SourcePath)
	details := &Details{
		Failure: cause.Failure,
		File:    file,
		Server:  serverdiag.Read(cause.Err, opts.Redact),
	}
	if source, renamed := opts.Renamed[details.File]; renamed {
		details.File = source
	}
	if details.Server != nil && !serverdiag.Rewritten(cause.Err) {
		mapped := details.File
		if section {
			mapped = ""
		}
		details.Location = Locate(cause.Statement, details.Server.Position, opts.Source, mapped)
	}
	return details
}

// sourceFile is the directory's file a migrator source path names, and
// whether the path names a section of an Atlas txtar file in it rather than
// the whole file.
func sourceFile(path string) (string, bool) {
	for _, section := range []string{migrationfile.AtlasTxtarMigrationSection, migrationfile.AtlasTxtarDownSection} {
		if file, ok := strings.CutSuffix(path, "#"+section); ok {
			return file, true
		}
	}
	return path, false
}

// Locate places a server's 1-based character position in statement and, when
// source holds file, in that file. It returns nil when position does not fall
// in statement, one past its last character included, which is where the
// server places an error at the end of the input. An empty statement, such as
// the commit's, holds no position.
//
// The file is read from source and searched for statement byte for byte. The
// file line and column are set only when statement appears there exactly once,
// because a second occurrence or a statement the migrator changed leaves no
// place in the file that is certainly the one the server read.
func Locate(statement string, position int, source fs.FS, file string) *Location {
	offset, ok := characterOffset(statement, position)
	if !ok {
		return nil
	}
	location := &Location{}
	location.Line, location.Column = lineColumn(statement, offset)
	if source == nil || file == "" {
		return location
	}
	content, err := fs.ReadFile(source, file)
	if err != nil {
		return location
	}
	text := string(content)
	if strings.Count(text, statement) != 1 {
		return location
	}
	location.FileLine, location.FileColumn = lineColumn(text, strings.Index(text, statement)+offset)
	return location
}

// characterOffset is the byte offset of the 1-based character position in
// text, where one past the last character is the length of text.
func characterOffset(text string, position int) (int, bool) {
	if position <= 0 || text == "" || !utf8.ValidString(text) {
		return 0, false
	}
	character := 1
	for offset := range text {
		if character == position {
			return offset, true
		}
		character++
	}
	return len(text), character == position
}

// lineColumn is the 1-based line and character column of the byte offset in
// text.
func lineColumn(text string, offset int) (line, column int) {
	before := text[:offset]
	lineStart := strings.LastIndexByte(before, '\n') + 1
	return strings.Count(before, "\n") + 1, utf8.RuneCountInString(before[lineStart:]) + 1
}

// Text is the human-readable form, printed after the error line. Its first
// line says that what follows may contain application data.
//
// A character that could change how a terminal shows the text, other than a
// newline or a tab, is written as an escape such as \x1b, so a value the
// server quotes cannot move the cursor, change colors or reorder what is
// shown. A newline continues the field on an indented line. The field as
// printed, escapes and indentation included, is then cut to
// [serverdiag.MaxFieldBytes] bytes between two characters or escapes, the
// bound the JSON form holds the raw field to, and a note says that a field was
// cut. The JSON form carries the fields as the server sent them.
func (d *Details) Text() string {
	var b strings.Builder
	b.WriteString("server details (may contain application data):\n")
	truncated := d.Server != nil && d.Server.Truncated
	field := func(name, value string) {
		if value == "" {
			return
		}
		escaped, cut := printable(value, serverdiag.MaxFieldBytes)
		truncated = truncated || cut
		fmt.Fprintf(&b, "  %s: %s\n", name, escaped)
	}
	field("failed at", d.failedAt())
	answer := d.Server
	if answer == nil {
		field("server", "no details reported")
		return strings.TrimSuffix(b.String(), "\n")
	}
	field("sqlstate", answer.SQLState)
	field("severity", answer.Severity)
	field("message", answer.Message)
	field("detail", answer.Detail)
	field("hint", answer.Hint)
	field("position", d.position())
	field("where", answer.Where)
	field("schema", answer.Schema)
	field("table", answer.Table)
	field("column", answer.Column)
	field("data type", answer.DataType)
	field("constraint", answer.Constraint)
	if truncated {
		field("note", fmt.Sprintf("a field longer than %d bytes is cut", serverdiag.MaxFieldBytes))
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// printable writes text as a field of the text form and cuts the result to at
// most limit bytes, between two characters or escapes, reporting whether it
// cut. A newline is followed by the indentation of a continuation line. Of the
// characters [escaped] selects, and of the bytes that are not UTF-8, one up to
// U+007F is written as \xHH, one up to U+FFFF as \uHHHH, and one above as
// \UHHHHHHHH.
func printable(text string, limit int) (string, bool) {
	var b strings.Builder
	for offset := 0; offset < len(text); {
		r, size := utf8.DecodeRuneInString(text[offset:])
		piece := text[offset : offset+size]
		switch {
		case r == '\n':
			piece = "\n    "
		case r == utf8.RuneError && size == 1:
			piece = fmt.Sprintf(`\x%02x`, text[offset])
		case !escaped(r):
		case r <= 0x7f:
			piece = fmt.Sprintf(`\x%02x`, r)
		case r <= 0xffff:
			piece = fmt.Sprintf(`\u%04x`, r)
		default:
			piece = fmt.Sprintf(`\U%08x`, r)
		}
		if b.Len()+len(piece) > limit {
			return b.String(), true
		}
		b.WriteString(piece)
		offset += size
	}
	return b.String(), false
}

// escaped reports whether the text form writes r as an escape: every control
// character but a newline and a tab, every format character (Unicode category
// Cf, such as the bidirectional overrides and the zero-width characters), and
// the line and paragraph separators.
func escaped(r rune) bool {
	if r == '\n' || r == '\t' {
		return false
	}
	return unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == '\u2028' || r == '\u2029'
}

// failedAt names the statement or the commit.
func (d *Details) failedAt() string {
	migration := d.Failure.VersionKey
	if migration == "" {
		migration = strconv.FormatInt(d.Failure.Version, 10)
	}
	if d.Failure.Commit {
		return "the commit of the transaction that ran migration " + migration
	}
	at := fmt.Sprintf("statement %d of %d of migration %s", d.Failure.Statement, d.Failure.Statements, migration)
	if d.File != "" {
		at += " (" + d.File + ")"
	}
	return at
}

// position is the server's position with where it falls, when it gave one.
func (d *Details) position() string {
	if d.Server.Position == 0 {
		return ""
	}
	position := strconv.Itoa(d.Server.Position)
	if d.Location == nil {
		return position
	}
	position += fmt.Sprintf(" (statement line %d, column %d", d.Location.Line, d.Location.Column)
	if d.Location.FileLine > 0 {
		position += fmt.Sprintf("; %s line %d, column %d", d.File, d.Location.FileLine, d.Location.FileColumn)
	}
	return position + ")"
}
