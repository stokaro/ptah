// Package sqlident quotes SQL identifiers (schema, table, and column names) for
// a given SQL dialect. It centralizes the per-dialect quote style and the
// standard "double the quote character" escaping so that identifiers taken from
// catalog metadata, annotations, or any other untrusted-shaped string cannot
// terminate the quoted identifier and inject SQL. It also reads back the names
// of a dialect that writes them its own way, as YDB writes one path in YQL
// backticks, so a SQL reader asks it rather than naming the dialect itself.
package sqlident

import (
	"strings"
	"sync"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/sqlutil"
)

// spelling is how one dialect writes an identifier, read once from its
// descriptor's Quoting.
type spelling struct {
	style    descriptor.QuoteStyle
	bare     descriptor.BareNames
	keywords map[string]bool
	paths    bool
}

// spellings caches the spelling of each dialect name a caller has asked
// about. Quoting runs for every identifier a statement names, and a lookup
// copies the whole descriptor, so the answer is read once per name. The cache
// is safe because a registered spelling keeps the descriptor it was first
// given for the life of the process; a name no descriptor carries is not
// cached, since a later platform.Register may describe it.
var spellings sync.Map

// spellingOf returns how dialect writes an identifier. A dialect no
// descriptor describes, the empty name among them, writes the SQL standard's
// spelling: double quotes, and a plain name bare only where one is recorded
// bare.
func spellingOf(dialect string) spelling {
	if cached, ok := spellings.Load(dialect); ok {
		return cached.(spelling)
	}
	described, found := platform.Lookup(dialect)
	if !found {
		return spelling{}
	}
	quoting := described.Quoting
	written := spelling{style: quoting.Style, bare: quoting.Bare, paths: quoting.Paths}
	if len(quoting.Keywords) > 0 {
		written.keywords = make(map[string]bool, len(quoting.Keywords))
		for _, keyword := range quoting.Keywords {
			written.keywords[strings.ToUpper(keyword)] = true
		}
	}
	spellings.Store(dialect, written)
	return written
}

// Quote returns name as a safely-quoted identifier for dialect. The quote
// style is the dialect descriptor's: backticks for MySQL, MariaDB, ClickHouse
// and YDB; square brackets for SQL Server; and double quotes for the
// PostgreSQL family, SQLite, Oracle and any unrecognized dialect. Embedded
// quote characters are doubled per the SQL standard so the value cannot
// terminate the quoted identifier, except on YDB, where a backslash escapes
// them; see [sqlutil.QuoteYQLIdentifier]. The dialect is resolved through the
// dialect registry, so every documented spelling of an engine (`mssql`,
// `tsql`, `sql-server`, `sql_server`; `ch`) picks the same quote style as its
// canonical name. name itself is quoted verbatim (it is not trimmed).
func Quote(dialect, name string) string {
	switch spellingOf(dialect).style {
	case descriptor.QuoteYQL:
		return sqlutil.QuoteYQLIdentifier(name)
	case descriptor.QuoteBacktick:
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	case descriptor.QuoteBracket:
		return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
	default:
		return quoteDouble(name)
	}
}

// quoteDouble quotes name the SQL standard's way: in double quotes, with each
// quote inside doubled.
func quoteDouble(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// Ident returns the spelling that refers to name in dialect.
//
// For every dialect but Oracle this is [Quote]: a quoted name is the same name
// in the catalog, so quoting always is both safe and unambiguous.
//
// Oracle is the exception, because there quoting CHANGES which object is named.
// An unquoted identifier folds to upper case and a quoted one does not, and
// measured on 23.26 the two coexist: `CREATE TABLE ptah_fold (...)` and
// `CREATE TABLE "ptah_fold" (...)` produce two tables, reported by user_tables
// as PTAH_FOLD and ptah_fold. So the choice has to be the SAME one everywhere a
// name is written -- in the DDL that creates a table and in the query that
// reads it. When they disagreed, the Oracle DDL renderer wrote `ora_posts` and
// the query builder wrote `"ora_posts"`, which is a table nobody created.
//
// See the Oracle renderer's escapeIdentifier for why bare is the side both
// chose: a CHECK or a generated expression is author text this repository does
// not rewrite, and only the bare form agrees with it.
func Ident(dialect, name string) string {
	if spellingOf(dialect).bare == descriptor.BareEverywhere {
		return BareOrQuoted(dialect, name)
	}
	return Quote(dialect, name)
}

// BareOrQuoted returns name unquoted when it is a plain, nonreserved identifier
// for dialect, and dialect-quoted otherwise.
//
// It exists for the positions where an engine records an identifier without
// quotes and reproducing that spelling matters: SQLite writes
// `USING fts5` rather than `USING "fts5"`, and a module named `fts-5` writes
// `USING "fts-5"`. Two callers have to agree about which form a render
// produces -- the renderer that writes the statement and the inspection check
// that looks for it -- and when they disagreed, a SQL document that carried the
// declaration perfectly was reported as lossy and refused under strict
// compatibility. See stokaro/ptah#1028.
//
// "Plain" is the conservative intersection: an ASCII letter or underscore
// followed by ASCII letters, digits or underscores. Anything else, including
// the empty string, is quoted. SQLite keywords are also quoted because their
// bare forms are parsed as syntax in the virtual-table module position.
//
// A dialect whose descriptor writes no name bare, as YDB's does, is always
// quoted. A bare and a quoted YQL name are the same name, since YQL compares
// names case-sensitively either way, so nothing is gained by the bare form,
// and a keyword list would only have to be kept up to date.
func BareOrQuoted(dialect, name string) string {
	written := spellingOf(dialect)
	if written.bare == descriptor.BareNever {
		return Quote(dialect, name)
	}
	if !isPlainIdentifier(name) || written.keywords[strings.ToUpper(name)] {
		return Quote(dialect, name)
	}
	return name
}

func isPlainIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for index := 0; index < len(name); index++ {
		char := name[index]
		switch {
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char == '_':
			continue
		case char >= '0' && char <= '9' && index > 0:
			continue
		}
		return false
	}
	return true
}

// QualifiedIdent returns the spelling that refers to name in dialect, qualified
// by schema when schema is not blank. Each part is spelled by [Ident], so a
// statement that names a table this way names the table the DDL renderers
// created: on Oracle `ora_flags` stays bare and folds to ORA_FLAGS, where
// [Qualified] would write `"ora_flags"`, a different table. On YDB the result
// is one path, as [Qualified] writes it.
func QualifiedIdent(dialect, schema, name string) string {
	if strings.TrimSpace(schema) == "" {
		return Ident(dialect, name)
	}
	if NamesArePaths(dialect) {
		return Qualified(dialect, schema, name)
	}
	return Ident(dialect, schema) + "." + Ident(dialect, name)
}

// Qualified returns a dialect-quoted identifier optionally qualified by schema,
// as in "schema"."name". A whitespace-only schema yields just the quoted name.
// Otherwise, schema and name are quoted verbatim.
//
// YDB has no schemas: a table is a path, and Ptah's schema is the directory
// that holds it (stokaro/ptah#4015, decision 2). So a qualified YDB name is
// one quoted path, `dir/sub/t`, and never `dir`.`t`, which YQL reads as a
// cluster and a table: measured on YDB 26.2.1.14, SELECT * FROM `dir`.`t`
// answers `Unknown cluster: dir` while `dir/t` reads the table. The directory
// is joined with one slash; a trailing slash on it names the same directory
// and is dropped.
func Qualified(dialect, schema, name string) string {
	if strings.TrimSpace(schema) == "" {
		return Quote(dialect, name)
	}
	if NamesArePaths(dialect) {
		return Quote(dialect, strings.TrimRight(schema, "/")+"/"+name)
	}
	return Quote(dialect, schema) + "." + Quote(dialect, name)
}

// SplitQualified splits a dotted name whose parts may be double-quoted, as the
// PostgreSQL family writes one, into its parts. It reads no other quote style:
// a backticked part is split at its dots. A dot inside a quoted part is
// part of the name, and a doubled quote inside a quoted part is SQL's escape for
// a literal quote and does not end the part. Each part keeps its quotes.
//
// Each part is a SLICE of the input, never a character-by-character copy. The
// two delimiters this scan recognizes are ASCII, and UTF-8 is self
// synchronizing -- no byte of a multi-byte sequence is ever below 0x80 -- so a
// byte scan can find them without decoding, and slicing hands every other byte
// back exactly as it arrived. A form that accumulated `string(character)` from
// a byte re-encoded each byte as its own code point: `Ä` (C3 84) came back out
// as `Ã` plus U+0084, renaming every non-ASCII object (stokaro/ptah#1352).
//
// Decoding to runes would fix that case and introduce another: text that is not
// valid UTF-8 -- a Latin-1 schema file, say -- decodes to U+FFFD per bad byte
// and would be rewritten just as silently. A splitter owes its caller the bytes
// it was given.
func SplitQualified(name string) []string {
	var parts []string
	start := 0
	inQuotes := false
	for i := 0; i < len(name); i++ {
		switch {
		case name[i] == '"' && inQuotes && i+1 < len(name) && name[i+1] == '"':
			i++
		case name[i] == '"':
			inQuotes = !inQuotes
		case name[i] == '.' && !inQuotes:
			parts = append(parts, name[start:i])
			start = i + 1
		}
	}
	return append(parts, name[start:])
}

// UnquoteDoubleQuoted removes one level of double quoting from one name part,
// as the PostgreSQL family quotes one, and undoubles the quotes inside it. A
// part that is not wrapped in double quotes is returned as it is.
func UnquoteDoubleQuoted(part string) string {
	if len(part) >= 2 && part[0] == '"' && part[len(part)-1] == '"' {
		return strings.ReplaceAll(part[1:len(part)-1], `""`, `"`)
	}
	return part
}

// QuotePostgresQualified quotes every part of a dotted PostgreSQL-family name,
// split by [SplitQualified], in double quotes. Each part loses one level of
// double quoting first, so a part that arrives quoted is not quoted twice. It
// is the PostgreSQL family's spelling alone: another dialect quotes and
// qualifies differently, and a YDB name is one path.
func QuotePostgresQualified(name string) string {
	parts := SplitQualified(name)
	for i, part := range parts {
		parts[i] = quoteDouble(UnquoteDoubleQuoted(part))
	}
	return strings.Join(parts, ".")
}

// StringLiteral returns value as a SQL string literal: single quotes around
// it, and each single quote inside doubled, which is the standard's escape.
func StringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
