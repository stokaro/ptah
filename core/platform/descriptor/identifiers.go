package descriptor

import (
	"fmt"
	"slices"
)

// Quoting states how an engine writes an identifier. The zero value is the
// SQL standard's spelling: double quotes with a doubled quote inside, a plain
// name written bare only where the engine records it bare, no keywords to
// avoid there, and a qualified name written as dotted parts.
type Quoting struct {
	// Style is the quotes an identifier is written in.
	Style QuoteStyle `json:"style,omitempty"`
	// Bare is where a plain, nonreserved name is written without quotes.
	Bare BareNames `json:"bare,omitempty"`
	// Keywords are the words the engine parses as syntax where an identifier
	// stands, compared without regard to case. A name equal to one is quoted
	// wherever a plain name would be written bare.
	Keywords []string `json:"keywords,omitempty"`
	// Paths reports that a qualified name is one path, `dir/sub/name`, inside
	// one pair of quotes, rather than dot-separated parts: the engine has no
	// schemas, and Ptah's schema is the directory that holds the object.
	Paths bool `json:"paths,omitempty"`
}

// QuoteStyle is the quotes an engine writes an identifier in.
type QuoteStyle string

const (
	// QuoteDouble is the SQL standard's: "name", with a quote inside doubled.
	// It is what an empty style means.
	QuoteDouble QuoteStyle = "double"
	// QuoteBacktick is `name`, with a backtick inside doubled.
	QuoteBacktick QuoteStyle = "backtick"
	// QuoteBracket is [name], with a closing bracket inside doubled.
	QuoteBracket QuoteStyle = "bracket"
	// QuoteYQL is YQL's `name`, with a backtick or a backslash inside escaped
	// by a backslash.
	QuoteYQL QuoteStyle = "yql"
)

// QuoteStyles returns every declared quote style.
func QuoteStyles() []QuoteStyle {
	return []QuoteStyle{QuoteDouble, QuoteBacktick, QuoteBracket, QuoteYQL}
}

// BareNames is where an engine's plain, nonreserved names are written without
// quotes.
type BareNames string

const (
	// BareWhereRecorded writes a plain name bare only in the positions where
	// the engine records it bare, such as a SQLite virtual-table module, and
	// quotes it everywhere else, since a quoted name is the same name there.
	// It is what an empty value means.
	BareWhereRecorded BareNames = "where-recorded"
	// BareEverywhere writes a plain name bare in every position, for an
	// engine where quoting changes which object is named: Oracle folds a bare
	// name to upper case and keeps a quoted one as written.
	BareEverywhere BareNames = "everywhere"
	// BareNever quotes every name, for an engine where a bare name gains
	// nothing and a keyword list would only have to be kept up to date.
	BareNever BareNames = "never"
)

// BareNamesValues returns every declared value of [BareNames].
func BareNamesValues() []BareNames {
	return []BareNames{BareWhereRecorded, BareEverywhere, BareNever}
}

// ColumnKeyNaming is how a server names the key a column's own UNIQUE builds.
type ColumnKeyNaming string

const (
	// ColumnKeyAfterColumn names the key after the column, with `_2` and on
	// where another index of the table holds the name, and compares index
	// names without regard to case: the MySQL family's naming.
	ColumnKeyAfterColumn ColumnKeyNaming = "after-column"
	// ColumnKeyTableColumnKey names the key `<table>_<column>_key`, with `1`
	// and on where a relation or a constraint of the schema holds the name:
	// PostgreSQL's naming.
	ColumnKeyTableColumnKey ColumnKeyNaming = "table-column-key"
)

// ColumnKeyNamings returns every declared value of [ColumnKeyNaming].
func ColumnKeyNamings() []ColumnKeyNaming {
	return []ColumnKeyNaming{ColumnKeyAfterColumn, ColumnKeyTableColumnKey}
}

// validateIdentifiers checks the enumerated values of the identifier fields.
// Each may be empty, which means the default its type documents.
func (d Dialect) validateIdentifiers() error {
	if style := d.Quoting.Style; style != "" && !slices.Contains(QuoteStyles(), style) {
		return fmt.Errorf("%w: dialect %q: quote style %q is not one of %q", ErrInvalidDescriptor, d.Name, style, QuoteStyles())
	}
	if bare := d.Quoting.Bare; bare != "" && !slices.Contains(BareNamesValues(), bare) {
		return fmt.Errorf("%w: dialect %q: bare names %q is not one of %q", ErrInvalidDescriptor, d.Name, bare, BareNamesValues())
	}
	if slices.Contains(d.Quoting.Keywords, "") {
		return fmt.Errorf("%w: dialect %q: an empty keyword", ErrInvalidDescriptor, d.Name)
	}
	if naming := d.ColumnKeys; naming != "" && !slices.Contains(ColumnKeyNamings(), naming) {
		return fmt.Errorf("%w: dialect %q: column key naming %q is not one of %q", ErrInvalidDescriptor, d.Name, naming, ColumnKeyNamings())
	}
	return nil
}
