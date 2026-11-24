package normalize

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/descriptortable"
)

// QuotingOf returns where quoted text ends on dialect, as its descriptor
// states it ([QuotingFromDescriptor]), and the SQL standard, the zero
// Quoting, for a dialect the registry does not know.
func QuotingOf(dialect string) Quoting {
	return quotings.Get(dialect)
}

// quotings holds the answer of [QuotingFromDescriptor] for every dialect, so
// a comparison that asks per expression does not copy a descriptor each time.
var quotings = descriptortable.New(QuotingFromDescriptor)

// QuotingFromDescriptor returns the Quoting a dialect descriptor states: the
// backslash rules of its Lexical section, and a double-quoted token read as a
// string, or a quoted name that keeps its case, where its Comparison section
// says the server reads one so.
func QuotingFromDescriptor(described descriptor.Dialect) Quoting {
	quoting := Quoting{
		BackslashEscapes:           described.Lexical.BackslashEscapes,
		IdentifierBackslashEscapes: described.Lexical.IdentifierBackslashEscapes,
		EscapeStrings:              described.Lexical.EscapeStrings,
		QuotedNamesKeepCase:        described.Comparison.QuotedNamesKeepCase,
	}
	if described.Comparison.DoubleQuotedStrings {
		quoting.Strings = `'"`
	}
	return quoting
}
