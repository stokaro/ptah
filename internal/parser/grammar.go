package parser

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/descriptortable"
)

// targetGrammar is what the descriptor of the dialect a document is read as
// says about the DDL grammar this parser reads.
//
// known is false for a document read with no dialect and for a dialect name
// no descriptor carries, and the grammar is then the zero value. A rule that
// reads both permissively, taking the spelling of any engine, asks known; a
// rule that reads only a document of no dialect so asks for the empty name,
// and an unknown name gets the zero grammar's answer.
type targetGrammar struct {
	known   bool
	grammar descriptor.Grammar
	// nullableTypes is the descriptor's Comparison.NullableTypes: a bare type
	// is NOT NULL, and nullability is spelled in the type.
	nullableTypes bool
	// checkOwnColumnOnly is the descriptor's Columns.CheckOwnColumnOnly: a
	// CHECK written on a column may name that column alone.
	checkOwnColumnOnly bool
}

// targetGrammars derives each dialect's grammar once per registry. A parser is
// made for every document and for every statement an owner declines, and a
// lookup copies the whole descriptor.
var targetGrammars = descriptortable.New(func(described descriptor.Dialect) targetGrammar {
	return targetGrammar{
		known:              described.Name != "",
		grammar:            described.Grammar,
		nullableTypes:      described.Comparison.NullableTypes,
		checkOwnColumnOnly: described.Columns.CheckOwnColumnOnly,
	}
})

// grammarOf returns what the descriptor of dialect says about its DDL
// grammar, and the zero value for a dialect no descriptor describes, the
// empty name among them.
func grammarOf(dialect string) targetGrammar {
	return targetGrammars.Get(dialect)
}
