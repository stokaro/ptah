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
	// viewSQLSecurity reports that CREATE VIEW accepts a SQL SECURITY mode.
	viewSQLSecurity bool
	// family is the descriptor's Family, whose head's string rules a routine
	// of the quoted-body grammar is read with.
	family string
	// defaultSchema is the descriptor's DefaultSchema, the schema an
	// unqualified table name resolves to.
	defaultSchema string
	// batches reports the descriptor's Scripts.BatchSeparator: a client sends a
	// batch of statements, and a statement needs no semicolon after it.
	batches bool
	// ownedSequences is the descriptor's Conversion.OwnedSequences: a column
	// owns a sequence, so ALTER SEQUENCE ... OWNED BY names one.
	ownedSequences bool
	// identityColumns is the descriptor's Comparison.IdentityColumns: ALTER
	// COLUMN adds an identity, so ALTER COLUMN ... ADD GENERATED is read.
	identityColumns bool
	// collations is the descriptor's Columns.Collations: a column's COLLATE
	// names a collation the renderer writes and the comparison reads, so the
	// name is read into the one spelling both use.
	collations bool
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
		viewSQLSecurity:    described.Comparison.ViewSQLSecurity,
		family:             described.Family,
		defaultSchema:      described.DefaultSchema,
		batches:            described.Scripts.BatchSeparator != "",
		ownedSequences:     described.Conversion.OwnedSequences,
		identityColumns:    described.Comparison.IdentityColumns,
		collations:         described.Columns.Collations,
	}
})

// providerEngine reports a document read for a named dialect no descriptor
// describes: an engine an executable provider serves.
func (p *Parser) providerEngine() bool {
	return p.dialect != "" && !p.target.known
}

// grammarOf returns what the descriptor of dialect says about its DDL
// grammar, and the zero value for a dialect no descriptor describes, the
// empty name among them.
func grammarOf(dialect string) targetGrammar {
	return targetGrammars.Get(dialect)
}
