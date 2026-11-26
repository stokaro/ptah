package sqlserverexec

import (
	"strings"

	"ptah.run/internal/lexer"
)

// RenameType is what sp_rename renames, as its @objtype argument names it.
type RenameType string

const (
	// RenameUntyped passes no @objtype, and sp_rename looks the name up as a
	// user data type, a column, an index and an object, in that order.
	RenameUntyped RenameType = ""
	// RenameColumn renames a column, named table.column or
	// schema.table.column.
	RenameColumn RenameType = "COLUMN"
	// RenameIndex renames an index, named table.index or schema.table.index.
	RenameIndex RenameType = "INDEX"
	// RenameObject renames an item sys.objects lists, such as a table, a view
	// or a constraint, named object or schema.object.
	RenameObject RenameType = "OBJECT"
)

// Rename is one call of sp_rename.
type Rename struct {
	// Name is the item to rename, as the parts of its name without quotes.
	Name []string
	// NewName is the new name, without quotes. sp_rename takes it as one
	// part, literally: a dot in it is a character of the name, so a rename
	// never moves an item to another schema.
	NewName string
	// Type is the @objtype argument.
	Type RenameType
}

// Statement renders r as the renderer writes it: EXEC sp_rename, the name with
// each part in brackets, the new name and the type, each as an N'...' literal,
// and no type argument for [RenameUntyped]. The literal keeps a name outside
// the database's code page, which a plain '...' literal turns into question
// marks.
func (r Rename) Statement() string {
	arguments := []string{UnicodeLiteral(QuoteQualifiedName(r.Name)), UnicodeLiteral(r.NewName)}
	if r.Type != RenameUntyped {
		arguments = append(arguments, UnicodeLiteral(string(r.Type)))
	}
	return "EXEC sp_rename " + strings.Join(arguments, ", ") + ";"
}

// ReadRename reads stmt back into the Rename whose [Rename.Statement] it is,
// with the type as stmt spells it. It reports false for every other
// statement: another procedure, or this one called under another name such as
// sys.sp_rename; an argument that is a variable, an expression or a named
// argument; a plain '...' literal; and a name in a quoting Statement does not
// write.
func ReadRename(stmt string) (Rename, bool) {
	read := tokens(stmt)
	if len(read) < 2 || read[0].Value != "EXEC" || read[0].Type != lexer.TokenIdentifier ||
		read[1].Value != "sp_rename" || read[1].Type != lexer.TokenIdentifier {
		return Rename{}, false
	}
	var arguments []string
	for index := 2; index < len(read); {
		value, next, ok := unicodeLiteralAt(read, index)
		if !ok {
			return Rename{}, false
		}
		arguments = append(arguments, value)
		if next < len(read) && !read[next].MatchOperatorValue(",") {
			return Rename{}, false
		}
		index = next + 1
	}
	if len(arguments) < 2 || len(arguments) > 3 {
		return Rename{}, false
	}
	name, ok := parseQualifiedName(arguments[0])
	if !ok {
		return Rename{}, false
	}
	rename := Rename{Name: name, NewName: arguments[1]}
	if len(arguments) == 3 {
		rename.Type = RenameType(arguments[2])
	}
	if !sameStatement(stmt, rename.Statement()) {
		return Rename{}, false
	}
	return rename, true
}
