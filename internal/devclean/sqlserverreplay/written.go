package sqlserverreplay

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlserverbatch"
	"ptah.run/internal/sqlserverexec"
)

// renameShape is the name sp_rename may be given for a type the guard
// accepts.
type renameShape struct {
	// reach is the longest name. One part more can name a database: sp_rename
	// refuses a database other than the one it runs in (Msg 15333), but the
	// guard refuses such a name before the server reads it, the dev database's
	// own name too, since the renderer never writes one. Measured on SQL
	// Server 2022 and 2025.
	reach int
	// ofTable is true where the last part names a column or an index of the
	// table before it, rather than the object itself.
	ofTable bool
}

// renameShapes holds the types the guard accepts. DATABASE renames a whole
// database, any database on the server; USERDATATYPE and STATISTICS rename an
// item of the dev database, and are refused because the renderer never writes
// them.
var renameShapes = map[sqlserverexec.RenameType]renameShape{
	sqlserverexec.RenameUntyped: {reach: 2},
	sqlserverexec.RenameObject:  {reach: 2},
	sqlserverexec.RenameColumn:  {reach: 3, ofTable: true},
	sqlserverexec.RenameIndex:   {reach: 3, ofTable: true},
}

// defaultTableReach is the longest name of the table whose default a default
// batch drops or replaces.
const defaultTableReach = 2

// validateWrittenStatement answers a statement the SQL Server
// renderer writes where T-SQL has no DDL for the change: an sp_rename call,
// and the batches that drop or replace a column's default, whose name the
// server gave it. Each is an EXEC, which [guard.validateReplayStatement]
// refuses because it can run anything. These do only what their shape says,
// and internal/sqlserverexec, which writes the shape, reads it back, so a
// statement in any other shape is not one of them: written is false, and the
// caller judges the statement as it judges every EXEC.
//
// The bare procedure name is part of the shape. A procedure named through a
// database, as in otherdb..sp_rename or master.sys.sp_rename, runs in that
// database and renames its objects there. Measured on SQL Server 2022 and
// 2025.
func (g guard) validateWrittenStatement(stmt string, scope replay.Scope) (written bool, err error) {
	if rename, ok := sqlserverexec.ReadRename(stmt); ok {
		return true, validateSQLServerRename(rename)
	}
	if drop, ok := sqlserverexec.ReadDefaultDrop(stmt); ok {
		return true, validateSQLServerDefaultTable(drop.Table)
	}
	if replace, ok := sqlserverexec.ReadDefaultReplace(stmt); ok {
		return true, g.validateDefaultReplace(replace, scope)
	}
	return false, nil
}

// validateSQLServerRename refuses an sp_rename whose type the guard does not
// accept, whose name is longer than that type's reach, or which names a
// temporary table, or renames a table to one.
func validateSQLServerRename(rename sqlserverexec.Rename) error {
	shape, accepted := renameShapes[rename.Type]
	if !accepted {
		return replay.Refuse(platform.SQLServer, "sp_rename @objtype "+string(rename.Type))
	}
	objects := rename.Name
	if shape.ofTable && len(objects) > 0 {
		objects = objects[:len(objects)-1]
	}
	if temporaryName(objects) || !shape.ofTable && temporaryName([]string{rename.NewName}) {
		return replay.Refuse(platform.SQLServer, "temporary object")
	}
	return validateSQLServerConfinedName(rename.Name, shape.reach)
}

// validateSQLServerDefaultTable refuses a default batch on a table outside
// the dev database.
func validateSQLServerDefaultTable(table []string) error {
	if temporaryName(table) {
		return replay.Refuse(platform.SQLServer, "temporary object")
	}
	return validateSQLServerConfinedName(table, defaultTableReach)
}

// validateDefaultReplace refuses the batch that replaces a default
// on a table outside the dev database, or whose new default is not one
// expression. The batch then runs one ALTER TABLE ... ADD DEFAULT ... FOR,
// which is judged as the same statement written on its own in a migration.
//
// T-SQL needs no separator between statements, so the text between ADD
// DEFAULT and the FOR that ends the batch could end the ALTER TABLE and start
// another statement. It cannot do so without a FOR of its own, outside
// parentheses: ALTER TABLE ... ADD DEFAULT is incomplete without one, and the
// server compiles the whole batch before it runs any of it, so a batch that
// does not parse runs nothing. NEXT VALUE FOR, which reads a sequence, is the
// one FOR an expression holds. internal/sqlserverbatch reads the ALTER TABLE
// too, and it has to be one statement there as well.
func (g guard) validateDefaultReplace(replace sqlserverexec.DefaultReplace, scope replay.Scope) error {
	if err := validateSQLServerDefaultTable(replace.Table); err != nil {
		return err
	}
	add := sqlserverexec.AddDefault(replace.Table, replace.Column, replace.Expression)
	statements, err := sqlserverbatch.Read(add)
	if !oneSQLServerExpression(replace.Expression) || err != nil || len(statements) != 1 ||
		statements[0].Kind != sqlserverbatch.Simple || statements[0].SQL != add {
		return replay.Refuse(platform.SQLServer, fmt.Sprintf("default expression %q", replace.Expression))
	}
	tokens, err := sqlServerTokens(add)
	if err != nil {
		return unreadableRefusal(err)
	}
	return g.validateReplayStatement(tokens, scope)
}

// oneSQLServerExpression reports whether text is one expression to the end of
// an ADD DEFAULT: no semicolon, parentheses that close in order, and no FOR
// outside them but the one NEXT VALUE FOR writes.
func oneSQLServerExpression(text string) bool {
	lex := lexer.NewLexerWithOptions(text, lexerOptions())
	var tokens []lexer.Token
	for token := lex.NextToken(); token.Type != lexer.TokenEOF; token = lex.NextToken() {
		switch token.Type {
		case lexer.TokenSemicolon:
			return false
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		default:
			tokens = append(tokens, token)
		}
	}
	depth := 0
	for index, token := range tokens {
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
			if depth < 0 {
				return false
			}
		case depth == 0 && replay.NormalizedIdentifier(token) == "FOR" && !nextValueFor(tokens, index):
			return false
		}
	}
	return depth == 0
}

// nextValueFor reports whether the FOR at index of tokens ends NEXT VALUE FOR.
func nextValueFor(tokens []lexer.Token, index int) bool {
	return index >= 2 && replay.NormalizedIdentifier(tokens[index-2]) == "NEXT" &&
		replay.NormalizedIdentifier(tokens[index-1]) == "VALUE"
}

// temporaryName reports a part that starts with #, which names a temporary
// object in tempdb whatever schema is written in front of it. A column or an
// index may have such a name, so the caller passes only the parts that name a
// table or its schema.
func temporaryName(parts []string) bool {
	return slices.ContainsFunc(parts, func(part string) bool { return strings.HasPrefix(part, "#") })
}

// validateSQLServerConfinedName refuses a name of more than reach parts, the
// first of which can name another database.
func validateSQLServerConfinedName(name []string, reach int) error {
	if len(name) <= reach {
		return nil
	}
	return replay.Refuse(platform.SQLServer, fmt.Sprintf("cross-database target %q", strings.Join(name, ".")))
}
