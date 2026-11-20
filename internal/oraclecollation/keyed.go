package oraclecollation

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// RefuseKeyedColumnChange refuses a diff that changes the collation of a
// column Oracle will not change it on, before any statement of the plan runs.
//
// Oracle commits each DDL statement on its own, so a plan that stops at such a
// column leaves the statements before it applied. Measured on Oracle Free
// 23.26 with MAX_STRING_SIZE=EXTENDED, `ALTER TABLE t MODIFY (c COLLATE x)`
// answers:
//
//	ORA-43923  the column is in an index, a primary key or a unique key
//	ORA-43922  the column is in a foreign key, a CHECK, or the expression of a
//	           generated column
//
// A plan removes an index only after the column changes, so a holder the same
// diff removes still stands when the change runs and is refused as well, with
// a message that says to apply the drop first. A plan also adds columns before
// it changes them, so a generated column or an inline CHECK the same diff adds
// holds the column when the change runs, and is refused the same way. Indexes,
// CHECKs and foreign keys added as constraints come after the change and hold
// nothing yet.
//
// The observation of the table before the change names the holders. A key
// Oracle named itself is not among its constraints: the reader carries it on the
// column's IsPrimaryKey and IsUnique flags, so the column's own flags are read
// too. A function-based index holds the column when an expression of its key
// names it.
func RefuseKeyedColumnChange(diff *difftypes.SchemaDiff) error {
	if diff == nil {
		return nil
	}
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsModified {
			change, changed := column.Changes["collation"]
			if !changed {
				continue
			}
			held := holderOf(&table, column.ColumnName)
			if held.name == "" {
				continue
			}
			transition := transitionOf(&table, column.ColumnName, change)
			switch {
			case held.planned:
				return fmt.Errorf("%w: oracle: the diff changes the collation of column %q of table %q (%s), and Oracle "+
					"refuses that while %s uses the column (ORA-43923 or ORA-43922); a plan adds columns before it changes "+
					"them, so apply the collation change on its own first and add it in a later migration",
					ptaherr.ErrUnsupportedFeature, column.ColumnName, table.TableName, transition, held.name)
			case dropsIndex(diff, table.TableName, held.index):
				return fmt.Errorf("%w: oracle: the diff changes the collation of column %q of table %q (%s), and Oracle "+
					"refuses that while %s uses the column (ORA-43923 or ORA-43922); the same diff drops the index, but a plan "+
					"changes columns before it drops indexes, so apply the index drop on its own first and the collation change after",
					ptaherr.ErrUnsupportedFeature, column.ColumnName, table.TableName, transition, held.name)
			default:
				return fmt.Errorf("%w: oracle: the diff changes the collation of column %q of table %q (%s), and Oracle "+
					"refuses that while %s uses the column (ORA-43923 or ORA-43922); drop it, apply the collation change, "+
					"and declare it again in a later migration",
					ptaherr.ErrUnsupportedFeature, column.ColumnName, table.TableName, transition, held.name)
			}
		}
	}
	return nil
}

// holder is an object that keeps a column from changing its collation.
type holder struct {
	// name says what it is, for a message: `index "T_IDX"`.
	name string
	// index is the name of the object when it is an index, and empty otherwise.
	index string
	// planned marks an object the same diff adds ahead of the change, which
	// the table does not hold yet.
	planned bool
}

// transitionOf spells the collation change for a message as `from -> to`.
// The comparison reports an inherited collation as an empty side, which would
// print as `( -> BINARY_CI)`, so each empty side reads as the collation Oracle
// gives a character column that names none. The collation the column holds is
// the one the read reported, which is the table's default where the column takes
// it.
func transitionOf(table *difftypes.TableDiff, column, change string) string {
	_, after, _ := strings.Cut(change, "->")
	before := ""
	for _, observed := range table.Current.Table.Columns {
		if strings.EqualFold(observed.Name, column) {
			before = cmp.Or(observed.EffectiveCollate, observed.Collate)
		}
	}
	return cmp.Or(before, Inherited) + " -> " + cmp.Or(strings.TrimSpace(after), Inherited)
}

// holderOf names the first object that keeps column from changing its
// collation, or returns a holder with no name when none does.
func holderOf(table *difftypes.TableDiff, column string) holder {
	current := table.Current
	for _, candidate := range current.Indexes {
		if named(candidate.Columns, column) || expressionPartNames(candidate, column) {
			return holder{name: fmt.Sprintf("index %q", candidate.Name), index: candidate.Name}
		}
	}
	for _, constraint := range current.Constraints {
		if named(constraint.ColumnNames, column) || named([]string{constraint.ColumnName}, column) {
			return holder{name: fmt.Sprintf("%s constraint %q", strings.ToLower(constraint.Type), constraint.Name)}
		}
	}
	for _, other := range current.Table.Columns {
		if strings.EqualFold(other.Name, column) {
			// The key Oracle named itself is carried here and not among the
			// constraints (see the reader's withoutGeneratedKeys).
			switch {
			case other.IsPrimaryKey:
				return holder{name: "the primary key"}
			case other.IsUnique:
				return holder{name: "a unique key"}
			}
		}
		if other.GeneratedExpression != nil && expressionNames(*other.GeneratedExpression, column) {
			return holder{name: fmt.Sprintf("the generated column %q", other.Name)}
		}
	}
	for _, added := range table.ColumnsAdded {
		switch {
		case added.GeneratedExpression != "" && expressionNames(added.GeneratedExpression, column):
			return holder{name: fmt.Sprintf("the generated column %q the same diff adds", added.Name), planned: true}
		case added.Check != "" && expressionNames(added.Check, column):
			return holder{name: fmt.Sprintf("the CHECK of the column %q the same diff adds", added.Name), planned: true}
		}
	}
	return holder{}
}

// dropsIndex reports whether the diff drops the named index of the table.
func dropsIndex(diff *difftypes.SchemaDiff, table, index string) bool {
	return index != "" && slices.ContainsFunc(diff.IndexesRemoved, func(ref difftypes.IndexRef) bool {
		return strings.EqualFold(ref.Name, index) && strings.EqualFold(ref.TableName, table)
	})
}

// expressionPartNames reports whether a key part of the index is an expression
// that names column. A function-based index, such as one over UPPER("C"),
// keeps ORA-43922 from changing the collation of C.
func expressionPartNames(index catalog.Index, column string) bool {
	return slices.ContainsFunc(index.Parts, func(part catalog.IndexPart) bool {
		return part.Expr != "" && expressionNames(part.Expr, column)
	})
}

func named(columns []string, column string) bool {
	return slices.ContainsFunc(columns, func(candidate string) bool { return strings.EqualFold(candidate, column) })
}

// expressionNames reports whether expression names column as an identifier,
// read with the Oracle lexer: a string literal, q'[...]' ones included, and a
// comment name nothing, so `UPPER(ab) || 'a'` does not name column a and
// `q'[a]' || b` does not name it either.
//
// Oracle folds an unquoted identifier to upper case and keeps a quoted one as
// written. column is the spelling the declaration or the catalog gives, so an
// unquoted token names it without regard to case, and a quoted token names it
// when the text is the column's spelling or its stored upper-case form:
// `"C"` and `"c"` name column c, and `"Abc"` does not name column abc.
func expressionNames(expression, column string) bool {
	if column == "" {
		return false
	}
	stored := strings.ToUpper(column)
	// The descriptor's options leave alternative quoting unread, which
	// internal/sqlreach depends on to refuse it. Here a literal's extent is what
	// matters: q'[it's]' || a names a, and read without it the apostrophe would
	// end the literal and swallow the name.
	options := dialectlexer.Options(platform.Oracle)
	options.AlternativeQuoting = true
	tokens := lexer.NewLexerWithOptions(expression, options)
	for token := tokens.NextToken(); token.Type != lexer.TokenEOF; token = tokens.NextToken() {
		switch {
		case token.Type == lexer.TokenIdentifier:
			if strings.EqualFold(token.Value, column) {
				return true
			}
		case token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`):
			quoted := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(token.Value, `"`), `"`), `""`, `"`)
			if quoted == column || quoted == stored {
				return true
			}
		}
	}
	return false
}
