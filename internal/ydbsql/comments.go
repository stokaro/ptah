package ydbsql

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/sqlschema"
	"ptah.run/internal/ydbcomment"
)

// applyComment attaches a YQL COMMENT ON statement to the table, column,
// index or view it names, in the statement's file or an earlier one.
func applyComment(scope sqlschema.Scope, text string) error {
	query, recognized, err := ydbcomment.Recognize(text)
	if err != nil {
		return err
	}
	if !recognized {
		return fmt.Errorf("%w: %s", sqlschema.ErrUnmodeledStatement, text)
	}
	target := commentTarget(scope, query.Statement)
	if target == nil {
		return undeclared(fmt.Errorf("%w: COMMENT ON %s %s names an object this schema does not declare", sqlschema.ErrUnmodeledStatement, query.Object.Keyword(), query.Path))
	}
	*target = query.Comment
	return nil
}

func commentTarget(scope sqlschema.Scope, statement ydbcomment.Statement) *string {
	// Quote the decoded path before the shared conversion reads it again: a
	// backtick or a dot in a YDB name is data, not a qualification boundary.
	written := sqlident.Quote(platform.YDB, statement.Path)
	if statement.Object == ydbcomment.View {
		if view := scope.View(written); view != nil {
			return &view.Comment
		}
		return nil
	}
	table, found := scope.Table(written)
	if !found {
		return nil
	}
	switch statement.Object {
	case ydbcomment.Table:
		return &table.Declared.Comment
	case ydbcomment.Column:
		field := scope.Column(table, sqlident.Quote(platform.YDB, statement.Name))
		if field != nil {
			return &field.Comment
		}
	case ydbcomment.Index:
		for _, database := range table.Databases {
			for i := range database.Indexes {
				index := &database.Indexes[i]
				if index.TableName == table.Qualified && index.Name == statement.Name {
					return &index.Comment
				}
			}
		}
	}
	return nil
}
