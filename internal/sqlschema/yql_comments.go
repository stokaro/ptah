package sqlschema

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbcomment"
)

func applyYQLComment(database, base *schemamodel.Database, text string) error {
	query, recognized, err := ydbcomment.Recognize(text)
	if err != nil {
		return err
	}
	if !recognized {
		return fmt.Errorf("%w: %s", ErrUnmodeledStatement, text)
	}
	databases := []*schemamodel.Database{database}
	if base != nil {
		databases = append(databases, base)
	}
	target := yqlCommentTarget(databases, query.Statement)
	if target == nil {
		return fmt.Errorf("%w: COMMENT ON %s %s names an object this schema does not declare", ErrUnmodeledStatement, query.Object.Keyword(), query.Path)
	}
	*target = query.Comment
	return nil
}

func yqlCommentTarget(databases []*schemamodel.Database, statement ydbcomment.Statement) *string {
	// Quote the decoded path before the SQL identifier adapter reads it again:
	// a backtick or a dot in a YDB name is data, not a qualification boundary.
	qualified := normalizeSQLTableReference(platform.YDB, sqlident.Quote(platform.YDB, statement.Path))
	if statement.Object == ydbcomment.View {
		target, found := viewCommentTarget(databases, qualified, platform.YDB)
		if found {
			return target
		}
		return nil
	}
	table := resolveTable(databases, qualified, platform.YDB)
	if table == nil {
		return nil
	}
	switch statement.Object {
	case ydbcomment.Table:
		return &table.Comment
	case ydbcomment.Column:
		field := resolveColumn(databases, table.StructName, sqlident.Quote(platform.YDB, statement.Name), platform.YDB)
		if field != nil {
			return &field.Comment
		}
	case ydbcomment.Index:
		for _, database := range databases {
			for i := range database.Indexes {
				index := &database.Indexes[i]
				if index.TableName == table.QualifiedName() && index.Name == statement.Name {
					return &index.Comment
				}
			}
		}
	}
	return nil
}
