package ydbsql_test

import (
	"fmt"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/sqlschema"
	"ptah.run/internal/ydbsql"
)

// yql selects the YDB owner alone, whose grammar reads a YQL document and
// claims the YDB coverage it makes.
func yql(c *qt.C) sqlschema.Options {
	c.Helper()
	return sqlschema.Options{Owners: must.Must(sqlext.NewSet(ydbsql.SQL()))}
}

// readYQL reads one YQL document on its own through the owner's grammar, as a
// production read of one file does.
func readYQL(c *qt.C, source string) (schemamodel.Database, error) {
	c.Helper()
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(source), "ydb", nil, yql(c))
	return database, err
}

// readYQLLoad reads documents as one load through the owner's grammar, each
// file against what the earlier ones declared, with root as the database path
// absolute paths are read against.
func readYQLLoad(c *qt.C, root string, documents ...string) (sqlschema.Load, error) {
	c.Helper()
	files := make([]sqlext.SourceFile, 0, len(documents))
	for i, document := range documents {
		files = append(files, sqlext.SourceFile{Path: fmt.Sprintf("%03d.sql", i+1), Text: document})
	}
	return sqlschema.ReadLoad(c.Context(), sqlext.GrammarRequest{Target: "ydb", DatabasePath: root, Files: files}, yql(c))
}
