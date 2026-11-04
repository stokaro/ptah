package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
)

// mysqlPeopleSource declares a table whose table and column comments are
// written the common way.
const mysqlPeopleSource = `package entities

//ptah:schema:table name="people" comment="People who sign in"
type Person struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true" comment="Key"
	ID int64
}
`

// mysqlCommentsSource adds a table that also states a MySQL comment of its
// own for the table.
const mysqlCommentsSource = mysqlPeopleSource + `
//ptah:schema:table name="teams" comment="Teams" platform.mysql.comment="Teams on MySQL"
type Team struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true" comment="Team key"
	ID int64
}
`

// TestMySQLGoComments_FollowTheTarget pins how a Go source's comments are
// written on the MySQL family: the common comment of each table and column,
// unless a platform.mysql.comment states the table's MySQL comment.
func TestMySQLGoComments_FollowTheTarget(t *testing.T) {
	database := must.Must(goschema.ParseSource(builtintest.Annotations(), "entities.go", mysqlCommentsSource))
	for _, test := range []struct {
		dialect string
		caps    capability.Capabilities
		want    []string
	}{
		{dialect: platform.MySQL, caps: capability.MySQL84(), want: []string{
			"CREATE TABLE `people` (\n  `id` BIGINT PRIMARY KEY COMMENT 'Key'\n) COMMENT='People who sign in';\n\n",
			"CREATE TABLE `teams` (\n  `id` BIGINT PRIMARY KEY COMMENT 'Team key'\n) COMMENT='Teams on MySQL';\n\n",
		}},
		{dialect: platform.MariaDB, caps: capability.ForDialect(platform.MariaDB), want: []string{
			"CREATE TABLE `people` (\n  `id` BIGINT PRIMARY KEY COMMENT 'Key'\n) COMMENT='People who sign in';\n\n",
			"CREATE TABLE `teams` (\n  `id` BIGINT PRIMARY KEY COMMENT 'Team key'\n) COMMENT='Teams';\n\n",
		}},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(&database, test.dialect, test.caps)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// TestMySQLGoComments_SyncedWithTheTableTheyCreate pins the comparison: a
// MySQL table holding the comments a Go source writes there plans nothing.
func TestMySQLGoComments_SyncedWithTheTableTheyCreate(t *testing.T) {
	c := qt.New(t)
	desired := must.Must(goschema.ParseSource(builtintest.Annotations(), "entities.go", mysqlPeopleSource))
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "people", Comment: "People who sign in", Columns: []catalog.Column{{Name: "id", DataType: "bigint",
			ColumnType: "bigint", IsNullable: "NO", IsPrimaryKey: true, Comment: "Key"}}}},
		Constraints: []catalog.Constraint{{Name: "PRIMARY", TableName: "people", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}}},
	}

	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), &desired, current, catalog.ServerInfo{Dialect: platform.MySQL}, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}
