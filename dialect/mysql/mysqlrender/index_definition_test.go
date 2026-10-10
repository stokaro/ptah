package mysqlrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/mysql/mysqlast"
	"ptah.run/dialect/mysql/mysqlrender"
)

// IndexDefinition writes every element a MySQL-family index can carry, in
// the order SHOW CREATE TABLE prints them, with each engine's word for an
// index the optimizer does not use.
func TestIndexDefinition(t *testing.T) {
	full := mysqlast.Index{Name: "k`s", Unique: true, Type: "HASH", Comment: "it's",
		Parts:        []mysqlast.IndexPart{{Column: "name", Prefix: "7", Descending: true}, {Expression: "lower(email)"}, {Column: "app.id"}},
		KeyBlockSize: 8, Invisible: true}
	for _, test := range []struct {
		name, dialect, placement string
		index                    mysqlast.Index
		want                     string
	}{
		{"every element", "mysql", "ON `t`", full,
			"UNIQUE INDEX `k``s` ON `t` (`name` (7) DESC, (lower(email)), `app`.`id`) USING HASH KEY_BLOCK_SIZE=8 COMMENT 'it''s' INVISIBLE"},
		{"MariaDB's word", "mariadb", "", full,
			"UNIQUE INDEX `k``s` (`name` (7) DESC, (lower(email)), `app`.`id`) USING HASH KEY_BLOCK_SIZE=8 COMMENT 'it''s' IGNORED"},
		{"a FULLTEXT parser", "mysql", "", mysqlast.Index{Name: "ft", Type: "FULLTEXT", Parser: "ngram", Parts: []mysqlast.IndexPart{{Column: "body"}}},
			"FULLTEXT INDEX `ft` (`body`) /*!50100 WITH PARSER `ngram` */"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(strings.Join(mysqlrender.IndexDefinition(test.dialect, test.index, test.placement), " "), qt.Equals, test.want)
		})
	}
}

func replaceIndex(tableCopy bool) *mysqlast.ReplaceIndex {
	return &mysqlast.ReplaceIndex{Index: mysqlast.Index{Name: "k", Parts: []mysqlast.IndexPart{{Column: "a"}}, KeyBlockSize: 8}, TableCopy: tableCopy}
}

// The replacement drops and adds the index in one statement of its ALTER
// TABLE parent, with the table copy it asks for, or the online clause the
// parent asks for when it asks for none. A required copy survives an online
// request, so the server refuses the blocking rebuild rather than keeping
// the old hint.
func TestHandlers_RenderTheReplacement(t *testing.T) {
	registry := must.Must(mysqlrender.Registry())
	for _, test := range []struct {
		name, target string
		parent       ast.AlterTableNode
		payload      *mysqlast.ReplaceIndex
		want         string
	}{
		{"MySQL", "mysql", ast.AlterTableNode{Name: "app.t"}, replaceIndex(true),
			"ALTER TABLE `app`.`t` DROP INDEX `k`, ADD INDEX `k` (`a`) KEY_BLOCK_SIZE=8, ALGORITHM=COPY;"},
		{"MySQL, online", "mysql", ast.AlterTableNode{Name: "t", Algorithm: "INPLACE", Lock: "NONE"}, replaceIndex(true),
			"ALTER TABLE `t` DROP INDEX `k`, ADD INDEX `k` (`a`) KEY_BLOCK_SIZE=8, ALGORITHM=COPY, LOCK=NONE;"},
		{"MariaDB", "mariadb", ast.AlterTableNode{Name: "t"}, replaceIndex(false),
			"ALTER TABLE `t` DROP INDEX `k`, ADD INDEX `k` (`a`) KEY_BLOCK_SIZE=8;"},
		{"MariaDB, online", "mariadb", ast.AlterTableNode{Name: "t", Algorithm: "INPLACE", Lock: "NONE"}, replaceIndex(false),
			"ALTER TABLE `t` DROP INDEX `k`, ADD INDEX `k` (`a`) KEY_BLOCK_SIZE=8, ALGORITHM=INPLACE, LOCK=NONE;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := renderer.ExtensionContext{Target: test.target, Capabilities: capability.ForDialect(test.target), Parent: &test.parent}

			statements, err := registry.Render(ctx, ast.AlterExtension, test.payload)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}

// A replacement is refused where it cannot store the hint it writes: on a
// target without the ALGORITHM clause when it needs the copy, with a hint
// above MariaDB's limit, and on another engine.
func TestHandlers_RenderTheReplacement_FailurePath(t *testing.T) {
	registry := must.Must(mysqlrender.Registry())
	large := replaceIndex(false)
	large.Index.KeyBlockSize = 65536
	for _, test := range []struct {
		name, target string
		caps         capability.Capabilities
		payload      *mysqlast.ReplaceIndex
		wantIs       error
		wantErr      string
	}{
		{"no ALGORITHM clause", "mysql", capability.ForDialect("mysql").With(capability.AlterTableAlgorithmLock, false), replaceIndex(true),
			ptaherr.ErrUnsupportedFeature, `.*replacing index "k" needs ALGORITHM=COPY.*`},
		{"above the MariaDB limit", "mariadb", capability.ForDialect("mariadb"), large,
			ptaherr.ErrInvalidSchemaDiff, `.*KEY_BLOCK_SIZE 65536 exceeds the mariadb limit 65535.*`},
		{"another engine", "postgres", capability.ForDialect("postgres"), replaceIndex(false),
			ptaherr.ErrUnsupportedDialect, `.*MySQL index replacement on "postgres".*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := renderer.ExtensionContext{Target: test.target, Capabilities: test.caps, Parent: &ast.AlterTableNode{Name: "t"}}

			statements, err := registry.Render(ctx, ast.AlterExtension, test.payload)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
