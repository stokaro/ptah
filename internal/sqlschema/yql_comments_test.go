package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
)

func TestReadYQLComments(t *testing.T) {
	c := qt.New(t)
	source := "CREATE TABLE `a.b` (id Int64 NOT NULL, body Utf8, PRIMARY KEY (id), INDEX same GLOBAL ON (body));" +
		"CREATE TABLE `a/b` (id Int64 NOT NULL, body Utf8, PRIMARY KEY (id), INDEX same GLOBAL ON (body));" +
		"CREATE VIEW `a.v` WITH (security_invoker = TRUE) AS SELECT id FROM `a.b`;" +
		"CREATE VIEW `a/v` WITH (security_invoker = TRUE) AS SELECT id FROM `a/b`;" +
		"COMMENT ON TABLE `a.b` IS 'root; table';" +
		"COMMENT ON COLUMN `a/b`.`body` IS \"line\\nnext\"u;" +
		"COMMENT ON INDEX same ON `a/b` IS 'nested index';" +
		"COMMENT ON VIEW `a/v` IS 'nested view';"
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables[0].Comment, qt.Equals, "root; table")
	c.Assert(database.Tables[1].Comment, qt.Equals, "")
	c.Assert(database.Fields[1].Comment, qt.Equals, "")
	c.Assert(database.Fields[3].Comment, qt.Equals, "line\nnext")
	c.Assert(database.Indexes[0].Comment, qt.Equals, "")
	c.Assert(database.Indexes[1].Comment, qt.Equals, "nested index")
	c.Assert(database.Views[0].Comment, qt.Equals, "")
	c.Assert(database.Views[1].Comment, qt.Equals, "nested view")
	statements, err := builtin.GetOrderedCreateStatements(&database, "ydb")
	c.Assert(err, qt.IsNil)
	again, _, err := sqlschema.Read([]byte(strings.Join(statements, "\n")), "ydb")
	c.Assert(err, qt.IsNil)
	rendered, err := builtin.GetOrderedCreateStatements(&again, "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.DeepEquals, statements)
}

func TestReadYQLCommentRemoval(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte("CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)); COMMENT ON TABLE t IS 'old'; COMMENT ON TABLE t IS NULL;"), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables[0].Comment, qt.Equals, "")
}

func TestReadYQLCommentRefusals(t *testing.T) {
	for _, text := range []string{
		"COMMENT ON TABLE absent IS 'x';",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)); COMMENT ON COLUMN t.absent IS 'x';",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)); COMMENT ON INDEX absent ON t IS 'x';",
		"COMMENT ON VIEW absent IS 'x';",
		"COMMENT ON TABLE `/local/t` IS 'x';",
		"COMMENT ON TABLE t IS CurrentUtcTimestamp();",
		"COMMENT ON SEQUENCE s IS 'x';",
		"COMMENT ON TABLE t IS 'unterminated",
		"COMMENT ON TABLE t IS 'x' trailing;",
	} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			_, _, err := sqlschema.Read([]byte(text), "ydb")
			c.Assert(err, qt.IsNotNil)
		})
	}
}
