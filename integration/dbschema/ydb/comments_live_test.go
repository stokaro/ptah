//go:build integration

package ydb_test

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/cli/introspect"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/ydbcomment"
	"ptah.run/migration/migrator"
)

// commentsSchema is the directory the comment tests write into.
const commentsSchema = "ptah_ydb_comments"

var commentsSchemas = []string{commentsSchema}

// commentedModels declares, through the annotations every engine reads, a
// table, its columns, its indexes, a UNIQUE constraint that YDB keeps as a
// unique index, and a view, each with a comment.
const commentedModels = `package models

//ptah:schema:table name="users" schema="ptah_ydb_comments" comment="People who sign in"
//ptah:schema:constraint name="users_email_key" type="UNIQUE" columns="email" comment="One login each"
type User struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true" comment="Key"
	ID int64
	//ptah:schema:field name="email" type="TEXT" comment="Login"
	Email string
	//ptah:schema:field name="label" type="TEXT" comment="Shown name"
	//ptah:schema:index name="users_by_label" fields="label" comment="Lookup by name"
	Label string
	//ptah:schema:field name="legacy" type="TEXT" comment="Going away"
	//ptah:schema:index name="users_by_legacy" fields="legacy" comment="Going away too"
	Legacy string
}

//ptah:schema:view name="active_users" schema="ptah_ydb_comments" body="SELECT id FROM ` + "`ptah_ydb_comments/users`" + `" comment="Users who signed in"
type ActiveUsers struct{}
`

// changedModels is commentedModels with every kind of comment change: the
// table's comment rewritten, the email column's removed, the label column's
// rewritten, its index renamed with its comment, the legacy column and its
// index dropped, a commented column and index added, the UNIQUE constraint's
// comment rewritten, and the view's rewritten.
const changedModels = `package models

//ptah:schema:table name="users" schema="ptah_ydb_comments" comment="Customers"
//ptah:schema:constraint name="users_email_key" type="UNIQUE" columns="email" comment="One login per person"
type User struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true" comment="Key"
	ID int64
	//ptah:schema:field name="email" type="TEXT"
	Email string
	//ptah:schema:field name="label" type="TEXT" comment="Display name"
	//ptah:schema:index name="users_label_idx" fields="label" comment="Lookup by name"
	Label string
	//ptah:schema:field name="phone" type="TEXT" comment="Mobile"
	//ptah:schema:index name="users_by_phone" fields="phone" comment="By phone"
	Phone string
}

//ptah:schema:view name="active_users" schema="ptah_ydb_comments" body="SELECT id FROM ` + "`ptah_ydb_comments/users`" + `" comment="Signed in lately"
type ActiveUsers struct{}
`

// parseModels reads Go models from source.
func parseModels(c *qt.C, source string) *schemamodel.Database {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(source), 0o600), qt.IsNil)
	models, err := goschema.ParseDir(builtintest.Annotations(), dir)
	c.Assert(err, qt.IsNil)
	return models
}

// openDriver opens the SDK's own driver on the line's database, for what
// Ptah's reader does not show: the raw user attributes of an object.
func openDriver(c *qt.C, line ydbLine) *ydbsdk.Driver {
	c.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	driver, err := ydbsdk.Open(ctx, dbtarget.DriverDSN(c, line.engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = driver.Close(context.Background()) })
	return driver
}

// attributesOf reads the user attributes of the table or view at relative.
func attributesOf(c *qt.C, line ydbLine, relative string) map[string]string {
	c.Helper()
	driver := openDriver(c, line)
	client := Ydb_Table_V1.NewTableServiceClient(ydbsdk.GRPCConn(driver))
	response, err := client.DescribeTable(c.Context(), &Ydb_Table.DescribeTableRequest{Path: path.Join(driver.Name(), relative)})
	c.Assert(err, qt.IsNil)
	c.Assert(response.GetOperation().GetStatus(), qt.Equals, Ydb.StatusIds_SUCCESS,
		qt.Commentf("issues: %v", response.GetOperation().GetIssues()))
	var described Ydb_Table.DescribeTableResult
	c.Assert(response.GetOperation().GetResult().UnmarshalTo(&described), qt.IsNil)
	return described.GetAttributes()
}

// setAttributes sets user attributes of the table at relative the way another
// tool would, outside Ptah.
func setAttributes(c *qt.C, line ydbLine, relative string, attributes map[string]string) {
	c.Helper()
	driver := openDriver(c, line)
	client := Ydb_Table_V1.NewTableServiceClient(ydbsdk.GRPCConn(driver))
	response, err := client.AlterTable(c.Context(), &Ydb_Table.AlterTableRequest{
		Path: path.Join(driver.Name(), relative), AlterAttributes: attributes,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(response.GetOperation().GetStatus(), qt.Equals, Ydb.StatusIds_SUCCESS,
		qt.Commentf("issues: %v", response.GetOperation().GetIssues()))
}

// commentsOf lists every comment a read describes, keyed by what it belongs
// to.
func commentsOf(live *catalog.Database) map[string]string {
	comments := make(map[string]string)
	for _, table := range live.Tables {
		comments["table "+table.Name] = table.Comment
		for _, column := range table.Columns {
			comments["column "+table.Name+"."+column.Name] = column.Comment
		}
	}
	for _, index := range live.Indexes {
		comments["index "+index.TableName+"."+index.Name] = index.Comment
	}
	for _, view := range live.Views {
		comments["view "+view.Name] = view.Comment
	}
	return comments
}

// TestYDBComments_RoundTrip_NothingLeftToPlan is the comments family's round
// trip: models whose annotations comment a table, its columns, its indexes, a
// UNIQUE constraint and a view, applied statement by statement, read back,
// and compared with nothing left to plan; and the same models applied again
// with nothing planned after it. Each comment is a user attribute of its table
// or view under Ptah's key.
func TestYDBComments_RoundTrip_NothingLeftToPlan(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropViewsAndTables(c, conn, commentsSchemas)
			c.Cleanup(func() { dropViewsAndTables(c, conn, commentsSchemas) })
			declared := parseModels(c, commentedModels)

			apply(c, conn, planAgainst(c, conn, declared, commentsSchemas))

			c.Assert(planAgainst(c, conn, declared, commentsSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, commentsSchemas))
			c.Assert(planAgainst(c, conn, declared, commentsSchemas), qt.HasLen, 0)
			c.Assert(commentsOf(readScoped(c, conn, commentsSchemas)), qt.DeepEquals, map[string]string{
				"table users":                 "People who sign in",
				"column users.id":             "Key",
				"column users.email":          "Login",
				"column users.label":          "Shown name",
				"column users.legacy":         "Going away",
				"index users.users_by_label":  "Lookup by name",
				"index users.users_by_legacy": "Going away too",
				"index users.users_email_key": "One login each",
				"view active_users":           "Users who signed in",
			})
			c.Assert(attributesOf(c, line, commentsSchema+"/users"), qt.DeepEquals, map[string]string{
				"ptah.comment":                       "People who sign in",
				"ptah.comment.column.id":             "Key",
				"ptah.comment.column.email":          "Login",
				"ptah.comment.column.label":          "Shown name",
				"ptah.comment.column.legacy":         "Going away",
				"ptah.comment.index.users_by_label":  "Lookup by name",
				"ptah.comment.index.users_by_legacy": "Going away too",
				"ptah.comment.index.users_email_key": "One login each",
			})
			c.Assert(attributesOf(c, line, commentsSchema+"/active_users"), qt.DeepEquals,
				map[string]string{"ptah.comment": "Users who signed in"})
		})
	}
}

// TestYDBComments_ChangesInPlace changes every kind of comment at once. The
// plan writes each in place, moves a renamed index's comment to its new
// name, and removes the comments a dropped column and a dropped index leave
// on the table, which YDB would otherwise keep, so the migration it writes
// passes the rule that reports one left behind (YD150). Attributes another
// tool set -- one under no key of Ptah's, and one under Ptah's prefix that
// names no column or index -- are neither read nor removed. Nothing is left
// to plan.
func TestYDBComments_ChangesInPlace(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropViewsAndTables(c, conn, commentsSchemas)
			c.Cleanup(func() { dropViewsAndTables(c, conn, commentsSchemas) })
			first := planAgainst(c, conn, parseModels(c, commentedModels), commentsSchemas)
			apply(c, conn, first)
			setAttributes(c, line, commentsSchema+"/users", map[string]string{"owner": "team", "ptah.comment.other": "kept"})
			changed := parseModels(c, changedModels)

			planned := planAgainst(c, conn, changed, commentsSchemas)
			apply(c, conn, planned)

			c.Assert(planned, qt.DeepEquals, []string{
				"ALTER TABLE `ptah_ydb_comments/users` DROP INDEX `users_by_legacy`",
				"ALTER TABLE `ptah_ydb_comments/users` RENAME INDEX `users_by_label` TO `users_label_idx`",
				"ALTER TABLE `ptah_ydb_comments/users` ADD COLUMN `phone` Utf8",
				"COMMENT ON COLUMN `ptah_ydb_comments/users`.`phone` IS 'Mobile'",
				"ALTER TABLE `ptah_ydb_comments/users` DROP COLUMN `legacy`",
				"COMMENT ON TABLE `ptah_ydb_comments/users` IS 'Customers'",
				"COMMENT ON COLUMN `ptah_ydb_comments/users`.`email` IS NULL",
				"COMMENT ON COLUMN `ptah_ydb_comments/users`.`label` IS 'Display name'",
				"COMMENT ON COLUMN `ptah_ydb_comments/users`.`legacy` IS NULL",
				"ALTER TABLE `ptah_ydb_comments/users` ADD INDEX `users_by_phone` GLOBAL SYNC ON (`phone`)",
				"COMMENT ON INDEX `users_by_phone` ON `ptah_ydb_comments/users` IS 'By phone'",
				"COMMENT ON INDEX `users_by_legacy` ON `ptah_ydb_comments/users` IS NULL",
				"COMMENT ON INDEX `users_email_key` ON `ptah_ydb_comments/users` IS 'One login per person'",
				"COMMENT ON INDEX `users_label_idx` ON `ptah_ydb_comments/users` IS 'Lookup by name'",
				"COMMENT ON INDEX `users_by_label` ON `ptah_ydb_comments/users` IS NULL",
				"COMMENT ON VIEW `ptah_ydb_comments/active_users` IS 'Signed in lately'",
			})
			c.Assert(planAgainst(c, conn, changed, commentsSchemas), qt.HasLen, 0)
			c.Assert(lintPlans(c, conn, first, planned), qt.Not(qt.Contains), "YD150")
			c.Assert(attributesOf(c, line, commentsSchema+"/users"), qt.DeepEquals, map[string]string{
				"ptah.comment":                       "Customers",
				"ptah.comment.column.id":             "Key",
				"ptah.comment.column.label":          "Display name",
				"ptah.comment.column.phone":          "Mobile",
				"ptah.comment.index.users_label_idx": "Lookup by name",
				"ptah.comment.index.users_by_phone":  "By phone",
				"ptah.comment.index.users_email_key": "One login per person",
				"owner":                              "team",
				"ptah.comment.other":                 "kept",
			})
			c.Assert(attributesOf(c, line, commentsSchema+"/active_users"), qt.DeepEquals,
				map[string]string{"ptah.comment": "Signed in lately"})
		})
	}
}

// TestYDBComments_TextRoundTrips declares comments holding what a string
// literal has to escape, text beyond ASCII, a NUL, and the longest value YDB
// keeps; each reads back byte for byte, and nothing is left to plan.
func TestYDBComments_TextRoundTrips(t *testing.T) {
	texts := map[string]string{
		"quotes":    `it's "quoted" and ` + "`ticked`",
		"escapes":   `a \ backslash, a \n written out`,
		"controls":  "a newline\nand a tab\tand a NUL \x00 here",
		"non_ascii": "текст и 漢字",
		"longest":   strings.Repeat("x", ydbcomment.MaxValueBytes),
	}
	declared := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Note", Name: "notes", Schema: commentsSchema, Comment: "Notes"}},
		Fields: []schemamodel.Field{{StructName: "Note", Name: "id", Type: "BIGINT", Primary: true}},
	}
	for name, text := range texts {
		declared.Fields = append(declared.Fields, schemamodel.Field{StructName: "Note", Name: name, Type: "TEXT", Nullable: true, Comment: text})
	}
	schemamodel.Finalize(declared)
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropViewsAndTables(c, conn, commentsSchemas)
			c.Cleanup(func() { dropViewsAndTables(c, conn, commentsSchemas) })

			apply(c, conn, planAgainst(c, conn, declared, commentsSchemas))

			read := commentsOf(readScoped(c, conn, commentsSchemas))
			for name, text := range texts {
				c.Assert(read["column notes."+name], qt.Equals, text, qt.Commentf("column %s", name))
			}
			c.Assert(planAgainst(c, conn, declared, commentsSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBComments_IntrospectedModelsPlanNothing writes the commented schema
// as Go models with ptah introspect: the comments reach the annotations, and
// the models, planned against the database they came from, plan nothing.
func TestYDBComments_IntrospectedModelsPlanNothing(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropViewsAndTables(c, conn, commentsSchemas)
			c.Cleanup(func() { dropViewsAndTables(c, conn, commentsSchemas) })
			apply(c, conn, planAgainst(c, conn, parseModels(c, commentedModels), commentsSchemas))
			out := c.TempDir()

			stdout, err := runCommand(introspect.NewIntrospectCommand(),
				"--db-url", dbtarget.URL(c, line.engine), "--schemas", commentsSchema, "--out", out)
			c.Assert(err, qt.IsNil, qt.Commentf("introspect:\n%s", stdout))
			models, err := goschema.ParseDir(builtintest.Annotations(), out)
			c.Assert(err, qt.IsNil)

			c.Assert(models.Tables, qt.HasLen, 1)
			c.Assert(models.Tables[0].Comment, qt.Equals, "People who sign in")
			c.Assert(planAgainst(c, conn, models, commentsSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBComments_StatementRefusals runs Ptah's own COMMENT ON statements by
// hand, as a migration file would. What YDB would accept and lose, or keep
// where nothing reads it, is refused before the server is asked, and so is a
// statement inside a transaction. Removing the comment of a column the table
// no longer has is accepted, since that is how a plan cleans up after a drop.
func TestYDBComments_StatementRefusals(t *testing.T) {
	refusals := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{name: "a column table", statement: "COMMENT ON TABLE `ptah_ydb_comments/events` IS 'x'",
			wantErr: `invalid comment statement: /\w+/ptah_ydb_comments/events is a column table, which takes an attribute and does not keep it, .*`},
		{name: "a column the table lacks", statement: "COMMENT ON COLUMN `ptah_ydb_comments/notes`.`missing` IS 'x'",
			wantErr: `invalid comment statement: table /\w+/ptah_ydb_comments/notes has no column "missing"`},
		{name: "an index the table lacks", statement: "COMMENT ON INDEX `missing` ON `ptah_ydb_comments/notes` IS 'x'",
			wantErr: `invalid comment statement: table /\w+/ptah_ydb_comments/notes has no index "missing"`},
		{name: "a table named as a view", statement: "COMMENT ON VIEW `ptah_ydb_comments/notes` IS 'x'",
			wantErr: `invalid comment statement: COMMENT ON VIEW names /\w+/ptah_ydb_comments/notes, which is a TABLE`},
		{name: "a missing table", statement: "COMMENT ON TABLE `ptah_ydb_comments/absent` IS 'x'",
			wantErr: `set the comment on table /\w+/ptah_ydb_comments/absent: describe YDB path .*: SCHEME_ERROR: .*`},
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropViewsAndTables(c, conn, commentsSchemas)
			c.Cleanup(func() { dropViewsAndTables(c, conn, commentsSchemas) })
			apply(c, conn, []string{
				"DROP TABLE IF EXISTS `ptah_ydb_comments/events`",
				"CREATE TABLE `ptah_ydb_comments/notes` (id Int64 NOT NULL, body Utf8, PRIMARY KEY (id))",
				"CREATE TABLE `ptah_ydb_comments/events` (id Int64 NOT NULL, PRIMARY KEY (id)) " +
					"PARTITION BY HASH(id) WITH (STORE = COLUMN)",
			})
			c.Cleanup(func() {
				c.Assert(conn.Writer().ExecuteSQL(context.Background(), "DROP TABLE `ptah_ydb_comments/events`"), qt.IsNil)
			})

			for _, refusal := range refusals {
				err := conn.Writer().ExecuteSQL(c.Context(), refusal.statement)
				c.Assert(err, qt.ErrorMatches, `(?s).*`+refusal.wantErr+`.*`, qt.Commentf("%s", refusal.name))
			}
			transaction, err := conn.BeginTx(c.Context(), nil)
			c.Assert(err, qt.IsNil)
			_, err = transaction.ExecContext(c.Context(), "COMMENT ON TABLE `ptah_ydb_comments/notes` IS 'x'")
			c.Assert(err, qt.ErrorIs, ydbcomment.ErrStatement)
			c.Assert(transaction.Rollback(), qt.IsNil)

			c.Assert(conn.Writer().ExecuteSQL(c.Context(), "COMMENT ON COLUMN `ptah_ydb_comments/notes`.`gone` IS NULL"), qt.IsNil)
			c.Assert(attributesOf(c, line, commentsSchema+"/notes"), qt.HasLen, 0)
		})
	}
}

// TestYDBComments_AttributesShareOneBudget fills most of a table's 10240
// bytes of attributes outside Ptah, and the comment that would pass them is
// refused by the server, with its answer: YDB counts every attribute, not
// only Ptah's.
func TestYDBComments_AttributesShareOneBudget(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropViewsAndTables(c, conn, commentsSchemas)
			c.Cleanup(func() { dropViewsAndTables(c, conn, commentsSchemas) })
			apply(c, conn, []string{"CREATE TABLE `ptah_ydb_comments/notes` (id Int64 NOT NULL, body Utf8, PRIMARY KEY (id))"})
			setAttributes(c, line, commentsSchema+"/notes", map[string]string{
				"a": strings.Repeat("a", 4096), "b": strings.Repeat("b", 4096),
			})

			err := conn.Writer().ExecuteSQL(c.Context(),
				"COMMENT ON COLUMN `ptah_ydb_comments/notes`.`body` IS '"+strings.Repeat("c", 2100)+"'")

			c.Assert(err, qt.ErrorMatches, `(?s).*set the comment on column "body" of table .*: BAD_REQUEST: .*user attributes too big: 10318.*`)
			c.Assert(attributesOf(c, line, commentsSchema+"/notes"), qt.HasLen, 2)
		})
	}
}

// TestYDBComments_MigrationFile runs a hand-written migration that comments
// a table and a column through the migrator: each COMMENT ON is a scheme
// query of its own, counted and recorded like any other, and the comments
// read back.
func TestYDBComments_MigrationFile(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dir := commentsSchema + "/mig"
			dropDirectory(c, conn, dir, "notes")
			c.Cleanup(func() { dropDirectory(c, conn, dir, "notes") })
			files := map[string]string{
				"0000000001_notes.up.sql": "CREATE TABLE `" + dir + "/notes` (id Int64 NOT NULL, body Utf8, PRIMARY KEY (id));\n" +
					"COMMENT ON TABLE `" + dir + "/notes` IS 'Notes';\n" +
					"UPSERT INTO `" + dir + "/notes` (id, body) VALUES (1l, 'first'u);\n" +
					"COMMENT ON COLUMN `" + dir + "/notes`.`body` IS 'The text';\n",
				"0000000001_notes.down.sql": "DROP TABLE `" + dir + "/notes`;\n",
			}
			m := newMigrator(c, conn, files, migrator.RevisionTableFormatPtah, dir)

			c.Assert(m.MigrateUp(c.Context()), qt.IsNil)

			c.Assert(revisionProgress(c, m), qt.DeepEquals, []progress{{Version: 1, State: "applied", Applied: 4, Total: 4}})
			c.Assert(commentsOf(readScoped(c, conn, []string{dir})), qt.DeepEquals, map[string]string{
				"table notes": "Notes", "column notes.id": "", "column notes.body": "The text",
			})
		})
	}
}
