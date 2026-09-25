package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// commentFixture holds a second table and index beside the ones the comments
// name, so a comment that lands on the wrong object is seen.
const commentFixture = "CREATE SCHEMA app;\nCREATE TABLE app.u (id int PRIMARY KEY, a int);\n" +
	"CREATE TABLE app.t (id int PRIMARY KEY, a int);\n" +
	"CREATE INDEX u_a_idx ON app.u (a);\nCREATE INDEX t_a_idx ON app.t (a);\nCREATE ROLE reader;\n"

// appTStructName is the struct name the reader gives app.t, read back rather
// than spelled here.
var appTStructName = func() string {
	database, _, err := sqlschema.Read([]byte("CREATE TABLE app.t (id int);"), "postgres")
	if err != nil || len(database.Tables) != 1 {
		panic("the app.t fixture does not read as one table")
	}
	return database.Tables[0].StructName
}()

// commentsOf lists every comment the model carries for the fixture's objects.
func commentsOf(database schemamodel.Database) map[string]string {
	comments := make(map[string]string)
	for _, schema := range database.Schemas {
		comments["schema "+schema.Name] = schema.Comment
	}
	for _, table := range database.Tables {
		comments["table "+table.QualifiedName()] = table.Comment
	}
	for _, field := range database.Fields {
		comments["column "+field.StructName+"."+field.Name] = field.Comment
	}
	for _, index := range database.Indexes {
		comments["index "+index.Name] = index.Comment
	}
	for _, role := range database.Roles {
		comments["role "+role.Name] = role.Comment
	}
	return comments
}

// setComments lists the objects whose comment is not empty.
func setComments(database schemamodel.Database) []string {
	var set []string
	for key, comment := range commentsOf(database) {
		if comment != "" {
			set = append(set, key)
		}
	}
	return set
}

// A COMMENT ON reaches the object it names, as the same comment declared
// inline does. It parsed and was dropped for every kind but a role, so a
// schema pg_dump wrote lost every comment (stokaro/ptah#3610).
func TestRead_CommentOn_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantKey   string
		want      string
	}{
		{name: "a table", statement: "COMMENT ON TABLE app.t IS 'the table';", wantKey: "table app.t", want: "the table"},
		{name: "a column", statement: "COMMENT ON COLUMN app.t.a IS 'the column';", wantKey: "column " + appTStructName + ".a", want: "the column"},
		{name: "an index", statement: "COMMENT ON INDEX app.t_a_idx IS 'the index';", wantKey: "index t_a_idx", want: "the index"},
		{name: "a schema", statement: "COMMENT ON SCHEMA app IS 'the schema';", wantKey: "schema app", want: "the schema"},
		{name: "a role", statement: "COMMENT ON ROLE reader IS 'reads';", wantKey: "role reader", want: "reads"},
		{name: "IS inside the text", statement: "COMMENT ON TABLE app.t IS 'it IS here';", wantKey: "table app.t", want: "it IS here"},
		{name: "a quote inside the text", statement: "COMMENT ON COLUMN app.t.a IS 'it''s a';", wantKey: "column " + appTStructName + ".a", want: "it's a"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(commentFixture+test.statement), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(commentsOf(database)[test.wantKey], qt.Equals, test.want)
			// Exactly one comment is set: the one on the object named.
			c.Assert(setComments(database), qt.DeepEquals, []string{test.wantKey})
		})
	}
}

// A comment the model keeps no place for, one the plan would never write, and
// one on an object the document does not declare are each refused by name
// rather than dropped.
func TestRead_CommentOn_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name:      "an object the document does not declare",
			statement: "COMMENT ON TABLE app.nope IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON TABLE app.nope names an object this schema does not declare`,
		},
		{
			name:      "a column the table does not have",
			statement: "COMMENT ON COLUMN app.t.zz IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON COLUMN app.t.zz names an object this schema does not declare`,
		},
		{
			name:      "a role the document does not declare",
			statement: "COMMENT ON ROLE writer IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON ROLE writer names an object this schema does not declare`,
		},
		{
			name:      "a schema the document does not declare",
			statement: "COMMENT ON SCHEMA other IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON SCHEMA other names an object this schema does not declare`,
		},
		{
			name:      "a lower-case name for a quoted mixed-case table",
			statement: "CREATE TABLE \"Docs\" (id int);\nCOMMENT ON TABLE docs IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON TABLE docs names an object this schema does not declare`,
		},
		{
			name:      "a kind the model has no comment for",
			statement: "COMMENT ON DATABASE app IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON DATABASE: Ptah keeps no comment for this kind of object`,
		},
		{
			name:      "a kind the plan never writes",
			statement: "CREATE SEQUENCE app.s;\nCOMMENT ON SEQUENCE app.s IS 'x';",
			wantErr: `the schema model has no place for this statement: COMMENT ON SEQUENCE app.s: the model keeps this comment ` +
				`and the plan never writes it \(stokaro/ptah#3627\), so reading it would drop it`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, statements, err := sqlschema.Read([]byte(commentFixture+test.statement), "postgres")

			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A COMMENT ON names its object the way the CREATE did: PostgreSQL folds an
// unquoted name to lower case in both statements, so the two spellings below
// name one table, one column and one index.
func TestRead_CommentOn_FoldsAnUnquotedNameAsTheCreateDid(t *testing.T) {
	c := qt.New(t)
	const file = "CREATE SCHEMA App;\nCREATE TABLE App.Docs (Id int PRIMARY KEY, Title text);\n" +
		"CREATE INDEX Docs_Title_Idx ON App.Docs (Title);\n" +
		"COMMENT ON TABLE app.DOCS IS 'the table';\nCOMMENT ON COLUMN APP.docs.TITLE IS 'the column';\n" +
		"COMMENT ON INDEX app.DOCS_TITLE_IDX IS 'the index';\nCOMMENT ON SCHEMA APP IS 'the schema';"

	database, _, err := sqlschema.Read([]byte(file), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	comments := commentsOf(database)
	c.Assert(comments["table app.docs"], qt.Equals, "the table")
	c.Assert(comments["column "+database.Tables[0].StructName+".title"], qt.Equals, "the column")
	c.Assert(comments["index docs_title_idx"], qt.Equals, "the index")
	c.Assert(comments["schema app"], qt.Equals, "the schema")
}

// A later file of a directory may comment on what an earlier file declared.
func TestReadOnto_CommentsOnAnEarlierTable(t *testing.T) {
	c := qt.New(t)
	earlier, _, err := sqlschema.Read([]byte(commentFixture), "postgres")
	c.Assert(err, qt.IsNil)

	later, _, err := sqlschema.ReadOnto([]byte("COMMENT ON TABLE app.t IS 'later';\nCOMMENT ON COLUMN app.t.a IS 'a';"), "postgres", &earlier)

	c.Assert(err, qt.IsNil)
	c.Assert(later.Tables, qt.HasLen, 0)
	c.Assert(commentsOf(earlier)["table app.t"], qt.Equals, "later")
	c.Assert(commentsOf(earlier)["column "+appTStructName+".a"], qt.Equals, "a")
}
