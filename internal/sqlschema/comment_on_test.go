package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// commentFixture holds a second object of every kind beside the ones the
// comments name, so a comment that lands on the wrong object is seen.
const commentFixture = "CREATE SCHEMA app;\nCREATE TABLE app.u (id int PRIMARY KEY, a int);\n" +
	"CREATE TABLE app.t (id int PRIMARY KEY, a int);\n" +
	"CREATE INDEX u_a_idx ON app.u (a);\nCREATE INDEX t_a_idx ON app.t (a);\nCREATE ROLE reader;\n" +
	"CREATE VIEW app.w AS SELECT id FROM app.u;\nCREATE VIEW app.v AS SELECT id FROM app.t;\n" +
	"CREATE SEQUENCE app.q;\nCREATE SEQUENCE app.s;\n" +
	"CREATE DOMAIN app.e AS int;\nCREATE DOMAIN app.d AS int;\n" +
	"CREATE TYPE app.k AS (n int);\nCREATE TYPE app.c AS (n int);\n" +
	"CREATE TYPE app.x AS RANGE (subtype = int4);\nCREATE TYPE app.r AS RANGE (subtype = int8);\n" +
	"CREATE TYPE app.calm AS ENUM ('ok');\nCREATE TYPE app.mood AS ENUM ('ok');\n" +
	"CREATE EXTENSION hstore;\nCREATE EXTENSION pgcrypto;\n" +
	"CREATE MATERIALIZED VIEW app.mu AS SELECT id FROM app.u;\nCREATE MATERIALIZED VIEW app.mt AS SELECT id FROM app.t;\n" +
	"CREATE FUNCTION app.f(a int, b text DEFAULT 'x') RETURNS int LANGUAGE sql AS 'SELECT 2';\n" +
	"CREATE FUNCTION app.g() RETURNS int LANGUAGE sql AS 'SELECT 3';\n" +
	"CREATE PROCEDURE app.p(a int) LANGUAGE sql AS 'SELECT 1';\n" +
	"CREATE FUNCTION app.touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;\n" +
	"CREATE TRIGGER audit BEFORE INSERT ON app.u FOR EACH ROW EXECUTE FUNCTION app.touch();\n" +
	"CREATE TRIGGER audit BEFORE INSERT ON app.t FOR EACH ROW EXECUTE FUNCTION app.touch();\n" +
	"ALTER TABLE app.u ENABLE ROW LEVEL SECURITY;\nALTER TABLE app.t ENABLE ROW LEVEL SECURITY;\n" +
	"CREATE POLICY own ON app.u USING (true);\nCREATE POLICY own ON app.t USING (true);\n" +
	"ALTER TABLE app.u ADD CONSTRAINT positive CHECK (a > 0);\n" +
	"ALTER TABLE app.t ADD CONSTRAINT positive CHECK (a > 0);\n"

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
	for _, view := range database.Views {
		comments["view "+view.Name] = view.Comment
	}
	for _, sequence := range database.Sequences {
		comments["sequence "+sequence.QualifiedName()] = sequence.Comment
	}
	for _, domain := range database.Domains {
		comments["domain "+domain.QualifiedName()] = domain.Comment
	}
	for _, composite := range database.CompositeTypes {
		comments["composite "+composite.QualifiedName()] = composite.Comment
	}
	for _, rangeType := range database.Ranges {
		comments["range "+rangeType.QualifiedName()] = rangeType.Comment
	}
	for _, extension := range database.Extensions {
		comments["extension "+extension.Name] = extension.Comment
	}
	for _, enum := range database.Enums {
		comments["enum "+enum.QualifiedName()] = enum.Comment
	}
	for _, view := range database.MaterializedViews {
		comments["materialized view "+view.Name] = view.Comment
	}
	for _, function := range database.Functions {
		kind := "function"
		if function.IsProcedure() {
			kind = "procedure"
		}
		comments[kind+" "+function.Name+"("+function.Parameters+")"] = function.Comment
	}
	for _, trigger := range database.Triggers {
		comments["trigger "+trigger.Name+" on "+trigger.Table] = trigger.Comment
	}
	for _, policy := range database.RLSPolicies {
		comments["policy "+policy.Name+" on "+policy.Table] = policy.Comment
	}
	for _, constraint := range database.Constraints {
		comments["constraint "+constraint.Name+" on "+constraint.Table] = constraint.Comment
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
		{name: "a view", statement: "COMMENT ON VIEW app.v IS 'the view';", wantKey: "view app.v", want: "the view"},
		{name: "a sequence", statement: "COMMENT ON SEQUENCE app.s IS 'the sequence';", wantKey: "sequence app.s", want: "the sequence"},
		{name: "a domain", statement: "COMMENT ON DOMAIN app.d IS 'the domain';", wantKey: "domain app.d", want: "the domain"},
		{name: "a composite type", statement: "COMMENT ON TYPE app.c IS 'the composite';", wantKey: "composite app.c", want: "the composite"},
		{name: "a range type", statement: "COMMENT ON TYPE app.r IS 'the range';", wantKey: "range app.r", want: "the range"},
		{name: "an extension", statement: "COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';", wantKey: "extension pgcrypto", want: "cryptographic functions"},
		{name: "a quoted extension", statement: `COMMENT ON EXTENSION "pgcrypto" IS 'quoted';`, wantKey: "extension pgcrypto", want: "quoted"},
		{name: "an enum type", statement: "COMMENT ON TYPE app.mood IS 'the enum';", wantKey: "enum app.mood", want: "the enum"},
		{
			name: "a materialized view", statement: "COMMENT ON MATERIALIZED VIEW app.mt IS 'the matview';",
			wantKey: "materialized view app.mt", want: "the matview",
		},
		{
			name: "a materialized view, the kind spread over lines", statement: "COMMENT ON MATERIALIZED\n  VIEW app.mt IS 'x';",
			wantKey: "materialized view app.mt", want: "x",
		},
		{
			name: "a function by its argument types", statement: "COMMENT ON FUNCTION app.f(integer, text) IS 'two';",
			wantKey: "function app.f(a int, b text default 'x')", want: "two",
		},
		{
			name: "a function by names and types", statement: "COMMENT ON FUNCTION app.f(a int4, b text) IS 'two';",
			wantKey: "function app.f(a int, b text default 'x')", want: "two",
		},
		{
			name: "a function with one overload, by its name alone", statement: "COMMENT ON FUNCTION app.g IS 'g';",
			wantKey: "function app.g()", want: "g",
		},
		{
			name: "a function with no arguments", statement: "COMMENT ON FUNCTION app.g() IS 'g';",
			wantKey: "function app.g()", want: "g",
		},
		{
			name: "a procedure", statement: "COMMENT ON PROCEDURE app.p(int) IS 'the procedure';",
			wantKey: "procedure app.p(a int)", want: "the procedure",
		},
		{
			name: "a trigger on its table", statement: "COMMENT ON TRIGGER audit ON app.t IS 'the trigger';",
			wantKey: "trigger audit on app.t", want: "the trigger",
		},
		{
			name: "a policy on its table", statement: "COMMENT ON POLICY own ON app.t IS 'the policy';",
			wantKey: "policy own on app.t", want: "the policy",
		},
		{
			name: "a constraint on its table", statement: "COMMENT ON CONSTRAINT positive ON app.t IS 'the check';",
			wantKey: "constraint positive on app.t", want: "the check",
		},
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

// A comment the model keeps no place for and one on an object the document
// does not declare are each refused by name rather than dropped.
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
			name:      "a lower-case name for a quoted mixed-case view",
			statement: "CREATE VIEW \"Recent\" AS SELECT 1;\nCOMMENT ON VIEW recent IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON VIEW recent names an object this schema does not declare`,
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
			name:      "an overload the document does not declare",
			statement: "COMMENT ON FUNCTION app.f(bigint) IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON FUNCTION app.f names an object this schema does not declare`,
		},
		{
			name:      "a function named as a procedure",
			statement: "COMMENT ON PROCEDURE app.g() IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON PROCEDURE app.g names an object this schema does not declare`,
		},
		{
			name:      "a trigger on a table it is not on",
			statement: "COMMENT ON TRIGGER audit ON app.w IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON TRIGGER audit names an object this schema does not declare`,
		},
		{
			name:      "a policy without its table",
			statement: "COMMENT ON POLICY own IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON POLICY own names an object this schema does not declare`,
		},
		{
			name:      "a domain's constraint",
			statement: "COMMENT ON CONSTRAINT positive ON DOMAIN app.d IS 'x';",
			wantErr: `the schema model has no place for this statement: COMMENT ON CONSTRAINT positive ON DOMAIN app.d: ` +
				`Ptah keeps no comment for a domain's constraint`,
		},
		{
			name:      "a kind of more than one word the model has no comment for",
			statement: "COMMENT ON FOREIGN TABLE app.ft IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON FOREIGN TABLE: Ptah keeps no comment for this kind of object`,
		},
		{
			name:      "a kind with a form of its own",
			statement: "COMMENT ON CAST (int AS text) IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON CAST: Ptah keeps no comment for this kind of object`,
		},
		{
			name:      "a view the document does not declare",
			statement: "COMMENT ON VIEW app.nope IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON VIEW app.nope names an object this schema does not declare`,
		},
		{
			name:      "a type in another schema",
			statement: "COMMENT ON TYPE public.c IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON TYPE public.c names an object this schema does not declare`,
		},
		{
			name:      "an extension the document does not declare",
			statement: "COMMENT ON EXTENSION citext IS 'x';",
			wantErr:   `the schema model has no place for this statement: COMMENT ON EXTENSION citext names an object this schema does not declare`,
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

// The objects whose comment is a statement of its own fold the same way: the
// view, the sequence, the domain, the two types and the extension below are
// each named once in upper and once in lower case (stokaro/ptah#3627).
func TestRead_CommentOn_FoldsTheNameOfEveryObjectKind(t *testing.T) {
	c := qt.New(t)
	const file = "CREATE SCHEMA App;\nCREATE TABLE App.T (Id int PRIMARY KEY);\n" +
		"CREATE VIEW App.V AS SELECT Id FROM App.T;\nCREATE SEQUENCE App.S;\nCREATE DOMAIN App.D AS int;\n" +
		"CREATE TYPE App.C AS (N int);\nCREATE TYPE App.R AS RANGE (subtype = int4);\nCREATE EXTENSION HStore;\n" +
		"COMMENT ON VIEW App.v IS 'the view';\nCOMMENT ON SEQUENCE APP.S IS 'the sequence';\n" +
		"COMMENT ON DOMAIN app.D IS 'the domain';\nCOMMENT ON TYPE APP.c IS 'the composite';\n" +
		"COMMENT ON TYPE app.R IS 'the range';\nCOMMENT ON EXTENSION HSTORE IS 'the extension';"

	database, _, err := sqlschema.Read([]byte(file), "postgres")

	c.Assert(err, qt.IsNil)
	comments := commentsOf(database)
	c.Assert(comments["view app.v"], qt.Equals, "the view")
	c.Assert(comments["sequence app.s"], qt.Equals, "the sequence")
	c.Assert(comments["domain app.d"], qt.Equals, "the domain")
	c.Assert(comments["composite app.c"], qt.Equals, "the composite")
	c.Assert(comments["range app.r"], qt.Equals, "the range")
	c.Assert(comments["extension hstore"], qt.Equals, "the extension")
}

// A COMMENT ON reaches a view by the rule a table is reached by: exact after
// the source dialect's fold, and without case where the server compares names
// without case (stokaro/ptah#3642).
func TestRead_CommentOn_ResolvesAViewAsTheDialectDoes(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		file    string
		want    string
	}{
		{
			name:    "SQLite, another ASCII case",
			dialect: "sqlite",
			file:    "CREATE VIEW Recent AS SELECT 1;\nCOMMENT ON VIEW recent IS 'x';",
			want:    "Recent",
		},
		{
			name:    "SQL Server, another case past ASCII",
			dialect: "sqlserver",
			file:    "CREATE VIEW Ärger AS SELECT 1;\nCOMMENT ON VIEW ärger IS 'x';",
			want:    "Ärger",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.file), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(commentsOf(database)["view "+test.want], qt.Equals, "x")
		})
	}
}

// overloadedBase is a document from an earlier file holding two overloads of
// one function. It is built rather than read, because a read keeps one
// function per name (stokaro/ptah#3672).
func overloadedBase() *schemamodel.Database {
	return &schemamodel.Database{Functions: []schemamodel.Function{
		{Name: "app.f", Parameters: "a int", Returns: "int", Language: "sql", Body: "SELECT 1"},
		{Name: "app.f", Parameters: "a int, b text", Returns: "int", Language: "sql", Body: "SELECT 2"},
	}}
}

// A comment reaches the overload its argument types name, and a bare name
// that could be either overload is refused rather than applied to one.
func TestReadOnto_CommentOnOneOverload(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      []string
	}{
		{name: "the first", statement: "COMMENT ON FUNCTION app.f(integer) IS 'x';", want: []string{"x", ""}},
		{name: "the second", statement: "COMMENT ON FUNCTION app.f(int, text) IS 'x';", want: []string{"", "x"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			base := overloadedBase()

			_, _, err := sqlschema.ReadOnto([]byte(test.statement), "postgres", base)

			c.Assert(err, qt.IsNil)
			c.Assert([]string{base.Functions[0].Comment, base.Functions[1].Comment}, qt.DeepEquals, test.want)
		})
	}
}

func TestReadOnto_CommentOnAnAmbiguousOverload(t *testing.T) {
	c := qt.New(t)
	base := overloadedBase()

	_, _, err := sqlschema.ReadOnto([]byte("COMMENT ON FUNCTION app.f IS 'x';"), "postgres", base)

	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(err, qt.ErrorMatches, `the schema model has no place for this statement: COMMENT ON FUNCTION app.f names more than one `+
		`declared overload; write its argument types to name one`)
	c.Assert([]string{base.Functions[0].Comment, base.Functions[1].Comment}, qt.DeepEquals, []string{"", ""})
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
