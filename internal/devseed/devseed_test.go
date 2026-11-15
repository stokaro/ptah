package devseed_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/devseed"
)

// qualified spells schema.name, or name alone for an object a read left in
// the connection's own schema.
func qualified(schema, name string) string {
	return strings.TrimPrefix(schema+"."+name, ".")
}

// render stands in for the planner: one statement per table, foreign key,
// policy and function, spelled so the names they read are in their code.
// Feature objects render nothing; the statements that use them name them.
func render(state *catalog.Database) ([]string, error) {
	var statements []string
	for _, table := range state.Tables {
		columns := make([]string, 0, len(table.Columns))
		for _, column := range table.Columns {
			columns = append(columns, column.Name+" "+column.DataType)
		}
		statements = append(statements, fmt.Sprintf("CREATE TABLE %s (%s)", qualified(table.Schema, table.Name), strings.Join(columns, ", ")))
	}
	for _, constraint := range state.Constraints {
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (id)",
			qualified(constraint.Schema, constraint.TableName), constraint.Name, constraint.ColumnName,
			qualified(constraint.ForeignSchema, *constraint.ForeignTable)))
	}
	for _, policy := range state.RLSPolicies {
		statements = append(statements, fmt.Sprintf("CREATE POLICY %s ON %s USING (%s)", policy.Name, policy.Table, policy.UsingExpression))
	}
	for _, function := range state.Functions {
		statements = append(statements, fmt.Sprintf("CREATE FUNCTION %s() RETURNS %s LANGUAGE %s AS $$%s$$",
			qualified(function.Schema, function.Name), function.Returns, function.Language, function.Body))
	}
	return statements, nil
}

// renderRefusingDanglingKeys is render that refuses a foreign key to a table
// the state does not hold, as the conversion before the planner does:
// `invalid foreign key: field "org_id" references unknown table "rebase.orgs"`.
func renderRefusingDanglingKeys(state *catalog.Database) ([]string, error) {
	tables := make(map[string]bool)
	for _, table := range state.Tables {
		tables[qualified(table.Schema, table.Name)] = true
	}
	for _, constraint := range state.Constraints {
		if referenced := qualified(constraint.ForeignSchema, *constraint.ForeignTable); !tables[referenced] {
			return nil, fmt.Errorf("invalid foreign key: references unknown table %q", referenced)
		}
	}
	return render(state)
}

// readOf is a target that holds read whatever it is asked for, and records
// the schemas each read was asked for in calls.
func readOf(read *catalog.Database, calls *[][]string) devseed.Target {
	return func(schemas []string) (*catalog.Database, error) {
		*calls = append(*calls, schemas)
		return read, nil
	}
}

func table(schema, name string, columns ...catalog.Column) catalog.Table {
	return catalog.Table{Schema: schema, Name: name, Columns: columns}
}

func column(name, dataType string) catalog.Column {
	return catalog.Column{Name: name, DataType: dataType}
}

func function(schema, name, body string) catalog.Function {
	return catalog.Function{Schema: schema, Name: name, Returns: "uuid", Language: "sql", Body: body}
}

func foreignKey(schema, tableName, name, columnName, foreignSchema, foreignTable string) catalog.Constraint {
	return catalog.Constraint{
		Schema: schema, TableName: tableName, Name: name, Type: "FOREIGN KEY", ColumnName: columnName,
		ForeignSchema: foreignSchema, ForeignTable: new(foreignTable), ForeignColumn: new("id"),
	}
}

func schemas(names ...string) []catalog.Schema {
	out := make([]catalog.Schema, 0, len(names))
	for _, name := range names {
		out = append(out, catalog.Schema{Name: name})
	}
	return out
}

func features(objects ...schemaext.Object) schemaext.Objects {
	return must.Must(schemaext.NewObjects(objects...))
}

// notes is the compared state of the --include public.notes case: one table
// whose policy calls a function in the auth schema the selection leaves out.
func notes() *catalog.Database {
	return &catalog.Database{
		Tables:      []catalog.Table{table("", "notes", column("id", "integer"), column("owner", "uuid"))},
		RLSPolicies: []catalog.RLSPolicy{{Name: "notes_owner", Table: "notes", UsingExpression: "owner = auth.uid()"}},
	}
}

var options = devseed.Options{Dialect: "postgres", DefaultSchema: "public", SearchPath: []string{"$user", "public"}}

func TestClosure_HappyPath(t *testing.T) {
	t.Run("a policy that calls a function outside the scope adds the function and its schema", func(t *testing.T) {
		c := qt.New(t)
		read := &catalog.Database{
			Schemas:   schemas("public", "auth"),
			Tables:    notes().Tables,
			Functions: []catalog.Function{function("auth", "uid", "SELECT NULL::uuid"), function("auth", "unused", "SELECT 1")},
		}
		var calls [][]string
		got, err := devseed.Closure(notes(), nil, render, readOf(read, &calls), options)
		c.Assert(err, qt.IsNil)
		c.Assert(got.Added, qt.DeepEquals, []devseed.Object{
			{Kind: "function", Schema: "auth", Name: "uid"},
			{Kind: "schema", Name: "auth"},
		})
		c.Assert(got.State.Functions, qt.DeepEquals, []catalog.Function{function("auth", "uid", "SELECT NULL::uuid")})
		c.Assert(got.State.Schemas, qt.DeepEquals, schemas("auth"))
		c.Assert(got.Statements, qt.Contains, "CREATE FUNCTION auth.uid() RETURNS uuid LANGUAGE sql AS $$SELECT NULL::uuid$$")
		c.Assert(calls, qt.DeepEquals, [][]string{{"auth", "public"}})
	})

	t.Run("what an added object needs is added in turn, reading a schema it names", func(t *testing.T) {
		c := qt.New(t)
		read := &catalog.Database{
			Schemas: schemas("public", "auth", "util"),
			Functions: []catalog.Function{
				function("auth", "uid", "SELECT util.claim('sub')"),
				function("util", "claim", "SELECT NULL::uuid"),
			},
		}
		var calls [][]string
		got, err := devseed.Closure(notes(), nil, render, readOf(read, &calls), options)
		c.Assert(err, qt.IsNil)
		c.Assert(got.Added, qt.DeepEquals, []devseed.Object{
			{Kind: "function", Schema: "auth", Name: "uid"},
			{Kind: "schema", Name: "auth"},
			{Kind: "function", Schema: "util", Name: "claim"},
			{Kind: "schema", Name: "util"},
		})
		c.Assert(calls, qt.DeepEquals, [][]string{{"auth", "public"}, {"auth", "public", "util"}})
	})

	t.Run("a planned foreign key adds the table it references, with that table's own foreign keys", func(t *testing.T) {
		c := qt.New(t)
		seed := &catalog.Database{Tables: []catalog.Table{table("", "tasks", column("id", "uuid"), column("user_id", "uuid"))}}
		read := &catalog.Database{
			Schemas: schemas("public", "rebase"),
			Tables: []catalog.Table{
				table("rebase", "users", column("id", "uuid"), column("org_id", "uuid")),
				table("rebase", "orgs", column("id", "uuid")),
				table("rebase", "audit", column("id", "uuid")),
			},
			Constraints: []catalog.Constraint{foreignKey("rebase", "users", "users_org_fk", "org_id", "rebase", "orgs")},
		}
		executed := []string{`ALTER TABLE "public"."tasks" ADD CONSTRAINT "tasks_user_fk" FOREIGN KEY ("user_id") REFERENCES "rebase"."users" ("id")`}
		var calls [][]string
		got, err := devseed.Closure(seed, executed, render, readOf(read, &calls), options)
		c.Assert(err, qt.IsNil)
		c.Assert(got.Added, qt.DeepEquals, []devseed.Object{
			{Kind: "table", Schema: "rebase", Name: "users"},
			{Kind: "schema", Name: "rebase"},
			{Kind: "table", Schema: "rebase", Name: "orgs"},
		})
		c.Assert(got.State.Constraints, qt.DeepEquals, read.Constraints)
	})

	t.Run("a foreign key of the compared state is followed before the state is rendered", func(t *testing.T) {
		c := qt.New(t)
		seed := &catalog.Database{
			Tables:      []catalog.Table{table("", "tasks", column("id", "uuid"), column("user_id", "uuid"))},
			Constraints: []catalog.Constraint{foreignKey("", "tasks", "tasks_user_fk", "user_id", "rebase", "users")},
		}
		read := &catalog.Database{
			Schemas: schemas("public", "rebase"),
			Tables: []catalog.Table{
				table("rebase", "users", column("id", "uuid"), column("org_id", "uuid")),
				table("rebase", "orgs", column("id", "uuid")),
			},
			Constraints: []catalog.Constraint{foreignKey("rebase", "users", "users_org_fk", "org_id", "rebase", "orgs")},
		}
		var calls [][]string
		got, err := devseed.Closure(seed, nil, renderRefusingDanglingKeys, readOf(read, &calls), options)
		c.Assert(err, qt.IsNil)
		c.Assert(got.Added, qt.DeepEquals, []devseed.Object{
			{Kind: "table", Schema: "rebase", Name: "users"},
			{Kind: "schema", Name: "rebase"},
			{Kind: "table", Schema: "rebase", Name: "orgs"},
		})
		c.Assert(calls, qt.DeepEquals, [][]string{{"public", "rebase"}})
	})

	t.Run("a column whose type is in a schema outside the scope adds the type", func(t *testing.T) {
		c := qt.New(t)
		seed := &catalog.Database{Tables: []catalog.Table{table("", "people",
			catalog.Column{Name: "feeling", DataType: "USER-DEFINED", UDTSchema: "auth", UDTName: "mood"})}}
		read := &catalog.Database{Schemas: schemas("public", "auth"), Enums: []catalog.Enum{{Schema: "auth", Name: "mood", Values: []string{"happy"}}}}
		var calls [][]string
		got, err := devseed.Closure(seed, nil, render, readOf(read, &calls), options)
		c.Assert(err, qt.IsNil)
		c.Assert(got.Added, qt.DeepEquals, []devseed.Object{
			{Kind: "enum", Schema: "auth", Name: "mood"},
			{Kind: "schema", Name: "auth"},
		})
	})

	t.Run("an unqualified name resolves along the search path only", func(t *testing.T) {
		c := qt.New(t)
		read := &catalog.Database{
			Schemas: schemas("public", "util", "other"),
			Functions: []catalog.Function{
				function("util", "touch", "SELECT NULL::uuid"),
				function("other", "touch", "SELECT NULL::uuid"),
			},
		}
		opts := devseed.Options{Dialect: "postgres", DefaultSchema: "public", SearchPath: []string{"public", "util"}}
		var calls [][]string
		got, err := devseed.Closure(&catalog.Database{}, []string{"SELECT touch()"}, render, readOf(read, &calls), opts)
		c.Assert(err, qt.IsNil)
		c.Assert(got.Added, qt.DeepEquals, []devseed.Object{
			{Kind: "function", Schema: "util", Name: "touch"},
			{Kind: "schema", Name: "util"},
		})
	})

	t.Run("a name inside a string literal or a comment names nothing", func(t *testing.T) {
		c := qt.New(t)
		read := &catalog.Database{
			Schemas:   schemas("public", "auth"),
			Functions: []catalog.Function{function("auth", "uid", "SELECT NULL::uuid"), function("auth", "jwt", "SELECT NULL::uuid")},
		}
		executed := []string{"SELECT 'auth.uid()' -- auth.jwt()\n/* auth.uid() */"}
		var calls [][]string
		got, err := devseed.Closure(&catalog.Database{}, executed, render, readOf(read, &calls), options)
		c.Assert(err, qt.IsNil)
		c.Assert(got.Added, qt.IsNil)
		c.Assert(got.State.Functions, qt.IsNil)
	})

	t.Run("an extension is added through the schema it is installed in", func(t *testing.T) {
		c := qt.New(t)
		read := &catalog.Database{
			Schemas: schemas("public", "extensions"),
			FeatureObjects: features(
				must.Must(pgextension.ObservedObject(pgextension.Ref("pgcrypto"), pgextension.ObservedExtension{Schema: "extensions", Version: "1.3"})),
				must.Must(pgextension.ObservedObject(pgextension.Ref("citext"), pgextension.ObservedExtension{Schema: "public", Version: "1.6"})),
			),
		}
		executed := []string{"ALTER TABLE tokens ADD COLUMN secret bytea DEFAULT extensions.gen_random_bytes(16)"}
		var calls [][]string
		got, err := devseed.Closure(&catalog.Database{}, executed, render, readOf(read, &calls), options)
		c.Assert(err, qt.IsNil)
		c.Assert(got.Added, qt.DeepEquals, []devseed.Object{
			{Kind: "extension", Name: "pgcrypto"},
			{Kind: "schema", Name: "extensions"},
		})
		_, found, err := got.State.FeatureObjects.Get(pgextension.Ref("pgcrypto"))
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
	})

	t.Run("a column whose type is a domain outside the scope adds the domain", func(t *testing.T) {
		c := qt.New(t)
		ref := pgtype.Ref(pgtype.DomainKind, "auth", "email")
		seed := &catalog.Database{Tables: []catalog.Table{table("", "users", column("mail", "auth.email"))}}
		read := &catalog.Database{
			Schemas:        schemas("public", "auth"),
			FeatureObjects: features(must.Must(pgtype.ObservedObject(ref, &pgtype.ObservedDomain{BaseType: "text"}))),
		}
		var calls [][]string
		got, err := devseed.Closure(seed, nil, render, readOf(read, &calls), options)
		c.Assert(err, qt.IsNil)
		c.Assert(got.Added, qt.DeepEquals, []devseed.Object{
			{Kind: "domain", Schema: "auth", Name: "email"},
			{Kind: "schema", Name: "auth"},
		})
		_, found, err := got.State.FeatureObjects.Get(ref)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
	})

	t.Run("a routine body in SQL is read as code and one in another language is not", func(t *testing.T) {
		c := qt.New(t)
		python := function("", "summary", "return auth.jwt()")
		python.Language = "plpython3u"
		seed := &catalog.Database{Functions: []catalog.Function{function("", "owner_of", "SELECT auth.uid()"), python}}
		read := &catalog.Database{
			Schemas:   schemas("public", "auth"),
			Functions: []catalog.Function{function("auth", "uid", "SELECT NULL::uuid"), function("auth", "jwt", "SELECT NULL::uuid")},
		}
		var calls [][]string
		got, err := devseed.Closure(seed, nil, render, readOf(read, &calls), options)
		c.Assert(err, qt.IsNil)
		c.Assert(got.Added, qt.DeepEquals, []devseed.Object{
			{Kind: "function", Schema: "auth", Name: "uid"},
			{Kind: "schema", Name: "auth"},
		})
	})

	t.Run("a nil target adds nothing and leaves the state as it is", func(t *testing.T) {
		c := qt.New(t)
		seed := notes()
		got, err := devseed.Closure(seed, nil, render, nil, options)
		c.Assert(err, qt.IsNil)
		c.Assert(got.State, qt.Equals, seed)
		c.Assert(got.Added, qt.IsNil)
		c.Assert(got.Statements, qt.DeepEquals, must.Must(render(seed)))
	})

	t.Run("the compared state is not changed", func(t *testing.T) {
		c := qt.New(t)
		seed := notes()
		read := &catalog.Database{Schemas: schemas("public", "auth"), Functions: []catalog.Function{function("auth", "uid", "SELECT NULL::uuid")}}
		var calls [][]string
		_, err := devseed.Closure(seed, nil, render, readOf(read, &calls), options)
		c.Assert(err, qt.IsNil)
		c.Assert(seed, qt.DeepEquals, notes())
	})
}

// authUID is the read of a target whose auth schema holds uid() and the
// accounts table, which the statements below name from public.
func authUID() *catalog.Database {
	return &catalog.Database{
		Schemas:   schemas("public", "auth"),
		Tables:    []catalog.Table{table("auth", "accounts", column("id", "uuid"))},
		Functions: []catalog.Function{function("auth", "uid", "SELECT NULL::uuid")},
	}
}

// addsAuthUID is what a statement that needs auth.uid() adds.
var addsAuthUID = []devseed.Object{{Kind: "function", Schema: "auth", Name: "uid"}, {Kind: "schema", Name: "auth"}}

// addsAuthAccounts is what a statement that needs auth.accounts adds.
var addsAuthAccounts = []devseed.Object{{Kind: "table", Schema: "auth", Name: "accounts"}, {Kind: "schema", Name: "auth"}}

// TestClosure_ReadsTheBodiesStatementsQuote runs, after the compared state,
// statements that quote a routine body as the planner writes them: the body
// as a string, the language before or after it. A body in SQL or PL/pgSQL
// names what its code names, as a routine of the state does; one in another
// language names nothing, and neither does a string that is not a body.
func TestClosure_ReadsTheBodiesStatementsQuote(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      []devseed.Object
	}{
		{
			name:      "CREATE FUNCTION with an SQL body",
			statement: "CREATE FUNCTION \"public\".\"whoami\"() RETURNS uuid AS $$\nSELECT auth.uid()\n$$\nLANGUAGE sql STABLE",
			want:      addsAuthUID,
		},
		{
			name:      "CREATE OR REPLACE FUNCTION with a tagged dollar quote",
			statement: `CREATE OR REPLACE FUNCTION whoami() RETURNS uuid LANGUAGE sql AS $body$ SELECT auth.uid() $body$`,
			want:      addsAuthUID,
		},
		{
			name:      "CREATE PROCEDURE",
			statement: `CREATE PROCEDURE touch() LANGUAGE sql AS $$ SELECT auth.uid() $$`,
			want:      addsAuthUID,
		},
		{
			name:      "a body quoted as a string, with a doubled quote",
			statement: `CREATE FUNCTION whoami() RETURNS uuid AS 'SELECT auth.uid() WHERE ''x'' = ''x''' LANGUAGE sql`,
			want:      addsAuthUID,
		},
		{
			name:      "a row type a PL/pgSQL body declares, its language spelled as a string",
			statement: `CREATE FUNCTION whoami() RETURNS uuid AS $$ DECLARE r auth.accounts%ROWTYPE; BEGIN RETURN auth.uid(); END $$ LANGUAGE 'plpgsql'`,
			want:      addsAuthAccounts,
		},
		{
			name:      "a column type a PL/pgSQL procedure declares",
			statement: `CREATE PROCEDURE touch() LANGUAGE plpgsql AS $$ DECLARE v auth.accounts.id%TYPE; BEGIN NULL; END $$`,
			want:      addsAuthAccounts,
		},
		{
			name:      "a call in a PL/pgSQL body, which PostgreSQL resolves when it runs",
			statement: `CREATE OR REPLACE FUNCTION whoami() RETURNS uuid LANGUAGE plpgsql AS $$ BEGIN PERFORM auth.uid(); RETURN NULL; END $$`,
		},
		{
			name:      "a call in a PL/pgSQL declaration's default",
			statement: `CREATE FUNCTION whoami() RETURNS uuid LANGUAGE plpgsql AS $$ DECLARE v uuid := auth.uid(); BEGIN RETURN v; END $$`,
		},
		{
			name:      "a call in a default spelled with DEFAULT or =",
			statement: `CREATE FUNCTION whoami() RETURNS uuid LANGUAGE plpgsql AS $$ DECLARE v uuid DEFAULT auth.uid(); w uuid = auth.uid(); BEGIN RETURN v; END $$`,
		},
		{
			name:      "a row type declared after a default that calls",
			statement: `CREATE FUNCTION whoami() RETURNS uuid LANGUAGE plpgsql AS $$ DECLARE v uuid := auth.uid(); r auth.accounts%ROWTYPE; BEGIN RETURN v; END $$`,
			want:      addsAuthAccounts,
		},
		{
			name:      "a row type a nested block declares",
			statement: `CREATE FUNCTION whoami() RETURNS uuid LANGUAGE plpgsql AS $$ <<outer>> DECLARE v uuid := auth.uid(); BEGIN DECLARE r auth.accounts%ROWTYPE; BEGIN NULL; END; RETURN v; END $$`,
			want:      addsAuthAccounts,
		},
		{
			name:      "an SQL-standard body, which is code",
			statement: `CREATE FUNCTION whoami() RETURNS uuid LANGUAGE sql BEGIN ATOMIC SELECT auth.uid(); END`,
			want:      addsAuthUID,
		},
		{
			name:      "a DO block, in the language a block that names none is in",
			statement: `DO $$ BEGIN PERFORM auth.uid(); END $$`,
			want:      addsAuthUID,
		},
		{
			name:      "a DO block that names its language first",
			statement: `DO LANGUAGE plpgsql $$ BEGIN PERFORM auth.uid(); END $$`,
			want:      addsAuthUID,
		},
		{
			name:      "a body after another statement in one text",
			statement: `SELECT 1; CREATE FUNCTION whoami() RETURNS uuid AS $$ SELECT auth.uid() $$ LANGUAGE sql`,
			want:      addsAuthUID,
		},
		{
			name:      "a body in another language",
			statement: `CREATE FUNCTION whoami() RETURNS uuid AS $$ return auth.uid() $$ LANGUAGE plpython3u`,
		},
		{
			name:      "a DO block in another language",
			statement: `DO LANGUAGE plpython3u $$ auth.uid() $$`,
		},
		{
			name:      "a string that is not a body",
			statement: `COMMENT ON TABLE notes IS $$ calls auth.uid() $$`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var calls [][]string
			got, err := devseed.Closure(&catalog.Database{}, []string{test.statement}, render, readOf(authUID(), &calls), options)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Added, qt.DeepEquals, test.want)
		})
	}
}

// TestClosure_ReadsWhatAPLpgSQLRoutineOfTheStateDeclares recreates a PL/pgSQL
// routine of the compared state with the row type its body declares, which
// PostgreSQL resolves when the routine is created, and without the routine its
// body calls, which PostgreSQL resolves only when it runs.
func TestClosure_ReadsWhatAPLpgSQLRoutineOfTheStateDeclares(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []devseed.Object
	}{
		{name: "a declared row type", body: "DECLARE r auth.accounts%ROWTYPE; BEGIN RETURN NULL; END", want: addsAuthAccounts},
		{name: "a call", body: "BEGIN RETURN auth.uid(); END"},
		{name: "a call in a default", body: "DECLARE v uuid := auth.uid(); BEGIN RETURN v; END"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			routine := function("", "whoami", test.body)
			routine.Language = "plpgsql"
			var calls [][]string
			got, err := devseed.Closure(&catalog.Database{Functions: []catalog.Function{routine}}, nil, render, readOf(authUID(), &calls), options)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Added, qt.DeepEquals, test.want)
		})
	}
}

// TestClosure_ReadsATypeAPLpgSQLDeclarationModifies recreates the extension
// that provides a type a PL/pgSQL body declares with a modifier, which reads
// like a call and PostgreSQL resolves when the routine is created, and leaves
// out the one a default calls, which it resolves only when the block runs.
func TestClosure_ReadsATypeAPLpgSQLDeclarationModifies(t *testing.T) {
	read := &catalog.Database{
		Schemas: schemas("public", "extensions"),
		FeatureObjects: features(
			must.Must(pgextension.ObservedObject(pgextension.Ref("vector"), pgextension.ObservedExtension{Schema: "extensions", Version: "0.8.0"})),
		),
	}
	tests := []struct {
		name      string
		statement string
		want      []devseed.Object
	}{
		{
			name:      "a declared type with a modifier",
			statement: `CREATE FUNCTION embed() RETURNS void LANGUAGE plpgsql AS $$ DECLARE v extensions.vector(3); BEGIN NULL; END $$`,
			want:      []devseed.Object{{Kind: "extension", Name: "vector"}, {Kind: "schema", Name: "extensions"}},
		},
		{
			name:      "a call in a default",
			statement: `CREATE FUNCTION embed() RETURNS void LANGUAGE plpgsql AS $$ DECLARE v text := extensions.vector_dims(NULL); BEGIN NULL; END $$`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var calls [][]string
			got, err := devseed.Closure(&catalog.Database{}, []string{test.statement}, render, readOf(read, &calls), options)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Added, qt.DeepEquals, test.want)
		})
	}
}

// TestClosure_UnqualifiedNamesStandWhereTheyName runs, after the compared
// state, statements that spell the name of an object of public the selection
// left out without its schema. A column named like a table or a function needs
// neither; a call needs the function, and a FROM the table.
func TestClosure_UnqualifiedNamesStandWhereTheyName(t *testing.T) {
	read := &catalog.Database{
		Schemas:   schemas("public"),
		Tables:    []catalog.Table{table("public", "items", column("id", "integer"))},
		Functions: []catalog.Function{function("public", "title", "SELECT NULL::uuid")},
	}
	tests := []struct {
		name      string
		statement string
		want      []devseed.Object
	}{
		{
			name:      "columns named like a table and a function",
			statement: `ALTER TABLE "public"."notes" ADD COLUMN "title" text NULL, ADD COLUMN "items" jsonb NULL`,
		},
		{
			name:      "a call and a FROM",
			statement: `CREATE VIEW "public"."v" AS SELECT title() AS t FROM items`,
			want:      []devseed.Object{{Kind: "table", Schema: "public", Name: "items"}, {Kind: "function", Schema: "public", Name: "title"}},
		},
		{
			name:      "a FROM list and a join as PostgreSQL prints them",
			statement: `CREATE VIEW v AS SELECT n.id FROM notes n, (notes m JOIN items i ON true)`,
			want:      []devseed.Object{{Kind: "table", Schema: "public", Name: "items"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var calls [][]string
			got, err := devseed.Closure(notes(), []string{test.statement}, render, readOf(read, &calls), options)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Added, qt.DeepEquals, test.want)
		})
	}
}

// TestClosure_FoldsAQuotedMixedCaseName names a function in a schema whose
// quoted names hold upper case and a dot. The statement's names and the
// read's are folded alike, so the function is found, and the read is asked
// for the schema folded, as [devseed.Target] says.
func TestClosure_FoldsAQuotedMixedCaseName(t *testing.T) {
	c := qt.New(t)
	seed := &catalog.Database{
		Tables:      notes().Tables,
		RLSPolicies: []catalog.RLSPolicy{{Name: "notes_owner", Table: "notes", UsingExpression: `owner = "Au.th"."Uid"()`}},
	}
	read := &catalog.Database{Schemas: schemas("public", "Au.th"), Functions: []catalog.Function{function("Au.th", "Uid", "SELECT NULL::uuid")}}
	var calls [][]string

	got, err := devseed.Closure(seed, nil, render, readOf(read, &calls), options)

	c.Assert(err, qt.IsNil)
	c.Assert(got.Added, qt.DeepEquals, []devseed.Object{{Kind: "function", Schema: "Au.th", Name: "Uid"}, {Kind: "schema", Name: "Au.th"}})
	c.Assert(calls, qt.Any(qt.DeepEquals), []string{"au.th", "public"})
}

// TestObjectsIn_HappyPath lists what a state holds in the schemas asked for,
// whatever their case, in the order a closure adds objects: an extension by
// the schema it is installed in, and an object a read left unqualified by the
// default schema.
func TestObjectsIn_HappyPath(t *testing.T) {
	state := &catalog.Database{
		Schemas:   schemas("auth", "util"),
		Tables:    []catalog.Table{table("auth", "accounts"), table("", "notes"), table("util", "logs")},
		Functions: []catalog.Function{function("auth", "uid", "SELECT NULL::uuid")},
		FeatureObjects: features(
			must.Must(pgextension.ObservedObject(pgextension.Ref("pgcrypto"), pgextension.ObservedExtension{Schema: "auth", Version: "1.3"})),
			must.Must(pgtype.ObservedObject(pgtype.Ref(pgtype.DomainKind, "util", "email"), &pgtype.ObservedDomain{BaseType: "text"})),
		),
	}
	tests := []struct {
		name    string
		schemas []string
		want    []devseed.Object
	}{
		{
			name:    "a schema the state holds objects in, spelled in another case",
			schemas: []string{"AUTH"},
			want: []devseed.Object{
				{Kind: "table", Schema: "auth", Name: "accounts"},
				{Kind: "function", Schema: "auth", Name: "uid"},
				{Kind: "extension", Name: "pgcrypto"},
			},
		},
		{
			name:    "the default schema",
			schemas: []string{"public"},
			want:    []devseed.Object{{Kind: "table", Name: "notes"}},
		},
		{
			name:    "a domain",
			schemas: []string{"util"},
			want:    []devseed.Object{{Kind: "table", Schema: "util", Name: "logs"}, {Kind: "domain", Schema: "util", Name: "email"}},
		},
		{name: "a schema it holds nothing in", schemas: []string{"other"}},
		{name: "no schema at all"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := devseed.ObjectsIn(state, test.schemas, options)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestObjectsIn_FailurePath is a state that is not there.
func TestObjectsIn_FailurePath(t *testing.T) {
	c := qt.New(t)
	got, err := devseed.ObjectsIn(nil, []string{"auth"}, options)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.IsNil)
}

var errRender = errors.New("render refused")

// renderRefusingFunctions is render that refuses a state holding a function,
// as a dev database refuses an object it cannot build.
func renderRefusingFunctions(state *catalog.Database) ([]string, error) {
	if len(state.Functions) > 0 {
		return nil, errRender
	}
	return render(state)
}

func TestClosure_FailurePath(t *testing.T) {
	t.Run("a failure after an object was added names what was added", func(t *testing.T) {
		c := qt.New(t)
		read := &catalog.Database{Schemas: schemas("public", "auth"), Functions: []catalog.Function{function("auth", "uid", "SELECT NULL::uuid")}}
		var calls [][]string
		got, err := devseed.Closure(notes(), nil, renderRefusingFunctions, readOf(read, &calls), options)
		c.Assert(err, qt.ErrorIs, errRender)
		c.Assert(err, qt.ErrorMatches, `render refused; the rehearsal recreates these objects from outside the compared scope because the planned statements need them: function "auth.uid", schema "auth"`)
		added, ok := errors.AsType[*devseed.AddedError](err)
		c.Assert(ok, qt.IsTrue)
		c.Assert(added.Added, qt.DeepEquals, []devseed.Object{{Kind: "function", Schema: "auth", Name: "uid"}, {Kind: "schema", Name: "auth"}})
		c.Assert(got, qt.DeepEquals, devseed.Result{})
	})

	t.Run("a failure before anything was added is returned as it is", func(t *testing.T) {
		c := qt.New(t)
		var calls [][]string
		got, err := devseed.Closure(notes(), nil, func(*catalog.Database) ([]string, error) {
			return nil, errRender
		}, readOf(&catalog.Database{}, &calls), options)
		c.Assert(err, qt.Equals, errRender)
		c.Assert(got, qt.DeepEquals, devseed.Result{})
		c.Assert(calls, qt.IsNil)
	})

	t.Run("a read of the target that fails is returned with what it was reading", func(t *testing.T) {
		c := qt.New(t)
		errRead := errors.New("permission denied for schema auth")
		got, err := devseed.Closure(notes(), nil, render, func([]string) (*catalog.Database, error) {
			return nil, errRead
		}, options)
		c.Assert(err, qt.ErrorIs, errRead)
		c.Assert(err, qt.ErrorMatches, `read the target outside the compared scope: permission denied for schema auth`)
		c.Assert(got, qt.DeepEquals, devseed.Result{})
	})
}

func TestWithAdded_HappyPath(t *testing.T) {
	t.Run("nothing added returns the error as it is", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(devseed.WithAdded(errRender, nil), qt.Equals, errRender)
	})
	t.Run("no error stays no error", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(devseed.WithAdded(nil, []devseed.Object{{Kind: "schema", Name: "auth"}}), qt.IsNil)
	})
	t.Run("an object of the connection's own schema is named without one", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(devseed.Names([]devseed.Object{{Kind: "table", Name: "orgs"}, {Kind: "function", Schema: "auth", Name: "uid"}}),
			qt.Equals, `table "orgs", function "auth.uid"`)
	})
}
