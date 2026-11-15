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
		statements = append(statements, fmt.Sprintf("CREATE FUNCTION %s() RETURNS %s AS $$%s$$",
			qualified(function.Schema, function.Name), function.Returns, function.Body))
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
		c.Assert(got.Statements, qt.Contains, "CREATE FUNCTION auth.uid() RETURNS uuid AS $$SELECT NULL::uuid$$")
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
