package renderer_test

import (
	"errors"
	"fmt"

	"github.com/go-extras/go-kit/must"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/astbuilder"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/core/yamlschema"
)

// ExampleRenderSQL is the one-call path: hand over the dialect and the nodes
// and get SQL back, with no renderer to hold on to. The node comes from
// core/astbuilder, the intended companion for writing AST nodes without struct
// literals.
func ExampleRenderSQL() {
	table := astbuilder.NewTable("products").
		Column("id", "SERIAL").Primary().End().
		Column("name", "VARCHAR(255)").NotNull().End().
		Build()

	sql, err := renderer.RenderSQL("postgres", table)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Print(sql)

	// Output:
	// -- POSTGRES TABLE: products --
	// CREATE TABLE "products" (
	//   "id" SERIAL PRIMARY KEY NOT NULL,
	//   "name" VARCHAR(255) NOT NULL
	// );
}

// ExampleNewRenderer constructs one renderer and reuses it across several
// nodes, which is what RenderSQL recommends once there is more than one call.
// Each Render returns only the SQL for the node it was handed, so sequential
// reuse is safe; one renderer must not be shared between goroutines.
func ExampleNewRenderer() {
	r := must.Must(renderer.NewRenderer("postgres"))

	users := astbuilder.NewTable("users").
		Column("id", "BIGSERIAL").Primary().End().
		Column("email", "VARCHAR(255)").NotNull().Unique().End().
		Build()
	index := astbuilder.NewIndex("idx_users_email", "users", "email").Build()

	for _, node := range []ast.Node{users, index} {
		sql, err := r.Render(node)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Print(sql)
	}

	// Output:
	// -- POSTGRES TABLE: users --
	// CREATE TABLE "users" (
	//   "id" BIGSERIAL PRIMARY KEY NOT NULL,
	//   "email" VARCHAR(255) UNIQUE NOT NULL
	// );
	//
	// CREATE INDEX "idx_users_email" ON "users" ("email");
}

// ExampleNewRenderer_unsupportedDialect shows the error contract for a dialect
// nothing renders for: an error satisfying errors.Is with
// ptaherr.ErrUnsupportedDialect. The message is printed here to show what an
// operator sees; branch on the sentinel rather than on its wording.
func ExampleNewRenderer_unsupportedDialect() {
	_, err := renderer.NewRenderer("db2")

	fmt.Println(err)
	fmt.Println(errors.Is(err, ptaherr.ErrUnsupportedDialect))

	// Output:
	// unsupported database dialect: db2
	// true
}

// ExampleGetOrderedCreateStatements renders a whole schema with the two-phase
// foreign key guarantee visible: users and posts reference each other, so no
// table order could satisfy both keys inline, and both CREATE TABLE statements
// are emitted before the foreign keys arrive as phase-two ALTER TABLE
// statements. The schema is authored in YAML for readability; Go annotations,
// HCL, SQL, and DBML produce the same schemamodel.Database.
func ExampleGetOrderedCreateStatements() {
	document := []byte(`
tables:
  users:
    columns:
      id:
        type: SERIAL
        primary: true
      favorite_post_id:
        type: INTEGER
        foreign: posts(id)
        foreign_key_name: fk_users_favorite_post
  posts:
    columns:
      id:
        type: SERIAL
        primary: true
      user_id:
        type: INTEGER
        not_null: true
        foreign: users(id)
        foreign_key_name: fk_posts_user
`)

	db := must.Must(yamlschema.Parse(document))
	statements, err := renderer.GetOrderedCreateStatements(db, "postgres")
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, statement := range statements {
		fmt.Print(statement)
	}

	// Output:
	// -- POSTGRES TABLE: posts --
	// CREATE TABLE "posts" (
	//   "id" SERIAL PRIMARY KEY NOT NULL,
	//   "user_id" INTEGER NOT NULL
	// );
	//
	// -- POSTGRES TABLE: users --
	// CREATE TABLE "users" (
	//   "id" SERIAL PRIMARY KEY NOT NULL,
	//   "favorite_post_id" INTEGER
	// );
	//
	// -- ALTER statements: --
	// ALTER TABLE "posts" ADD CONSTRAINT "fk_posts_user" FOREIGN KEY ("user_id") REFERENCES "users"("id");
	//
	// -- ALTER statements: --
	// ALTER TABLE "users" ADD CONSTRAINT "fk_users_favorite_post" FOREIGN KEY ("favorite_post_id") REFERENCES "posts"("id");
}

// ExampleValidateSchema fails closed without rendering: the same schema passes
// for a dialect that supports covering indexes and is refused, against the
// ptaherr.ErrUnsupportedFeature sentinel, for one that does not. It is the
// pre-flight an embedder runs to learn whether a schema is renderable on a
// target before asking for any SQL.
func ExampleValidateSchema() {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Order", Name: "customer_id", Type: "BIGINT"},
			{StructName: "Order", Name: "total", Type: "NUMERIC(10,2)"},
		},
		Indexes: []schemamodel.Index{{
			StructName:     "Order",
			Name:           "idx_orders_customer",
			Fields:         []string{"customer_id"},
			IncludeColumns: []string{"total"},
		}},
	}

	fmt.Println(renderer.ValidateSchema(db, "postgres"))

	err := renderer.ValidateSchema(db, "mysql")
	fmt.Println(err)
	fmt.Println(errors.Is(err, ptaherr.ErrUnsupportedFeature))

	// Output:
	// <nil>
	// mysql does not support INCLUDE columns on index "idx_orders_customer"; target postgres, yugabytedb, or spanner
	// true
}
