package astbuilder_test

import (
	"fmt"

	"github.com/go-extras/go-kit/must"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/astbuilder"
	"go.5x5.cz/ptah/core/renderer"
)

// ExampleNewTable builds one CREATE TABLE and renders it for two dialects. The
// builder produces a dialect-agnostic *ast.CreateTableNode; the renderer is what
// decides quoting, constraint order, and which constraints a dialect leaves
// implicit.
func ExampleNewTable() {
	table := astbuilder.NewTable("users").
		Column("id", "SERIAL").Primary().End().
		Column("email", "VARCHAR(255)").NotNull().Unique().End().
		Column("created_at", "TIMESTAMP").NotNull().DefaultExpression("NOW()").End().
		Build()

	for _, dialect := range []string{"postgresql", "mysql"} {
		r := must.Must(renderer.NewRenderer(dialect))
		fmt.Print(must.Must(r.Render(table)))
	}

	// Output:
	// -- POSTGRES TABLE: users --
	// CREATE TABLE "users" (
	//   "id" SERIAL PRIMARY KEY NOT NULL,
	//   "email" VARCHAR(255) UNIQUE NOT NULL,
	//   "created_at" TIMESTAMP NOT NULL DEFAULT NOW()
	// );
	//
	// -- MYSQL TABLE: users --
	// CREATE TABLE `users` (
	//   `id` SERIAL PRIMARY KEY,
	//   `email` VARCHAR(255) NOT NULL UNIQUE,
	//   `created_at` TIMESTAMP NOT NULL DEFAULT NOW()
	// );
}

// ExampleNewTable_tableLevelConstraints uses the table-level constraint and
// option methods, which cover what a single column cannot say: a composite
// primary key, a named unique constraint, a storage engine, and a
// dialect-specific table option.
func ExampleNewTable_tableLevelConstraints() {
	table := astbuilder.NewTable("order_items").
		Engine("InnoDB").
		Option("CHARSET", "utf8mb4").
		Column("order_id", "BIGINT").NotNull().End().
		Column("sku", "VARCHAR(50)").NotNull().End().
		PrimaryKey("order_id", "sku").
		Unique("uk_order_items_sku", "sku").
		Build()

	r := must.Must(renderer.NewRenderer("mysql"))
	fmt.Print(must.Must(r.Render(table)))

	// Output:
	// -- MYSQL TABLE: order_items --
	// CREATE TABLE `order_items` (
	//   `order_id` BIGINT NOT NULL,
	//   `sku` VARCHAR(50) NOT NULL,
	//   PRIMARY KEY (`order_id`, `sku`),
	//   CONSTRAINT `uk_order_items_sku` UNIQUE (`sku`)
	// ) ENGINE=InnoDB CHARSET=utf8mb4;
}

// ExampleNewIndex builds standalone CREATE INDEX statements. Columns are
// positional, so the order given is the index order.
func ExampleNewIndex() {
	plain := astbuilder.NewIndex("idx_posts_author_date", "posts", "author_id", "created_at").Build()
	unique := astbuilder.NewIndex("uq_users_email", "users", "email").
		Unique().
		IfNotExists().
		Build()

	r := must.Must(renderer.NewRenderer("postgresql"))
	for _, index := range []*ast.IndexNode{plain, unique} {
		fmt.Print(must.Must(r.Render(index)))
	}

	// Output:
	// CREATE INDEX "idx_posts_author_date" ON "posts" ("author_id", "created_at");
	// CREATE UNIQUE INDEX IF NOT EXISTS "uq_users_email" ON "users" ("email");
}

// ExampleNewTable_mixedWithAst appends a hand-built core/ast constraint to a
// table the builder produced. Build returns a plain *ast.CreateTableNode, so a
// construct the builders do not model — here a PostgreSQL EXCLUDE constraint —
// stays reachable through core/ast directly, and the mixed result renders like
// any other node.
func ExampleNewTable_mixedWithAst() {
	table := astbuilder.NewTable("bookings").
		Column("room_id", "INTEGER").NotNull().End().
		Column("during", "TSRANGE").NotNull().End().
		Build()

	table.AddConstraint(ast.NewExcludeConstraint(
		"no_overlapping_bookings", "gist", "room_id WITH =, during WITH &&"))

	r := must.Must(renderer.NewRenderer("postgresql"))
	fmt.Print(must.Must(r.Render(table)))

	// Output:
	// -- POSTGRES TABLE: bookings --
	// CREATE TABLE "bookings" (
	//   "room_id" INTEGER NOT NULL,
	//   "during" TSRANGE NOT NULL,
	//   CONSTRAINT "no_overlapping_bookings" EXCLUDE USING gist (room_id WITH =, during WITH &&)
	// );
}

// ExampleNewTable_generatedColumn declares a computed column with
// Generated(expression, kind). The expression is raw SQL rendered inside
// GENERATED ALWAYS AS (...); kind selects the storage form, and each dialect
// renderer decides what it accepts — PostgreSQL takes only STORED.
func ExampleNewTable_generatedColumn() {
	table := astbuilder.NewTable("order_lines").
		Column("price", "NUMERIC(10,2)").NotNull().Check("price >= 0").End().
		Column("quantity", "INTEGER").NotNull().Default("1").End().
		Column("total", "NUMERIC(10,2)").Generated("price * quantity", "STORED").End().
		Build()

	for _, dialect := range []string{"postgresql", "mysql"} {
		r := must.Must(renderer.NewRenderer(dialect))
		fmt.Print(must.Must(r.Render(table)))
	}

	// Output:
	// -- POSTGRES TABLE: order_lines --
	// CREATE TABLE "order_lines" (
	//   "price" NUMERIC(10,2) NOT NULL CHECK (price >= 0),
	//   "quantity" INTEGER NOT NULL DEFAULT 1,
	//   "total" NUMERIC(10,2) GENERATED ALWAYS AS (price * quantity) STORED
	// );
	//
	// -- MYSQL TABLE: order_lines --
	// CREATE TABLE `order_lines` (
	//   `price` NUMERIC(10,2) NOT NULL CHECK (price >= 0),
	//   `quantity` INTEGER NOT NULL DEFAULT 1,
	//   `total` NUMERIC(10,2) GENERATED ALWAYS AS (price * quantity) STORED
	// );
}

// ExampleNewSchema builds a whole schema in one chain. Build returns an
// *ast.StatementList holding the nodes in the order they were added, so an enum
// a column depends on is declared before the table that uses it.
func ExampleNewSchema() {
	schema := astbuilder.NewSchema().
		Enum("user_status", "active", "suspended").
		Table("users").
		Column("id", "SERIAL").Primary().End().
		Column("status", "user_status").NotNull().Default("'active'").End().
		End().
		Table("posts").
		Column("id", "SERIAL").Primary().End().
		Column("user_id", "INTEGER").NotNull().
		ForeignKey("users", "id", "fk_posts_user").
		OnDelete("CASCADE").
		End().
		End().
		Index("idx_posts_user", "posts", "user_id").End().
		Build()

	for _, statement := range schema.Statements {
		fmt.Printf("%T\n", statement)
	}

	r := must.Must(renderer.NewRenderer("postgresql"))
	fmt.Print(must.Must(r.Render(schema)))

	// Output:
	// *ast.EnumNode
	// *ast.CreateTableNode
	// *ast.CreateTableNode
	// *ast.IndexNode
	// CREATE TYPE "user_status" AS ENUM ('active', 'suspended');
	// -- POSTGRES TABLE: users --
	// CREATE TABLE "users" (
	//   "id" SERIAL PRIMARY KEY NOT NULL,
	//   "status" user_status NOT NULL DEFAULT 'active'
	// );
	//
	// -- POSTGRES TABLE: posts --
	// CREATE TABLE "posts" (
	//   "id" SERIAL PRIMARY KEY NOT NULL,
	//   "user_id" INTEGER NOT NULL,
	//   CONSTRAINT "fk_posts_user" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE
	// );
	//
	// CREATE INDEX "idx_posts_user" ON "posts" ("user_id");
}
