package ast_test

import (
	"fmt"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
)

// ExampleNewCreateTable builds one CREATE TABLE with the fluent node API and
// renders it for two dialects. The node records what the author asked for and
// nothing about how it is written: quoting, clause order, and the spelling of
// a request such as auto-increment are the renderer's decisions, and a target
// that carries such a request in the column type rather than in a clause of
// its own adds nothing beside the column. Rendering is deterministic — the
// same node renders to the same bytes on every run.
func ExampleNewCreateTable() {
	table := ast.NewCreateTable("users").
		AddColumn(
			ast.NewColumn("id", "INTEGER").
				SetPrimary().
				SetAutoIncrement(),
		).
		AddColumn(
			ast.NewColumn("email", "VARCHAR(255)").
				SetNotNull(),
		).
		AddColumn(
			ast.NewColumn("created_at", "TIMESTAMP").
				SetNotNull().
				SetDefaultExpression("CURRENT_TIMESTAMP"),
		).
		AddConstraint(ast.NewUniqueConstraint("uk_users_email", "email"))

	for _, dialect := range []string{"postgresql", "mysql"} {
		r := must.Must(renderer.NewRenderer(dialect))
		fmt.Print(must.Must(r.Render(table)))
	}

	// Output:
	// -- POSTGRES TABLE: users --
	// CREATE TABLE "users" (
	//   "id" INTEGER PRIMARY KEY NOT NULL,
	//   "email" VARCHAR(255) NOT NULL,
	//   "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	//   CONSTRAINT "uk_users_email" UNIQUE ("email")
	// );
	//
	// -- MYSQL TABLE: users --
	// CREATE TABLE `users` (
	//   `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
	//   `email` VARCHAR(255) NOT NULL,
	//   `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	//   CONSTRAINT `uk_users_email` UNIQUE (`email`)
	// );
}

// ExampleAlterTableNode composes ALTER TABLE from a list of operations. An
// operation is not a statement on its own: the renderer is handed the whole
// node through [ast.Visitor], and it decides how many statements the list
// becomes and what each one looks like — which is why calling an operation's
// own Accept renders nothing.
func ExampleAlterTableNode() {
	alter := &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{
				Column: ast.NewColumn("nickname", "VARCHAR(64)").SetNotNull(),
			},
			&ast.DropColumnOperation{ColumnName: "legacy_flags"},
			&ast.AddConstraintOperation{
				Constraint: ast.NewUniqueConstraint("uk_users_nickname", "nickname"),
			},
		},
	}

	for _, dialect := range []string{"postgresql", "mysql"} {
		r := must.Must(renderer.NewRenderer(dialect))
		fmt.Print(must.Must(r.Render(alter)))
	}

	// Output:
	// -- ALTER statements: --
	// ALTER TABLE "users" ADD COLUMN "nickname" VARCHAR(64) NOT NULL;
	// ALTER TABLE "users" DROP COLUMN "legacy_flags";
	// ALTER TABLE "users" ADD CONSTRAINT "uk_users_nickname" UNIQUE ("nickname");
	//
	// -- ALTER statements: --
	// ALTER TABLE `users` ADD COLUMN `nickname` VARCHAR(64) NOT NULL;
	// ALTER TABLE `users` DROP COLUMN `legacy_flags`;
	// ALTER TABLE `users` ADD CONSTRAINT `uk_users_nickname` UNIQUE (`nickname`);
}

// ExampleNewCreateType declares a PostgreSQL enum type and then widens it with
// an ALTER TYPE operation. CreateTypeNode carries a TypeDefinition (here an
// enum); AlterTypeNode carries TypeOperation values, and SetAfter places the
// new value inside the existing order.
func ExampleNewCreateType() {
	createType := ast.NewCreateType("status", ast.NewEnumTypeDef("active", "inactive"))
	alterType := ast.NewAlterType("status").
		AddOperation(ast.NewAddEnumValueOperation("archived").SetAfter("inactive"))

	r := must.Must(renderer.NewRenderer("postgresql"))
	fmt.Print(must.Must(r.Render(createType)))
	fmt.Print(must.Must(r.Render(alterType)))

	// Output:
	// CREATE TYPE "status" AS ENUM ('active', 'inactive');
	// ALTER TYPE "status" ADD VALUE 'archived' AFTER 'inactive';
}
