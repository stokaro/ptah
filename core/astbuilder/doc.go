// Package astbuilder constructs DDL abstract syntax trees programmatically,
// without writing nested struct literals by hand.
//
// # Relationship to core/ast
//
// ptah/core/ast is the model: dialect-agnostic nodes for CREATE TABLE, columns,
// constraints, indexes, enums, and comments, plus a StatementList that holds a
// whole schema. Those nodes are exported and can be assembled directly, but a
// table with a dozen columns and a foreign key is several levels of nesting, and
// the shape of the literal says little about the schema it describes.
//
// This package is one way to populate that model. Every builder returns
// core/ast nodes and nothing of its own: SchemaBuilder.Build returns an
// *ast.StatementList, TableBuilder.Build returns an *ast.CreateTableNode,
// IndexBuilder.Build returns an *ast.IndexNode, and the column and foreign key
// builders configure nodes already attached to their parent. A caller can mix
// the two styles freely, and an AST node this package does not cover is still
// reachable by building it directly.
//
// What consumes the result is unchanged either way. ptah/core/renderer turns the
// nodes into SQL for every dialect renderer.SupportedDialects names;
// ptah/migration/planner emits the same nodes when it plans a change.
//
// This package covers DDL. ptah/core/query is the DML counterpart, building
// SELECT, INSERT, UPDATE, and DELETE against the same AST.
//
// # Two entry points
//
// NewTable and NewIndex build one statement:
//
//	table := astbuilder.NewTable("users").
//		Comment("User accounts").
//		Column("id", "SERIAL").Primary().End().
//		Column("email", "VARCHAR(255)").NotNull().Unique().End().
//		Build()
//
// NewSchema builds a statement list, so tables, indexes, enums, and comments
// arrive in one AST in the order they were added:
//
//	schema := astbuilder.NewSchema().
//		Comment("User management schema").
//		Enum("user_status", "active", "inactive", "suspended").
//		Table("users").
//			Column("id", "SERIAL").Primary().End().
//			Column("status", "user_status").NotNull().Default("'active'").End().
//		End().
//		Index("idx_users_status", "users", "status").End().
//		Build()
//
// The schema entry point has its own builder types — SchemaTableBuilder,
// SchemaColumnBuilder, SchemaIndexBuilder, SchemaForeignKeyBuilder — with the
// same configuration methods as the standalone ones. They differ in where the
// chain returns to: End walks back up to the enclosing schema, and the enclosing
// schema is what Build reads. A standalone TableBuilder has no End, because
// there is nothing above it.
//
// # Navigation
//
// Every configuration method returns its own builder, so calls chain. Methods
// that open a nested scope return the nested builder, and End closes it:
//
//	NewSchema -> Table    -> Column     -> ForeignKey       -> End -> End -> End
//	             (table)     (column)      (foreign key)
//
// A column or a table-level constraint is attached to its parent as soon as it
// is opened, so End is navigation rather than a commit: forgetting it costs the
// rest of the chain, not the column.
//
// # Coverage
//
// Columns carry the constraints and attributes core/ast models for them:
// primary key, nullability, uniqueness, auto-increment, literal and expression
// defaults, check expressions, generated expressions, MySQL and MariaDB ON
// UPDATE expressions, character set, collation, and comments. Tables carry
// composite primary keys, named unique constraints, table-level foreign keys, a
// storage engine, and arbitrary key/value table options. Indexes carry
// uniqueness, IF NOT EXISTS, an index type, and a comment.
//
// Referential actions are passed as SQL text — "CASCADE", "RESTRICT",
// "SET NULL", "SET DEFAULT", "NO ACTION" — and reach the rendered constraint as
// written.
//
// # What the builders do not do
//
// They do not validate. A type name no dialect knows, a foreign key to a table
// that is not in the schema, or a default that does not parse is built into the
// AST and reported later: by ptah/core/renderer when the SQL is generated, or by
// the database when it is executed. Rendering a whole schema through
// renderer.GetOrderedCreateStatements is what checks foreign key ordering and
// capability support.
//
// They are not safe for concurrent use. One builder chain belongs to one
// goroutine; separate chains are independent.
package astbuilder
