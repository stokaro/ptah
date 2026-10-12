package main

import (
	"fmt"
	"io"

	"ptah.run/core/ast"
	"ptah.run/core/astbuilder"
	"ptah.run/engine/builtin"
)

// DemonstrateASTApproach shows how to use the new AST-based SQL generation
func DemonstrateASTApproach(w io.Writer) error {
	fmt.Fprintln(w, "=== AST-Based SQL Generation Example ===")

	// Example 1: Building a simple table using the fluent API
	fmt.Fprintln(w, "1. Simple table using fluent API:")

	table := astbuilder.NewTable("users").
		Comment("User accounts table").
		Column("id", "SERIAL").Primary().End().
		Column("email", "VARCHAR(255)").NotNull().Unique().End().
		Column("name", "VARCHAR(100)").NotNull().End().
		Column("created_at", "TIMESTAMP").DefaultExpression("NOW()").End().
		Column("is_active", "BOOLEAN").Default("true").End().
		Build()

	// Render for PostgreSQL
	pgRenderer, err := builtin.NewRenderer("postgresql")
	if err != nil {
		return err
	}
	pgSQL, err := pgRenderer.Render(table)
	if err != nil {
		return err
	}

	fmt.Fprintln(w, "PostgreSQL:")
	fmt.Fprintln(w, pgSQL)

	// Render for MySQL
	mysqlRenderer, err := builtin.NewRenderer("mysql")
	if err != nil {
		return err
	}
	mysqlSQL, err := mysqlRenderer.Render(table)
	if err != nil {
		return err
	}

	fmt.Fprintln(w, "MySQL:")
	fmt.Fprintln(w, mysqlSQL)

	// Example 2: Building a more complex schema with enums and foreign keys
	fmt.Fprintln(w, "\n2. Complex schema with enums and foreign keys:")

	schema := astbuilder.NewSchema().
		Comment("E-commerce database schema").
		Enum("order_status", "pending", "processing", "shipped", "delivered", "canceled").
		Table("categories").
		Column("id", "SERIAL").Primary().End().
		Column("name", "VARCHAR(100)").NotNull().Unique().End().
		Column("description", "TEXT").End().
		End().
		Table("products").
		Column("id", "SERIAL").Primary().End().
		Column("name", "VARCHAR(255)").NotNull().End().
		Column("price", "DECIMAL(10,2)").NotNull().Check("price > 0").End().
		Column("category_id", "INTEGER").NotNull().
		ForeignKey("categories", "id", "fk_products_category").
		OnDelete("CASCADE").
		End().
		End().
		Table("orders").
		Column("id", "SERIAL").Primary().End().
		Column("user_id", "INTEGER").NotNull().End().
		Column("status", "order_status").NotNull().Default("pending").End().
		Column("total", "DECIMAL(10,2)").NotNull().End().
		Column("created_at", "TIMESTAMP").DefaultExpression("NOW()").End().
		End().
		Index("idx_orders_user_id", "orders", "user_id").End().
		Index("idx_orders_status", "orders", "status").End().
		Build()

	// Render complete schema for PostgreSQL
	pgSchemaSQL, err := builtin.RenderSQL("postgresql", schema)
	if err != nil {
		return err
	}

	fmt.Fprintln(w, "PostgreSQL Schema:")
	fmt.Fprintln(w, pgSchemaSQL)

	// Example 3: Building ALTER statements
	fmt.Fprintln(w, "\n3. ALTER statements using AST:")

	alterTable := &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{
				Column: ast.NewColumn("phone", "VARCHAR(20)").SetNotNull(),
			},
			&ast.ModifyColumnOperation{
				Column: ast.NewColumn("email", "VARCHAR(320)").SetNotNull().SetUnique(),
			},
			&ast.DropColumnOperation{
				ColumnName: "is_active",
			},
		},
	}

	alterSQL, err := builtin.RenderSQL("postgresql", alterTable)
	if err != nil {
		return err
	}

	fmt.Fprintln(w, "PostgreSQL ALTER statements:")
	fmt.Fprintln(w, alterSQL)

	// Example 4: Direct AST construction (lower-level approach)
	fmt.Fprintln(w, "\n4. Direct AST construction:")

	directTable := ast.NewCreateTable("products").
		AddColumn(
			ast.NewColumn("id", "SERIAL").SetPrimary(),
		).
		AddColumn(
			ast.NewColumn("name", "VARCHAR(255)").SetNotNull(),
		).
		AddColumn(
			ast.NewColumn("price", "DECIMAL(10,2)").SetNotNull().SetCheck("price > 0"),
		).
		AddConstraint(
			ast.NewUniqueConstraint("uk_products_name", "name"),
		)

	directSQL, err := builtin.RenderSQL("postgresql", directTable)
	if err != nil {
		return err
	}

	fmt.Fprintln(w, "Direct AST construction result:")
	fmt.Fprintln(w, directSQL)
	return nil
}

// CompareApproaches demonstrates the difference between old and new approaches
func CompareApproaches(w io.Writer) {
	fmt.Fprintln(w, "=== Comparison: Old vs New Approach ===")

	fmt.Fprintln(w, "OLD APPROACH (string-based):")
	fmt.Fprintf(w, `
// Hard to read, error-prone, not composable
func generateOldWay() string {
    var buf strings.Builder
    fmt.Fprintf(&buf, "-- %%s TABLE: %%s --\n", "POSTGRES", "users")
    fmt.Fprintf(&buf, "CREATE TABLE %%s (\n", "users")
    fmt.Fprintf(&buf, "  %%s %%s PRIMARY KEY,\n", "id", "SERIAL")
    fmt.Fprintf(&buf, "  %%s %%s NOT NULL UNIQUE,\n", "email", "VARCHAR(255)")
    fmt.Fprintf(&buf, "  %%s %%s NOT NULL\n", "name", "VARCHAR(100)")
    fmt.Fprintf(&buf, ");\n")
    return buf.String()
}`)

	fmt.Fprintln(w, "\nNEW APPROACH (AST-based):")
	fmt.Fprintln(w, `
// Readable, type-safe, composable, testable
func generateNewWay() string {
    table := astbuilder.NewTable("users").
        Column("id", "SERIAL").Primary().End().
        Column("email", "VARCHAR(255)").NotNull().Unique().End().
        Column("name", "VARCHAR(100)").NotNull().End().
        Build()

    sql, _ := renderer.RenderSQL("postgresql", table)
    return sql
}`)

	fmt.Fprintln(w, "\nBENEFITS OF AST APPROACH:")
	fmt.Fprintln(w, "✓ Type safety - compile-time validation")
	fmt.Fprintln(w, "✓ Readability - fluent API mirrors SQL structure")
	fmt.Fprintln(w, "✓ Composability - build complex schemas from parts")
	fmt.Fprintln(w, "✓ Testability - easy to unit test individual components")
	fmt.Fprintln(w, "✓ Maintainability - changes in one place affect all dialects")
	fmt.Fprintln(w, "✓ Extensibility - easy to add new SQL features")
	fmt.Fprintln(w, "✓ Dialect independence - same AST, different renderers")
}

// ShowAdvancedFeatures demonstrates advanced AST capabilities
func ShowAdvancedFeatures(w io.Writer) error {
	fmt.Fprintln(w, "=== Advanced AST Features ===")

	// Custom visitor for SQL analysis
	fmt.Fprintln(w, "1. Custom visitor for analyzing schema:")

	analyzer := &SchemaAnalyzer{
		TableCount:  0,
		ColumnCount: 0,
		IndexCount:  0,
	}

	schema := astbuilder.NewSchema().
		Table("users").
		Column("id", "SERIAL").Primary().End().
		Column("email", "VARCHAR(255)").NotNull().End().
		End().
		Table("posts").
		Column("id", "SERIAL").Primary().End().
		Column("user_id", "INTEGER").End().
		End().
		Index("idx_posts_user", "posts", "user_id").End().
		Build()

	if err := schema.Accept(analyzer); err != nil {
		return err
	}
	fmt.Fprintf(w, "Analysis result: %d tables, %d columns, %d indexes\n",
		analyzer.TableCount, analyzer.ColumnCount, analyzer.IndexCount)

	// Schema transformation
	fmt.Fprintln(w, "\n2. Schema transformation (add audit columns to all tables):")

	transformer := &AuditTransformer{}
	transformedSchema := transformer.Transform(schema)

	sql, err := builtin.RenderSQL("postgresql", transformedSchema)
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "Transformed schema with audit columns:")
	fmt.Fprintln(w, sql)
	return nil
}

// SchemaAnalyzer is a custom visitor that analyzes schema structure
type SchemaAnalyzer struct {
	TableCount  int
	ColumnCount int
	IndexCount  int
}

// VisitNode counts the tables, columns and indexes a schema declares.
//
// One method, and the kinds this analyzer does not care about need no mention.
// A renderer must answer an unrecognized node with an error, because silence
// there is indistinguishable from a deliberate skip; an analyzer is the other
// case, and ignoring what it does not count is the whole point of it.
//
// The statement list is walked here rather than by Accept. Accept hands a
// visitor the node it was given and nothing else, so a visitor that counts only
// top-level statements and one that descends into every nested list are both
// expressible, and neither is imposed by the AST.
func (a *SchemaAnalyzer) VisitNode(node ast.Node) error {
	switch n := node.(type) {
	case *ast.StatementList:
		for _, statement := range n.Statements {
			if err := statement.Accept(a); err != nil {
				return err
			}
		}
	case *ast.CreateTableNode:
		a.TableCount++
		a.ColumnCount += len(n.Columns)
	case *ast.IndexNode:
		a.IndexCount++
	}
	return nil
}

// AuditTransformer adds audit columns to all tables
type AuditTransformer struct{}

func (t *AuditTransformer) Transform(schema *ast.StatementList) *ast.StatementList {
	newStatements := make([]ast.Node, 0, len(schema.Statements))

	for _, stmt := range schema.Statements {
		if table, ok := stmt.(*ast.CreateTableNode); ok {
			// Add audit columns to table
			table.AddColumn(ast.NewColumn("created_at", "TIMESTAMP").SetDefaultExpression("NOW()"))
			table.AddColumn(ast.NewColumn("updated_at", "TIMESTAMP").SetDefaultExpression("NOW()"))
			table.AddColumn(ast.NewColumn("created_by", "VARCHAR(100)"))
			table.AddColumn(ast.NewColumn("updated_by", "VARCHAR(100)"))
		}
		newStatements = append(newStatements, stmt)
	}

	return &ast.StatementList{Statements: newStatements}
}
