package parser_test

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/internal/parser"
)

// ExampleWithSQL_tablePrefix selects a modifier owner while the shared parser
// reads the table's columns. The owner receives the table's qualified name.
func ExampleWithSQL_tablePrefix() {
	var requests []sqlext.Request
	owners, err := sqlext.NewSet(sqlext.Extension{
		Owner: "example.org/prefix", Targets: []string{"postgres"},
		Facets: []schemaext.Kind{prefixKind}, Parser: prefixReader{requests: &requests},
		Patterns: []sqlext.Pattern{{Name: "tagged", Site: sqlext.TablePrefix, Words: []string{"TAGGED"}}},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	parsed, err := parser.NewParser("CREATE TAGGED cache TABLE app.entries (id int);",
		parser.WithDialect("postgres"), parser.WithSQL(owners)).Parse()
	if err != nil {
		fmt.Println(err)
		return
	}
	table := parsed.Statements[0].(*ast.CreateTableNode)
	fmt.Println(requests[0].Claims[0].Subject)
	fmt.Println(table.Columns[0].Name, table.Facets.Len())
	// Output:
	// app.entries
	// id 1
}
