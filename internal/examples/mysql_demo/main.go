package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

func main() {
	if err := run(os.Stdout); err != nil {
		log.Fatal(err)
	}
}

// run parses the DDL below and writes what it found to w.
func run(w io.Writer) error {
	// MySQL-style CREATE TABLE statement with backticks, specific data types, and MySQL-specific syntax
	sql := `CREATE TABLE ` + "`sample`" + ` (
		` + "`id`" + ` int NOT NULL AUTO_INCREMENT,
		` + "`name`" + ` varchar(50) DEFAULT 'John',
		` + "`age`" + ` int DEFAULT 30,
		` + "`created_at`" + ` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
		` + "`active`" + ` tinyint(1) DEFAULT 1,
		PRIMARY KEY (` + "`id`" + `)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`

	fmt.Fprintln(w, "Parsing MySQL-style CREATE TABLE statement:")
	fmt.Fprintln(w, "==========================================")
	fmt.Fprintln(w, sql)
	fmt.Fprintln(w)

	// Create parser and parse the SQL
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	fmt.Fprintf(w, "Successfully parsed %d SQL statement(s):\n\n", len(statements.Statements))

	// Print detailed information about the parsed statement
	for i, stmt := range statements.Statements {
		fmt.Fprintf(w, "Statement %d:\n", i+1)

		if createTable, ok := stmt.(*ast.CreateTableNode); ok {
			fmt.Fprintf(w, "  Type: CREATE TABLE\n")
			fmt.Fprintf(w, "  Table: %s\n", createTable.Name)
			fmt.Fprintf(w, "  Columns: %d\n", len(createTable.Columns))
			fmt.Fprintf(w, "  Constraints: %d\n", len(createTable.Constraints))

			fmt.Fprintln(w, "\n  Column Details:")
			for j, col := range createTable.Columns {
				fmt.Fprintf(w, "    %d. %s %s", j+1, col.Name, col.Type)

				var attributes []string
				if !col.Nullable {
					attributes = append(attributes, "NOT NULL")
				}
				if col.AutoInc {
					attributes = append(attributes, "AUTO_INCREMENT")
				}
				if col.Primary {
					attributes = append(attributes, "PRIMARY KEY")
				}
				if col.Unique {
					attributes = append(attributes, "UNIQUE")
				}
				if col.Default != nil {
					if col.Default.Expression != "" {
						attributes = append(attributes, fmt.Sprintf("DEFAULT %s", col.Default.Expression))
					} else {
						attributes = append(attributes, fmt.Sprintf("DEFAULT %s", col.Default.Value))
					}
				}

				if len(attributes) > 0 {
					fmt.Fprintf(w, " [%s]", fmt.Sprintf("%v", attributes))
				}
				fmt.Fprintln(w)
			}

			if len(createTable.Constraints) > 0 {
				fmt.Fprintln(w, "\n  Constraints:")
				for j, constraint := range createTable.Constraints {
					fmt.Fprintf(w, "    %d. %s", j+1, constraint.Type.String())
					if len(constraint.Columns) > 0 {
						fmt.Fprintf(w, " (%v)", constraint.Columns)
					}
					if constraint.Name != "" {
						fmt.Fprintf(w, " [name: %s]", constraint.Name)
					}
					fmt.Fprintln(w)
				}
			}

			if len(createTable.Options) > 0 {
				fmt.Fprintln(w, "\n  Table Options:")
				for key, value := range createTable.Options {
					fmt.Fprintf(w, "    %s = %s\n", key, value)
				}
			}

			if createTable.Comment != "" {
				fmt.Fprintf(w, "\n  Comment: %s\n", createTable.Comment)
			}
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintln(w, "MySQL parser demo completed successfully!")
	fmt.Fprintln(w, "\nKey MySQL features demonstrated:")
	fmt.Fprintln(w, "- Backticked identifiers (`table_name`, `column_name`)")
	fmt.Fprintln(w, "- MySQL-specific data types (int, varchar, tinyint, timestamp)")
	fmt.Fprintln(w, "- Numeric default values (30, 1)")
	fmt.Fprintln(w, "- String default values ('John')")
	fmt.Fprintln(w, "- Function default values (CURRENT_TIMESTAMP)")
	fmt.Fprintln(w, "- MySQL table options (ENGINE, CHARSET, COLLATE)")
	fmt.Fprintln(w, "- AUTO_INCREMENT columns")
	fmt.Fprintln(w, "- NULL/NOT NULL specifications")
	fmt.Fprintln(w, "- PRIMARY KEY constraints")
	return nil
}
