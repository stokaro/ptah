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
	// Example SQL DDL statements
	sql := `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL UNIQUE,
			name VARCHAR(100) NOT NULL,
			age INTEGER CHECK (age >= 0),
			created_at TIMESTAMP DEFAULT NOW(),
			FOREIGN KEY (id) REFERENCES profiles(user_id) ON DELETE CASCADE
		) ENGINE=InnoDB CHARSET=utf8mb4;

		CREATE UNIQUE INDEX idx_users_email ON users (email);

		CREATE TYPE status AS ENUM ('active', 'inactive', 'pending');

		ALTER TABLE users ADD COLUMN status VARCHAR(20) DEFAULT 'active';
	`

	// Create parser and parse the SQL
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	fmt.Fprintf(w, "Successfully parsed %d SQL statements:\n\n", len(statements.Statements))

	// Print information about each parsed statement
	for i, stmt := range statements.Statements {
		fmt.Fprintf(w, "Statement %d:\n", i+1)

		switch s := stmt.(type) {
		case *ast.CreateTableNode:
			fmt.Fprintf(w, "  Type: CREATE TABLE\n")
			fmt.Fprintf(w, "  Table: %s\n", s.Name)
			fmt.Fprintf(w, "  Columns: %d\n", len(s.Columns))
			fmt.Fprintf(w, "  Constraints: %d\n", len(s.Constraints))
			if len(s.Options) > 0 {
				fmt.Fprintf(w, "  Options: %v\n", s.Options)
			}
			if s.Comment != "" {
				fmt.Fprintf(w, "  Comment: %s\n", s.Comment)
			}

		case *ast.IndexNode:
			fmt.Fprintf(w, "  Type: CREATE INDEX\n")
			fmt.Fprintf(w, "  Index: %s\n", s.Name)
			fmt.Fprintf(w, "  Table: %s\n", s.Table)
			fmt.Fprintf(w, "  Columns: %v\n", s.Columns)
			fmt.Fprintf(w, "  Unique: %t\n", s.Unique)

		case *ast.EnumNode:
			fmt.Fprintf(w, "  Type: CREATE TYPE (ENUM)\n")
			fmt.Fprintf(w, "  Name: %s\n", s.Name)
			fmt.Fprintf(w, "  Values: %v\n", s.Values)

		case *ast.AlterTableNode:
			fmt.Fprintf(w, "  Type: ALTER TABLE\n")
			fmt.Fprintf(w, "  Table: %s\n", s.Name)
			fmt.Fprintf(w, "  Operations: %d\n", len(s.Operations))

		default:
			fmt.Fprintf(w, "  Type: Unknown\n")
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintln(w, "Parser demo completed successfully!")
	return nil
}
