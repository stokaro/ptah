package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

func main() {
	if err := run(os.Stdout); err != nil {
		log.Fatal(err)
	}
}

// run parses the PostgreSQL DDL below, then the EXCLUDE constraint
// declarations in exclude_constraints_example.go, and writes what it found to
// w.
func run(w io.Writer) error {
	// PostgreSQL-style DDL statements with advanced features
	sql := `
		CREATE TYPE status_enum AS ENUM ('pending', 'active', 'archived');

		CREATE TABLE public.full_demo (
			serial_id SERIAL PRIMARY KEY,
			big_id BIGSERIAL UNIQUE,
			uuid_id UUID DEFAULT gen_random_uuid() NOT NULL,
			char_fixed CHAR(10),
			varchar_var VARCHAR(255) NOT NULL,
			text_field TEXT CHECK (char_length(text_field) <= 5000),
			small_value SMALLINT DEFAULT 1 CHECK (small_value > 0),
			numeric_precise NUMERIC(12,4) NOT NULL DEFAULT 0.0000,
			is_active BOOLEAN DEFAULT TRUE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMPTZ DEFAULT now(),
			status status_enum DEFAULT 'pending',
			tags TEXT[] DEFAULT ARRAY[]::TEXT[],
			matrix INT[][],
			json_field JSON,
			jsonb_field JSONB NOT NULL DEFAULT '{}'::jsonb,
			data BYTEA,
			email_address TEXT,
			full_name TEXT GENERATED ALWAYS AS (char_fixed || ' ' || varchar_var) STORED,
			case_insensitive TEXT COLLATE "C",
			user_id INTEGER REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL,
			CONSTRAINT uq_tag_and_status UNIQUE (tags, status),
			CONSTRAINT chk_price_non_negative CHECK (numeric_precise >= 0),
			CHECK (created_at <= updated_at)
		);

		COMMENT ON TABLE public.full_demo IS 'Comprehensive PostgreSQL demo table';
		COMMENT ON COLUMN public.full_demo.varchar_var IS 'Variable length string';
	`

	fmt.Fprintln(w, "Parsing PostgreSQL DDL statements:")
	fmt.Fprintln(w, "==================================")
	fmt.Fprintln(w, sql)
	fmt.Fprintln(w)

	// Create parser and parse the SQL
	p := parser.NewParser(sql)
	statements, err := p.Parse()
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	fmt.Fprintf(w, "Successfully parsed %d SQL statement(s):\n\n", len(statements.Statements))

	// Print detailed information about each parsed statement
	for i, stmt := range statements.Statements {
		fmt.Fprintf(w, "Statement %d:\n", i+1)

		switch s := stmt.(type) {
		case *ast.EnumNode:
			fmt.Fprintf(w, "  Type: CREATE TYPE (ENUM)\n")
			fmt.Fprintf(w, "  Name: %s\n", s.Name)
			fmt.Fprintf(w, "  Values: %v\n", s.Values)

		case *ast.CommentNode:
			fmt.Fprintf(w, "  Type: COMMENT\n")
			fmt.Fprintf(w, "  Content: %s\n", s.Text)

		case *ast.CreateTableNode:
			fmt.Fprintf(w, "  Type: CREATE TABLE\n")
			fmt.Fprintf(w, "  Table: %s\n", s.Name)
			fmt.Fprintf(w, "  Columns: %d\n", len(s.Columns))
			fmt.Fprintf(w, "  Constraints: %d\n", len(s.Constraints))

			fmt.Fprintln(w, "\n  PostgreSQL Column Features:")
			for j, col := range s.Columns {
				fmt.Fprintf(w, "    %d. %s %s", j+1, col.Name, col.Type)

				var features []string
				if !col.Nullable {
					features = append(features, "NOT NULL")
				}
				if col.Primary {
					features = append(features, "PRIMARY KEY")
				}
				if col.Unique {
					features = append(features, "UNIQUE")
				}
				if col.Default != nil {
					if col.Default.Expression != "" {
						features = append(features, fmt.Sprintf("DEFAULT %s", col.Default.Expression))
					} else {
						features = append(features, fmt.Sprintf("DEFAULT %s", col.Default.Value))
					}
				}
				if col.Check != "" {
					if len(col.Check) > 50 {
						features = append(features, "CHECK(...)")
					} else {
						features = append(features, fmt.Sprintf("CHECK(%s)", col.Check))
					}
				}
				if col.ForeignKey != nil {
					features = append(features, fmt.Sprintf("REFERENCES %s(%s)", col.ForeignKey.Table, col.ForeignKey.Column))
				}
				if col.Comment != "" {
					features = append(features, fmt.Sprintf("COLLATE %s", col.Comment))
				}

				if len(features) > 0 {
					fmt.Fprintf(w, " [%s]", fmt.Sprintf("%v", features))
				}
				fmt.Fprintln(w)
			}

			if len(s.Constraints) > 0 {
				fmt.Fprintln(w, "\n  Table Constraints:")
				for j, constraint := range s.Constraints {
					fmt.Fprintf(w, "    %d. %s", j+1, constraint.Type.String())
					if constraint.Name != "" {
						fmt.Fprintf(w, " [%s]", constraint.Name)
					}
					if len(constraint.Columns) > 0 {
						fmt.Fprintf(w, " (%v)", constraint.Columns)
					}
					if constraint.Expression != "" {
						fmt.Fprintf(w, " CHECK(%s)", constraint.Expression)
					}
					fmt.Fprintln(w)
				}
			}

		default:
			fmt.Fprintf(w, "  Type: Unknown (%T)\n", s)
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintln(w, "PostgreSQL parser demo completed successfully!")
	fmt.Fprintln(w, "\nKey PostgreSQL features demonstrated:")
	fmt.Fprintln(w, "- ENUM types with CREATE TYPE")
	fmt.Fprintln(w, "- Schema-qualified table names (public.table_name)")
	fmt.Fprintln(w, "- SERIAL and BIGSERIAL auto-increment types")
	fmt.Fprintln(w, "- UUID with gen_random_uuid() function")
	fmt.Fprintln(w, "- Array types (TEXT[], INT[][])")
	fmt.Fprintln(w, "- JSON and JSONB types with type casting ('{}'::jsonb)")
	fmt.Fprintln(w, "- PostgreSQL-specific data types (TIMESTAMPTZ, BYTEA)")
	fmt.Fprintln(w, "- GENERATED ALWAYS AS computed columns")
	fmt.Fprintln(w, "- COLLATE specifications")
	fmt.Fprintln(w, "- Complex CHECK constraints with functions")
	fmt.Fprintln(w, "- Named table constraints")
	fmt.Fprintln(w, "- COMMENT ON statements")
	fmt.Fprintln(w, "- Foreign keys with CASCADE and SET NULL actions")

	fmt.Fprintln(w, "\n"+strings.Repeat("=", 50))

	// Run the EXCLUDE constraints example
	return runExcludeConstraintsExample(w)
}
