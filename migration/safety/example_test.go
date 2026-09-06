package safety_test

import (
	"fmt"
	"os"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// ExampleAssessSQL classifies raw SQL strings — the zero-dependency entry
// point for a caller who already holds rendered statements rather than AST
// nodes. Matching is keyword-based and insensitive to casing and layout; a
// statement matching no known risky shape is Safe, so an unknown construct
// never blocks by accident.
func ExampleAssessSQL() {
	statements := []string{
		"DROP TABLE users;",
		"ALTER TABLE users RENAME COLUMN name TO display_name",
		"CREATE INDEX idx_users_email ON users (email)",
	}

	for _, statement := range statements {
		assessment := safety.AssessSQL(statement)
		fmt.Printf("%s: %s\n", assessment.Severity, assessment.Reason)
	}

	// Output:
	// destructive: DROP TABLE removes the table and all rows
	// warning: rename can break deployed readers and writers
	// safe: does not remove data or tighten constraints
}

// ExampleClassifySchemaDiff summarizes a schema comparison as per-category
// findings, sorted most severe first and then by category name, so the output
// is deterministic and diffable. Highest and HasDestructive are what a gate
// reads off the result.
func ExampleClassifySchemaDiff() {
	diff := &difftypes.SchemaDiff{
		TablesAdded:   difftypes.TableChanges{{Name: "audit_log"}},
		TablesRemoved: []string{"legacy_sessions"},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx_orders_status", TableName: "orders"},
		},
	}

	findings := safety.ClassifySchemaDiff(diff)
	for _, finding := range findings {
		fmt.Printf("%s: %d (%s)\n", finding.Category, finding.Count, finding.Severity)
	}
	fmt.Println("highest:", safety.Highest(findings))
	fmt.Println("destructive:", safety.HasDestructive(findings))

	// Output:
	// tables_removed: 1 (destructive)
	// indexes_removed: 1 (warning)
	// tables_added: 1 (safe)
	// highest: destructive
	// destructive: true
}

// ExampleAssessRendered renders AST nodes for a dialect and classifies every
// resulting statement. One node can yield several assessments — this column
// modification does on PostgreSQL — and Index counts over the flattened list,
// so the numbering matches the script an operator will review. Each statement
// is classified from its own SQL, and the node-level verdict, here a narrowing
// type change, is folded in where it applies rather than lowering a
// statement's own classification. How a node decomposes belongs to the
// renderer, not to this classification.
func ExampleAssessRendered() {
	nodes := []ast.Node{
		ast.NewDropTable("legacy_sessions"),
		&ast.AlterTableNode{
			Name: "users",
			Operations: []ast.AlterOperation{
				&ast.ModifyColumnOperation{
					Column:       ast.NewColumn("nickname", "varchar(100)"),
					PreviousType: "varchar(255)",
				},
			},
		},
	}

	assessments, err := safety.AssessRendered(nodes, "postgres")
	if err != nil {
		fmt.Println("render failed:", err)
		return
	}
	for _, assessment := range assessments {
		fmt.Printf("%d %-11s %s\n", assessment.Index, assessment.Severity, assessment.Reason)
	}

	// Output:
	// 1 destructive DROP TABLE removes the table and all rows
	// 2 destructive column type narrows from varchar(255) to varchar(100)
	// 3 destructive DROP NOT NULL removes an existing data protection
	// 4 safe        does not remove data or tighten constraints
}

// ExampleRenderText writes the compact text table an operator reads in a
// terminal — the same rendering `ptah migrations plan --report text` prints.
// The subject column falls back to the node type when a statement names no
// subject.
func ExampleRenderText() {
	assessments := safety.Assess([]ast.Node{
		ast.NewDropTable("legacy_sessions"),
		ast.NewIndex("users_email_key", "users", "email").SetUnique(),
	})

	must.Assert(safety.RenderText(os.Stdout, assessments))

	// Output:
	// Safety classification:
	//   #  severity      subject                  reason
	//   1  destructive  legacy_sessions          DROP TABLE removes the table and all rows
	//   2  warning      users_email_key          CREATE UNIQUE INDEX can fail on existing duplicate values
}

// ExampleNewReport builds the machine-readable report envelope and writes it
// as JSON — the document `ptah migrations plan --report json` prints on
// standard output. Highest and Destructive are precomputed so a pipeline can
// gate on the envelope without re-deriving them from the assessments.
func ExampleNewReport() {
	assessments := must.Must(safety.AssessRendered(
		[]ast.Node{ast.NewDropTable("legacy_sessions")}, "postgres"))

	report := safety.NewReport(assessments)
	fmt.Println("highest:", report.Highest)

	must.Assert(safety.RenderJSON(os.Stdout, assessments))

	// Output:
	// highest: destructive
	// {
	//   "highest": "destructive",
	//   "destructive": true,
	//   "assessments": [
	//     {
	//       "index": 1,
	//       "node_type": "*ast.DropTableNode",
	//       "subject": "legacy_sessions",
	//       "statement": "DROP TABLE \"legacy_sessions\"",
	//       "severity": "destructive",
	//       "reason": "DROP TABLE removes the table and all rows"
	//     }
	//   ]
	// }
}
