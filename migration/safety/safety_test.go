package safety_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/migration/safety"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestClassifySchemaDiff_HighestSeverity(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesAdded: []string{"users"},
		TablesModified: []types.TableDiff{
			{
				TableName:      "products",
				ColumnsAdded:   []string{"sku"},
				ColumnsRemoved: []string{"legacy_code"},
			},
		},
		IndexesRemoved: []string{"idx_products_old"},
		RLSPoliciesRemoved: []types.RLSPolicyRef{
			{PolicyName: "tenant_isolation", TableName: "accounts"},
		},
	}

	findings := safety.ClassifySchemaDiff(diff)

	c.Assert(findings, qt.Contains, safety.Finding{
		Category: "columns_removed",
		Count:    1,
		Severity: safety.Destructive,
	})
	c.Assert(findings, qt.Contains, safety.Finding{
		Category: "rls_policies_removed",
		Count:    1,
		Severity: safety.Destructive,
	})
	c.Assert(findings, qt.Contains, safety.Finding{
		Category: "indexes_removed",
		Count:    1,
		Severity: safety.Warning,
	})
	c.Assert(safety.Highest(findings), qt.Equals, safety.Destructive)
	c.Assert(safety.HasDestructive(findings), qt.IsTrue)
}

func TestClassifySchemaDiff_AggregatesRepeatedCategories(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{
			{TableName: "users", ColumnsAdded: []string{"email"}},
			{TableName: "posts", ColumnsAdded: []string{"slug", "status"}},
		},
	}

	findings := safety.ClassifySchemaDiff(diff)

	c.Assert(findings, qt.DeepEquals, []safety.Finding{
		{
			Category: "columns_added",
			Count:    3,
			Severity: safety.Warning,
		},
	})
	c.Assert(safety.Highest(findings), qt.Equals, safety.Warning)
}

func TestClassifyASTStatements(t *testing.T) {
	c := qt.New(t)

	c.Assert(safety.Classify(ast.NewDropTable("users")), qt.Equals, safety.Destructive)
	c.Assert(safety.Classify(&ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.DropColumnOperation{ColumnName: "legacy_name"},
		},
	}), qt.Equals, safety.Destructive)
	c.Assert(safety.Classify(&ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.ModifyColumnOperation{
				Column:       ast.NewColumn("name", "varchar(100)"),
				PreviousType: "varchar(255)",
			},
		},
	}), qt.Equals, safety.Destructive)
	c.Assert(safety.Classify(&ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.ModifyColumnOperation{
				Column:       ast.NewColumn("count", "bigint"),
				PreviousType: "integer",
			},
		},
	}), qt.Equals, safety.Warning)
	c.Assert(safety.Classify(&ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.ModifyColumnOperation{
				Column:              ast.NewColumn("nickname", "text"),
				PreviousNullable:    false,
				HasPreviousNullable: true,
			},
		},
	}), qt.Equals, safety.Destructive)
	c.Assert(safety.Classify(&ast.AlterTableNode{
		Name: "old_users",
		Operations: []ast.AlterOperation{
			&ast.RenameTableOperation{NewName: "users"},
		},
	}), qt.Equals, safety.Warning)
	c.Assert(safety.Classify(ast.NewIndex("users_email_key", "users", "email").SetUnique()), qt.Equals, safety.Warning)
	c.Assert(safety.Classify(ast.NewRawSQL("DELETE FROM pg_enum WHERE enumlabel = 'archived'")), qt.Equals, safety.Destructive)
	c.Assert(safety.Classify(ast.NewRawSQL("DROP TYPE status")), qt.Equals, safety.Destructive)
	c.Assert(safety.Classify(ast.NewRawSQL("DROP EXTENSION hstore")), qt.Equals, safety.Destructive)
	c.Assert(safety.Classify(ast.NewRawSQL("DROP FUNCTION refresh_user()")), qt.Equals, safety.Destructive)
	c.Assert(safety.Classify(ast.NewRawSQL("DROP ROLE old_role")), qt.Equals, safety.Destructive)
	c.Assert(safety.Classify(ast.NewRawSQL("DROP POLICY tenant_isolation ON accounts")), qt.Equals, safety.Destructive)
	c.Assert(safety.Classify(ast.NewRawSQL("TRUNCATE TABLE audit_log")), qt.Equals, safety.Destructive)
	c.Assert(safety.Classify(ast.NewRawSQL("ALTER TABLE accounts DISABLE ROW LEVEL SECURITY")), qt.Equals, safety.Destructive)
}

func TestAssessHighestSeverity(t *testing.T) {
	c := qt.New(t)

	assessments := safety.Assess([]ast.Node{
		ast.NewIndex("idx_users_email", "users", "email"),
		&ast.AlterTableNode{
			Name: "users",
			Operations: []ast.AlterOperation{
				&ast.RenameColumnOperation{OldName: "name", NewName: "display_name"},
			},
		},
	})

	c.Assert(assessments, qt.HasLen, 2)
	c.Assert(assessments[0].Index, qt.Equals, 1)
	c.Assert(safety.HighestAssessment(assessments), qt.Equals, safety.Warning)
	c.Assert(safety.HasDestructiveAssessment(assessments), qt.IsFalse)
}

func TestRenderJSON(t *testing.T) {
	c := qt.New(t)

	assessments := []safety.StatementAssessment{{
		Index:     1,
		NodeType:  "sql",
		Subject:   "legacy",
		Statement: "DROP TABLE legacy;",
		Severity:  safety.Destructive,
		Reason:    "DROP TABLE removes the table and all rows",
	}}
	var buf bytes.Buffer

	err := safety.RenderJSON(&buf, assessments)

	c.Assert(err, qt.IsNil)
	var report safety.Report
	c.Assert(json.Unmarshal(buf.Bytes(), &report), qt.IsNil)
	c.Assert(report.Highest, qt.Equals, safety.Destructive)
	c.Assert(report.Destructive, qt.IsTrue)
	c.Assert(report.Assessments, qt.DeepEquals, assessments)
}

func TestAssessRenderedSplitsPostgresModifyColumnStatements(t *testing.T) {
	c := qt.New(t)

	assessments, err := safety.AssessRendered([]ast.Node{
		&ast.AlterTableNode{
			Name: "users",
			Operations: []ast.AlterOperation{
				&ast.ModifyColumnOperation{
					Column:              ast.NewColumn("nickname", "varchar(100)"),
					PreviousType:        "varchar(255)",
					PreviousNullable:    false,
					HasPreviousNullable: true,
				},
			},
		},
	}, "postgres")
	c.Assert(err, qt.IsNil)

	byStatement := make(map[string]safety.Severity)
	for _, assessment := range assessments {
		switch {
		case strings.Contains(assessment.Statement, " TYPE "):
			byStatement["type"] = assessment.Severity
		case strings.Contains(assessment.Statement, " DROP NOT NULL"):
			byStatement["drop_not_null"] = assessment.Severity
		case strings.Contains(assessment.Statement, " DROP DEFAULT"):
			byStatement["drop_default"] = assessment.Severity
		}
	}
	c.Assert(byStatement["type"], qt.Equals, safety.Destructive)
	c.Assert(byStatement["drop_not_null"], qt.Equals, safety.Destructive)
	c.Assert(byStatement["drop_default"], qt.Equals, safety.Safe)
}

func TestIsTypeNarrowing(t *testing.T) {
	c := qt.New(t)

	c.Assert(safety.IsTypeNarrowing("varchar(255)", "varchar(100)"), qt.IsTrue)
	c.Assert(safety.IsTypeNarrowing("text", "varchar(255)"), qt.IsTrue)
	c.Assert(safety.IsTypeNarrowing("bigint", "integer"), qt.IsTrue)
	c.Assert(safety.IsTypeNarrowing("integer", "bigint"), qt.IsFalse)
	c.Assert(safety.IsTypeNarrowing("numeric(12,2)", "numeric(10,2)"), qt.IsTrue)
	c.Assert(safety.IsTypeNarrowing("numeric(12,2)", "numeric(12,4)"), qt.IsTrue)
}
