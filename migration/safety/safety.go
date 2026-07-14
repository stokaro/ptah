// Package safety classifies schema changes by operational risk.
package safety

import (
	"fmt"
	"html/template"
	"io"
	"sort"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/migration/schemadiff/types"
	"github.com/stokaro/ptah/migration/typechange"
)

// Severity is the operational risk level for a schema change.
type Severity string

const (
	// Safe changes should not remove data or tighten existing constraints.
	Safe Severity = "safe"
	// Warning changes are data-dependent or may affect runtime behavior.
	Warning Severity = "warning"
	// Destructive changes remove data, database objects, or protections.
	Destructive Severity = "destructive"
)

// Finding summarizes one non-empty schema-diff category.
type Finding struct {
	Category string   `json:"category"`
	Count    int      `json:"count"`
	Severity Severity `json:"severity"`
}

// StatementAssessment classifies one generated migration statement.
type StatementAssessment struct {
	Index     int      `json:"index"`
	NodeType  string   `json:"node_type"`
	Subject   string   `json:"subject,omitempty"`
	Statement string   `json:"statement,omitempty"`
	Severity  Severity `json:"severity"`
	Reason    string   `json:"reason"`
}

// ClassifySchemaDiff returns severity findings for every non-empty diff
// category.
func ClassifySchemaDiff(diff *types.SchemaDiff) []Finding {
	if diff == nil {
		return nil
	}

	var findings []Finding
	add(&findings, "tables_added", len(diff.TablesAdded), Safe)
	add(&findings, "tables_removed", len(diff.TablesRemoved), Destructive)
	add(&findings, "enums_added", len(diff.EnumsAdded), Safe)
	add(&findings, "enums_removed", len(diff.EnumsRemoved), Destructive)
	add(&findings, "indexes_added", len(diff.IndexesAdded), Warning)
	add(&findings, "indexes_removed", len(diff.IndexesRemoved), Warning)
	add(&findings, "extensions_added", len(diff.ExtensionsAdded), Safe)
	add(&findings, "extensions_removed", len(diff.ExtensionsRemoved), Destructive)
	add(&findings, "functions_added", len(diff.FunctionsAdded), Safe)
	add(&findings, "functions_removed", len(diff.FunctionsRemoved), Destructive)
	add(&findings, "functions_modified", len(diff.FunctionsModified), Warning)
	add(&findings, "rls_policies_added", len(diff.RLSPoliciesAdded), Safe)
	add(&findings, "rls_policies_removed", len(diff.RLSPoliciesRemoved), Destructive)
	add(&findings, "rls_policies_modified", len(diff.RLSPoliciesModified), Warning)
	add(&findings, "rls_enabled_tables_added", len(diff.RLSEnabledTablesAdded), Safe)
	add(&findings, "rls_enabled_tables_removed", len(diff.RLSEnabledTablesRemoved), Destructive)
	add(&findings, "roles_added", len(diff.RolesAdded), Safe)
	add(&findings, "roles_removed", len(diff.RolesRemoved), Destructive)
	add(&findings, "roles_modified", len(diff.RolesModified), Warning)
	add(&findings, "constraints_added", len(diff.ConstraintsAdded), Warning)
	add(&findings, "constraints_removed", len(diff.ConstraintsRemoved), Destructive)

	for _, table := range diff.TablesModified {
		add(&findings, "columns_added", len(table.ColumnsAdded), Warning)
		add(&findings, "columns_removed", len(table.ColumnsRemoved), Destructive)
		add(&findings, "columns_modified", len(table.ColumnsModified), Warning)
		add(&findings, "table_constraints_added", len(table.ConstraintsAdded), Warning)
		add(&findings, "table_constraints_removed", len(table.ConstraintsRemoved), Destructive)
	}
	for _, enum := range diff.EnumsModified {
		add(&findings, "enum_values_added", len(enum.ValuesAdded), Warning)
		add(&findings, "enum_values_removed", len(enum.ValuesRemoved), Destructive)
	}

	findings = aggregate(findings)
	sort.SliceStable(findings, func(i, j int) bool {
		if findings[i].Severity != findings[j].Severity {
			return severityRank(findings[i].Severity) > severityRank(findings[j].Severity)
		}
		return findings[i].Category < findings[j].Category
	})
	return findings
}

// Highest returns the highest severity from findings.
func Highest(findings []Finding) Severity {
	highest := Safe
	for _, finding := range findings {
		if severityRank(finding.Severity) > severityRank(highest) {
			highest = finding.Severity
		}
	}
	return highest
}

// HasDestructive returns true when any finding is destructive.
func HasDestructive(findings []Finding) bool {
	return Highest(findings) == Destructive
}

// Classify returns the highest operational risk for a migration AST node.
func Classify(node ast.Node) Severity {
	return assessNode(node).Severity
}

// Assess returns per-statement risk classifications for generated AST nodes.
func Assess(nodes []ast.Node) []StatementAssessment {
	assessments := make([]StatementAssessment, 0, len(nodes))
	for i, node := range nodes {
		assessment := assessNode(node)
		assessment.Index = i + 1
		assessments = append(assessments, assessment)
	}
	return assessments
}

// AssessRendered returns per-rendered-SQL-statement risk classifications for
// generated AST nodes.
func AssessRendered(nodes []ast.Node, dialect string) ([]StatementAssessment, error) {
	return AssessRenderedWithCapabilities(nodes, dialect, capability.ForDialect(dialect))
}

// AssessRenderedWithCapabilities returns per-rendered-SQL-statement risk
// classifications using the same server-version capability set as planning and
// rendering on live database paths.
func AssessRenderedWithCapabilities(
	nodes []ast.Node,
	dialect string,
	caps capability.Capabilities,
) ([]StatementAssessment, error) {
	var assessments []StatementAssessment
	for _, node := range nodes {
		nodeAssessment := assessNode(node)
		rendered, err := renderer.RenderSQLWithCapabilities(dialect, caps, node)
		if err != nil {
			return nil, err
		}
		statements := sqlutil.SplitSQLStatements(rendered)
		if len(statements) == 0 && strings.TrimSpace(rendered) != "" {
			statements = []string{strings.TrimSpace(rendered)}
		}
		for _, statement := range statements {
			assessment := AssessSQL(statement)
			assessment.NodeType = nodeAssessment.NodeType
			if assessment.Subject == "" {
				assessment.Subject = nodeAssessment.Subject
			}
			if len(statements) == 1 || isTypeChangeSQL(statement) {
				raiseAssessment(&assessment, nodeAssessment)
			}
			assessment.Index = len(assessments) + 1
			assessments = append(assessments, assessment)
		}
	}
	return assessments, nil
}

// AssessSQL returns a best-effort classification for one rendered SQL
// statement.
func AssessSQL(statement string) StatementAssessment {
	assessment := StatementAssessment{
		NodeType:  "sql",
		Statement: strings.TrimSpace(statement),
		Severity:  Safe,
		Reason:    "does not remove data or tighten constraints",
	}
	return assessRawSQL(statement, assessment)
}

// HighestAssessment returns the highest severity from statement assessments.
func HighestAssessment(assessments []StatementAssessment) Severity {
	highest := Safe
	for _, assessment := range assessments {
		if severityRank(assessment.Severity) > severityRank(highest) {
			highest = assessment.Severity
		}
	}
	return highest
}

// HasDestructiveAssessment returns true when any statement is destructive.
func HasDestructiveAssessment(assessments []StatementAssessment) bool {
	return HighestAssessment(assessments) == Destructive
}

// RenderText writes a compact text table for statement assessments.
func RenderText(w io.Writer, assessments []StatementAssessment) error {
	if len(assessments) == 0 {
		_, err := fmt.Fprintln(w, "Safety: no executable migration statements")
		return err
	}
	_, err := fmt.Fprintln(w, "Safety classification:")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, "  #  severity      subject                  reason")
	if err != nil {
		return err
	}
	for _, assessment := range assessments {
		subject := assessment.Subject
		if subject == "" {
			subject = assessment.NodeType
		}
		if _, err := fmt.Fprintf(w, "  %-2d %-12s %-24s %s\n", assessment.Index, assessment.Severity, subject, assessment.Reason); err != nil {
			return err
		}
	}
	return nil
}

// RenderHTML writes a standalone HTML safety report.
func RenderHTML(w io.Writer, assessments []StatementAssessment) error {
	const report = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Ptah migration safety report</title>
<style>
body { font-family: system-ui, sans-serif; margin: 2rem; color: #1f2937; }
table { border-collapse: collapse; width: 100%; }
th, td { border: 1px solid #d1d5db; padding: 0.5rem; text-align: left; vertical-align: top; }
th { background: #f3f4f6; }
.safe { color: #047857; font-weight: 700; }
.warning { color: #b45309; font-weight: 700; }
.destructive { color: #b91c1c; font-weight: 700; }
pre { margin: 0; white-space: pre-wrap; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
</style>
</head>
<body>
<h1>Ptah migration safety report</h1>
<table>
<thead><tr><th>#</th><th>Severity</th><th>Subject</th><th>Reason</th><th>Statement</th></tr></thead>
<tbody>
{{range .}}
<tr>
<td>{{.Index}}</td>
<td class="{{.Severity}}">{{.Severity}}</td>
<td>{{if .Subject}}{{.Subject}}{{else}}{{.NodeType}}{{end}}</td>
<td>{{.Reason}}</td>
<td><pre>{{.Statement}}</pre></td>
</tr>
{{end}}
</tbody>
</table>
</body>
</html>`
	tmpl, err := template.New("safety-report").Parse(report)
	if err != nil {
		return err
	}
	return tmpl.Execute(w, assessments)
}

func assessNode(node ast.Node) StatementAssessment {
	assessment := StatementAssessment{
		NodeType: fmt.Sprintf("%T", node),
		Severity: Safe,
		Reason:   "does not remove data or tighten constraints",
	}

	switch n := node.(type) {
	case *ast.AlterTableNode:
		assessment.Subject = n.Name
		return assessAlterTable(n, assessment)
	case *ast.DropTableNode:
		assessment.Subject = n.Name
		assessment.Severity = Destructive
		assessment.Reason = "DROP TABLE removes the table and all rows"
	case *ast.DropTypeNode:
		assessment.Subject = n.Name
		assessment.Severity = Destructive
		assessment.Reason = "DROP TYPE removes an existing database type"
	case *ast.DropExtensionNode:
		assessment.Subject = n.Name
		assessment.Severity = Destructive
		assessment.Reason = "DROP EXTENSION removes database objects owned by the extension"
	case *ast.DropFunctionNode:
		assessment.Subject = n.Name
		assessment.Severity = Destructive
		assessment.Reason = "DROP FUNCTION removes executable database behavior"
	case *ast.DropRoleNode:
		assessment.Subject = n.Name
		assessment.Severity = Destructive
		assessment.Reason = "DROP ROLE removes an existing database principal"
	case *ast.DropPolicyNode:
		assessment.Subject = n.Name
		assessment.Severity = Destructive
		assessment.Reason = "DROP POLICY removes an access-control protection"
	case *ast.AlterTableDisableRLSNode:
		assessment.Subject = n.Table
		assessment.Severity = Destructive
		assessment.Reason = "DISABLE ROW LEVEL SECURITY removes an access-control protection"
	case *ast.IndexNode:
		assessment.Subject = n.Name
		if n.Unique {
			assessment.Severity = Warning
			assessment.Reason = "CREATE UNIQUE INDEX can fail on existing duplicate values"
		}
	case *ast.DropIndexNode:
		assessment.Subject = n.Name
		assessment.Severity = Warning
		assessment.Reason = "DROP INDEX can affect query plans and constraints"
	case *ast.AlterTypeNode:
		assessment.Subject = n.Name
		return assessAlterType(n, assessment)
	case *ast.RawSQLNode:
		assessment.Statement = n.SQL
		return assessRawSQL(n.SQL, assessment)
	}
	return assessment
}

func assessAlterTable(n *ast.AlterTableNode, assessment StatementAssessment) StatementAssessment {
	for _, op := range n.Operations {
		severity, reason := classifyAlterOperation(op)
		if severityRank(severity) > severityRank(assessment.Severity) {
			assessment.Severity = severity
			assessment.Reason = reason
		}
	}
	return assessment
}

func assessAlterType(n *ast.AlterTypeNode, assessment StatementAssessment) StatementAssessment {
	for _, op := range n.Operations {
		severity, reason := classifyTypeOperation(op)
		if severityRank(severity) > severityRank(assessment.Severity) {
			assessment.Severity = severity
			assessment.Reason = reason
		}
	}
	return assessment
}

func classifyAlterOperation(op ast.AlterOperation) (Severity, string) {
	switch o := op.(type) {
	case *ast.DropColumnOperation:
		return Destructive, "DROP COLUMN removes existing column data"
	case *ast.DropConstraintOperation:
		return Destructive, "DROP CONSTRAINT removes an existing data protection"
	case *ast.RenameColumnOperation:
		return Warning, "RENAME COLUMN can break deployed readers and writers"
	case *ast.AddConstraintOperation:
		return Warning, "ADD CONSTRAINT can fail on existing rows"
	case *ast.AddColumnOperation:
		if o.Column != nil && !o.Column.Nullable {
			return Warning, "ADD COLUMN with NOT NULL can fail on existing rows"
		}
		return Safe, "ADD COLUMN is additive"
	case *ast.ModifyColumnOperation:
		return classifyModifyColumn(o)
	case *ast.AddSkippingIndexOperation:
		return Warning, "ADD INDEX can affect write workload during build"
	case *ast.ModifyTTLOperation:
		return Warning, "MODIFY TTL can delete or move existing rows"
	default:
		return Safe, "does not remove data or tighten constraints"
	}
}

func classifyModifyColumn(op *ast.ModifyColumnOperation) (Severity, string) {
	if op == nil || op.Column == nil {
		return Warning, "column modification needs manual review"
	}
	if IsTypeNarrowing(op.PreviousType, op.Column.Type) {
		return Destructive, fmt.Sprintf("column type narrows from %s to %s", op.PreviousType, op.Column.Type)
	}
	if op.HasPreviousNullable && !op.PreviousNullable && op.Column.Nullable {
		return Destructive, "DROP NOT NULL removes a column-level data protection"
	}
	if op.PreviousType != "" && !sameType(op.PreviousType, op.Column.Type) {
		return Warning, fmt.Sprintf("column type changes from %s to %s", op.PreviousType, op.Column.Type)
	}
	if !op.Column.Nullable {
		return Warning, "SET NOT NULL can fail when existing rows contain NULL"
	}
	return Warning, "column modification needs manual review"
}

func classifyTypeOperation(op ast.TypeOperation) (Severity, string) {
	switch op.(type) {
	case *ast.RenameEnumValueOperation:
		return Warning, "RENAME VALUE can break deployed readers and writers"
	case *ast.RenameTypeOperation:
		return Warning, "RENAME TYPE can break deployed readers and writers"
	case *ast.AddEnumValueOperation:
		return Warning, "ADD VALUE can affect cross-version enum compatibility"
	default:
		return Safe, "type change is additive"
	}
}

func assessRawSQL(sql string, assessment StatementAssessment) StatementAssessment {
	words := rawWords(sql)
	switch {
	case hasWordPrefix(words, "DROP", "TABLE"):
		assessment.Severity = Destructive
		assessment.Reason = "DROP TABLE removes the table and all rows"
	case hasWordPrefix(words, "DROP", "TYPE"):
		assessment.Severity = Destructive
		assessment.Reason = "DROP TYPE removes an existing database type"
	case hasWordPrefix(words, "DROP", "EXTENSION"):
		assessment.Severity = Destructive
		assessment.Reason = "DROP EXTENSION removes database objects owned by the extension"
	case hasWordPrefix(words, "DROP", "FUNCTION"):
		assessment.Severity = Destructive
		assessment.Reason = "DROP FUNCTION removes executable database behavior"
	case hasWordPrefix(words, "DROP", "ROLE"):
		assessment.Severity = Destructive
		assessment.Reason = "DROP ROLE removes an existing database principal"
	case hasWordPrefix(words, "DROP", "POLICY"):
		assessment.Severity = Destructive
		assessment.Reason = "DROP POLICY removes an access-control protection"
	case hasWordPrefix(words, "TRUNCATE"):
		assessment.Severity = Destructive
		assessment.Reason = "TRUNCATE removes all rows from a table"
	case hasWordSequence(words, "DISABLE", "ROW", "LEVEL", "SECURITY"):
		assessment.Severity = Destructive
		assessment.Reason = "DISABLE ROW LEVEL SECURITY removes an access-control protection"
	case hasWordSequence(words, "DROP", "COLUMN"):
		assessment.Severity = Destructive
		assessment.Reason = "DROP COLUMN removes existing column data"
	case hasWordSequence(words, "DROP", "CONSTRAINT"):
		assessment.Severity = Destructive
		assessment.Reason = "DROP CONSTRAINT removes an existing data protection"
	case hasWordSequence(words, "DROP", "NOT", "NULL"):
		assessment.Severity = Destructive
		assessment.Reason = "DROP NOT NULL removes an existing data protection"
	case hasWordSequence(words, "DROP", "VALUE"), hasWordSequence(words, "DELETE", "FROM", "PG_ENUM"):
		assessment.Severity = Destructive
		assessment.Reason = "removing an enum value can invalidate existing rows"
	case hasWordSequence(words, "RENAME", "COLUMN"), hasWordSequence(words, "RENAME", "TO"):
		assessment.Severity = Warning
		assessment.Reason = "rename can break deployed readers and writers"
	case hasWordSequence(words, "SET", "NOT", "NULL"):
		assessment.Severity = Warning
		assessment.Reason = "SET NOT NULL can fail when existing rows contain NULL"
	case hasWordPrefix(words, "CREATE", "UNIQUE", "INDEX"):
		assessment.Severity = Warning
		assessment.Reason = "CREATE UNIQUE INDEX can fail on existing duplicate values"
	}
	return assessment
}

func raiseAssessment(target *StatementAssessment, source StatementAssessment) {
	if severityRank(source.Severity) <= severityRank(target.Severity) {
		return
	}
	target.Severity = source.Severity
	target.Reason = source.Reason
}

func isTypeChangeSQL(statement string) bool {
	words := rawWords(statement)
	return hasWordSequence(words, "ALTER", "COLUMN") && hasWordSequence(words, "TYPE") ||
		hasWordSequence(words, "MODIFY", "COLUMN") ||
		hasWordSequence(words, "CHANGE", "COLUMN")
}

func add(findings *[]Finding, category string, count int, severity Severity) {
	if count == 0 {
		return
	}
	*findings = append(*findings, Finding{
		Category: category,
		Count:    count,
		Severity: severity,
	})
}

func aggregate(findings []Finding) []Finding {
	byCategory := make(map[string]Finding, len(findings))
	for _, finding := range findings {
		existing, ok := byCategory[finding.Category]
		if !ok {
			byCategory[finding.Category] = finding
			continue
		}
		existing.Count += finding.Count
		if severityRank(finding.Severity) > severityRank(existing.Severity) {
			existing.Severity = finding.Severity
		}
		byCategory[finding.Category] = existing
	}

	out := make([]Finding, 0, len(byCategory))
	for _, finding := range byCategory {
		out = append(out, finding)
	}
	return out
}

func severityRank(severity Severity) int {
	switch severity {
	case Destructive:
		return 3
	case Warning:
		return 2
	default:
		return 1
	}
}

// IsTypeNarrowing reports whether changing from oldType to newType can lose
// data by reducing the representable range or length.
func IsTypeNarrowing(oldType, newType string) bool {
	return typechange.IsNarrowing(oldType, newType)
}

func sameType(left, right string) bool {
	return typechange.Same(left, right)
}

func rawWords(sql string) []string {
	replacer := strings.NewReplacer("(", " ", ")", " ", ",", " ", ";", " ", "\n", " ", "\t", " ")
	clean := replacer.Replace(strings.ToUpper(sql))
	return strings.Fields(clean)
}

func hasWordPrefix(words []string, prefix ...string) bool {
	if len(words) < len(prefix) {
		return false
	}
	for i, word := range prefix {
		if words[i] != word {
			return false
		}
	}
	return true
}

func hasWordSequence(words []string, sequence ...string) bool {
	if len(sequence) == 0 || len(words) < len(sequence) {
		return false
	}
	for i := 0; i <= len(words)-len(sequence); i++ {
		matched := true
		for j, word := range sequence {
			if words[i+j] != word {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}
