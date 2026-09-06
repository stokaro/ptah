// Package safety classifies a schema diff into findings, each carrying a
// [ptah.run/migration/risk.Severity].
//
// It is an analysis rather than a vocabulary: it reads a comparison and decides
// which changes remove data, drop objects or tighten constraints. The scale
// those decisions are reported on belongs to migration/risk, which several
// other producers share (stokaro/ptah#2246 section 2.2).
package safety

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"sort"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/htmlstyle"
	"ptah.run/internal/typechange"
	"ptah.run/migration/risk"
	"ptah.run/migration/schemadiff/difftypes"
)

// Severity is the operational risk level for a schema change.
type Severity = risk.Severity

const (
	// Safe changes should not remove data or tighten existing constraints.
	Safe Severity = risk.Safe
	// Warning changes are data-dependent or may affect runtime behavior.
	Warning Severity = risk.Warning
	// Destructive changes remove data, database objects, or protections.
	Destructive Severity = risk.Destructive
)

// Finding summarizes one non-empty schema-diff category.
type Finding struct {
	Category string   `json:"category"`
	Count    int      `json:"count"`
	Severity Severity `json:"severity"`
}

// StatementAssessment classifies one generated migration statement. Index is
// 1-based in the slices [Assess], [AssessRendered], and
// [AssessRenderedWithCapabilities] return; [AssessSQL] classifies one
// statement and leaves Index zero.
type StatementAssessment struct {
	Index     int      `json:"index"`
	NodeType  string   `json:"node_type"`
	Subject   string   `json:"subject,omitempty"`
	Statement string   `json:"statement,omitempty"`
	Severity  Severity `json:"severity"`
	Reason    string   `json:"reason"`
}

// Report is the machine-readable safety report envelope.
type Report struct {
	Highest     Severity              `json:"highest"`
	Destructive bool                  `json:"destructive"`
	Assessments []StatementAssessment `json:"assessments"`
}

// ClassifySchemaDiff returns severity findings for every non-empty diff
// category. A nil diff returns nil.
//
// Each category appears at most once, with counts summed across all modified
// tables and enums, and findings are sorted by severity (most severe first),
// then by category name, so the output is deterministic and diffable.
//
// The categories are deliberately not disjoint: a change severe enough to
// deserve its own category is counted there as well as under the generic
// category that also covers it — a removal that takes a UNIQUE constraint's
// enforcement with it, or a column modification the server refuses to cast
// while rows hold values. Read the findings as reasons to look, not as a
// partition whose counts sum to a statement total.
func ClassifySchemaDiff(diff *difftypes.SchemaDiff) []Finding {
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
	// A removal whose object a UNIQUE constraint enforces takes the uniqueness
	// with it, whichever statement the engine spells it as, so it is counted
	// again under its own destructive category rather than folded into the
	// warning above. Losing a uniqueness protection is not a query-plan change
	// and must not pass a destructive gate or a drift threshold as one.
	add(&findings, "unique_protections_removed", len(diff.ConstraintBackedIndexRemovals), Destructive)
	add(&findings, "extensions_added", len(diff.ExtensionsAdded), Safe)
	add(&findings, "extensions_removed", len(diff.ExtensionsRemoved), Destructive)
	add(&findings, "extensions_modified", len(diff.ExtensionsModified), Warning)
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
		// Counted separately from columns_modified, and Destructive rather than
		// Warning, because it is the one column modification the server refuses
		// outright while any row holds a value (stokaro/ptah#2068). A reader
		// who sees "columns_modified: 1 (warning)" is told a cast is planned.
		add(&findings, "vector_dimension_changed", vectorDimensionChanges(table.ColumnsModified), Destructive)
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

// Highest returns the highest severity from findings. Empty or nil findings
// answer Safe. Severities compare by [risk.Rank], and Destructive is
// preferred over Error when both are present at the same rank, so a gate
// switching on the result sees the data-loss verdict.
func Highest(findings []Finding) Severity {
	highest := Safe
	for _, finding := range findings {
		if severityOutranks(finding.Severity, highest) {
			highest = finding.Severity
		}
	}
	return highest
}

// HasDestructive returns true when any finding is destructive.
func HasDestructive(findings []Finding) bool {
	for _, finding := range findings {
		if finding.Severity == Destructive {
			return true
		}
	}
	return false
}

// Classify returns the highest operational risk for a migration AST node.
func Classify(node ast.Node) Severity {
	return assessNode(node).Severity
}

// Assess returns per-statement risk classifications for generated AST nodes,
// in input order with 1-based Index. A node type the classifier does not
// recognize is reported Safe with the default reason, so an unknown construct
// never blocks a migration by accident.
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
// generated AST nodes, using the dialect's default capability preset
// ([capability.ForDialect]). See [AssessRenderedWithCapabilities] for the
// assessment and error contract.
func AssessRendered(nodes []ast.Node, dialect string) ([]StatementAssessment, error) {
	return AssessRenderedWithCapabilities(nodes, dialect, capability.ForDialect(dialect))
}

// AssessRenderedWithCapabilities returns per-rendered-SQL-statement risk
// classifications using the same server-version capability set as planning and
// rendering on live database paths. The dialect accepts any spelling
// platform.NormalizeDialect resolves.
//
// Rendering is the only error source: a construct the dialect or capability
// set cannot render fails here with the typed error documented on
// [renderer.RenderSQLWithCapabilities] rather than being classified.
//
// There is one assessment per rendered statement, not per node, because a node
// can render into several statements on some dialects, and Index is 1-based
// over that flattened list. Each statement is classified from its own SQL, and
// the node-level verdict is folded into the statements it applies to — so a
// narrowing type change stays destructive where the SQL alone would not say so
// — never lowering a statement's own classification.
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
		statements := sqlutil.SplitSQLStatementsForDialect(rendered, dialect)
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
// statement. Matching is keyword-based and insensitive to the statement's
// casing and layout, so the same statement classifies the same however it is
// spelled; a statement matching no known destructive or warning shape is
// reported Safe with the default reason.
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
// Empty or nil assessments answer Safe; severities compare the way [Highest]
// documents.
func HighestAssessment(assessments []StatementAssessment) Severity {
	highest := Safe
	for _, assessment := range assessments {
		if severityOutranks(assessment.Severity, highest) {
			highest = assessment.Severity
		}
	}
	return highest
}

// HasDestructiveAssessment returns true when any statement is destructive.
func HasDestructiveAssessment(assessments []StatementAssessment) bool {
	for _, assessment := range assessments {
		if assessment.Severity == Destructive {
			return true
		}
	}
	return false
}

// NewReport returns a machine-readable safety report envelope.
func NewReport(assessments []StatementAssessment) Report {
	return Report{
		Highest:     HighestAssessment(assessments),
		Destructive: HasDestructiveAssessment(assessments),
		Assessments: assessments,
	}
}

// RenderJSON writes a machine-readable safety report.
func RenderJSON(w io.Writer, assessments []StatementAssessment) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(NewReport(assessments))
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
//
// The document fetches nothing: its appearance is inlined from
// internal/htmlstyle, the same declaration the exported schema document and
// the database test report read, so "destructive" is the same red on all
// three.
//
// The shell is written directly and only the rows go through a template, so
// nothing trusted has to be handed to html/template as pre-escaped markup.
func RenderHTML(w io.Writer, assessments []StatementAssessment) error {
	if _, err := io.WriteString(w, htmlstyle.Head("Ptah migration safety report", reportCSS)); err != nil {
		return err
	}
	tmpl, err := template.New("safety-report").Parse(reportBodyHTML)
	if err != nil {
		return err
	}
	body := struct {
		Assessments []StatementAssessment
		Counts      severityCounts
	}{assessments, countBySeverity(assessments)}
	if err := tmpl.Execute(w, body); err != nil {
		return err
	}
	footer := htmlstyle.Footer("Rendered by Ptah from the planned migration. " +
		"This file is self-contained: opening it fetches nothing.")
	_, err = io.WriteString(w, footer+"</div></body>\n</html>\n")
	return err
}

// reportCSS is what this report adds to the shared appearance: the three
// severity words mapped onto the shared severity colors, and the statement
// column.
//
// The mapping lives here rather than as a method on the assessment because
// Severity is a fact about the change and the color is a fact about this page.
const reportCSS = `
.tag.safe { background: var(--ok-soft); border-color: transparent; color: var(--ok); }
.tag.warning { background: var(--warn-soft); border-color: transparent; color: var(--warn); }
.tag.destructive { background: var(--danger-soft); border-color: transparent; color: var(--danger); }
td.stmt pre { color: var(--text-dim); }
`

// severityCounts is how many statements fall in each level, for the strip above
// the table.
//
// A reader opens a safety report to find out whether anything is destructive,
// and the old report made them read every row to answer that.
type severityCounts struct {
	Total       int
	Safe        int
	Warning     int
	Destructive int
}

func countBySeverity(assessments []StatementAssessment) severityCounts {
	counts := severityCounts{Total: len(assessments)}
	for _, assessment := range assessments {
		switch assessment.Severity {
		case Destructive:
			counts.Destructive++
		case Warning:
			counts.Warning++
		default:
			counts.Safe++
		}
	}
	return counts
}

const reportBodyHTML = `<body><div class="page">
<h1>Migration safety report</h1>
<div class="lede">Planned statements, classified by what they remove or tighten</div>
<div class="stats">
<div class="stat"><div class="stat-n">{{.Counts.Total}}</div><div class="stat-l">statements</div></div>
<div class="stat"><div class="stat-n">{{.Counts.Safe}}</div><div class="stat-l">safe</div></div>
<div class="stat"><div class="stat-n">{{.Counts.Warning}}</div><div class="stat-l">warning</div></div>
<div class="stat"><div class="stat-n">{{.Counts.Destructive}}</div><div class="stat-l">destructive</div></div>
</div>
<h2>Statements</h2>
<div class="card"><div class="scroller"><table>
<thead><tr><th>#</th><th>Severity</th><th>Subject</th><th>Reason</th><th>Statement</th></tr></thead>
<tbody>
{{range .Assessments}}
<tr>
<td class="num">{{.Index}}</td>
<td><span class="tag {{.Severity}}">{{.Severity}}</span></td>
<td class="name">{{if .Subject}}{{.Subject}}{{else}}{{.NodeType}}{{end}}</td>
<td class="comment">{{.Reason}}</td>
<td class="stmt"><pre>{{.Statement}}</pre></td>
</tr>
{{end}}
</tbody>
</table></div></div>
`

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
		if n.Domain {
			assessment.Reason = "DROP DOMAIN removes an existing database domain"
		} else {
			assessment.Reason = "DROP TYPE removes an existing database type"
		}
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
		if n.EnforcesUniqueConstraint {
			assessment.Severity = Destructive
			assessment.Reason = "DROP INDEX removes the uniqueness a UNIQUE constraint enforces"
			return assessment
		}
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
	case *ast.RenameTableOperation:
		return Warning, "RENAME TABLE can break deployed readers and writers"
	case *ast.AddConstraintOperation:
		return Warning, "ADD CONSTRAINT can fail on existing rows"
	case *ast.AddColumnOperation:
		if o.Column != nil && !o.Column.Nullable {
			return Warning, "ADD COLUMN with NOT NULL can fail on existing rows"
		}
		return Safe, "ADD COLUMN is additive"
	case *ast.ModifyColumnOperation:
		return classifyModifyColumn(o)
	case *ast.AlterGeneratedColumnExpressionOperation:
		return Warning, "SET EXPRESSION rewrites generated column values"
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
	if from, to, ok := typechange.VectorDimensionChange(op.PreviousType, op.Column.Type); ok {
		return Destructive, fmt.Sprintf(
			"vector dimension changes from %d to %d: the server refuses the cast while any row holds a vector, "+
				"and every value has to be recomputed rather than converted", from, to)
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

// vectorDimensionChanges counts the columns whose pgvector dimension changes.
//
// The transition is read from the diff's own `type` entry, which the comparator
// writes as "old -> new". A column whose entry cannot be split that way is not
// counted: reporting a change nobody can name would be worse than reporting
// none, and the generic columns_modified finding still covers it.
func vectorDimensionChanges(columns []difftypes.ColumnDiff) int {
	changed := 0
	for _, column := range columns {
		before, after, ok := strings.Cut(column.Changes["type"], " -> ")
		if !ok {
			continue
		}
		if _, _, isVector := typechange.VectorDimensionChange(before, after); isVector {
			changed++
		}
	}
	return changed
}

func severityRank(severity Severity) int {
	return risk.Rank(severity)
}

func severityOutranks(candidate, current Severity) bool {
	candidateRank := severityRank(candidate)
	currentRank := severityRank(current)
	return candidateRank > currentRank ||
		(candidateRank == currentRank && candidate == Destructive && current != Destructive)
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
