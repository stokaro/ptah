// Package safety classifies a schema diff into findings, each carrying a
// [ptah.run/migration/risk.Severity].
//
// It is an analysis rather than a vocabulary: it reads a comparison and decides
// which changes remove data, drop objects or tighten constraints. The scale
// those decisions are reported on belongs to migration/risk, which several
// other producers share (stokaro/ptah#2246 section 2.2).
package safety

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"slices"
	"sort"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/htmlstyle"
	"ptah.run/internal/notnullfill"
	"ptah.run/internal/typechange"
	"ptah.run/internal/ydbstream"
	"ptah.run/internal/ydbtopic"
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
	// A dropped database takes every object in it; a changed character set or
	// collation changes what a table created later in the database defaults to.
	add(&findings, "schemas_added", len(diff.SchemasAdded), Safe)
	add(&findings, "schemas_removed", len(diff.SchemasRemoved), Destructive)
	add(&findings, "schemas_modified", len(diff.SchemasModified), Warning)
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
	// NO FORCE counts as destructive for the reason a DISABLE does: afterwards
	// the table's owner, often the role the application connects as, reads and
	// writes past every policy the table keeps.
	forced, unforced := rlsForceDirections(diff.RLSForceChanged)
	add(&findings, "rls_force_added", forced, Safe)
	add(&findings, "rls_force_removed", unforced, Destructive)
	add(&findings, "coordination_nodes_added", len(diff.CoordinationNodesAdded), Safe)
	add(&findings, "coordination_nodes_removed", len(diff.CoordinationNodesRemoved), Destructive)
	add(&findings, "coordination_nodes_modified", len(diff.CoordinationNodesModified), Warning)
	add(&findings, "roles_added", len(diff.RolesAdded), Safe)
	add(&findings, "roles_removed", len(diff.RolesRemoved), Destructive)
	add(&findings, "roles_modified", len(diff.RolesModified), Warning)
	// A dropped or changed resource pool or classifier removes no data; it
	// moves queries to another pool, which changes what limits them.
	add(&findings, "resource_pools_added", len(diff.ResourcePoolsAdded), Safe)
	add(&findings, "resource_pools_removed", len(diff.ResourcePoolsRemoved), Warning)
	add(&findings, "resource_pools_modified", len(diff.ResourcePoolsModified), Warning)
	add(&findings, "resource_pool_classifiers_added", len(diff.ResourcePoolClassifiersAdded), Warning)
	add(&findings, "resource_pool_classifiers_removed", len(diff.ResourcePoolClassifiersRemoved), Warning)
	add(&findings, "resource_pool_classifiers_modified", len(diff.ResourcePoolClassifiersModified), Warning)
	add(&findings, "constraints_added", len(diff.ConstraintsAdded), Warning)
	add(&findings, "constraints_removed", len(diff.ConstraintsRemoved), Destructive)
	// A topic holds messages and each consumer's position in them, so dropping
	// either loses what a reader has not read or where it was.
	add(&findings, "topics_added", len(diff.TopicsAdded), Safe)
	add(&findings, "topics_removed", len(diff.TopicsRemoved), Destructive)
	add(&findings, "topics_modified", len(diff.TopicsModified), Warning)
	add(&findings, "topic_consumers_removed", droppedTopicConsumers(diff.TopicsModified), Destructive)
	// Dropping an async replication drops the replica tables it created, or
	// leaves them read-only for good, and dropping a transfer drops the
	// consumer YDB created for it, with its position in the topic. A change
	// of either moves where data comes from or how it is written.
	add(&findings, "async_replications_added", len(diff.AsyncReplicationsAdded), Safe)
	add(&findings, "async_replications_removed", len(diff.AsyncReplicationsRemoved), Destructive)
	add(&findings, "async_replications_modified", len(diff.AsyncReplicationsModified), Warning)
	add(&findings, "transfers_added", len(diff.TransfersAdded), Safe)
	add(&findings, "transfers_removed", len(diff.TransfersRemoved), Destructive)
	add(&findings, "transfers_modified", len(diff.TransfersModified), Warning)
	// A dropped YDB secret takes a value nothing can read back, and an
	// external data source that names it fails at its next read; a rotated
	// one replaces the value every such source uses.
	add(&findings, "secrets_added", len(diff.SecretsAdded), Safe)
	add(&findings, "secrets_removed", len(diff.SecretsRemoved), Destructive)
	add(&findings, "secrets_rotated", len(diff.SecretsRotated), Warning)
	// A YDB external data source or table holds no data in YDB, so dropping
	// or replacing one loses none; a query that reads one fails or reads
	// something else afterwards.
	add(&findings, "external_data_sources_added", len(diff.ExternalDataSourcesAdded), Safe)
	add(&findings, "external_data_sources_removed", len(diff.ExternalDataSourcesRemoved), Warning)
	add(&findings, "external_data_sources_changed", len(diff.ExternalDataSourcesChanged), Warning)
	add(&findings, "external_tables_added", len(diff.ExternalTablesAdded), Safe)
	add(&findings, "external_tables_removed", len(diff.ExternalTablesRemoved), Warning)
	add(&findings, "external_tables_changed", len(diff.ExternalTablesChanged), Warning)

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
func AssessRendered(ctx context.Context, service renderer.Service, nodes []ast.Node, dialect string) ([]StatementAssessment, error) {
	return AssessRenderedWithCapabilities(ctx, service, nodes, dialect, capability.ForDialect(dialect))
}

// AssessRenderedWithCapabilities returns per-rendered-SQL-statement risk
// classifications using the same server-version capability set as planning and
// rendering on live database paths. The dialect accepts any spelling
// platform.NormalizeDialect resolves.
//
// The caller selects the rendering service. All assessment units are rendered
// in one batch, with node provenance retained in the reply. Missing services,
// rendering errors, incomplete replies, and cancellation return no assessments.
//
// There is one assessment per rendered statement, not per node, because a node
// can render into several statements on some dialects, and Index is 1-based
// over that flattened list. Each statement is classified from its own SQL, and
// the node-level verdict is folded into the statements it applies to — so a
// narrowing type change stays destructive where the SQL alone would not say so
// — never lowering a statement's own classification.
func AssessRenderedWithCapabilities(
	ctx context.Context,
	service renderer.Service,
	nodes []ast.Node,
	dialect string,
	caps capability.Capabilities,
) ([]StatementAssessment, error) {
	if err := schemaext.RequireRuntime(ctx, service); err != nil {
		return nil, err
	}
	var units []ast.Node
	for _, whole := range nodes {
		units = append(units, assessmentUnits(whole, dialect)...)
	}
	target := platform.NormalizeDialect(dialect)
	if target == "" {
		target = dialect
	}
	result, err := renderer.Render(ctx, service, renderer.Request{Target: target, Capabilities: caps, Nodes: units})
	if err != nil {
		return nil, err
	}
	var assessments []StatementAssessment
	for i, node := range units {
		nodeAssessment := assessNode(node)
		rendered := result.Fragments[i]
		statements := sqlutil.SplitSQLStatementsForDialect(rendered, dialect)
		if len(statements) == 0 && strings.TrimSpace(rendered) != "" {
			statements = []string{strings.TrimSpace(rendered)}
		}
		keepsNull := keepsNullability(node)
		fills := fillsNullRows(node)
		for _, statement := range statements {
			assessment := assessStatement(statement, keepsNull)
			assessment.NodeType = nodeAssessment.NodeType
			if assessment.Subject == "" {
				assessment.Subject = nodeAssessment.Subject
			}
			if len(statements) == 1 || hasExtensionEffect(node) || isTypeChangeSQL(statement) {
				raiseAssessment(&assessment, nodeAssessment)
			}
			if fills {
				judgeNullFillPair(&assessment, statement)
			}
			assessment.Index = len(assessments) + 1
			assessments = append(assessments, assessment)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return assessments, nil
}

// assessmentUnits is node, or one ALTER TABLE per operation when node is a
// PostgreSQL-family ALTER TABLE carrying several operations and one of them
// fills a column's NULL rows. Rendered one operation at a time, each fill and
// SET NOT NULL is assessed with the operation that wrote it, so the SET NOT
// NULL of a column that is filled is not confused with the one of a column
// that is not. A plan carries one column modification per ALTER TABLE, so
// this changes nothing about the statements a planned node reports.
func assessmentUnits(node ast.Node, dialect string) []ast.Node {
	alter, ok := node.(*ast.AlterTableNode)
	if !ok || len(alter.Operations) < 2 || !platform.IsPostgresFamily(dialect) ||
		!slices.ContainsFunc(alter.Operations, operationFillsNullRows) {
		return []ast.Node{node}
	}
	units := make([]ast.Node, 0, len(alter.Operations))
	for _, operation := range alter.Operations {
		unit := *alter
		unit.Operations = []ast.AlterOperation{operation}
		units = append(units, &unit)
	}
	return units
}

// fillsNullRows reports whether node is an ALTER TABLE whose one operation
// fills a column's NULL rows before SET NOT NULL, as the PostgreSQL family
// renders it.
//
// The dialect is not asked here. Only a PostgreSQL-family renderer writes the
// DO block and the SET NOT NULL judgeNullFillPair recognizes; MySQL's MODIFY,
// SQL Server's ALTER COLUMN and Oracle's MODIFY carry the whole definition in
// one statement that matches neither, and keep the verdict they have.
func fillsNullRows(node ast.Node) bool {
	alter, ok := node.(*ast.AlterTableNode)
	return ok && len(alter.Operations) == 1 && operationFillsNullRows(alter.Operations[0])
}

func operationFillsNullRows(operation ast.AlterOperation) bool {
	modify, ok := operation.(*ast.ModifyColumnOperation)
	return ok && notnullfill.FillsNullRows(modify)
}

// judgeNullFillPair judges the two statements a filled SET NOT NULL renders
// as, each for what it does (stokaro/ptah#3660).
//
// Read by its words alone, the fill is a DO block that matches no rule and
// reads safe, while it rewrites every NULL row of the column. The SET NOT NULL
// after it reads as a statement that can fail on a NULL row, and the fill
// before it has just removed those rows. So the fill is a warning for the rows
// it rewrites, and the SET NOT NULL is safe. A NULL another session writes
// between the two still fails it, the window every fill-then-constrain
// migration has; the report judges the plan's statements, not concurrent
// writers.
//
// The fill is the one DO block the PostgreSQL renderer writes for a column
// modification, which is how it is told from the other statements here;
// [notnullfill.FillsNullRows] decided that the operation writes it.
func judgeNullFillPair(assessment *StatementAssessment, statement string) {
	words := rawWords(sqlutil.StripCommentsForDialect(statement, platform.Postgres))
	switch {
	case hasWordPrefix(words, "DO"):
		assessment.Severity = Warning
		assessment.Reason = "UPDATE rewrites the column's NULL rows with its declared default before SET NOT NULL"
	case hasWordSequence(words, "SET", "NOT", "NULL"):
		assessment.Severity = Safe
		assessment.Reason = "SET NOT NULL follows the UPDATE that fills the column's NULL rows"
	}
}

// AssessSQL returns a best-effort classification for one rendered SQL
// statement. Matching is keyword-based and insensitive to the statement's
// casing and layout, so the same statement classifies the same however it is
// spelled; a statement matching no known destructive or warning shape is
// reported Safe with the default reason.
func AssessSQL(statement string) StatementAssessment {
	return assessStatement(statement, false)
}

// assessStatement is [AssessSQL] for a statement whose operation is known to
// leave the column's nullability alone; see [assessRawSQL].
func assessStatement(statement string, keepsNullability bool) StatementAssessment {
	assessment := StatementAssessment{
		NodeType:  "sql",
		Statement: strings.TrimSpace(statement),
		Severity:  Safe,
		Reason:    "does not remove data or tighten constraints",
	}
	return assessRawSQL(statement, assessment, keepsNullability)
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
	if subject, reason, dropped := destructiveDrop(node); dropped {
		assessment.Subject, assessment.Severity, assessment.Reason = subject, Destructive, reason
		return assessment
	}

	switch n := node.(type) {
	case *ast.StatementList:
		return assessStatementList(n, assessment)
	case *ast.ExtensionStatement, *ast.ExtensionAlterOperation:
		assessment.Severity, assessment.Reason = classifyExtensionNode(node)
	case *ast.AlterTableNode:
		assessment.Subject = n.Name
		return assessAlterTable(n, assessment)
	case *ast.DropTypeNode:
		assessment.Subject = n.Name
		assessment.Severity = Destructive
		if n.Domain {
			assessment.Reason = "DROP DOMAIN removes an existing database domain"
		} else {
			assessment.Reason = "DROP TYPE removes an existing database type"
		}
	case *ast.AlterTableDisableRLSNode:
		assessment.Subject = n.Table
		assessment.Severity = Destructive
		assessment.Reason = "DISABLE ROW LEVEL SECURITY removes an access-control protection"
	case *ast.AlterTableForceRLSNode:
		assessment.Subject = n.Table
		if n.NoForce {
			assessment.Severity = Destructive
			assessment.Reason = noForceReason
		}
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
	case *ast.DropResourcePoolNode, *ast.DropResourcePoolClassifierNode:
		return assessResourcePoolNode(node, assessment)
	case *ast.DropTopicNode, *ast.AlterTopicNode, *ast.DropAsyncReplicationNode, *ast.DropTransferNode,
		*ast.AlterAsyncReplicationNode, *ast.AlterTransferNode:
		return assessYDBObjectNode(n, assessment)
	case *ast.DropSecretNode, *ast.AlterSecretNode, *ast.DropExternalDataSourceNode, *ast.DropExternalTableNode,
		*ast.CreateExternalDataSourceNode, *ast.CreateExternalTableNode:
		return assessYDBObject(n, assessment)
	case *ydbstream.Node:
		return assessStreamingQuery(n, assessment)
	case *ast.RawSQLNode:
		assessment.Statement = n.SQL
		return assessRawSQL(n.SQL, assessment, false)
	}
	return assessment
}

// destructiveDrop is the subject and the reason of a statement that drops an
// object and always removes data or behavior with it, and false for any other
// node.
func destructiveDrop(node ast.Node) (subject, reason string, dropped bool) {
	switch n := node.(type) {
	case *ast.DropTableNode:
		return n.Name, "DROP TABLE removes the table and all rows", true
	case *ast.DropExtensionNode:
		return n.Name, "DROP EXTENSION removes database objects owned by the extension", true
	case *ast.DropFunctionNode:
		return n.Name, "DROP FUNCTION removes executable database behavior", true
	case *ast.DropRoleNode:
		return n.Name, "DROP ROLE removes an existing database principal", true
	case *ast.DropPolicyNode:
		return n.Name, "DROP POLICY removes an access-control protection", true
	case *ast.DropCoordinationNodeNode:
		return n.Name, dropCoordinationNodeReason, true
	case *ast.DropTopicNode:
		return n.Name, dropTopicReason, true
	default:
		return "", "", false
	}
}

func assessStatementList(nodes *ast.StatementList, assessment StatementAssessment) StatementAssessment {
	if nodes != nil {
		for _, child := range nodes.Statements {
			raiseAssessment(&assessment, assessNode(child))
		}
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
	case *ast.AlterColumnOperation:
		return classifyAlterColumn(o)
	case *ast.AlterGeneratedColumnExpressionOperation:
		return Warning, "SET EXPRESSION rewrites generated column values"
	case *ast.AddSkippingIndexOperation:
		return Warning, "ADD INDEX can affect write workload during build"
	case *ast.ReplaceIndexOperation:
		return classifyReplaceIndex(o)
	case *ast.AlterIndexVisibilityOperation:
		return Warning, "ALTER INDEX changes which index the optimizer can use, and so query plans"
	case *ast.ExtensionAlterOperation:
		if o == nil {
			return classifyExtension(nil)
		}
		return classifyExtension(o.Payload)
	default:
		return Safe, "does not remove data or tighten constraints"
	}
}

// classifyReplaceIndex judges an index dropped and added again in one
// statement, as a DROP INDEX and the CREATE INDEX after it are judged.
func classifyReplaceIndex(op *ast.ReplaceIndexOperation) (Severity, string) {
	unique := op.Index != nil && op.Index.Unique
	switch {
	case op.DropsUniqueConstraint && !unique:
		return Destructive, "the index rebuild removes the uniqueness a UNIQUE constraint enforces"
	case unique:
		return Warning, "rebuilding a UNIQUE index can fail on existing duplicate values"
	default:
		return Warning, "rebuilding an index can affect query plans and constraints"
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
	// A modification that says which properties it changes is judged by
	// those. Judged by the column alone, every NOT NULL column reads as a SET
	// NOT NULL, and a plan whose only statement is SET DEFAULT is reported as
	// one that can fail on NULL rows (stokaro/ptah#3645).
	if !op.Column.Nullable && (!op.HasChanged || op.Changed.Nullability) {
		return Warning, "SET NOT NULL can fail when existing rows contain NULL"
	}
	if op.HasChanged && op.Changed.Default && !op.Changed.Type && !op.Changed.Nullability {
		return classifyDefaultChange(op.Column)
	}
	return Warning, "column modification needs manual review"
}

// classifyDefaultChange judges a modification that changes only the default.
// Neither direction touches a stored row; dropping one changes what an INSERT
// that leaves the column out writes, and on a NOT NULL column makes it fail.
func classifyDefaultChange(column *ast.ColumnNode) (Severity, string) {
	if column.Default == nil {
		return Warning, dropDefaultReason
	}
	return Safe, "SET DEFAULT changes only rows inserted later"
}

// dropDefaultReason is why dropping a column's default is a warning, in the
// words the AST and the SQL-text classifiers both report.
const dropDefaultReason = "DROP DEFAULT can break writers that leave the column out"

// classifyAlterColumn judges an ALTER COLUMN action. Dropping the default is
// judged as it is in a restatement; the other actions are left to the words
// of the statement they render, which name them.
func classifyAlterColumn(op *ast.AlterColumnOperation) (Severity, string) {
	if op.Action == ast.AlterColumnDropDefault {
		return Warning, dropDefaultReason
	}
	return Safe, "does not remove data or tighten constraints"
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

// assessRawSQL classifies one statement by its words. keepsNullability says
// the statement comes from a column modification that states its changes and
// leaves nullability alone, so a NOT NULL it restates is the column's
// existing constraint and not a new one.
func assessRawSQL(sql string, assessment StatementAssessment, keepsNullability bool) StatementAssessment {
	words, dropsDefault := withoutDefaultConstraintDrop(rawWords(sql))

	if hasWordPrefix(words, "DROP", "ASYNC", "REPLICATION") && !slices.Contains(words, "CASCADE") {
		assessment.Severity = Warning
		assessment.Reason = keepReplicaTablesReason
		return assessment
	}
	if reason, found := destructivePrefixReason(words); found {
		assessment.Severity = Destructive
		assessment.Reason = reason
		return assessment
	}
	if reason, found := runtimeObjectChangeReason(words); found {
		assessment.Severity, assessment.Reason = Warning, reason
		return assessment
	}
	switch {
	case hasWordSequence(words, "DISABLE", "ROW", "LEVEL", "SECURITY"):
		assessment.Severity = Destructive
		assessment.Reason = "DISABLE ROW LEVEL SECURITY removes an access-control protection"
	case hasWordSequence(words, "NO", "FORCE", "ROW", "LEVEL", "SECURITY"):
		assessment.Severity = Destructive
		assessment.Reason = noForceReason
	case hasWordSequence(words, "DROP", "COLUMN"):
		assessment.Severity = Destructive
		assessment.Reason = "DROP COLUMN removes existing column data"
	case hasWordSequence(words, "DROP", "CONSUMER"):
		assessment.Severity = Destructive
		assessment.Reason = dropConsumerReason
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
	case !keepsNullability && restatesNotNull(sql):
		assessment.Severity = Warning
		assessment.Reason, _ = restatedNotNullReason(sql)
	case hasWordPrefix(words, "CREATE", "UNIQUE", "INDEX"):
		assessment.Severity = Warning
		assessment.Reason = "CREATE UNIQUE INDEX can fail on existing duplicate values"
	case dropsDefault:
		assessment.Severity = Warning
		assessment.Reason = dropDefaultReason
	}
	return assessment
}

// defaultConstraintDrop is the DROP CONSTRAINT with which a SQL Server
// migration drops a column's default, in the words [rawWords] reads it as. The
// constraint's name is the server's own, so the statement reads it from
// sys.default_constraints when it runs; the renderer writes it inside the
// literal sp_executesql runs, which is where the quotes come from.
//
// The SQL Server renderer writes this spelling and this list recognizes it. A
// rendering that stops matching reads as DROP CONSTRAINT again, which is the
// louder verdict, and the SQL Server tests of AssessRendered fail on it.
var defaultConstraintDrop = []string{"DROP", "CONSTRAINT", "''", "+", "QUOTENAME", "DC.NAME", "FROM", "SYS.DEFAULT_CONSTRAINTS"}

// withoutDefaultConstraintDrop removes the DROP CONSTRAINT of every
// [defaultConstraintDrop] from words, and reports whether the statement drops
// a default without adding another.
//
// Read as words, that DROP CONSTRAINT says the statement removes a data
// protection. A default constrains no row: dropping one changes what an INSERT
// that leaves the column out writes, which is how dropping a default reads
// everywhere else. Any other DROP CONSTRAINT in the statement stays, and is
// read as it was. A statement that drops the default and adds one replaces it,
// and is judged as a replaced default is elsewhere.
func withoutDefaultConstraintDrop(words []string) (kept []string, dropsDefault bool) {
	found := false
	kept = make([]string, 0, len(words))
	for i := 0; i < len(words); i++ {
		if hasWordPrefix(words[i:], defaultConstraintDrop...) {
			found = true
			i++
			continue
		}
		kept = append(kept, words[i])
	}
	return kept, found && !hasWordSequence(kept, "ADD", "DEFAULT")
}

func raiseAssessment(target *StatementAssessment, source StatementAssessment) {
	if severityRank(source.Severity) <= severityRank(target.Severity) {
		return
	}
	target.Severity = source.Severity
	target.Reason = source.Reason
}

// isTypeChangeSQL reports whether a statement changes a column's type or
// restates its whole definition, the statements the node's own verdict is
// folded into when a node renders several. A restatement carries whatever
// the operation changes, so its words alone under-report it: SQL Server's
// ALTER COLUMN [c] INT NULL reads safe and may drop a NOT NULL.
func isTypeChangeSQL(statement string) bool {
	words := rawWords(statement)
	return hasWordSequence(words, "ALTER", "COLUMN") && hasWordSequence(words, "TYPE") ||
		hasWordSequence(words, "MODIFY", "COLUMN") ||
		hasWordSequence(words, "CHANGE", "COLUMN") ||
		restatesColumn(statement)
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

// replaceTableReason is why replacing a table is destructive.
const replaceTableReason = "CREATE OR REPLACE TABLE drops the existing table and all its rows"

// destructivePrefixes are the statements whose leading words alone make them
// destructive, each with the reason it reports, in the order
// [destructivePrefixReason] tries them.
//
// The two replace forms are MariaDB's and ClickHouse's CREATE OR REPLACE TABLE
// and ClickHouse's REPLACE TABLE. Each drops the table it names and creates an
// empty one: measured on MariaDB 11.8.9 and 12.3.3 and on ClickHouse 26.9.3, a
// table holding two rows holds none after any of them. MySQL and PostgreSQL
// refuse both forms as a syntax error, so reading the words the same way on
// every dialect costs nothing. Without them the statement reads as a CREATE
// TABLE and is reported Safe, and an edited plan that swaps a CREATE TABLE for
// one keeps its destructive=false marker.
//
// CREATE OR REPLACE TEMPORARY TABLE is left out. It replaces only a temporary
// table of the session: measured on MariaDB 12.3.3, over a table holding two
// rows it leaves both rows there once the temporary table is dropped.
var destructivePrefixes = []struct {
	words  []string
	reason string
}{
	{words: []string{"DROP", "TABLE"}, reason: "DROP TABLE removes the table and all rows"},
	{words: []string{"DROP", "DATABASE"}, reason: "DROP DATABASE removes the database and every object in it"},
	{words: []string{"DROP", "SCHEMA"}, reason: "DROP SCHEMA removes the schema and every object in it"},
	{words: []string{"CREATE", "OR", "REPLACE", "TABLE"}, reason: replaceTableReason},
	{words: []string{"REPLACE", "TABLE"}, reason: replaceTableReason},
	{words: []string{"DROP", "TYPE"}, reason: "DROP TYPE removes an existing database type"},
	{words: []string{"DROP", "EXTENSION"}, reason: "DROP EXTENSION removes database objects owned by the extension"},
	{words: []string{"DROP", "FUNCTION"}, reason: "DROP FUNCTION removes executable database behavior"},
	{words: []string{"DROP", "ROLE"}, reason: "DROP ROLE removes an existing database principal"},
	{words: []string{"DROP", "POLICY"}, reason: "DROP POLICY removes an access-control protection"},
	{words: []string{"DROP", "TOPIC"}, reason: dropTopicReason},
	{words: []string{"DROP", "ASYNC", "REPLICATION"}, reason: dropReplicationReason},
	{words: []string{"DROP", "TRANSFER"}, reason: dropTransferReason},
	{words: []string{"DROP", "COORDINATION", "NODE"}, reason: dropCoordinationNodeReason},
	{words: []string{"DROP", "SECRET"}, reason: dropSecretReason},
	{words: []string{"TRUNCATE"}, reason: "TRUNCATE removes all rows from a table"},
}

// assessYDBObjectNode judges a change of a YDB topic, async replication or
// transfer.
func assessYDBObjectNode(node ast.Node, assessment StatementAssessment) StatementAssessment {
	switch n := node.(type) {
	case *ast.DropTopicNode:
		assessment.Subject = n.Name
		assessment.Severity = Destructive
		assessment.Reason = dropTopicReason
	case *ast.AlterTopicNode:
		assessment.Subject = n.Name
		return assessAlterTopic(n, assessment)
	case *ast.DropAsyncReplicationNode:
		assessment.Subject = n.Name
		assessment.Severity = Destructive
		assessment.Reason = dropReplicationReason
		if !n.Cascade {
			assessment.Severity = Warning
			assessment.Reason = keepReplicaTablesReason
		}
	case *ast.DropTransferNode:
		assessment.Subject = n.Name
		assessment.Severity = Destructive
		assessment.Reason = dropTransferReason
	case *ast.AlterAsyncReplicationNode:
		assessment.Subject = n.Name
		assessment.Severity = Warning
		assessment.Reason = "ALTER ASYNC REPLICATION points the replication at another source or credential"
	case *ast.AlterTransferNode:
		assessment.Subject = n.Name
		assessment.Severity = Warning
		assessment.Reason = "ALTER TRANSFER changes the rows the transfer writes from each message"
	}
	return assessment
}

// dropReplicationReason and dropTransferReason are why dropping a YDB async
// replication with CASCADE or a transfer is destructive, and
// keepReplicaTablesReason why a replication dropped without CASCADE is a
// warning, in the words both the AST and the SQL-text classifiers report.
// Measured on 25.1.4.7 and 26.2.1.14: with CASCADE the replica tables go, and
// without it they stay, read-only for good where the replication was not
// failed over first. A plan drops only a failed-over replication without
// CASCADE, whose tables are ordinary; a statement read as text cannot tell
// which it is, and lint rule YD115 reads the migration directory for it.
const (
	dropReplicationReason   = "DROP ASYNC REPLICATION ... CASCADE drops the replica tables with the replication"
	keepReplicaTablesReason = "DROP ASYNC REPLICATION without CASCADE ends the replication and keeps its tables, " +
		"read-only for good unless it was failed over first"
	dropTransferReason = "DROP TRANSFER stops the transfer and drops the topic consumer YDB created for it, with " +
		"its position in the topic"
)

// dropCoordinationNodeReason is why dropping a YDB coordination node is
// destructive, in the words both the AST and the SQL-text classifiers report.
const dropCoordinationNodeReason = "DROP COORDINATION NODE removes the node with its semaphores and rate limiter " +
	"resources, even while a session holds a lock on it"

// dropSecretReason is why DROP SECRET is destructive, in the words both the
// AST and the SQL-text classifiers report.
const dropSecretReason = "DROP SECRET removes a YDB secret whose value nothing can read back"

// assessYDBObject judges a YDB topic, secret or external object statement. A
// dropped topic loses the messages it holds, and a dropped secret a value
// nothing can read back; a rotated secret replaces the value every external
// data source naming it uses; and no external object holds data in YDB, so
// dropping or replacing one warns.
func assessYDBObject(node ast.Node, assessment StatementAssessment) StatementAssessment {
	switch n := node.(type) {
	case *ast.DropTopicNode:
		assessment.Subject, assessment.Severity, assessment.Reason = n.Name, Destructive, dropTopicReason
	case *ast.AlterTopicNode:
		assessment.Subject = n.Name
		return assessAlterTopic(n, assessment)
	case *ast.DropSecretNode:
		assessment.Subject, assessment.Severity, assessment.Reason = n.Name, Destructive, dropSecretReason
	case *ast.AlterSecretNode:
		assessment.Subject, assessment.Severity = n.Name, Warning
		assessment.Reason = "ALTER SECRET replaces the value every external data source naming the secret uses"
	case *ast.DropExternalDataSourceNode:
		assessment.Subject, assessment.Severity, assessment.Reason = n.Name, Warning, dropExternalDataSourceReason
	case *ast.DropExternalTableNode:
		assessment.Subject, assessment.Severity, assessment.Reason = n.Name, Warning, dropExternalTableReason
	case *ast.CreateExternalDataSourceNode:
		assessment.Subject = n.Name
		if n.Replace {
			assessment.Severity, assessment.Reason = Warning, replaceExternalReason
		}
	case *ast.CreateExternalTableNode:
		assessment.Subject = n.Name
		if n.Replace {
			assessment.Severity, assessment.Reason = Warning, replaceExternalReason
		}
	}
	return assessment
}

// The reasons a YDB external object's statements warn with, in the words both
// classifiers report. Neither object holds data in YDB.
const (
	dropExternalDataSourceReason = "DROP EXTERNAL DATA SOURCE removes what YDB reads another system through; " +
		"no data YDB stores is lost"
	dropExternalTableReason = "DROP EXTERNAL TABLE removes the columns YDB reads files through; the files stay"
	replaceExternalReason   = "CREATE OR REPLACE changes what queries reading the external object read"
)

// destructivePrefixReason returns the reason of the first [destructivePrefixes]
// entry the statement's words start with.
func destructivePrefixReason(words []string) (string, bool) {
	if ydbstream.LosesCheckpoint(words) {
		return streamingCheckpointLoss, true
	}
	for _, prefix := range destructivePrefixes {
		if hasWordPrefix(words, prefix.words...) {
			return prefix.reason, true
		}
	}
	return "", false
}

// dropTopicReason and dropConsumerReason are why dropping a YDB topic or one of
// its consumers is destructive, in the words both the AST and the SQL-text
// classifiers report. A consumer's position is where its reader resumes;
// adding the consumer again starts it over at the beginning of the topic.
const (
	dropTopicReason    = "DROP TOPIC removes the topic, every message it holds and every consumer's position in it"
	dropConsumerReason = "DROP CONSUMER removes a topic consumer and its position in the topic"
)

// assessAlterTopic judges a change of a YDB topic: destructive where it drops
// a consumer, including one it adds again because YDB cannot change it in
// place, safe where it only adds consumers, and a warning otherwise, since a
// changed setting can shorten how long the topic keeps a message and a
// changed consumer can read from another point.
func assessAlterTopic(node *ast.AlterTopicNode, assessment StatementAssessment) StatementAssessment {
	consumers := ydbtopic.Compare(node.Spec, node.Previous)
	switch {
	case len(consumers.Removed)+len(consumers.Restarted) > 0:
		assessment.Severity = Destructive
		assessment.Reason = dropConsumerReason
	case ydbtopic.SettingsEqual(node.Spec, node.Previous) && len(consumers.Changed) == 0:
		return assessment
	default:
		assessment.Severity = Warning
		assessment.Reason = "ALTER TOPIC can shorten how long the topic keeps a message, or move where a consumer reads from"
	}
	return assessment
}

// noForceReason is why NO FORCE ROW LEVEL SECURITY is destructive, in the
// words both the AST and the SQL-text classifiers report.
const noForceReason = "NO FORCE ROW LEVEL SECURITY exempts the table owner from its policies"

// droppedTopicConsumers counts the consumers the topic changes drop, those
// added again because YDB cannot change them in place included.
func droppedTopicConsumers(changes []difftypes.TopicDiff) int {
	count := 0
	for _, change := range changes {
		count += len(change.ConsumersRemoved) + len(change.ConsumersRestarted)
	}
	return count
}

// rlsForceDirections counts the FORCE changes that turn the flag on and off.
func rlsForceDirections(changes difftypes.RLSForceChanges) (forced, unforced int) {
	for _, change := range changes {
		if change.Forced {
			forced++
			continue
		}
		unforced++
	}
	return forced, unforced
}

// assessResourcePoolNode assesses changes to YDB resource pools and their
// classifiers. Any other node is safe.
func assessResourcePoolNode(node ast.Node, assessment StatementAssessment) StatementAssessment {
	switch n := node.(type) {
	case *ast.DropResourcePoolNode:
		assessment.Subject = n.Name
		assessment.Severity = Warning
		assessment.Reason = "DROP RESOURCE POOL runs the queries a classifier sends to the pool in the pool default"
	case *ast.DropResourcePoolClassifierNode:
		assessment.Subject = n.Name
		assessment.Severity = Warning
		assessment.Reason = "DROP RESOURCE POOL CLASSIFIER sends its member's queries to another classifier's pool " +
			"or to the pool default"
	}
	return assessment
}

// runtimeObjectChangeReason covers schema operations that change ongoing
// execution or external reads without deleting stored table data.
func runtimeObjectChangeReason(words []string) (string, bool) {
	switch {
	case hasWordPrefix(words, "ALTER", "STREAMING", "QUERY"):
		return streamingExecutionChange, true
	case hasWordPrefix(words, "DROP", "EXTERNAL", "DATA", "SOURCE"):
		return dropExternalDataSourceReason, true
	case hasWordPrefix(words, "DROP", "EXTERNAL", "TABLE"):
		return dropExternalTableReason, true
	case hasWordPrefix(words, "CREATE", "OR", "REPLACE", "EXTERNAL"):
		return replaceExternalReason, true
	default:
		return "", false
	}
}
