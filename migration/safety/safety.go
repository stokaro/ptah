// Package safety classifies a schema diff into findings, each carrying a
// [ptah.run/migration/risk.Severity].
//
// It is an analysis rather than a vocabulary: it reads a comparison and decides
// which changes remove data, drop objects or tighten constraints. The scale
// those decisions are reported on belongs to migration/risk, which several
// other producers share (stokaro/ptah#2246 section 2.2).
//
// SQL text is read by the words every dialect shares. A statement only its
// owner recognizes, such as a dropped YDB topic, is the owner's to judge: the
// owner reports its judgment as ptah.run/core/lintext's FactSafetyEffect, and
// the classifier takes it in place of the words.
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
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/htmlstyle"
	"ptah.run/internal/lintscan"
	"ptah.run/internal/notnullfill"
	"ptah.run/internal/typechange"
	"ptah.run/migration/risk"
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
// 1-based in the slices [Assess], [AssessSQL], [AssessRendered], and
// [AssessRenderedWithCapabilities] return.
//
// Access and AccessReason carry the owner's access assessment for a statement
// whose owned operation declares one ([schemaext.AccessEffectSource]), and are
// empty for every other statement. They are reported apart from Severity,
// which already includes them: a widening or unknown access effect is
// Destructive and a narrowing is a Warning. An assessment the owner did not
// establish is reported as unknown, never as unchanged. A statement carrying
// several owned operations reports the strongest assessment, in the order
// unchanged, narrows, unknown, widens.
type StatementAssessment struct {
	// NativeKind identifies an assessed native operation. Statement is empty for it.
	NativeKind   schemaext.Kind   `json:"native_kind,omitempty"`
	Index        int              `json:"index"`
	NodeType     string           `json:"node_type"`
	Subject      string           `json:"subject,omitempty"`
	Statement    string           `json:"statement,omitempty"`
	Severity     Severity         `json:"severity"`
	Reason       string           `json:"reason"`
	Access       schemaext.Access `json:"access,omitempty"`
	AccessReason string           `json:"access_reason,omitempty"`
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
// functions_removed counts the procedures a diff removes with the functions,
// as functions_added and functions_modified count both.
//
// Every change a comparison reports is counted under at least one category,
// so a diff that has changes ([difftypes.SchemaDiff.HasChanges]) has
// findings, and every category is one [FindingCategories] lists. A modified table or enum type is counted by what changed in it:
// its columns, its values, its comment, or a change its feature owner reports.
//
// The categories are deliberately not disjoint: a change severe enough to
// deserve its own category is counted there as well as under the generic
// category that also covers it — a removal that takes a UNIQUE constraint's
// enforcement with it, or a column modification the server refuses to cast
// while rows hold values. Read the findings as reasons to look, not as a
// partition whose counts sum to a statement total.
//
// A feature change that cannot be snapshotted, an invalid access assessment
// included, is counted as Destructive rather than dropped, under its kind when
// it names a valid one. Encoding the same diff through the owners' codecs
// refuses that change instead, because a document must not carry an assessment
// its owner did not establish. Both fail closed. Comparison snapshots every
// change through its owner's codec, so only a diff assembled by hand reaches
// either path.
func ClassifySchemaDiff(diff *difftypes.SchemaDiff) []Finding {
	if diff == nil {
		return nil
	}

	var findings []Finding
	// A feature owner's changes are classified by what the owner reports,
	// under its change kind; see [featureCategories].
	appendFeatureFindings(&findings, diff.FeatureChanges)
	for _, view := range diff.MaterializedViewsModified {
		appendViewFeatureFindings(&findings, view)
	}
	for _, table := range diff.TablesModified {
		appendFeatureFindings(&findings, table.FeatureChanges)
	}
	for _, category := range countedCategories {
		add(&findings, category.name, category.count(diff), category.severity)
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
func AssessRendered(ctx context.Context, runtime RenderingRuntime, nodes []ast.Node, dialect string) ([]StatementAssessment, error) {
	return AssessRenderedWithCapabilities(ctx, runtime, nodes, dialect, capability.ForDialect(dialect))
}

// AssessRenderedWithCapabilities returns per-rendered-SQL-statement risk
// classifications using the same server-version capability set as planning and
// rendering on live database paths. The dialect accepts any spelling
// platform.NormalizeDialect resolves.
//
// The caller selects the runtime. All assessment units are rendered in one
// batch, with node provenance retained in the reply, and the dialect's owner
// judges every rendered statement in one call more, as [AssessSQL] describes.
// A missing runtime, rendering or judging errors, incomplete replies, and
// cancellation return no assessments.
//
// There is one assessment per rendered statement, not per node, because a node
// can render into several statements on some dialects, and Index is 1-based
// over that flattened list. Each statement is classified from its own SQL, and
// the node-level verdict is folded into the statements it applies to — so a
// narrowing type change stays destructive where the SQL alone would not say so
// — never lowering a statement's own classification. A raw SQL node's verdict
// is a reading of its words, so it is not folded into a statement its owner
// judged: the owner's judgment takes the place of that reading.
//
// An owner operation's verdict, its access assessment included, applies to
// every statement its node renders when the node is that one operation
// ([ast.IsolatedExtension]), which is how the planners build them. A node
// carrying an owner operation beside other work ([ast.MixedExtension]) cannot
// say which of its statements the operation wrote, so each of them takes the
// node's verdict raised to Destructive, with an unknown access effect when the
// operation makes an access claim. Raising never lowers what the node
// established: a known widening stays a widening.
func AssessRenderedWithCapabilities(
	ctx context.Context,
	runtime RenderingRuntime,
	nodes []ast.Node,
	dialect string,
	caps capability.Capabilities,
) ([]StatementAssessment, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, err
	}
	target := platform.NormalizeDialect(dialect)
	if target == "" {
		target = dialect
	}
	units, err := assessmentUnits(ctx, runtime, nodes, target, caps)
	if err != nil {
		return nil, err
	}
	result, err := renderer.Render(ctx, runtime, renderer.Request{Target: target, Capabilities: caps, Nodes: units})
	if err != nil {
		return nil, err
	}
	_, err = result.SQLFragments()
	if err != nil {
		return nil, err
	}
	rendered := make([][]string, len(units))
	var all []string
	for i := range units {
		statements := renderedFragmentStatements(result, i, dialect)
		rendered[i] = statements
		all = append(all, statements...)
	}
	judgments, err := judgeStatements(ctx, runtime, target, all)
	if err != nil {
		return nil, err
	}
	var assessments []StatementAssessment
	for i, node := range units {
		nodeAssessment := assessNode(node)
		placement := ast.PlacementOf(node)
		if placement == ast.MixedExtension {
			nodeAssessment = mixedVerdict(node)
		}
		keepsNull := keepsNullability(node)
		fills := fillsNullRows(node)
		_, raw := node.(*ast.RawSQLNode)
		for _, statement := range rendered[i] {
			judged := judgments[len(assessments)]
			assessment := assessStatement(statement, keepsNull, judged)
			assessment.NodeType = nodeAssessment.NodeType
			if assessment.Subject == "" {
				assessment.Subject = nodeAssessment.Subject
			}
			ownedText := raw && judged != nil
			if !ownedText && (placement != ast.NoExtension || len(rendered[i]) == 1 || isTypeChangeSQL(statement)) {
				raiseAssessment(&assessment, nodeAssessment)
			}
			if fills {
				judgeNullFillPair(&assessment, statement, target)
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

// assessmentUnits is nodes, with each ALTER TABLE that carries several
// operations, and a column's NULL rows filled by its target's renderer, split
// into one ALTER TABLE per operation. Rendered one operation at a time, each
// fill and SET NOT NULL is assessed with the operation that wrote it, so the
// SET NOT NULL of a column that is filled is not confused with the one of a
// column that is not. A plan that fills carries one column modification per
// ALTER TABLE, so this changes nothing about the statements a planned node
// reports.
//
// The renderer says whether it fills: an ALTER TABLE is split only when its
// render holds the fill, an UPDATE, which is what the PostgreSQL family
// writes. MySQL's MODIFY, SQL Server's ALTER COLUMN and Oracle's MODIFY carry
// the whole definition in one statement, write no UPDATE, and stay whole.
func assessmentUnits(
	ctx context.Context, runtime RenderingRuntime, nodes []ast.Node, target string, caps capability.Capabilities,
) ([]ast.Node, error) {
	var candidates []ast.Node
	for _, node := range nodes {
		if alter, ok := node.(*ast.AlterTableNode); ok && len(alter.Operations) >= 2 &&
			slices.ContainsFunc(alter.Operations, operationFillsNullRows) {
			candidates = append(candidates, node)
		}
	}
	if len(candidates) == 0 {
		return nodes, nil
	}
	rendered, err := renderer.Render(ctx, runtime, renderer.Request{Target: target, Capabilities: caps, Nodes: candidates})
	if err != nil {
		return nil, err
	}
	fragments, err := rendered.SQLFragments()
	if err != nil {
		return nil, err
	}
	filled := make(map[ast.Node]bool, len(candidates))
	for i, node := range candidates {
		filled[node] = slices.ContainsFunc(sqlutil.SplitSQLStatementsForDialect(fragments[i], target), func(statement string) bool {
			return hasWordPrefix(rawWords(sqlutil.StripCommentsForDialect(statement, target)), "UPDATE")
		})
	}
	units := make([]ast.Node, 0, len(nodes))
	for _, node := range nodes {
		alter, ok := node.(*ast.AlterTableNode)
		if !ok || !filled[node] {
			units = append(units, node)
			continue
		}
		for _, operation := range alter.Operations {
			unit := *alter
			unit.Operations = []ast.AlterOperation{operation}
			units = append(units, &unit)
		}
	}
	return units, nil
}

// fillsNullRows reports whether node is an ALTER TABLE whose one operation
// fills a column's NULL rows before SET NOT NULL, as the PostgreSQL family
// renders it.
//
// The dialect is not asked here. Only a PostgreSQL-family renderer writes the
// UPDATE and the SET NOT NULL judgeNullFillPair recognizes; MySQL's MODIFY,
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
// Read by its words alone, the fill is an UPDATE of the column's NULL rows. The SET NOT NULL
// after it reads as a statement that can fail on a NULL row, and the fill
// before it has just removed those rows. So the fill is a warning for the rows
// it rewrites, and the SET NOT NULL is safe. A NULL another session writes
// between the two still fails it, the window every fill-then-constrain
// migration has; the report judges the plan's statements, not concurrent
// writers.
//
// The fill is the one UPDATE the PostgreSQL renderer writes for a column
// modification, which is how it is told from the other statements here;
// [notnullfill.FillsNullRows] decided that the operation writes it.
func judgeNullFillPair(assessment *StatementAssessment, statement, target string) {
	words := rawWords(sqlutil.StripCommentsForDialect(statement, target))
	switch {
	case hasWordPrefix(words, "UPDATE"):
		assessment.Severity = Warning
		assessment.Reason = "UPDATE rewrites the column's NULL rows with its declared default before SET NOT NULL"
	case hasWordSequence(words, "SET", "NOT", "NULL"):
		assessment.Severity = Safe
		assessment.Reason = "SET NOT NULL follows the UPDATE that fills the column's NULL rows"
	}
}

// AssessSQL returns a best-effort classification of rendered SQL statements
// for a target, one per statement, in input order with 1-based Index.
//
// A statement whose effect only its owner knows is classified by the owner's
// judgment ([lintext.FactSafetyEffect]), which takes the place of the reading
// of its words: a behavioral impact is a Warning and a destructive one
// Destructive. With a target, which accepts any name the runtime registers,
// the target's splitter judges every statement in one call, and a target whose
// splitter judges no safety effect leaves every statement to its words. Where
// the owner's lexing reads one statement as several, each piece is classified
// on its own, a piece the owner did not judge by its words, and the most
// severe piece decides. With
// an empty target, every owner that registers an annotator judges the
// statements, in one call, over a split that knows no dialect. Where several
// owners judge one statement, the most severe impact wins and their reasons
// are joined in target name order. A statement an owner declined to judge,
// because its own lexing reads the text otherwise, is Destructive, with the
// decline's reason after the judgments: its effect under that reading is not
// established.
//
// Every other statement is classified by its words, insensitive to its casing
// and layout, so the same statement classifies the same however it is spelled;
// one matching no known destructive or warning shape is reported Safe with the
// default reason.
//
// A missing context or runtime wraps [schemaext.ErrInvalidValue]; a target
// without a splitter wraps [ptah.run/core/ptaherr.ErrUnsupportedDialect]. A
// runtime error or cancellation returns no assessments.
func AssessSQL(ctx context.Context, runtime Runtime, target string, statements []string) ([]StatementAssessment, error) {
	judgments, err := judgeStatements(ctx, runtime, target, statements)
	if err != nil {
		return nil, err
	}
	assessments := make([]StatementAssessment, 0, len(statements))
	for i, statement := range statements {
		assessment := assessStatement(statement, false, judgments[i])
		assessment.Index = i + 1
		assessments = append(assessments, assessment)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return assessments, nil
}

// assessStatement classifies one statement, knowing whether its operation
// leaves the column's nullability alone (see [assessRawSQL]). A statement no
// owner judged is read by its words. One an owner judged a piece of is read
// piece by piece, as the owner's split reads it: a judged piece by the
// owner's judgment, and any other by its words, so the judgment of one piece
// does not hide what another does. The most severe piece decides.
func assessStatement(statement string, keepsNullability bool, pieces []statementPiece) StatementAssessment {
	assessment := StatementAssessment{
		NodeType:  "sql",
		Statement: strings.TrimSpace(statement),
		Severity:  Safe,
		Reason:    safeReason,
	}
	if pieces == nil {
		return assessRawSQL(statement, assessment, keepsNullability)
	}
	for _, piece := range pieces {
		verdict := StatementAssessment{Severity: Safe, Reason: safeReason}
		if piece.judgment != nil {
			verdict.Severity, verdict.Reason = piece.judgment.severity, piece.judgment.reason
		} else {
			verdict = assessRawSQL(piece.sql, verdict, keepsNullability)
		}
		raiseAssessment(&assessment, verdict)
	}
	return assessment
}

// safeReason is the reason of a statement nothing marks as risky.
const safeReason = "does not remove data or tighten constraints"

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
		if assessment.Access == "" {
			continue
		}
		if _, err := fmt.Fprintf(w, "     access %s: %s\n", assessment.Access, assessment.AccessReason); err != nil {
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
<td class="comment">{{.Reason}}{{if .Access}}<div class="access">access {{.Access}}: {{.AccessReason}}</div>{{end}}</td>
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
		verdict := classifyExtensionNode(node)
		assessment.Severity, assessment.Reason = verdict.severity, verdict.reason
		combineAccess(&assessment, verdict.access.Access, verdict.access.Reason)
		assessment.Subject = extensionSubject(node)
	case *ast.AlterTableNode:
		// A typed nil ALTER TABLE carries no operation. A statement list that
		// holds one beside an owner operation is assessed as a whole when it
		// fails closed, so it must not be dereferenced.
		if n == nil {
			return assessment
		}
		assessment.Subject = n.Name
		return assessAlterTable(n, assessment)
	case *ast.DropTypeNode:
		assessment.Subject = n.Name
		assessment.Severity = Destructive
		assessment.Reason = "DROP TYPE removes an existing database type"
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
	case *ast.DropFunctionNode:
		return n.Name, "DROP FUNCTION removes executable database behavior", true
	case *ast.DropRoleNode:
		return n.Name, "DROP ROLE removes an existing database principal", true
	case *ast.DropPolicyNode:
		return n.Name, "DROP POLICY removes an access-control protection", true
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

// assessAlterTable reads an owned operation through its owner's verdict and
// every other operation through classifyAlterOperation, which has no case for
// an owned one.
func assessAlterTable(n *ast.AlterTableNode, assessment StatementAssessment) StatementAssessment {
	for _, op := range n.Operations {
		if extension, ok := op.(*ast.ExtensionAlterOperation); ok {
			applyExtensionVerdict(&assessment, classifyExtensionNode(extension))
			continue
		}
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
	case *ast.ReplaceIndexOperation:
		return classifyReplaceIndex(o)
	case *ast.AlterIndexVisibilityOperation:
		return Warning, "ALTER INDEX changes which index the optimizer can use, and so query plans"
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

// assessRawSQL classifies one statement by its words, which every dialect
// shares; a statement only its owner recognizes is the owner's to judge (see
// [AssessSQL]). keepsNullability says the statement comes from a column
// modification that states its changes and leaves nullability alone, so a NOT
// NULL it restates is the column's existing constraint and not a new one.
func assessRawSQL(sql string, assessment StatementAssessment, keepsNullability bool) StatementAssessment {
	words, dropsDefault := withoutDefaultConstraintDrop(rawWords(sql))

	if reason, found := destructivePrefixReason(words); found {
		assessment.Severity = Destructive
		assessment.Reason = reason
		return assessment
	}
	switch {
	case hasWordSequence(words, "DISABLE", "ROW", "LEVEL", "SECURITY"):
		assessment.Severity = Destructive
		assessment.Reason = "DISABLE ROW LEVEL SECURITY removes an access-control protection"
	case hasWordSequence(words, "NO", "FORCE", "ROW", "LEVEL", "SECURITY"):
		assessment.Severity = Destructive
		assessment.Reason = noForceReason
	case hasWordSequence(words, "DROP", "COLUMN"), dropsColumnWithoutKeyword(sql):
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

// notColumnDrops are the words that follow an ALTER TABLE's DROP when it
// drops something other than a column, in a dialect Ptah reads: an index, a
// key, a constraint, a partition or a projection, a YDB changefeed, a Spanner
// row deletion policy (`DROP ROW DELETION POLICY`, and `DROP TTL` in its
// PostgreSQL dialect), Oracle's unused columns, MariaDB's system versioning,
// and the property an ALTER COLUMN clause drops, such as its default. The
// list holds for every dialect, so a column named by one of these words and
// dropped without COLUMN is read as the other object.
var notColumnDrops = []string{
	"CHANGEFEED", "CHECK", "COLUMN", "CONSTRAINT", "DEFAULT", "DETACHED", "EXPRESSION", "FAMILY",
	"FOREIGN", "HIDDEN", "IDENTITY", "INDEX", "KEY", "MASKED", "NOT", "PART", "PARTITION", "PERIOD",
	"PERSISTED", "PRIMARY", "PROJECTION", "ROW", "ROWGUIDCOL", "SPARSE", "STATISTICS", "SUBPARTITION",
	"SYSTEM", "TTL", "UNIQUE", "UNUSED",
}

// dropsColumnWithoutKeyword reports whether an ALTER TABLE statement drops a
// column without the COLUMN keyword, which PostgreSQL, CockroachDB, MySQL,
// MariaDB and SQLite take as `DROP name` and `DROP IF EXISTS name`, and Oracle
// as `DROP (name, ...)`. Any word but those of [notColumnDrops] after DROP is
// the column's name. On SQL Server the same spelling drops the constraint it
// names, which is destructive as well.
//
// The statement is read as lint reads one with no dialect, so a DROP inside a
// string literal or a comment is not a DROP, and keywords are compared in
// ASCII alone: a column named with a letter that only uppercases to ASCII,
// such as U+017F, is no keyword.
func dropsColumnWithoutKeyword(sql string) bool {
	for _, statement := range lintscan.Split(sql, lintscan.ForDialect("")) {
		words := statement.SourceWords
		if len(words) < 2 || asciiUpper(words[0]) != "ALTER" || asciiUpper(words[1]) != "TABLE" {
			continue
		}
		for i := 2; i+1 < len(words); i++ {
			if asciiUpper(words[i]) == "DROP" && !slices.Contains(notColumnDrops, asciiUpper(words[i+1])) {
				return true
			}
		}
	}
	return false
}

// asciiUpper upper-cases the ASCII letters of word and leaves every other
// rune as it is, so a keyword comparison cannot match a name that only
// Unicode case mapping turns into the keyword.
func asciiUpper(word string) string {
	return strings.Map(func(r rune) rune {
		if 'a' <= r && r <= 'z' {
			return r - ('a' - 'A')
		}
		return r
	}, word)
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

// Fold raises target to source: the severity and its reason only rise, and
// the strongest access assessment either carries is kept. Index, NodeType,
// Subject, and Statement remain target's own. A caller uses it to attach an
// owner's verdict to the same statement classified from its text alone.
func Fold(target *StatementAssessment, source StatementAssessment) {
	raiseAssessment(target, source)
}

// raiseAssessment folds source into target: the severity only rises, and the
// strongest access assessment either carries is kept.
func raiseAssessment(target *StatementAssessment, source StatementAssessment) {
	combineAccess(target, source.Access, source.AccessReason)
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
	{words: []string{"TRUNCATE"}, reason: "TRUNCATE removes all rows from a table"},
}

// destructivePrefixReason returns the reason of the first [destructivePrefixes]
// entry the statement's words start with.
func destructivePrefixReason(words []string) (string, bool) {
	for _, prefix := range destructivePrefixes {
		if hasWordPrefix(words, prefix.words...) {
			return prefix.reason, true
		}
	}
	return "", false
}

// noForceReason is why NO FORCE ROW LEVEL SECURITY is destructive, in the
// words both the AST and the SQL-text classifiers report.
const noForceReason = "NO FORCE ROW LEVEL SECURITY exempts the table owner from its policies"

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

func renderedFragmentStatements(result renderer.Result, index int, dialect string) []string {
	fragment := result.Fragments[index].SQL
	if result.ExecutableUnits {
		if fragment == "" {
			return nil
		}
		return []string{fragment}
	}
	statements := sqlutil.SplitSQLStatementsForDialect(fragment, dialect)
	if len(statements) == 0 && strings.TrimSpace(fragment) != "" {
		return []string{strings.TrimSpace(fragment)}
	}
	return statements
}
