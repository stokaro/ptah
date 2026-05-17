// Package clickhouse provides a ClickHouse-specific SQL renderer.
//
// ClickHouse differs from the other dialects supported by Ptah in several
// fundamental ways:
//
//   - Tables require an explicit engine. The MergeTree family additionally
//     requires an ORDER BY clause (or, equivalently, a PRIMARY KEY).
//   - Foreign keys are accepted by the parser only on a handful of engines
//     and are never enforced; Ptah emits them as commented-out hints.
//   - There is no auto-increment / SERIAL.
//   - Secondary indexes are data-skipping (ADD INDEX … TYPE … GRANULARITY n).
//   - Functions, extensions, RLS policies and roles use syntax that is
//     incompatible with the PostgreSQL-shaped AST nodes Ptah produces; this
//     renderer emits a -- CLICKHOUSE: not supported comment for them so they
//     fall out of the migration cleanly instead of producing invalid SQL.
//
// Engine, ORDER BY, PARTITION BY, PRIMARY KEY, SAMPLE BY, SETTINGS and TTL
// are sourced from the table's `platform.clickhouse.<key>` annotation
// overrides (see core/goschema/types.go). The override mechanism stores
// keys uppercased on CreateTableNode.Options, so this renderer looks them
// up by their uppercase form.
package clickhouse

import (
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer/dialects/internal/bufwriter"
	"github.com/stokaro/ptah/core/renderer/types"
)

// DialectName is the canonical dialect identifier for ClickHouse.
const DialectName = "clickhouse"

var _ types.RenderVisitor = (*Renderer)(nil)

// Renderer renders an AST node tree to ClickHouse SQL.
type Renderer struct {
	w *bufwriter.Writer
}

// New constructs a ClickHouse renderer with an empty output buffer.
func New() *Renderer {
	return &Renderer{w: &bufwriter.Writer{}}
}

// Dialect returns the dialect identifier.
func (r *Renderer) Dialect() string { return DialectName }

// GetDialect is an alias for Dialect.
func (r *Renderer) GetDialect() string { return r.Dialect() }

// Reset clears the output buffer.
func (r *Renderer) Reset() { r.w.Reset() }

// Output returns the accumulated SQL.
func (r *Renderer) Output() string { return r.w.Output() }

// GetOutput is an alias for Output.
func (r *Renderer) GetOutput() string { return r.Output() }

// Render renders a single AST node into a fresh buffer and returns it.
func (r *Renderer) Render(node ast.Node) (string, error) {
	r.Reset()
	if err := node.Accept(r); err != nil {
		return "", err
	}
	return r.Output(), nil
}

// notSupported writes a CLICKHOUSE-prefixed comment line explaining why a
// PostgreSQL/MySQL-shaped node is being dropped from the migration.
func (r *Renderer) notSupported(feature, name string) {
	if name == "" {
		r.w.WriteLinef("-- CLICKHOUSE: %s is not supported", feature)
		return
	}
	r.w.WriteLinef("-- CLICKHOUSE: %s %q is not supported", feature, name)
}

// escapeStringLiteral doubles single quotes for safe embedding in a SQL
// string literal.
func escapeStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// mapColumnType translates a generic SQL column type spelling into the
// ClickHouse equivalent. Type names not recognised are returned verbatim;
// callers may legitimately write native ClickHouse type names in their
// annotations (e.g. `LowCardinality(String)`), and this function must not
// mangle them.
//
// The matcher is intentionally narrow: it splits the type on '(' so a
// `VARCHAR(255)` still maps to `String`, but anything that doesn't look like
// a known SQL type is passed through untouched.
func mapColumnType(t string) (string, error) {
	upper := strings.ToUpper(strings.TrimSpace(t))
	if upper == "" {
		return "", fmt.Errorf("clickhouse: column type is empty")
	}

	// Strip parametrisation for the base lookup, keeping it around for the
	// (small number of) types where the precision actually matters.
	base := upper
	var params string
	if idx := strings.Index(upper, "("); idx >= 0 {
		base = strings.TrimSpace(upper[:idx])
		params = strings.TrimSpace(upper[idx:])
	}

	switch base {
	case "SERIAL", "BIGSERIAL", "SMALLSERIAL":
		return "", fmt.Errorf("clickhouse: %s has no auto-increment equivalent; use UUID/Int64 + an explicit value or use a ReplacingMergeTree pattern", upper)
	case "TEXT", "VARCHAR", "CHAR", "CHARACTER", "STRING", "CHARACTER VARYING", "CITEXT":
		return "String", nil
	case "BYTEA", "BLOB":
		return "String", nil
	case "BOOLEAN", "BOOL":
		return "Bool", nil
	case "SMALLINT", "INT2":
		return "Int16", nil
	case "INTEGER", "INT", "INT4":
		return "Int32", nil
	case "BIGINT", "INT8":
		return "Int64", nil
	case "REAL", "FLOAT4":
		return "Float32", nil
	case "DOUBLE", "DOUBLE PRECISION", "FLOAT", "FLOAT8":
		return "Float64", nil
	case "NUMERIC", "DECIMAL":
		if params == "" {
			return "Decimal(38, 10)", nil
		}
		return "Decimal" + params, nil
	case "DATE":
		return "Date", nil
	case "TIMESTAMP", "TIMESTAMPTZ", "DATETIME", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITHOUT TIME ZONE":
		return "DateTime64(3)", nil
	case "TIME":
		// ClickHouse has no plain TIME type; surface this rather than silently
		// pick something lossy.
		return "", fmt.Errorf("clickhouse: TIME has no direct equivalent; map to String or DateTime64 explicitly via platform.clickhouse.type")
	case "UUID":
		return "UUID", nil
	case "JSON", "JSONB":
		// Treat JSON as a String; ClickHouse's native JSON type is still
		// experimental. Users who want it can override via platform.clickhouse.type.
		return "String", nil
	}

	// Pass through native ClickHouse types untouched.
	return t, nil
}

// renderColumnType produces the final type expression for a column,
// applying Nullable() wrapping for nullable columns.
func renderColumnType(col *ast.ColumnNode) (string, error) {
	mapped, err := mapColumnType(col.Type)
	if err != nil {
		return "", fmt.Errorf("column %q: %w", col.Name, err)
	}
	if col.Nullable && !col.Primary {
		// Don't wrap if already wrapped (e.g. user supplied native CH type).
		if !strings.HasPrefix(mapped, "Nullable(") {
			mapped = "Nullable(" + mapped + ")"
		}
	}
	return mapped, nil
}

// renderColumn renders a single column definition for use inside a
// CREATE TABLE column list.
func (r *Renderer) renderColumn(col *ast.ColumnNode) (string, error) {
	chType, err := renderColumnType(col)
	if err != nil {
		return "", err
	}

	parts := []string{fmt.Sprintf("  %s %s", col.Name, chType)}

	if col.Default != nil {
		switch {
		case col.Default.Expression != "":
			parts = append(parts, "DEFAULT "+col.Default.Expression)
		case col.Default.Value != "":
			parts = append(parts, "DEFAULT "+escapeStringLiteral(col.Default.Value))
		}
	}

	if col.Comment != "" {
		parts = append(parts, "COMMENT "+escapeStringLiteral(col.Comment))
	}

	return strings.Join(parts, " "), nil
}

// tableEngineSpec captures the parsed engine + family modifiers for a CH
// CREATE TABLE statement.
type tableEngineSpec struct {
	engine      string // e.g. "MergeTree", "ReplacingMergeTree(ver)"
	orderBy     string // raw ORDER BY expression, parenthesised if needed
	partitionBy string
	primaryKey  string
	sampleBy    string
	settings    string
	ttl         string
}

// isMergeTreeFamily reports whether the engine requires ORDER BY/PRIMARY KEY.
func (s tableEngineSpec) isMergeTreeFamily() bool {
	upper := strings.ToUpper(strings.TrimSpace(s.engine))
	// Strip any function-style args after the engine name.
	if i := strings.Index(upper, "("); i >= 0 {
		upper = strings.TrimSpace(upper[:i])
	}
	return strings.HasSuffix(upper, "MERGETREE")
}

// resolveTableEngineSpec extracts the engine + modifier set for a table.
// Annotation overrides live in node.Options under uppercased keys; the
// table's own Engine string is used as a fallback (it's the documented
// `platform.mysql.engine=` channel and is mirrored on the AST node).
func resolveTableEngineSpec(node *ast.CreateTableNode) tableEngineSpec {
	spec := tableEngineSpec{engine: "MergeTree"}

	if engineOpt, ok := node.Options["ENGINE"]; ok && strings.TrimSpace(engineOpt) != "" {
		spec.engine = strings.TrimSpace(engineOpt)
	}
	if v, ok := node.Options["ORDER_BY"]; ok && strings.TrimSpace(v) != "" {
		spec.orderBy = strings.TrimSpace(v)
	}
	if v, ok := node.Options["PARTITION_BY"]; ok && strings.TrimSpace(v) != "" {
		spec.partitionBy = strings.TrimSpace(v)
	}
	if v, ok := node.Options["PRIMARY_KEY"]; ok && strings.TrimSpace(v) != "" {
		spec.primaryKey = strings.TrimSpace(v)
	}
	if v, ok := node.Options["SAMPLE_BY"]; ok && strings.TrimSpace(v) != "" {
		spec.sampleBy = strings.TrimSpace(v)
	}
	if v, ok := node.Options["SETTINGS"]; ok && strings.TrimSpace(v) != "" {
		spec.settings = strings.TrimSpace(v)
	}
	if v, ok := node.Options["TTL"]; ok && strings.TrimSpace(v) != "" {
		spec.ttl = strings.TrimSpace(v)
	}

	return spec
}

// columnNames returns the list of column-level PRIMARY KEY columns in the
// order they were declared. This is the fallback ORDER BY for MergeTree
// tables whose annotation didn't specify one explicitly.
func tablePrimaryKeyColumns(node *ast.CreateTableNode) []string {
	var cols []string
	for _, col := range node.Columns {
		if col.Primary {
			cols = append(cols, col.Name)
		}
	}
	if len(cols) > 0 {
		return cols
	}
	// Also look at table-level PrimaryKey constraints.
	for _, c := range node.Constraints {
		if c.Type == ast.PrimaryKeyConstraint && len(c.Columns) > 0 {
			return c.Columns
		}
	}
	return nil
}

// parenList wraps a comma-separated expression in parentheses unless it
// already starts with one.
func parenList(expr string) string {
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, "(") {
		return expr
	}
	return "(" + expr + ")"
}

// splitColumns parses a comma-separated column list (with or without
// surrounding parentheses) into individual trimmed column names. It does
// not attempt to parse arbitrary ORDER BY expressions — when the user
// supplies an expression rather than a plain column list the prefix-of
// check is best-effort and only succeeds for the obvious case.
func splitColumns(expr string) []string {
	expr = strings.TrimSpace(expr)
	expr = strings.TrimPrefix(expr, "(")
	expr = strings.TrimSuffix(expr, ")")
	if expr == "" {
		return nil
	}
	parts := strings.Split(expr, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
	}
	return out
}

// VisitCreateTable renders a CREATE TABLE statement for ClickHouse.
//
// MergeTree-family engines require an ORDER BY; if the annotation does not
// supply one we fall back to the table's primary key columns and otherwise
// return an error. PRIMARY KEY must be a prefix of ORDER BY.
func (r *Renderer) VisitCreateTable(node *ast.CreateTableNode) error {
	spec, err := r.resolveAndValidateTableEngine(node)
	if err != nil {
		return err
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- CLICKHOUSE TABLE: %s (%s) --", node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- CLICKHOUSE TABLE: %s --", node.Name)
	}

	r.w.WriteLinef("CREATE TABLE %s (", node.Name)
	lines, err := r.renderTableBody(node)
	if err != nil {
		return err
	}
	for i, line := range lines {
		if i == len(lines)-1 {
			r.w.WriteLine(line)
		} else {
			r.w.WriteLinef("%s,", line)
		}
	}
	r.writeEngineClause(spec)
	r.w.WriteLine(";")
	r.w.WriteLine("")
	return nil
}

// resolveAndValidateTableEngine extracts the engine spec from the node and
// runs the two MergeTree-family validation rules (ORDER BY presence, and
// PRIMARY KEY being a prefix of ORDER BY).
func (r *Renderer) resolveAndValidateTableEngine(node *ast.CreateTableNode) (tableEngineSpec, error) {
	spec := resolveTableEngineSpec(node)

	if spec.isMergeTreeFamily() && spec.orderBy == "" {
		pkCols := tablePrimaryKeyColumns(node)
		if len(pkCols) == 0 {
			return spec, fmt.Errorf("clickhouse: table %q uses engine %s which requires ORDER BY; set platform.clickhouse.order_by or declare a primary key", node.Name, spec.engine)
		}
		spec.orderBy = strings.Join(pkCols, ", ")
	}

	if spec.primaryKey == "" || spec.orderBy == "" {
		return spec, nil
	}
	pkCols := splitColumns(spec.primaryKey)
	obCols := splitColumns(spec.orderBy)
	if len(pkCols) > len(obCols) {
		return spec, fmt.Errorf("clickhouse: table %q PRIMARY KEY must be a prefix of ORDER BY", node.Name)
	}
	for i, pkCol := range pkCols {
		if pkCol != obCols[i] {
			return spec, fmt.Errorf("clickhouse: table %q PRIMARY KEY must be a prefix of ORDER BY (got PK=%v, ORDER BY=%v)", node.Name, pkCols, obCols)
		}
	}
	return spec, nil
}

// renderTableBody renders the column list and any inline CHECK constraints
// for a CREATE TABLE statement. Other constraint types are silently dropped
// because ClickHouse has no equivalent.
func (r *Renderer) renderTableBody(node *ast.CreateTableNode) ([]string, error) {
	lines := make([]string, 0, len(node.Columns)+len(node.Constraints))
	for _, col := range node.Columns {
		line, err := r.renderColumn(col)
		if err != nil {
			return nil, fmt.Errorf("clickhouse: rendering column %q on table %q: %w", col.Name, node.Name, err)
		}
		lines = append(lines, line)
	}
	for _, c := range node.Constraints {
		if c.Type != ast.CheckConstraint || c.Expression == "" {
			continue
		}
		if c.Name != "" {
			lines = append(lines, fmt.Sprintf("  CONSTRAINT %s CHECK (%s)", c.Name, c.Expression))
			continue
		}
		lines = append(lines, fmt.Sprintf("  CHECK (%s)", c.Expression))
	}
	return lines, nil
}

// writeEngineClause emits the trailing ENGINE/PARTITION BY/... clause for a
// CREATE TABLE statement, in the order ClickHouse expects.
func (r *Renderer) writeEngineClause(spec tableEngineSpec) {
	r.w.Writef(") ENGINE = %s", spec.engine)
	if spec.partitionBy != "" {
		r.w.Writef(" PARTITION BY %s", spec.partitionBy)
	}
	if spec.orderBy != "" {
		r.w.Writef(" ORDER BY %s", parenList(spec.orderBy))
	}
	if spec.primaryKey != "" {
		r.w.Writef(" PRIMARY KEY %s", parenList(spec.primaryKey))
	}
	if spec.sampleBy != "" {
		r.w.Writef(" SAMPLE BY %s", spec.sampleBy)
	}
	if spec.ttl != "" {
		r.w.Writef(" TTL %s", spec.ttl)
	}
	if spec.settings != "" {
		r.w.Writef(" SETTINGS %s", spec.settings)
	}
}

// VisitAlterTable renders ALTER TABLE statements for ClickHouse.
//
// ClickHouse supports ADD/DROP/MODIFY COLUMN against MergeTree tables,
// though MODIFY COLUMN has restrictions on type changes that affect the
// sort key. Constraints translate to ADD/DROP CONSTRAINT (CHECK only);
// foreign keys, primary keys and unique constraints have no equivalent.
func (r *Renderer) VisitAlterTable(node *ast.AlterTableNode) error {
	for _, op := range node.Operations {
		switch op := op.(type) {
		case *ast.AddColumnOperation:
			colLine, err := r.renderColumn(op.Column)
			if err != nil {
				return fmt.Errorf("clickhouse: add column on %q: %w", node.Name, err)
			}
			colLine = strings.TrimPrefix(colLine, "  ")
			r.w.WriteLinef("ALTER TABLE %s ADD COLUMN %s;", node.Name, colLine)
		case *ast.DropColumnOperation:
			r.w.WriteLinef("ALTER TABLE %s DROP COLUMN %s;", node.Name, op.ColumnName)
		case *ast.ModifyColumnOperation:
			chType, err := renderColumnType(op.Column)
			if err != nil {
				return fmt.Errorf("clickhouse: modify column on %q: %w", node.Name, err)
			}
			r.w.WriteLinef("ALTER TABLE %s MODIFY COLUMN %s %s;", node.Name, op.Column.Name, chType)
		case *ast.AddConstraintOperation:
			if op.Constraint.Type != ast.CheckConstraint {
				r.notSupported(fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT (non-CHECK)", node.Name), op.Constraint.Name)
				continue
			}
			if op.Constraint.Name == "" {
				r.w.WriteLinef("-- CLICKHOUSE: ALTER TABLE %s ADD CHECK without a name is skipped (ClickHouse requires a constraint name)", node.Name)
				continue
			}
			r.w.WriteLinef("ALTER TABLE %s ADD CONSTRAINT %s CHECK %s;", node.Name, op.Constraint.Name, parenList(op.Constraint.Expression))
		case *ast.DropConstraintOperation:
			r.w.WriteLinef("ALTER TABLE %s DROP CONSTRAINT %s;", node.Name, op.ConstraintName)
		default:
			return fmt.Errorf("clickhouse: unknown ALTER TABLE operation %T", op)
		}
	}
	return nil
}

// VisitColumn is called from within VisitAlterTable / VisitCreateTable
// rather than as a top-level statement. The actual rendering happens in
// renderColumn; this method exists only to satisfy the visitor interface.
func (r *Renderer) VisitColumn(*ast.ColumnNode) error { return nil }

// VisitConstraint mirrors VisitColumn: constraints are rendered inline by
// the table / alter visitors. This stub satisfies the visitor interface.
func (r *Renderer) VisitConstraint(*ast.ConstraintNode) error { return nil }

// VisitIndex emits a ClickHouse data-skipping index. Without an explicit
// type annotation we emit a `minmax` index with GRANULARITY 8192, which is
// the most generally-useful default. Users wanting `set` / `bloom_filter`
// can override via platform-specific annotations once those are exposed at
// the goschema level.
func (r *Renderer) VisitIndex(node *ast.IndexNode) error {
	if node.Table == "" {
		r.w.WriteLinef("-- CLICKHOUSE: secondary index %q skipped (no target table)", node.Name)
		return nil
	}
	if len(node.Columns) == 0 {
		r.w.WriteLinef("-- CLICKHOUSE: secondary index %q skipped (no columns)", node.Name)
		return nil
	}
	idxType := node.Type
	if idxType == "" {
		idxType = "minmax"
	}
	expr := strings.Join(node.Columns, ", ")
	if len(node.Columns) > 1 {
		expr = "(" + expr + ")"
	}
	r.w.WriteLinef("ALTER TABLE %s ADD INDEX %s %s TYPE %s GRANULARITY 8192;", node.Table, node.Name, expr, idxType)
	return nil
}

// VisitDropIndex emits ALTER TABLE … DROP INDEX. The table name is
// required; without it we emit a self-explanatory comment.
func (r *Renderer) VisitDropIndex(node *ast.DropIndexNode) error {
	if node.Table == "" {
		r.w.WriteLinef("-- CLICKHOUSE: DROP INDEX %s skipped (no target table; ClickHouse requires ALTER TABLE ... DROP INDEX)", node.Name)
		return nil
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("ALTER TABLE %s DROP INDEX %s;", node.Table, node.Name)
	return nil
}

// VisitDropTable emits DROP TABLE [IF EXISTS] name. The SYNC modifier is
// not added here because it changes durability semantics; callers wanting
// it can opt in by raising a separate AST hook in the future.
func (r *Renderer) VisitDropTable(node *ast.DropTableNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	if node.IfExists {
		r.w.WriteLinef("DROP TABLE IF EXISTS %s;", node.Name)
	} else {
		r.w.WriteLinef("DROP TABLE %s;", node.Name)
	}
	return nil
}

// VisitComment passes through SQL comments verbatim.
func (r *Renderer) VisitComment(node *ast.CommentNode) error {
	r.w.WriteLinef("-- %s --", node.Text)
	return nil
}

// VisitRawSQL passes through raw SQL verbatim.
//
// ClickHouse-targeted migrations should not normally produce RawSQLNodes —
// those are emitted by the PostgreSQL planner for its DO-block constraint
// drop — but if a future caller routes one through, we just let it pass.
// Callers responsible for the raw text are also responsible for it being
// compatible with ClickHouse.
func (r *Renderer) VisitRawSQL(node *ast.RawSQLNode) error {
	r.w.WriteLine(node.SQL)
	return nil
}

// VisitEnum is a no-op for ClickHouse. ClickHouse has Enum8 / Enum16 column
// types, but they are declared inline at the column level (not as a
// separately-defined type), so emitting a top-level `CREATE TYPE … ENUM`
// statement here would be invalid SQL.
func (r *Renderer) VisitEnum(node *ast.EnumNode) error {
	r.notSupported("CREATE TYPE ... AS ENUM (use Enum8/Enum16 inline at the column level)", node.Name)
	return nil
}

// VisitCreateType emits a not-supported comment. ClickHouse has neither
// CREATE TYPE nor named domain types.
func (r *Renderer) VisitCreateType(node *ast.CreateTypeNode) error {
	r.notSupported("CREATE TYPE", node.Name)
	return nil
}

// VisitAlterType is a no-op (see VisitCreateType).
func (r *Renderer) VisitAlterType(node *ast.AlterTypeNode) error {
	r.notSupported("ALTER TYPE", node.Name)
	return nil
}

// VisitDropType is a no-op (see VisitCreateType).
func (r *Renderer) VisitDropType(node *ast.DropTypeNode) error {
	r.notSupported("DROP TYPE", node.Name)
	return nil
}

// VisitExtension is a no-op for ClickHouse, which has no equivalent of
// PostgreSQL extensions.
func (r *Renderer) VisitExtension(node *ast.ExtensionNode) error {
	r.notSupported("CREATE EXTENSION", node.Name)
	return nil
}

// VisitDropExtension mirrors VisitExtension.
func (r *Renderer) VisitDropExtension(node *ast.DropExtensionNode) error {
	r.notSupported("DROP EXTENSION", node.Name)
	return nil
}

// VisitCreateFunction is a no-op for ClickHouse. ClickHouse has UDFs but
// the syntax is incompatible with the PostgreSQL-shaped CreateFunctionNode.
func (r *Renderer) VisitCreateFunction(node *ast.CreateFunctionNode) error {
	r.notSupported("CREATE FUNCTION", node.Name)
	return nil
}

// VisitDropFunction mirrors VisitCreateFunction.
func (r *Renderer) VisitDropFunction(node *ast.DropFunctionNode) error {
	r.notSupported("DROP FUNCTION", node.Name)
	return nil
}

// VisitCreatePolicy is a no-op for ClickHouse. ClickHouse has row policies
// but with a different syntax that the PG-shaped node cannot describe.
func (r *Renderer) VisitCreatePolicy(node *ast.CreatePolicyNode) error {
	r.notSupported("CREATE POLICY", node.Name)
	return nil
}

// VisitDropPolicy mirrors VisitCreatePolicy.
func (r *Renderer) VisitDropPolicy(node *ast.DropPolicyNode) error {
	r.notSupported("DROP POLICY", node.Name)
	return nil
}

// VisitAlterTableEnableRLS is a no-op for ClickHouse.
func (r *Renderer) VisitAlterTableEnableRLS(node *ast.AlterTableEnableRLSNode) error {
	r.notSupported("ALTER TABLE ENABLE ROW LEVEL SECURITY", node.Table)
	return nil
}

// VisitAlterTableDisableRLS mirrors VisitAlterTableEnableRLS.
func (r *Renderer) VisitAlterTableDisableRLS(node *ast.AlterTableDisableRLSNode) error {
	r.notSupported("ALTER TABLE DISABLE ROW LEVEL SECURITY", node.Table)
	return nil
}

// VisitCreateRole is a no-op for ClickHouse. ClickHouse supports roles
// but with different syntax than the PG-shaped node represents.
func (r *Renderer) VisitCreateRole(node *ast.CreateRoleNode) error {
	r.notSupported("CREATE ROLE", node.Name)
	return nil
}

// VisitDropRole mirrors VisitCreateRole.
func (r *Renderer) VisitDropRole(node *ast.DropRoleNode) error {
	r.notSupported("DROP ROLE", node.Name)
	return nil
}

// VisitAlterRole mirrors VisitCreateRole.
func (r *Renderer) VisitAlterRole(node *ast.AlterRoleNode) error {
	r.notSupported("ALTER ROLE", node.Name)
	return nil
}
