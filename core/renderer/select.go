package renderer

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/sqlident"
)

// RenderSelect renders a SELECT statement to parameterized SQL for the given
// dialect, returning the SQL text and its positional arguments.
//
// Every value in the statement — comparison operands (including those inside a
// JOIN ON), IN list elements, and the LIMIT/OFFSET bounds — is emitted as a
// placeholder and returned in args, never interpolated into the SQL. Placeholder
// style follows the dialect: $1, $2, … for the PostgreSQL family and ? for
// MySQL, MariaDB, and SQLite. Placeholders are numbered in a single
// left-to-right pass over FROM, the joins, WHERE, then LIMIT/OFFSET, so args are
// ordered to match; a JOIN ON value is numbered before any WHERE value.
//
// Identifiers (table, alias, and column names) are quoted for the dialect via
// internal/sqlident, so a value can never be rendered as an identifier and an
// attacker-shaped identifier or alias cannot break out of its quotes. A
// qualified column renders as "alias"."col" with each part quoted independently.
//
// Supported dialects are PostgreSQL (including CockroachDB and YugabyteDB),
// MySQL, MariaDB, and SQLite. Any other dialect returns an error, as does a nil
// statement, a statement without a FROM table, an empty IN list, a malformed
// operator, a join without a table or ON condition, or a RIGHT/FULL join on
// SQLite (which cannot express one before version 3.39).
func RenderSelect(stmt *ast.SelectStatement, dialect string) (string, []any, error) {
	if stmt == nil {
		return "", nil, errors.New("renderer: nil select statement")
	}
	r, err := newSelectRenderer(dialect)
	if err != nil {
		return "", nil, err
	}
	if err := r.render(stmt); err != nil {
		return "", nil, err
	}
	return r.buf.String(), r.args, nil
}

// placeholderStyle selects how bound parameters are numbered in the output.
type placeholderStyle int

const (
	// placeholderDollar numbers parameters $1, $2, … (PostgreSQL family).
	placeholderDollar placeholderStyle = iota
	// placeholderQuestion emits ? for every parameter (MySQL, MariaDB, SQLite).
	placeholderQuestion
)

// selectRenderer holds the mutable state for one RenderSelect call: the target
// dialect, the placeholder style, the accumulated arguments, and the SQL buffer.
type selectRenderer struct {
	dialect     string
	placeholder placeholderStyle
	args        []any
	buf         strings.Builder
}

func newSelectRenderer(dialect string) (*selectRenderer, error) {
	normalized := platform.NormalizeDialect(dialect)
	style, ok := selectPlaceholderStyle(normalized)
	if !ok {
		return nil, fmt.Errorf("renderer: SELECT rendering is not supported for dialect %q", dialect)
	}
	return &selectRenderer{dialect: normalized, placeholder: style}, nil
}

func selectPlaceholderStyle(normalized string) (placeholderStyle, bool) {
	switch normalized {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		return placeholderDollar, true
	case platform.MySQL, platform.MariaDB, platform.SQLite:
		return placeholderQuestion, true
	default:
		return 0, false
	}
}

// bind records value as a positional argument and returns its placeholder.
func (r *selectRenderer) bind(value any) string {
	r.args = append(r.args, value)
	if r.placeholder == placeholderDollar {
		return "$" + strconv.Itoa(len(r.args))
	}
	return "?"
}

// quote returns identifier quoted for the renderer's dialect.
func (r *selectRenderer) quote(identifier string) string {
	return sqlident.Quote(r.dialect, identifier)
}

// writeQualifiedIdent writes name quoted for the dialect, prefixed by a quoted
// qualifier and a dot when qualifier is non-empty, as in "alias"."col". Each
// part is quoted independently so neither can break out of its quotes.
func (r *selectRenderer) writeQualifiedIdent(qualifier, name string) {
	if strings.TrimSpace(qualifier) != "" {
		r.buf.WriteString(r.quote(qualifier))
		r.buf.WriteString(".")
	}
	r.buf.WriteString(r.quote(name))
}

// writeTableRef writes a quoted table name followed by an optional quoted alias,
// as in "t" or "t" "a". The bare "table alias" form is used rather than "table
// AS alias" because every supported dialect accepts it.
func (r *selectRenderer) writeTableRef(table, alias string) {
	r.buf.WriteString(r.quote(table))
	if strings.TrimSpace(alias) != "" {
		r.buf.WriteString(" ")
		r.buf.WriteString(r.quote(alias))
	}
}

func (r *selectRenderer) render(stmt *ast.SelectStatement) error {
	if strings.TrimSpace(stmt.From) == "" {
		return errors.New("renderer: select statement requires a FROM table")
	}

	r.buf.WriteString("SELECT ")
	if err := r.renderColumns(stmt.Columns); err != nil {
		return err
	}

	r.buf.WriteString(" FROM ")
	r.writeTableRef(stmt.From, stmt.FromAlias)

	if err := r.renderJoins(stmt.Joins); err != nil {
		return err
	}

	if stmt.Where != nil {
		r.buf.WriteString(" WHERE ")
		if err := r.renderExpr(stmt.Where); err != nil {
			return err
		}
	}

	if err := r.renderOrderBy(stmt.OrderBy); err != nil {
		return err
	}

	r.renderLimitOffset(stmt.Limit, stmt.Offset)
	return nil
}

func (r *selectRenderer) renderColumns(columns []ast.ResultColumn) error {
	if len(columns) == 0 {
		r.buf.WriteString("*")
		return nil
	}
	for i, col := range columns {
		if i > 0 {
			r.buf.WriteString(", ")
		}
		if col.Star {
			r.buf.WriteString("*")
			continue
		}
		if strings.TrimSpace(col.Name) == "" {
			return errors.New("renderer: result column has an empty name")
		}
		r.writeQualifiedIdent(col.Qualifier, col.Name)
	}
	return nil
}

// renderJoins appends each join after the FROM clause in declared order. Their
// ON conditions bind their placeholders before the WHERE clause, because they
// render first.
func (r *selectRenderer) renderJoins(joins []ast.JoinClause) error {
	for i := range joins {
		if err := r.renderJoin(&joins[i]); err != nil {
			return err
		}
	}
	return nil
}

// renderJoin appends a single join: " <TYPE> JOIN <table> [alias] ON <cond>".
func (r *selectRenderer) renderJoin(join *ast.JoinClause) error {
	keyword := join.Type.String()
	if keyword == "" {
		return fmt.Errorf("renderer: unknown join type %d", join.Type)
	}
	if err := r.checkJoinDialect(join.Type); err != nil {
		return err
	}
	if strings.TrimSpace(join.Table) == "" {
		return errors.New("renderer: join requires a table")
	}
	if join.On == nil {
		return errors.New("renderer: join requires an ON condition")
	}
	r.buf.WriteString(" ")
	r.buf.WriteString(keyword)
	r.buf.WriteString(" ")
	r.writeTableRef(join.Table, join.Alias)
	r.buf.WriteString(" ON ")
	return r.renderExpr(join.On)
}

// checkJoinDialect rejects join types a dialect cannot express. SQLite gained
// RIGHT and FULL [OUTER] JOIN only in version 3.39 (2022); because Ptah targets
// a range of SQLite versions and cannot assume 3.39+, it rejects those at render
// time rather than emit SQL that fails at execution time on an older engine.
// INNER and LEFT joins are accepted by every supported dialect.
func (r *selectRenderer) checkJoinDialect(t ast.JoinType) error {
	if r.dialect != platform.SQLite {
		return nil
	}
	switch t {
	case ast.JoinRight, ast.JoinFull:
		return fmt.Errorf("renderer: SQLite does not support %s", t)
	default:
		return nil
	}
}

// renderExpr dispatches over the sealed ast.Expression sum type. Each case
// delegates to a small helper so the dispatch stays flat.
func (r *selectRenderer) renderExpr(expr ast.Expression) error {
	switch e := expr.(type) {
	case *ast.ColumnRef:
		return r.renderColumnRef(e)
	case *ast.BoundValue:
		if e == nil {
			return errors.New("renderer: nil bound value")
		}
		r.buf.WriteString(r.bind(e.Value))
		return nil
	case *ast.Comparison:
		return r.renderComparison(e)
	case *ast.InExpr:
		return r.renderIn(e)
	case *ast.NullTest:
		return r.renderNullTest(e)
	case *ast.LogicalExpr:
		return r.renderLogical(e)
	case *ast.NotExpr:
		return r.renderNot(e)
	default:
		return fmt.Errorf("renderer: unsupported expression type %T", expr)
	}
}

func (r *selectRenderer) renderColumnRef(ref *ast.ColumnRef) error {
	if ref == nil {
		return errors.New("renderer: nil column reference")
	}
	if strings.TrimSpace(ref.Name) == "" {
		return errors.New("renderer: column reference has an empty name")
	}
	r.writeQualifiedIdent(ref.Qualifier, ref.Name)
	return nil
}

func (r *selectRenderer) renderComparison(cmp *ast.Comparison) error {
	if cmp == nil {
		return errors.New("renderer: nil comparison")
	}
	symbol := cmp.Operator.String()
	if symbol == "" {
		return fmt.Errorf("renderer: unknown comparison operator %d", cmp.Operator)
	}
	if err := r.renderExpr(cmp.Left); err != nil {
		return err
	}
	r.buf.WriteString(" ")
	r.buf.WriteString(symbol)
	r.buf.WriteString(" ")
	return r.renderExpr(cmp.Right)
}

func (r *selectRenderer) renderIn(in *ast.InExpr) error {
	if in == nil {
		return errors.New("renderer: nil IN expression")
	}
	if len(in.Values) == 0 {
		return errors.New("renderer: IN requires at least one value")
	}
	if err := r.renderExpr(in.Operand); err != nil {
		return err
	}
	r.buf.WriteString(" IN (")
	for i, value := range in.Values {
		if i > 0 {
			r.buf.WriteString(", ")
		}
		if err := r.renderExpr(value); err != nil {
			return err
		}
	}
	r.buf.WriteString(")")
	return nil
}

func (r *selectRenderer) renderNullTest(test *ast.NullTest) error {
	if test == nil {
		return errors.New("renderer: nil null test")
	}
	if err := r.renderExpr(test.Operand); err != nil {
		return err
	}
	if test.Negated {
		r.buf.WriteString(" IS NOT NULL")
		return nil
	}
	r.buf.WriteString(" IS NULL")
	return nil
}

func (r *selectRenderer) renderLogical(logical *ast.LogicalExpr) error {
	if logical == nil {
		return errors.New("renderer: nil logical expression")
	}
	if len(logical.Operands) == 0 {
		return errors.New("renderer: logical expression requires at least one operand")
	}
	keyword := logical.Operator.String()
	if keyword == "" {
		return fmt.Errorf("renderer: unknown logical operator %d", logical.Operator)
	}
	r.buf.WriteString("(")
	for i, operand := range logical.Operands {
		if i > 0 {
			r.buf.WriteString(" ")
			r.buf.WriteString(keyword)
			r.buf.WriteString(" ")
		}
		if err := r.renderExpr(operand); err != nil {
			return err
		}
	}
	r.buf.WriteString(")")
	return nil
}

func (r *selectRenderer) renderNot(not *ast.NotExpr) error {
	if not == nil {
		return errors.New("renderer: nil NOT expression")
	}
	if not.Operand == nil {
		return errors.New("renderer: NOT requires an operand")
	}
	r.buf.WriteString("NOT (")
	if err := r.renderExpr(not.Operand); err != nil {
		return err
	}
	r.buf.WriteString(")")
	return nil
}

func (r *selectRenderer) renderOrderBy(terms []ast.OrderByClause) error {
	if len(terms) == 0 {
		return nil
	}
	r.buf.WriteString(" ORDER BY ")
	for i, term := range terms {
		if i > 0 {
			r.buf.WriteString(", ")
		}
		if strings.TrimSpace(term.Column) == "" {
			return errors.New("renderer: ORDER BY term has an empty column")
		}
		direction := term.Direction.String()
		if direction == "" {
			return fmt.Errorf("renderer: unknown sort direction %d", term.Direction)
		}
		r.writeQualifiedIdent(term.Qualifier, term.Column)
		r.buf.WriteString(" ")
		r.buf.WriteString(direction)
	}
	return nil
}

// When a caller sets OFFSET but not LIMIT, MySQL, MariaDB, and SQLite reject a
// bare OFFSET: it is only valid as a suffix of LIMIT. These sentinels express
// "no upper bound" so the OFFSET can still be emitted. PostgreSQL accepts a bare
// OFFSET and needs no sentinel.
const (
	// sqliteNoLimit is SQLite's documented "no limit" value.
	sqliteNoLimit = "-1"
	// mysqlNoLimit is the maximum BIGINT UNSIGNED, MySQL and MariaDB's documented
	// idiom for "all rows from the offset onward".
	mysqlNoLimit = "18446744073709551615"
)

// renderLimitOffset appends the LIMIT and OFFSET clauses, binding each present
// bound as a parameter so it is numbered after the WHERE-clause placeholders.
//
// When OFFSET is set without LIMIT, a dialect that cannot express a bare OFFSET
// gets a synthesized "no limit" sentinel in front of it. The sentinel is a
// structural constant, not caller data, so it is emitted as a literal and does
// not consume a placeholder; the OFFSET value remains bound.
func (r *selectRenderer) renderLimitOffset(limit, offset *int64) {
	if limit != nil {
		r.buf.WriteString(" LIMIT ")
		r.buf.WriteString(r.bind(*limit))
	} else if offset != nil {
		if sentinel, ok := r.offsetOnlyLimit(); ok {
			r.buf.WriteString(" LIMIT ")
			r.buf.WriteString(sentinel)
		}
	}
	if offset != nil {
		r.buf.WriteString(" OFFSET ")
		r.buf.WriteString(r.bind(*offset))
	}
}

// offsetOnlyLimit returns the sentinel LIMIT literal a dialect requires in front
// of a bare OFFSET, and whether one is needed. The PostgreSQL family needs none.
func (r *selectRenderer) offsetOnlyLimit() (string, bool) {
	switch r.dialect {
	case platform.SQLite:
		return sqliteNoLimit, true
	case platform.MySQL, platform.MariaDB:
		return mysqlNoLimit, true
	default:
		return "", false
	}
}
