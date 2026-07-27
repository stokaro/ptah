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
// JOIN ON or a HAVING), IN list elements, function-call value arguments, and the
// LIMIT/OFFSET bounds — is emitted as a placeholder and returned in args, never
// interpolated into the SQL. Placeholder style follows the dialect: $1, $2, … for
// the PostgreSQL family and ? for MySQL, MariaDB, and SQLite. Placeholders are
// numbered in a single left-to-right pass over the projection, FROM, the joins,
// WHERE, then HAVING, then LIMIT/OFFSET, so args are ordered to match; a JOIN ON
// value is numbered before any WHERE value, and a HAVING value after every WHERE
// value. GROUP BY carries only identifiers and never binds a placeholder.
//
// Identifiers (table, alias, and column names) are quoted for the dialect via
// internal/sqlident, so a value can never be rendered as an identifier and an
// attacker-shaped identifier or alias cannot break out of its quotes. A
// qualified column renders as "alias"."col" with each part quoted independently.
// A function name (COUNT, SUM, …) is a keyword emitted verbatim, never quoted;
// the renderer rejects a name that is not a simple identifier rather than emit it.
//
// Supported dialects are PostgreSQL (including CockroachDB and YugabyteDB),
// MySQL, MariaDB, and SQLite. Any other dialect returns an error, as does a nil
// statement, a statement without a FROM table, an empty IN list, a malformed
// operator, a function call with an invalid name or a bad argument shape, a GROUP
// BY term with an empty column, a join without a table or ON condition, or a
// RIGHT/FULL join on SQLite (which cannot express one before version 3.39).
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
// part is quoted independently so neither can break out of its quotes. The
// qualifier is trimmed before it is quoted, matching sqlident.Qualified, so
// surrounding whitespace never lands inside the quotes.
func (r *selectRenderer) writeQualifiedIdent(qualifier, name string) {
	if q := strings.TrimSpace(qualifier); q != "" {
		r.buf.WriteString(r.quote(q))
		r.buf.WriteString(".")
	}
	r.buf.WriteString(r.quote(name))
}

// writeTableRef writes a quoted table name followed by an optional quoted alias,
// as in "t" or "t" "a". The bare "table alias" form is used rather than "table
// AS alias" because every supported dialect accepts it. Both the table and the
// alias are trimmed before quoting, matching sqlident.Qualified.
func (r *selectRenderer) writeTableRef(table, alias string) {
	r.buf.WriteString(r.quote(strings.TrimSpace(table)))
	if a := strings.TrimSpace(alias); a != "" {
		r.buf.WriteString(" ")
		r.buf.WriteString(r.quote(a))
	}
}

func (r *selectRenderer) render(stmt *ast.SelectStatement) error {
	if strings.TrimSpace(stmt.From) == "" {
		return errors.New("renderer: select statement requires a FROM table")
	}

	r.buf.WriteString("SELECT ")
	if stmt.Distinct {
		r.buf.WriteString("DISTINCT ")
	}
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

	if err := r.renderGroupBy(stmt.GroupBy); err != nil {
		return err
	}

	if stmt.Having != nil {
		r.buf.WriteString(" HAVING ")
		if err := r.renderExpr(stmt.Having); err != nil {
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
	for i := range columns {
		if i > 0 {
			r.buf.WriteString(", ")
		}
		if err := r.renderResultColumn(columns[i]); err != nil {
			return err
		}
	}
	return nil
}

// renderResultColumn writes one projection entry and its optional alias. An Expr
// entry renders the expression (for example an aggregate); a Star entry renders
// "*"; every other entry renders a quoted column. Expr takes precedence over Star
// and Name, so an expression projection ignores the column fields.
func (r *selectRenderer) renderResultColumn(col ast.ResultColumn) error {
	switch {
	case col.Expr != nil:
		if err := r.renderExpr(col.Expr); err != nil {
			return err
		}
	case col.Star:
		r.buf.WriteString("*")
	default:
		if strings.TrimSpace(col.Name) == "" {
			return errors.New("renderer: result column has an empty name")
		}
		r.writeResultColumn(col)
	}
	if alias := strings.TrimSpace(col.Alias); alias != "" {
		r.buf.WriteString(" AS ")
		r.buf.WriteString(r.quote(alias))
	}
	return nil
}

// writeResultColumn writes one projection entry. A qualified star ("u".*) is
// rendered with the qualifier quoted and a bare, unquoted "*" — quoting the star
// would produce the invalid "u"."*" — so a qualified SELECT u.* renders as
// standard SQL. Every other column routes through writeQualifiedIdent.
func (r *selectRenderer) writeResultColumn(col ast.ResultColumn) {
	if strings.TrimSpace(col.Name) != "*" {
		r.writeQualifiedIdent(col.Qualifier, col.Name)
		return
	}
	if q := strings.TrimSpace(col.Qualifier); q != "" {
		r.buf.WriteString(r.quote(q))
		r.buf.WriteString(".*")
		return
	}
	r.buf.WriteString("*")
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

// checkJoinDialect rejects join types a dialect cannot express, so an
// unsupported join fails at render time rather than at execution time against
// the database.
//
//   - SQLite gained RIGHT and FULL [OUTER] JOIN only in version 3.39 (2022);
//     because Ptah targets a range of SQLite versions and cannot assume 3.39+,
//     both are rejected.
//   - MySQL and MariaDB have no FULL [OUTER] JOIN in any version (it must be
//     emulated with a UNION of a LEFT and a RIGHT join), so it is rejected.
//
// INNER and LEFT joins are accepted by every supported dialect; RIGHT is
// accepted everywhere except SQLite; FULL OUTER is accepted only by the
// PostgreSQL family.
func (r *selectRenderer) checkJoinDialect(t ast.JoinType) error {
	switch r.dialect {
	case platform.SQLite:
		if t == ast.JoinRight || t == ast.JoinFull {
			return fmt.Errorf("renderer: SQLite does not support %s", t)
		}
	case platform.MySQL, platform.MariaDB:
		if t == ast.JoinFull {
			return fmt.Errorf("renderer: %s does not support %s", r.dialect, t)
		}
	}
	return nil
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
	case *ast.FuncCall:
		return r.renderFuncCall(e)
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
	// A "*" is not a column: quoting it would yield "*" (or "q"."*"), which the
	// database reads as a column literally named *. The star belongs to the
	// projection (ResultColumn.Star / a qualified "q".*) and to the star form of
	// an aggregate (FuncCall.Star, e.g. COUNT(*)), never to a column reference in
	// an expression, so it is rejected here rather than mis-rendered.
	if strings.TrimSpace(ref.Name) == "*" {
		return errors.New(`renderer: "*" is not a valid column reference; use a star aggregate such as COUNT(*)`)
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

// renderFuncCall writes a function call such as COUNT(*), COUNT("col"),
// COUNT(DISTINCT "col"), or SUM("u"."total"). The function name is validated and
// emitted verbatim as a keyword — never quoted and never bound — while each
// argument routes through renderExpr so a column is quoted and a value is bound.
func (r *selectRenderer) renderFuncCall(fn *ast.FuncCall) error {
	if fn == nil {
		return errors.New("renderer: nil function call")
	}
	name := strings.TrimSpace(fn.Name)
	if name == "" {
		return errors.New("renderer: function call has an empty name")
	}
	if !isSafeFunctionName(name) {
		return fmt.Errorf("renderer: function name %q is not a valid identifier", fn.Name)
	}
	if fn.Star {
		return r.renderFuncStar(name, fn)
	}
	if len(fn.Args) == 0 {
		return fmt.Errorf("renderer: function %s requires at least one argument", name)
	}
	r.buf.WriteString(name)
	r.buf.WriteString("(")
	if fn.Distinct {
		r.buf.WriteString("DISTINCT ")
	}
	for i, arg := range fn.Args {
		if i > 0 {
			r.buf.WriteString(", ")
		}
		if err := r.renderExpr(arg); err != nil {
			return err
		}
	}
	r.buf.WriteString(")")
	return nil
}

// renderFuncStar writes the "*" argument form, as in COUNT(*). The star is
// mutually exclusive with explicit arguments and with DISTINCT, since neither
// COUNT(*, x) nor COUNT(DISTINCT *) is valid SQL.
func (r *selectRenderer) renderFuncStar(name string, fn *ast.FuncCall) error {
	if len(fn.Args) > 0 {
		return fmt.Errorf("renderer: function %s with a star takes no arguments", name)
	}
	if fn.Distinct {
		return fmt.Errorf("renderer: function %s cannot combine DISTINCT with a star", name)
	}
	r.buf.WriteString(name)
	r.buf.WriteString("(*)")
	return nil
}

// isSafeFunctionName reports whether name is a simple SQL identifier — a letter
// or underscore followed by letters, digits, or underscores. Because a function
// name is emitted as a keyword rather than a quoted identifier, restricting it to
// this shape prevents a hand-built FuncCall from smuggling SQL through the name.
func isSafeFunctionName(name string) bool {
	for i, r := range name {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return name != ""
}

// renderGroupBy appends the GROUP BY clause after WHERE. Each column is rendered
// as a quoted identifier, qualified when a qualifier is set; GROUP BY carries no
// values, so it never binds a placeholder. An empty list renders nothing.
func (r *selectRenderer) renderGroupBy(cols []ast.ColumnRef) error {
	if len(cols) == 0 {
		return nil
	}
	r.buf.WriteString(" GROUP BY ")
	for i := range cols {
		if i > 0 {
			r.buf.WriteString(", ")
		}
		col := cols[i]
		if strings.TrimSpace(col.Name) == "" {
			return errors.New("renderer: GROUP BY term has an empty column")
		}
		r.writeQualifiedIdent(col.Qualifier, col.Name)
	}
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
