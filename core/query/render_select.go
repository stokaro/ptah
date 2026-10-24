package query

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/sqlident"
)

// RenderSelect renders a SELECT statement to parameterized SQL for the given
// dialect, returning the SQL text and its positional arguments. It renders for
// the dialect's current release line, the capabilities
// capability.ForDialect returns; [RenderSelectWithCapabilities] renders for a
// line the caller names.
//
// Every value in the statement — comparison operands (including those inside a
// JOIN ON or a HAVING), IN list elements, function-call value arguments, and the
// LIMIT/OFFSET bounds — is emitted as a placeholder and returned in args, never
// interpolated into the SQL. Placeholder style follows the dialect: $1, $2, … for
// the PostgreSQL family; ? for MySQL, MariaDB, SQLite, and ClickHouse; @p1,
// @p2, … for SQL Server; :1, :2, … for Oracle; $p1, $p2, … for YDB, whose
// arguments are each a sql.NamedArg named p1, p2, … so that a YDB connection
// binds them by name. Placeholders are
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
// Supported dialects are the ones the process registry describes
// (platform.Lookup): every built-in engine, and any dialect a program adds
// with platform.Register. The descriptor's Placeholder and Query fields state
// how the dialect spells what dialects spell differently, so core/query names
// no dialect. Among the built-ins, SQL Server and Oracle both page with
// OFFSET/FETCH rather than LIMIT, which neither of them accepts. Only SQL
// Server also gets a synthesized ORDER BY (SELECT NULL) when the caller
// ordered nothing, because only T-SQL refuses the row-limiting clause without
// an ORDER BY; Oracle needs no sentinel and is given none, which keeps the
// query runnable on every Oracle release Ptah targets rather than only the
// ones that parse the sentinel. Any other dialect returns an error, as does a nil
// statement, a statement without a FROM table, an empty IN list, a malformed
// operator, a function call with an invalid name or a bad argument shape, a GROUP
// BY term with an empty column, a join without a table or ON condition, or a
// RIGHT/FULL join on SQLite (which cannot express one before version 3.39).
// On YDB it also returns an error for a construct the target's capabilities
// lack; see [RenderSelectWithCapabilities].
func RenderSelect(stmt *SelectStatement, dialect string) (string, []any, error) {
	return RenderSelectWithCapabilities(stmt, dialect, defaultCapabilities(dialect))
}

// RenderSelectWithCapabilities renders a SELECT statement as [RenderSelect]
// does, for a target whose release line has caps.
//
// The capabilities decide what YDB is sent. A WITH clause needs
// capability.CommonTableExpressions, a subquery that reads a column of an
// enclosing query needs capability.CorrelatedSubqueries, a JOIN condition
// other than a conjunction of equalities between two columns needs
// capability.NonEquiJoins, and an OFFSET without a LIMIT needs
// capability.OffsetWithoutLimit. A statement that needs a capability caps
// lacks is refused with a *ptaherr.CapabilityError wrapping
// ptaherr.ErrUnsupportedFeature, whose text names the key. YDB takes LIMIT
// and OFFSET as Uint64, so they are bound as uint64 there and a negative
// bound is an error.
//
// The other dialects render the statement as RenderSelect renders it for
// them, whatever caps holds: their spelling of each construct is decided by
// the dialect, and ClickHouse24, ClickHouse's default preset, lacks two of
// those keys while the ClickHouse lines after it run both.
func RenderSelectWithCapabilities(
	stmt *SelectStatement,
	dialect string,
	caps capability.Capabilities,
) (string, []any, error) {
	if stmt == nil {
		return "", nil, errors.New("renderer: nil select statement")
	}
	r, err := newSelectRenderer(dialect, caps)
	if err != nil {
		return "", nil, err
	}
	if err := r.render(stmt); err != nil {
		return "", nil, err
	}
	return r.buf.String(), r.args, nil
}

// selectRenderer holds the mutable state for one RenderSelect call: the target
// dialect, how it spells what dialects spell differently, its capabilities,
// the accumulated arguments, and the SQL buffer.
type selectRenderer struct {
	dialect     string
	placeholder descriptor.Placeholder
	query       descriptor.Query
	// quoting is how a registered spelling writes a name, nil for a dialect
	// the process registry describes, which quotes by its name.
	quoting *descriptor.Quoting
	caps    capability.Capabilities
	args    []any
	buf     strings.Builder
	// nestDepth bounds WITH nesting; see maxNestDepth.
	nestDepth int
}

// newSelectRenderer builds the renderer for a dialect the process registry
// describes. The descriptor is the whole answer: a dialect renders when it
// is described, with the placeholder style and spellings its descriptor
// states, and core/query names no dialect.
func newSelectRenderer(dialect string, caps capability.Capabilities) (*selectRenderer, error) {
	described, quoting, ok := lookupDialect(dialect)
	if !ok {
		return nil, fmt.Errorf("renderer: SELECT rendering is not supported for dialect %q", dialect)
	}
	return newDescribedRenderer(described, quoting, caps), nil
}

func newDescribedRenderer(described descriptor.Dialect, quoting *descriptor.Quoting, caps capability.Capabilities) *selectRenderer {
	return &selectRenderer{
		dialect:     described.Name,
		placeholder: described.Placeholder,
		query:       described.Query,
		quoting:     quoting,
		caps:        caps,
	}
}

// refuseWithout refuses subject when the dialect decides constructs by the
// target's capabilities (descriptor.Query.ChecksCapabilities) and the target
// lacks key. See RenderSelectWithCapabilities for why the other dialects do
// not.
func (r *selectRenderer) refuseWithout(key capability.Capability, subject string) error {
	if !r.query.ChecksCapabilities || r.caps.Has(key) {
		return nil
	}
	return capabilityRefusal(r.dialect, key, subject)
}

// capabilityRefusal is the error a construct the target lacks key for is
// refused with.
func capabilityRefusal(dialect string, key capability.Capability, subject string) error {
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: string(key),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("renderer: %s, which requires target capability %s, unavailable on this %s target",
			subject, key, dialect),
	}
}

// bind records value as an argument and returns its placeholder, in the
// style the dialect's descriptor states.
//
// SQL Server has no positional `?` in T-SQL itself; its drivers bind named
// parameters, and @pN is the name the standard Go driver generates for an
// ordinal argument. YQL has no positional parameter either: `$1` and `?` are
// parse errors there. So a dollar-named parameter is bound as a sql.NamedArg
// of the same name, the one sqlutil.Rebind writes for the same position,
// because ydb-go-sdk binds a parameter by name: measured on YDB 26.2.1.14 with
// ydb-go-sdk v3.153.2, a positional argument for `$p1` answers `Unknown name:
// $p1` through the SDK's own connector, and sql.Named("p1", v) binds there and
// through Ptah's connection alike.
func (r *selectRenderer) bind(value any) string {
	if r.placeholder == descriptor.PlaceholderDollarNamed {
		name := "p" + strconv.Itoa(len(r.args)+1)
		r.args = append(r.args, sql.Named(name, value))
		return "$" + name
	}
	r.args = append(r.args, value)
	switch r.placeholder {
	case descriptor.PlaceholderDollar:
		return "$" + strconv.Itoa(len(r.args))
	case descriptor.PlaceholderAtP:
		return "@p" + strconv.Itoa(len(r.args))
	case descriptor.PlaceholderColon:
		return ":" + strconv.Itoa(len(r.args))
	default:
		return "?"
	}
}

// bindBound binds a row limit or offset, as an unsigned integer where the
// dialect refuses a signed one. A negative bound was refused before, by
// renderLimitOffset.
func (r *selectRenderer) bindBound(value int64) string {
	if r.query.UnsignedBounds {
		return r.bind(uint64(value)) // #nosec G115 -- a negative bound is refused by renderLimitOffset
	}
	return r.bind(value)
}

// quote returns identifier quoted for the renderer's dialect.
func (r *selectRenderer) quote(identifier string) string {
	if r.quoting != nil {
		return sqlident.IdentWith(*r.quoting, identifier)
	}
	return sqlident.Ident(r.dialect, identifier)
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

func (r *selectRenderer) render(stmt *SelectStatement) error {
	if strings.TrimSpace(stmt.From) == "" {
		return errors.New("renderer: select statement requires a FROM table")
	}
	if len(stmt.With) > 0 {
		if err := r.refuseWithout(capability.CommonTableExpressions, "a WITH clause"); err != nil {
			return err
		}
	}
	if err := r.renderWith(stmt.With); err != nil {
		return err
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

	// An engine that accepts a row limit only after an ORDER BY answers an
	// unordered limited query with a syntax error rather than an unordered
	// result. Its descriptor names the term that means "no particular order",
	// which is the meaning every other dialect gives an unordered LIMIT, so
	// this states the same thing rather than imposing an order the caller did
	// not ask for (stokaro/ptah#941).
	if r.query.FetchOrderBy != "" &&
		len(stmt.OrderBy) == 0 && (stmt.Limit != nil || stmt.Offset != nil) {
		r.buf.WriteString(" ORDER BY ")
		r.buf.WriteString(r.query.FetchOrderBy)
	}

	return r.renderLimitOffset(stmt.Limit, stmt.Offset)
}

// maxNestDepth bounds how far one statement may nest inside another.
//
// Both a CTE and a subquery hold a *SelectStatement, so a caller can build a
// cycle by pointing either at a statement that already contains it. Rendering
// would then recurse until the stack ran out, which is a crash rather than a
// diagnostic. One counter covers both, because the two nest through each other.
const maxNestDepth = 16

// renderWith emits the WITH clause, if any, before the SELECT it precedes.
//
// The subqueries are rendered first and in order, which is what puts their
// bound values ahead of the outer query's in args: a positional driver reads
// $1 from the first CTE, not from the first WHERE.
func (r *selectRenderer) renderWith(ctes []CommonTableExpression) error {
	if len(ctes) == 0 {
		return nil
	}
	r.nestDepth++
	defer func() { r.nestDepth-- }()
	if r.nestDepth > maxNestDepth {
		return fmt.Errorf("renderer: statement nests deeper than %d, which a cycle would do", maxNestDepth)
	}

	r.buf.WriteString("WITH ")
	for i, cte := range ctes {
		if err := r.renderCTE(i, cte); err != nil {
			return err
		}
	}
	r.buf.WriteString(" ")
	return nil
}

func (r *selectRenderer) renderCTE(index int, cte CommonTableExpression) error {
	if strings.TrimSpace(cte.Name) == "" {
		return errors.New("renderer: common table expression requires a name")
	}
	if cte.Query == nil {
		return fmt.Errorf("renderer: common table expression %q requires a query", cte.Name)
	}
	if index > 0 {
		r.buf.WriteString(", ")
	}
	r.buf.WriteString(r.quote(cte.Name))
	r.buf.WriteString(" AS (")
	if err := r.render(cte.Query); err != nil {
		return err
	}
	r.buf.WriteString(")")
	return nil
}

func (r *selectRenderer) renderColumns(columns []ResultColumn) error {
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
func (r *selectRenderer) renderResultColumn(col ResultColumn) error {
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
func (r *selectRenderer) writeResultColumn(col ResultColumn) {
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
func (r *selectRenderer) renderJoins(joins []JoinClause) error {
	for i := range joins {
		if err := r.renderJoin(&joins[i]); err != nil {
			return err
		}
	}
	return nil
}

// renderJoin appends a single join: " <TYPE> JOIN <table> [alias] ON <cond>".
func (r *selectRenderer) renderJoin(join *JoinClause) error {
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
	if !isColumnEquality(join.On) {
		err := r.refuseWithout(capability.NonEquiJoins,
			"a JOIN condition other than equalities between columns of the joined tables")
		if err != nil {
			return err
		}
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
// the database. The dialect's descriptor lists them
// (descriptor.Query.UnsupportedJoins): SQLite, for one, has RIGHT and FULL
// joins only from 3.39, and MySQL and MariaDB have no FULL OUTER JOIN at all.
func (r *selectRenderer) checkJoinDialect(t JoinType) error {
	if slices.Contains(r.query.UnsupportedJoins, t.descriptorJoin()) {
		return fmt.Errorf("renderer: %s does not support %s", r.dialect, t)
	}
	return nil
}

// descriptorJoin is the descriptor's name for the join type, or "" for a type
// outside the defined range, which no descriptor lists.
func (t JoinType) descriptorJoin() descriptor.Join {
	switch t {
	case JoinInner:
		return descriptor.JoinInner
	case JoinLeft:
		return descriptor.JoinLeft
	case JoinRight:
		return descriptor.JoinRight
	case JoinFull:
		return descriptor.JoinFull
	default:
		return ""
	}
}

// renderExpr dispatches over the sealed Expression sum type. Each case
// delegates to a small helper so the dispatch stays flat.
func (r *selectRenderer) renderExpr(expr Expression) error {
	switch e := expr.(type) {
	case *ColumnRef:
		return r.renderColumnRef(e)
	case *BoundValue:
		if e == nil {
			return errors.New("renderer: nil bound value")
		}
		r.buf.WriteString(r.bind(e.Value))
		return nil
	case *Comparison:
		return r.renderComparison(e)
	case *ExistsExpr:
		return r.renderExists(e)
	case *InExpr:
		return r.renderIn(e)
	case *NullTest:
		return r.renderNullTest(e)
	case *LogicalExpr:
		return r.renderLogical(e)
	case *NotExpr:
		return r.renderNot(e)
	case *FuncCall:
		return r.renderFuncCall(e)
	case *Arithmetic:
		return r.renderArithmetic(e)
	default:
		return fmt.Errorf("renderer: unsupported expression type %T", expr)
	}
}

// renderArithmetic writes a binary arithmetic expression, always parenthesized.
//
// The parentheses are not decoration. The node is a tree and SQL is text, so
// without them `Mul(Add(a, b), c)` and `Add(a, Mul(b, c))` both render as
// `a + b * c` and the server picks one reading by its own precedence --
// agreeing with one tree and silently rewriting the other. See Arithmetic.
func (r *selectRenderer) renderArithmetic(expr *Arithmetic) error {
	if expr == nil {
		return errors.New("renderer: nil arithmetic expression")
	}
	symbol := expr.Operator.String()
	if symbol == "" {
		return fmt.Errorf("renderer: unknown arithmetic operator %d", expr.Operator)
	}
	r.buf.WriteString("(")
	if err := r.renderExpr(expr.Left); err != nil {
		return err
	}
	r.buf.WriteString(" ")
	r.buf.WriteString(symbol)
	r.buf.WriteString(" ")
	if err := r.renderExpr(expr.Right); err != nil {
		return err
	}
	r.buf.WriteString(")")
	return nil
}

func (r *selectRenderer) renderColumnRef(ref *ColumnRef) error {
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

func (r *selectRenderer) renderComparison(cmp *Comparison) error {
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

// renderNested renders one statement inside another, inside parentheses, under
// the shared nesting bound.
func (r *selectRenderer) renderNested(stmt *SelectStatement) error {
	r.nestDepth++
	defer func() { r.nestDepth-- }()
	if r.nestDepth > maxNestDepth {
		return fmt.Errorf("renderer: statement nests deeper than %d, which a cycle would do", maxNestDepth)
	}
	if name, ok := correlatedReference(stmt); ok {
		err := r.refuseWithout(capability.CorrelatedSubqueries,
			fmt.Sprintf("a subquery that reads %q from the query around it", name))
		if err != nil {
			return err
		}
	}
	r.buf.WriteString("(")
	if err := r.render(stmt); err != nil {
		return err
	}
	r.buf.WriteString(")")
	return nil
}

func (r *selectRenderer) renderExists(exists *ExistsExpr) error {
	if exists == nil {
		return errors.New("renderer: nil EXISTS expression")
	}
	if exists.Query == nil {
		return errors.New("renderer: EXISTS requires a subquery")
	}
	if exists.Negated {
		r.buf.WriteString("NOT ")
	}
	r.buf.WriteString("EXISTS ")
	return r.renderNested(exists.Query)
}

func (r *selectRenderer) renderIn(in *InExpr) error {
	if in == nil {
		return errors.New("renderer: nil IN expression")
	}
	if in.Subquery != nil {
		return r.renderInSubquery(in)
	}
	if len(in.Values) == 0 {
		return errors.New("renderer: IN requires at least one value or a subquery")
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

// renderInSubquery renders the membership form that reads its candidates from
// a query rather than from a value list.
func (r *selectRenderer) renderInSubquery(in *InExpr) error {
	if len(in.Values) > 0 {
		return errors.New("renderer: IN takes either values or a subquery, not both")
	}
	if err := r.renderExpr(in.Operand); err != nil {
		return err
	}
	r.buf.WriteString(" IN ")
	return r.renderNested(in.Subquery)
}

func (r *selectRenderer) renderNullTest(test *NullTest) error {
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

func (r *selectRenderer) renderLogical(logical *LogicalExpr) error {
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

func (r *selectRenderer) renderNot(not *NotExpr) error {
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
func (r *selectRenderer) renderFuncCall(fn *FuncCall) error {
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
	// A zero-argument call is meaningless without a window -- SUM() computes
	// nothing -- but with one it is the whole shape of a ranking function:
	// ROW_NUMBER(), RANK() and DENSE_RANK() take no arguments and get their
	// input from the OVER clause. So the argument requirement is lifted exactly
	// where the window supplies what the arguments would have
	// (stokaro/ptah#941).
	if len(fn.Args) == 0 && fn.Over == nil {
		return fmt.Errorf(
			"renderer: function %s requires at least one argument, or a window that supplies its input",
			name)
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
	return r.renderWindowSpec(fn.Over)
}

// renderWindowSpec writes the OVER (...) clause of a window function.
//
// An empty spec still renders `OVER ()`, which is valid and means the whole
// result set: the clause is what makes the call a window function, so omitting
// it for an empty spec would silently turn one back into an ordinary aggregate
// over the group -- a different query with a different answer
// (stokaro/ptah#941).
func (r *selectRenderer) renderWindowSpec(spec *WindowSpec) error {
	if spec == nil {
		return nil
	}
	r.buf.WriteString(" OVER (")
	if len(spec.PartitionBy) > 0 {
		r.buf.WriteString("PARTITION BY ")
		for i := range spec.PartitionBy {
			if i > 0 {
				r.buf.WriteString(", ")
			}
			column := spec.PartitionBy[i]
			if strings.TrimSpace(column.Name) == "" {
				return errors.New("renderer: window PARTITION BY term has an empty column")
			}
			r.writeQualifiedIdent(column.Qualifier, column.Name)
		}
	}
	if len(spec.OrderBy) > 0 {
		if len(spec.PartitionBy) > 0 {
			r.buf.WriteString(" ")
		}
		if err := r.writeOrderByTerms(spec.OrderBy); err != nil {
			return err
		}
	}
	r.buf.WriteString(")")
	return nil
}

// renderFuncStar writes the "*" argument form, as in COUNT(*). The star is
// mutually exclusive with explicit arguments and with DISTINCT, since neither
// COUNT(*, x) nor COUNT(DISTINCT *) is valid SQL.
func (r *selectRenderer) renderFuncStar(name string, fn *FuncCall) error {
	if len(fn.Args) > 0 {
		return fmt.Errorf("renderer: function %s with a star takes no arguments", name)
	}
	if fn.Distinct {
		return fmt.Errorf("renderer: function %s cannot combine DISTINCT with a star", name)
	}
	r.buf.WriteString(name)
	r.buf.WriteString("(*)")
	return r.renderWindowSpec(fn.Over)
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
func (r *selectRenderer) renderGroupBy(cols []ColumnRef) error {
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

func (r *selectRenderer) renderOrderBy(terms []OrderByClause) error {
	if len(terms) == 0 {
		return nil
	}
	r.buf.WriteString(" ")
	return r.writeOrderByTerms(terms)
}

// writeOrderByTerms writes "ORDER BY <term>, <term>" with no leading space.
//
// The statement's ORDER BY and a window's share this, rather than each spelling
// the loop: a term is a term, and two copies would let one learn a rule the
// other did not -- an empty column refused in one place and emitted in the
// other (stokaro/ptah#941).
func (r *selectRenderer) writeOrderByTerms(terms []OrderByClause) error {
	r.buf.WriteString("ORDER BY ")
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

// renderLimitOffset appends the row-limiting clause, binding each present
// bound as a parameter so it is numbered after the WHERE-clause placeholders.
//
// A dialect whose descriptor states descriptor.Query.OffsetFetch takes the
// ANSI clause instead of LIMIT; see renderFetchOffset. When OFFSET is set
// without LIMIT, a dialect that cannot express a bare OFFSET gets the "no
// limit" literal its descriptor names (descriptor.Query.OffsetOnlyLimit) in
// front of it. The literal is a structural constant, not caller data, so it
// does not consume a placeholder; the OFFSET value remains bound. A dialect
// that has no such literal and decides by capabilities refuses a bare OFFSET
// where the target lacks capability.OffsetWithoutLimit: YDB has no sentinel
// that works.
//
// A dialect that binds the bounds unsigned (descriptor.Query.UnsignedBounds)
// refuses a negative one here, before anything is written: YDB types LIMIT and
// OFFSET as Uint64, and no Uint64 holds a negative value.
func (r *selectRenderer) renderLimitOffset(limit, offset *int64) error {
	if r.query.UnsignedBounds {
		for _, bound := range []struct {
			name  string
			value *int64
		}{{"LIMIT", limit}, {"OFFSET", offset}} {
			if bound.value != nil && *bound.value < 0 {
				return fmt.Errorf("renderer: %s binds %s as an unsigned integer, and %d is negative", r.dialect, bound.name, *bound.value)
			}
		}
	}
	if r.query.OffsetFetch {
		r.renderFetchOffset(limit, offset)
		return nil
	}
	sentinel := r.query.OffsetOnlyLimit
	if offset != nil && limit == nil && sentinel == "" {
		if err := r.refuseWithout(capability.OffsetWithoutLimit, "an OFFSET without a LIMIT"); err != nil {
			return err
		}
	}
	if limit != nil {
		r.buf.WriteString(" LIMIT ")
		r.buf.WriteString(r.bindBound(*limit))
	} else if offset != nil && sentinel != "" {
		r.buf.WriteString(" LIMIT ")
		r.buf.WriteString(sentinel)
	}
	if offset != nil {
		r.buf.WriteString(" OFFSET ")
		r.buf.WriteString(r.bindBound(*offset))
	}
	return nil
}

// renderFetchOffset writes the ANSI row-limiting clause, for SQL Server and
// Oracle, which take no LIMIT.
//
// The spelling is `OFFSET n ROWS FETCH NEXT m ROWS ONLY`, and OFFSET is
// mandatory before FETCH -- so a query with only a limit still emits `OFFSET 0
// ROWS`. That zero is a
// structural constant rather than caller data, so it is written as a literal and
// consumes no placeholder, which keeps the argument order the doc comment
// promises (stokaro/ptah#941).
func (r *selectRenderer) renderFetchOffset(limit, offset *int64) {
	if limit == nil && offset == nil {
		return
	}
	r.buf.WriteString(" OFFSET ")
	if offset != nil {
		r.buf.WriteString(r.bindBound(*offset))
	} else {
		r.buf.WriteString("0")
	}
	r.buf.WriteString(" ROWS")
	if limit != nil {
		r.buf.WriteString(" FETCH NEXT ")
		r.buf.WriteString(r.bindBound(*limit))
		r.buf.WriteString(" ROWS ONLY")
	}
}
