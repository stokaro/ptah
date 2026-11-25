// Package roundingscan reads, before a plan runs, whether a column type change
// that keeps fewer digits after the point would change a value the table holds.
//
// A decimal given a lower scale, a decimal under an integer, or a temporal
// column given a lower fractional-second precision is converted by most
// servers without an error: each value is stored rounded or cut to the new
// digits. The comparison marks such a change ([difftypes.ColumnDiff.Rounding]);
// this package counts the rows whose value the server's own CAST to the
// declared type changes, with the condition the dialect descriptor states
// ([descriptor.Rounding]), and refuses the plan while any row would change. A
// value whose extra digits are all zero is not counted, and an empty table is
// never refused. A value the declared type cannot hold at all is refused too,
// with the server's error.
package roundingscan

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"unicode"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/dbschema"
	"ptah.run/internal/planrefusal"
	"ptah.run/internal/rundiag"
	"ptah.run/internal/servercode"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
	"ptah.run/internal/typechange"
)

// Change is one column type change that keeps fewer digits after the point,
// on an engine whose descriptor states how to read the values it would change.
type Change struct {
	// Table is the column's table, as the comparison names it.
	Table string
	// Column is the column's name.
	Column string
	// HeldType is the type the database holds, as its catalog spells it, and
	// DeclaredType the type the plan converts the column to, as the plan's
	// statement spells it.
	HeldType, DeclaredType string
	// Temporal reports a temporal type; false reports a decimal.
	Temporal bool
}

// Changes lists the column type changes of diff that keep fewer digits after
// the point and whose values the descriptor of dialect states a condition
// for, in the order the comparison lists the tables and each table's columns.
// A change on an engine that refuses the conversion itself, or that does not
// enforce a declared size, is left out. A nil diff holds none.
func Changes(diff *difftypes.SchemaDiff, dialect string) []Change {
	if diff == nil {
		return nil
	}
	described, found := platform.Lookup(dialect)
	if !found {
		return nil
	}
	var changes []Change
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsModified {
			if column.Rounding == nil {
				continue
			}
			change := Change{
				Table:        table.TableName,
				Column:       column.ColumnName,
				HeldType:     column.Rounding.HeldType,
				DeclaredType: column.Rounding.DeclaredType,
				Temporal:     column.Rounding.Temporal,
			}
			if template(described.Rounding, change) == "" {
				continue
			}
			changes = append(changes, change)
		}
	}
	return changes
}

// ErrNotAType is the error for a type spelling outside the grammar a condition
// takes: words, each with an optional parenthesized list of numbers or quoted
// names, separated by spaces.
var ErrNotAType = errors.New("not a type spelling")

// typeSpelling is that grammar. Nothing it accepts can end a quoted literal, a
// statement or a comment, so a spelling read from a plan file, which anyone can
// edit, cannot carry SQL of its own into the scan.
var typeSpelling = regexp.MustCompile(`^` + typeTerm + `(?: +` + typeTerm + `)*$`)

const (
	typeWord     = `[A-Za-z_][A-Za-z0-9_]*`
	typeArgument = `(?:[0-9]+|'[A-Za-z0-9_/+:.-]*')`
	typeTerm     = typeWord + `(?: *\( *` + typeArgument + `(?: *, *` + typeArgument + `)* *\))?`
)

// typeArguments matches one parenthesized list of a spelling the grammar took.
var typeArguments = regexp.MustCompile(` *\([^()]*\)`)

func spelling(typ string) (string, error) {
	typ = strings.TrimSpace(typ)
	if !typeSpelling.MatchString(typ) {
		return "", fmt.Errorf("%w: %q", ErrNotAType, typ)
	}
	return typ, nil
}

// Check reports whether change is one the comparison could have recorded for
// dialect, and an error saying why where it is not: the change names a table, a
// column and both types, each type fits the type grammar, the dialect's
// descriptor states a condition for the kind of type, and the held type under the
// declared one keeps fewer digits after the point, of the kind change says. A
// plan file's records are read through it before anything reaches a database.
func Check(dialect string, change Change) error {
	if strings.TrimSpace(change.Table) == "" || strings.TrimSpace(change.Column) == "" {
		return errors.New("it names no table or column")
	}
	held, err := spelling(change.HeldType)
	if err != nil {
		return fmt.Errorf("the held type is %w", err)
	}
	declared, err := spelling(change.DeclaredType)
	if err != nil {
		return fmt.Errorf("the declared type is %w", err)
	}
	described, found := platform.Lookup(dialect)
	if !found {
		return fmt.Errorf("dialect %q is not registered", dialect)
	}
	if template(described.Rounding, change) == "" {
		return fmt.Errorf("dialect %q reads no value of such a change", dialect)
	}
	rounding, rounds := typechange.EngineOf(described.Comparison).Rounds(held, declared)
	if !rounds || rounding.Temporal != change.Temporal {
		return fmt.Errorf("%s under %s is not a change that keeps fewer digits after the point", held, declared)
	}
	return nil
}

// Names reports whether the statement sqlText names the column and the table
// of change, the table by the last part of its name, each as a word or a quoted
// name, with case ignored. A single-quoted literal names nothing. It is how a
// record is held to a statement of its plan, not a parse of one: the type is
// left to [Check], since the statement may spell it as the renderer writes it,
// INT where the comparison reads int8.
func Names(sqlText string, change Change) bool {
	ref, parsed := tableref.Parse(change.Table)
	if !parsed {
		return false
	}
	var names []string
	for _, match := range statementName.FindAllStringSubmatch(sqlText, -1) {
		if name := match[1] + match[2] + match[3] + match[4]; name != "" {
			names = append(names, strings.ToLower(name))
		}
	}
	return slices.Contains(names, strings.ToLower(change.Column)) && slices.Contains(names, strings.ToLower(ref.Name))
}

// statementName matches a single-quoted literal, which it leaves out, a name
// quoted with double quotes, backquotes or brackets, and a word.
var statementName = regexp.MustCompile(`'[^']*'|"([^"]*)"|` + "`([^`]*)`" + `|\[([^\]]*)\]|([\p{L}\p{N}_]+)`)

// Condition is the condition, on one row, that converting the value of
// quotedColumn to the declared type of change would change it, as rules state
// it. It refuses a declared type outside the type grammar ([ErrNotAType]), and a
// kind of type rules state no condition for.
func Condition(rules descriptor.Rounding, quotedColumn string, change Change) (string, error) {
	condition := template(rules, change)
	if condition == "" {
		return "", errors.New("no condition reads the values of such a change")
	}
	declared, err := spelling(change.DeclaredType)
	if err != nil {
		return "", err
	}
	cast, err := CastType(rules, declared)
	if err != nil {
		return "", err
	}
	return strings.NewReplacer(
		descriptor.RoundingColumn, quotedColumn,
		descriptor.RoundingType, cast,
		descriptor.RoundingDomain, domain(rules, declared),
	).Replace(condition), nil
}

func template(rules descriptor.Rounding, change Change) string {
	if change.Temporal {
		return rules.Temporal
	}
	return rules.Decimal
}

// CastType is typ as the server's CAST takes it, by [descriptor.Rounding.CastTypes]:
// the spelling listed for the type's name, with the word unsigned where the type
// carries it and the rules list the pair, and typ itself where nothing is listed.
// It refuses a spelling outside the type grammar ([ErrNotAType]).
func CastType(rules descriptor.Rounding, typ string) (string, error) {
	typ, err := spelling(typ)
	if err != nil {
		return "", err
	}
	name, args, rest := splitType(typ)
	key := strings.ToLower(name)
	if spelling, ok := rules.CastTypes[key+" unsigned"]; ok && hasWord(rest, "unsigned") {
		return strings.ReplaceAll(spelling, descriptor.RoundingArguments, args), nil
	}
	if spelling, ok := rules.CastTypes[key]; ok {
		return strings.ReplaceAll(spelling, descriptor.RoundingArguments, args), nil
	}
	return typ, nil
}

// domain is the type a temporal condition compares in, for a declared type the
// grammar took: the spelling [descriptor.Rounding.Domains] lists for its name, or
// the type with its parenthesized lists taken out.
func domain(rules descriptor.Rounding, declared string) string {
	name, _, _ := splitType(declared)
	if spelling, ok := rules.Domains[strings.ToLower(name)]; ok {
		return spelling
	}
	return typeArguments.ReplaceAllString(declared, "")
}

// splitType parts typ into its name, what its parentheses hold with them, and
// what follows: `timestamp(3) with time zone` is `timestamp`, `(3)` and
// ` with time zone`.
func splitType(typ string) (name, args, rest string) {
	end := strings.IndexFunc(typ, func(r rune) bool { return r == '(' || unicode.IsSpace(r) })
	if end < 0 {
		return typ, "", ""
	}
	name, rest = typ[:end], typ[end:]
	trimmed := strings.TrimLeftFunc(rest, unicode.IsSpace)
	if !strings.HasPrefix(trimmed, "(") {
		return name, "", rest
	}
	depth := 0
	for i, r := range trimmed {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return name, trimmed[:i+1], trimmed[i+1:]
			}
		}
	}
	return name, "", rest
}

func hasWord(text, word string) bool {
	return slices.Contains(strings.Fields(strings.ToLower(text)), word)
}

// Refuse counts, on conn, the rows of each change whose value the change would
// round or cut, with one query for each table, and returns a
// [*planrefusal.RoundedValuesError] naming every column with such a row or with
// a value the declared type cannot hold. It returns nil when no row would
// change, when changes is empty, and for a dialect that is not registered. It
// refuses a change [Check] refuses with an error, before any query. request is
// how the caller asks for the change without the scan, which the refusal names.
func Refuse(ctx context.Context, conn *dbschema.DatabaseConnection, changes []Change, request string) error {
	if len(changes) == 0 {
		return nil
	}
	dialect := conn.Info().Dialect
	described, found := platform.Lookup(dialect)
	if !found {
		return nil
	}
	for _, change := range changes {
		if err := Check(dialect, change); err != nil {
			return fmt.Errorf("read the values of %s: %w", column(change), err)
		}
	}
	var refused []planrefusal.RoundedValue
	for _, table := range byTable(changes) {
		values, err := scanTable(ctx, conn, dialect, described.Rounding, table)
		if err != nil {
			return err
		}
		refused = append(refused, values...)
	}
	if len(refused) == 0 {
		return nil
	}
	return &planrefusal.RoundedValuesError{Columns: refused, Err: refusal(refused, request)}
}

// byTable groups changes by table, in the order each table first appears.
func byTable(changes []Change) [][]Change {
	var tables [][]Change
	index := make(map[string]int)
	for _, change := range changes {
		at, seen := index[change.Table]
		if !seen {
			at = len(tables)
			index[change.Table] = at
			tables = append(tables, nil)
		}
		tables[at] = append(tables[at], change)
	}
	return tables
}

// scanTable counts the rows of every change of one table in one query, and
// returns the columns with a row the change would alter. Where the query
// fails, each column is read alone, so a value the declared type cannot hold
// is named with the server's error rather than ending the plan with it.
func scanTable(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dialect string,
	rules descriptor.Rounding,
	changes []Change,
) ([]planrefusal.RoundedValue, error) {
	ref, parsed := tableref.Parse(changes[0].Table)
	if !parsed {
		return nil, fmt.Errorf("read the values of %s: the table name %q does not parse", column(changes[0]), changes[0].Table)
	}
	from := " FROM " + sqlident.QualifiedIdent(dialect, ref.Schema, ref.Name)
	counts := make([]string, 0, len(changes))
	scanned := make([]Change, 0, len(changes))
	for _, change := range changes {
		condition, err := Condition(rules, sqlident.Ident(dialect, change.Column), change)
		if err != nil {
			return nil, fmt.Errorf("read the values of %s: %w", column(change), err)
		}
		counts = append(counts, "COUNT(CASE WHEN "+condition+" THEN 1 END)")
		scanned = append(scanned, change)
	}
	rows := make([]int64, len(scanned))
	targets := make([]any, len(scanned))
	for i := range rows {
		targets[i] = &rows[i]
	}
	err := conn.QueryRowContext(ctx, "SELECT "+strings.Join(counts, ", ")+from).Scan(targets...)
	if err == nil {
		var rounded []planrefusal.RoundedValue
		for i, change := range scanned {
			if rows[i] > 0 {
				rounded = append(rounded, refusedValue(change, rows[i], ""))
			}
		}
		return rounded, nil
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("read the values of %s: %w", tableref.Phrase(changes[0].Table), err)
	}
	// The table is read without a conversion first: an error there is not the
	// conversion's, and no column is to blame for it.
	var total int64
	if probe := conn.QueryRowContext(ctx, "SELECT COUNT(*)"+from).Scan(&total); probe != nil {
		return nil, fmt.Errorf("read the values of %s: %w", tableref.Phrase(changes[0].Table), errors.Join(err, probe))
	}
	var refused []planrefusal.RoundedValue
	for i, change := range scanned {
		var count int64
		if err := conn.QueryRowContext(ctx, "SELECT "+counts[i]+from).Scan(&count); err != nil {
			// Only the server's refusal to convert a value is the type's to answer
			// for; a timeout or a lost connection is reported as what it is.
			if !servercode.ConversionRefused(err) {
				return nil, fmt.Errorf("read the values of %s: %w", column(change), err)
			}
			refused = append(refused, refusedValue(change, 0, err.Error()))
			continue
		}
		if count > 0 {
			refused = append(refused, refusedValue(change, count, ""))
		}
	}
	if len(refused) == 0 {
		return nil, fmt.Errorf("read the values of %s: %w", tableref.Phrase(changes[0].Table), err)
	}
	return refused, nil
}

func refusedValue(change Change, rows int64, unconvertible string) planrefusal.RoundedValue {
	return planrefusal.RoundedValue{
		Table: change.Table, Column: change.Column,
		HeldType: change.HeldType, DeclaredType: change.DeclaredType,
		Rows: rows, Unconvertible: unconvertible,
	}
}

func refusal(refused []planrefusal.RoundedValue, request string) error {
	described := make([]string, len(refused))
	rounding := false
	for i, value := range refused {
		name := column(Change{Table: value.Table, Column: value.Column})
		if value.Unconvertible != "" {
			described[i] = fmt.Sprintf("%s from %s to %s, where the server cannot convert a stored value to the new type: %s",
				name, value.HeldType, value.DeclaredType, value.Unconvertible)
			continue
		}
		rounding = true
		noun := "rows hold values"
		if value.Rows == 1 {
			noun = "row holds a value"
		}
		described[i] = fmt.Sprintf("%s from %s to %s, where %d %s with more digits, which the server rounds or cuts "+
			"without an error", name, value.HeldType, value.DeclaredType, value.Rows, noun)
	}
	how := "store values the new type keeps"
	if rounding && request != "" {
		how += ", or ask for the change with " + request
	}
	return errors.New("refusing a type change that keeps fewer digits after the point while the table holds values it " +
		"would change: " + strings.Join(described, "; ") + ". Nothing was changed; " + how)
}

// Warn raises the run warning for each change planned without reading the
// rows, and writes the warning before the plan is shown.
func Warn(ctx context.Context, changes []Change) {
	for _, change := range changes {
		name := column(change)
		_ = rundiag.Warn(ctx, rundiag.Stream(ctx), rundiag.UnscannedRoundings, rundiag.Record{
			Key: name,
			Line: fmt.Sprintf("%s changes from %s to %s, which keeps fewer digits after the point, without a scan of the values "+
				"the table holds: the server rounds or cuts each value to the new digits", name, change.HeldType, change.DeclaredType),
			Attrs: []slog.Attr{slog.String("column", name)},
		})
	}
	rundiag.Settle(ctx, rundiag.UnscannedRoundings)
}

// column names the column of change in a message: `column "price" of table
// "orders"`.
func column(change Change) string {
	return fmt.Sprintf("column %q of %s", change.Column, tableref.Phrase(change.Table))
}
