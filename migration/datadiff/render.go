package datadiff

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"ptah.run/core/dataext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/sqlident"
)

// Render renders diff into a pair of SQL scripts: up applies the desired state
// and down reverses it, so that applying up followed by down returns the table
// to its original contents.
//
// Statement order in up is Inserts, then Updates, then Deletes, preserving the
// deterministic per-key ordering of the DataDiff slices:
//
//   - Insert R  -> INSERT INTO <table> (<all columns, sorted>) VALUES (<literals>);
//   - Update U  -> UPDATE <table> SET <non-key columns of U.Desired, sorted> WHERE <key columns, sorted, AND-joined>;
//   - Delete R  -> DELETE FROM <table> WHERE <key columns, sorted, AND-joined>;
//
// down emits the exact inverse of every statement in fully reversed order (undo
// Deletes first, then Updates, then Inserts, each group iterated in reverse) so
// that up followed by down is a round-trip:
//
//   - the inverse of an INSERT is a DELETE keyed on the inserted row;
//   - the inverse of an UPDATE restores the prior values captured in RowUpdate.Live;
//   - the inverse of a DELETE re-inserts the captured live row.
//
// Both scripts are newline-terminated sequences of ';'-terminated statements,
// or empty strings when diff carries no changes (a no-op is valid). They are
// the statements [RenderStatements] returns, joined with "\n". A string value
// may carry a newline, which stays inside its literal, so the scripts cannot be
// cut into statements at line breaks; a caller that needs the statements one by
// one takes them from [RenderStatements]. Identifiers are spelled for dialect
// the way the schema renderers spell them, so a statement names the table and
// columns they created: quoted on every dialect but Oracle, and bare on Oracle
// wherever Oracle accepts a bare name, because there a quoted lower-case name is
// a different object from the upper-case one a bare CREATE TABLE made. Values
// are rendered as safely-escaped literals (see [renderLiteral]) so a string
// value can never terminate its literal or inject SQL.
//
// Render returns an error for a nil diff, for a non-empty diff with no key
// columns, for a row that is missing a key column or has no columns at all, and
// for any value that [renderLiteral] cannot represent safely.
//
// # Table name
//
// When diff.Schema is set the name is emitted schema-qualified (for PostgreSQL:
// "app"."regions"; for Oracle: app.regions); when it is empty only the table is
// named. diff.Table is always treated as a single identifier, so a dotted table
// name is quoted whole rather than split into schema and table — put the schema
// in diff.Schema.
//
// # Limitations
//
// The inverse of a DELETE re-inserts only the managed columns captured in the
// live row; columns the database held but that were not read back are not
// reconstructed. Likewise the inverse of an UPDATE restores exactly the non-key
// columns present in RowUpdate.Live, so a forward UPDATE that introduces a value
// for a column absent from the live row cannot be rolled back to that absence.
// A []byte value is written as a hexadecimal binary literal, and a dialect with
// no known binary literal is refused (see [binaryLiteral]).
//
// # Data services
//
// services are the dialect's data services, from
// engine.Runtime.DataServices. Most dialects have none and render every value
// from its Go value. A dialect whose descriptor says its owner writes some
// literals (descriptor.Literals.ByOwner), as YDB's and Oracle's do, is refused
// with an error naming it when services carries neither a Literals nor a
// Columns service; an empty diff needs none.
//
// # YDB
//
// YQL types every literal and converts few of them, so on YDB each value is
// written by the YDB owner in the type of its column, which
// [DataDiff.ColumnTypes] names: an Int32 column takes `5`, an Int64 column
// `5l`, a Utf8 column `'x'u`, a Timestamp column `Timestamp('...')`. A column the map does not name is an
// error rather than a guess, and so is a value its column cannot hold -- an
// integer outside the type's range, a negative one for an unsigned type -- which
// the error names together with the column and its type.
func Render(diff *DataDiff, dialect string, services dataext.Services) (up, down string, err error) {
	upStmts, downStmts, err := RenderStatements(diff, dialect, services)
	if err != nil {
		return "", "", err
	}
	return joinStatements(upStmts), joinStatements(downStmts), nil
}

// RenderStatements renders diff into the statements [Render] joins into its
// scripts, one element per statement, in the same order and with the same
// validation, quoting and escaping.
//
// up holds one statement per row: an INSERT for every element of
// diff.Inserts, then an UPDATE for every element of diff.Updates, then a
// DELETE for every element of diff.Deletes. down holds the inverse of each, in
// fully reversed order. Every element is a complete ';'-terminated statement
// with no trailing newline; an element spans several lines exactly when a
// string value carries a newline.
//
// Both slices are nil when diff carries no changes. On error both are nil and
// the error is the one [Render] returns for the same diff.
func RenderStatements(diff *DataDiff, dialect string, services dataext.Services) (up, down []string, err error) {
	if diff == nil {
		return nil, nil, errors.New("datadiff: nil diff")
	}

	if len(diff.Inserts) == 0 && len(diff.Updates) == 0 && len(diff.Deletes) == 0 {
		return nil, nil, nil
	}

	if len(diff.Keys) == 0 {
		return nil, nil, errors.New("datadiff: keys must be non-empty to render a non-empty diff")
	}

	t, err := newTarget(dialect, services)
	if err != nil {
		return nil, nil, err
	}
	table := sqlident.QualifiedIdent(dialect, diff.Schema, diff.Table)

	up, err = renderUp(t, table, diff)
	if err != nil {
		return nil, nil, err
	}
	down, err = renderDown(t, table, diff)
	if err != nil {
		return nil, nil, err
	}
	return up, down, nil
}

// renderUp builds the forward statements in Inserts, Updates, Deletes order.
func renderUp(t target, table string, diff *DataDiff) ([]string, error) {
	stmts := make([]string, 0, len(diff.Inserts)+len(diff.Updates)+len(diff.Deletes))
	for _, row := range diff.Inserts {
		s, err := insertStmt(t, table, diff.ColumnTypes, row)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
	}
	for _, u := range diff.Updates {
		s, err := updateStmt(t, table, diff.ColumnTypes, diff.Keys, u.Desired, u.Key)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
	}
	for _, row := range diff.Deletes {
		s, err := deleteStmt(t, table, diff.ColumnTypes, diff.Keys, row)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
	}
	return stmts, nil
}

// renderDown builds the inverse statements in fully reversed order: the inverse
// of each Delete (re-insert), then of each Update (restore Live), then of each
// Insert (delete), with every group iterated back to front.
func renderDown(t target, table string, diff *DataDiff) ([]string, error) {
	stmts := make([]string, 0, len(diff.Inserts)+len(diff.Updates)+len(diff.Deletes))
	for _, row := range slices.Backward(diff.Deletes) {
		s, err := insertStmt(t, table, diff.ColumnTypes, row)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
	}
	for _, u := range slices.Backward(diff.Updates) {
		s, err := updateStmt(t, table, diff.ColumnTypes, diff.Keys, u.Live, u.Key)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
	}
	for _, row := range slices.Backward(diff.Inserts) {
		s, err := deleteStmt(t, table, diff.ColumnTypes, diff.Keys, row)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
	}
	return stmts, nil
}

// insertStmt renders an INSERT for every column of row, columns sorted so the
// output is deterministic. A row with no columns is an error.
func insertStmt(t target, table string, types map[string]string, row Row) (string, error) {
	cols := sortedColumns(row)
	if len(cols) == 0 {
		return "", errors.New("datadiff: cannot render INSERT for a row with no columns")
	}
	quotedCols := make([]string, len(cols))
	literals := make([]string, len(cols))
	for i, col := range cols {
		lit, err := renderColumnLiteral(t, types[col], row[col])
		if err != nil {
			return "", fmt.Errorf("datadiff: column %q: %w", col, err)
		}
		quotedCols[i] = sqlident.Ident(t.dialect, col)
		literals[i] = lit
	}
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s);",
		table, strings.Join(quotedCols, ", "), strings.Join(literals, ", "),
	), nil
}

// updateStmt renders an UPDATE that sets the non-key columns of setRow (sorted)
// and matches rows on the key columns, whose values are taken from keyRow. Key
// columns are skipped in the SET clause. An empty setRow, a setRow with no
// non-key columns, or a keyRow missing a key column is an error.
func updateStmt(t target, table string, types map[string]string, keys []string, setRow, keyRow Row) (string, error) {
	if len(setRow) == 0 {
		return "", errors.New("datadiff: cannot render UPDATE for a row with no columns")
	}

	keySet := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keySet[k] = struct{}{}
	}

	setCols := make([]string, 0, len(setRow))
	for col := range setRow {
		if _, isKey := keySet[col]; !isKey {
			setCols = append(setCols, col)
		}
	}
	if len(setCols) == 0 {
		return "", errors.New("datadiff: cannot render UPDATE with no non-key columns to set")
	}
	slices.Sort(setCols)

	assignments := make([]string, len(setCols))
	for i, col := range setCols {
		lit, err := renderColumnLiteral(t, types[col], setRow[col])
		if err != nil {
			return "", fmt.Errorf("datadiff: column %q: %w", col, err)
		}
		assignments[i] = sqlident.Ident(t.dialect, col) + " = " + lit
	}

	where, err := keyPredicate(t, types, keys, keyRow)
	if err != nil {
		return "", err
	}
	if t.managed.AlterTableUpdate {
		return fmt.Sprintf(
			"ALTER TABLE %s UPDATE %s WHERE %s;",
			table, strings.Join(assignments, ", "), where,
		), nil
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s;", table, strings.Join(assignments, ", "), where), nil
}

// target is the dialect a diff renders for: the spelling the caller named,
// which identifiers are quoted for, what its descriptor states about literals
// and row changes, and the owner's data services.
//
// A row change is written as ALTER TABLE ... UPDATE where the descriptor says
// so (descriptor.ManagedData.AlterTableUpdate). ClickHouse answers a plain
// UPDATE with "Lightweight updates are not supported" unless the table
// carries a materialized _block_number column, which a reference table does
// not; DELETE needs no such arm (stokaro/ptah#3281).
type target struct {
	dialect  string
	name     string
	literals descriptor.Literals
	managed  descriptor.ManagedData
	services dataext.Services
}

// newTarget reads the descriptor of dialect from the process registry. A
// dialect it does not describe renders with the standard spelling, as an
// unrecognized dialect always has. A dialect whose owner writes some of its
// literals (descriptor.Literals.ByOwner) is refused when services carries no
// literal service, rather than written from each value alone.
func newTarget(dialect string, services dataext.Services) (target, error) {
	described, _ := platform.Lookup(dialect)
	if described.Literals.ByOwner && services.Literals == nil && services.Columns == nil {
		return target{}, fmt.Errorf(
			"datadiff: %s writes literals through its owner's data services, and the caller gave none; "+
				"pass the services engine.Runtime.DataServices returns for the dialect", described.Name)
	}
	return target{
		dialect:  dialect,
		name:     described.Name,
		literals: described.Literals,
		managed:  described.ManagedData,
		services: services,
	}, nil
}

// deleteStmt renders a DELETE that matches rows on the key columns, whose values
// are taken from row.
func deleteStmt(t target, table string, types map[string]string, keys []string, row Row) (string, error) {
	where, err := keyPredicate(t, types, keys, row)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s;", table, where), nil
}

// keyPredicate builds a "<key> = <literal>" predicate for every key column,
// sorted and joined with AND. Values are taken from values; a missing key column
// is an error.
func keyPredicate(t target, types map[string]string, keys []string, values Row) (string, error) {
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	parts := make([]string, len(sorted))
	for i, k := range sorted {
		v, ok := values[k]
		if !ok {
			return "", fmt.Errorf("datadiff: missing key column %q", k)
		}
		// A key column holding NULL is addressed with IS NULL. Written as
		// `= NULL` the predicate is UNKNOWN for every row, so the statement
		// succeeds and changes nothing: the row the diff found stays exactly
		// where it was, and the next run finds it again (stokaro/ptah#3279).
		if v == nil {
			parts[i] = sqlident.Ident(t.dialect, k) + " IS NULL"
			continue
		}
		lit, err := renderColumnLiteral(t, types[k], v)
		if err != nil {
			return "", fmt.Errorf("datadiff: key column %q: %w", k, err)
		}
		parts[i] = sqlident.Ident(t.dialect, k) + " = " + lit
	}
	return strings.Join(parts, " AND "), nil
}

// sortedColumns returns the column names of row in ascending order.
func sortedColumns(row Row) []string {
	cols := make([]string, 0, len(row))
	for col := range row {
		cols = append(cols, col)
	}
	slices.Sort(cols)
	return cols
}

// joinStatements joins statements with newlines and appends a trailing newline,
// or returns the empty string for no statements.
func joinStatements(stmts []string) string {
	if len(stmts) == 0 {
		return ""
	}
	return strings.Join(stmts, "\n") + "\n"
}

// renderColumnLiteral renders v for a column declared as declaredType.
//
// The declared type matters only where a dialect refuses the literal a Go value
// would otherwise take, and the dialect's owner says where through its data
// services. An owner's dataext.Literals service writes the literal itself:
// YQL types every literal, so on YDB every value is written in the type of its
// column, the one [DataDiff.ColumnTypes] names; see [Render]. An owner's
// dataext.Columns service names the columns that hold a moment and refuse
// text: a declaration writes a moment as text, and Oracle refuses text for a
// DATE or TIMESTAMP column, so there the text is read as the moment it names
// and rendered as a timestamp literal. Text that names no moment is refused
// here rather than sent to a server that would refuse it. An empty
// declaredType is a column whose type the caller does not know, and v renders
// from its Go value alone, unless the owner writes it.
func renderColumnLiteral(t target, declaredType string, v any) (string, error) {
	if t.services.Literals != nil {
		literal, handled, err := t.services.Literals.Literal(declaredType, v)
		if handled {
			return literal, err
		}
	}
	text, isText := textValue(v)
	if !isText || declaredType == "" || t.services.Columns == nil || !t.services.Columns.HoldsMoment(declaredType) {
		return renderLiteral(t, v)
	}
	moment, ok := parseMoment(text)
	if !ok {
		return "", fmt.Errorf(
			"datadiff: %q does not name a moment, and a %s column on %s does not accept text; write it as 2006-01-02, 2006-01-02 15:04:05 or RFC 3339",
			text, declaredType, t.name)
	}
	return timeLiteral(t, moment), nil
}

// textValue returns v as text when it is a string or a []byte.
func textValue(v any) (string, bool) {
	switch text := v.(type) {
	case string:
		return text, true
	case []byte:
		return string(text), true
	default:
		return "", false
	}
}

// renderLiteral renders v as a SQL value literal that is safe to embed directly
// in a statement for dialect. It is the security-sensitive core of the renderer:
// a string value can never terminate its literal or inject SQL.
//
// Mapping:
//
//   - nil            -> NULL
//   - bool           -> TRUE/FALSE, except 1/0 where the dialect's descriptor
//     says so (descriptor.Literals.NumericBool): MySQL, MariaDB, ClickHouse,
//     SQL Server and Oracle
//   - signed ints    -> decimal digits
//   - unsigned ints  -> decimal digits
//   - float32/float64 -> strconv.FormatFloat(f, 'g', -1, bitSize); NaN and
//     infinities are rejected as they have no SQL literal
//   - string         -> a single-quoted literal (see below)
//   - []byte         -> a hexadecimal binary literal (see [binaryLiteral])
//   - time.Time      -> a quoted timestamp literal (see [timeLiteral]); drivers
//     scan timestamp columns as time.Time, so a managed timestamp column's live
//     value reaches the renderer here
//
// Any other Go type is rejected with an error naming the type rather than being
// formatted into SQL, which would be an injection risk.
//
// # String escaping (per dialect)
//
// The single quote is always doubled per the SQL standard:
//
//	'  ->  ''
//
// Where the dialect's descriptor says string literals process C-style
// backslash escapes (descriptor.Literals.BackslashEscapes), as MySQL, MariaDB
// and ClickHouse do by default, the backslash is ALSO escaped (\ -> \\); for the PostgreSQL family, SQLite, SQL Server, and any
// unrecognized dialect the backslash is left untouched, as standard SQL treats
// it literally. A string containing a NUL byte is rejected because it is not
// portably representable as a SQL literal.
func renderLiteral(t target, v any) (string, error) {
	switch val := v.(type) {
	case nil:
		return "NULL", nil
	case bool:
		if t.literals.NumericBool {
			if val {
				return "1", nil
			}
			return "0", nil
		}
		if val {
			return "TRUE", nil
		}
		return "FALSE", nil
	case string:
		return stringLiteral(t, val)
	case []byte:
		return binaryLiteral(t, val)
	case time.Time:
		return timeLiteral(t, val), nil
	}
	if lit, ok, err := numericLiteral(v); ok {
		return lit, err
	}
	return "", fmt.Errorf(
		"datadiff: cannot render a SQL literal for a value of Go type %T; this data migration reads a value the renderer does not know how to represent safely — declare it in the desired rows or exclude the column",
		v)
}

// numericLiteral renders the Go numeric types as SQL numeric literals. The
// boolean return reports whether v was a recognized numeric type; when false the
// caller treats v as unsupported. Signed and unsigned integers render as decimal
// digits; floats use the shortest round-tripping form and reject NaN/infinities
// (see [floatLiteral]).
func numericLiteral(v any) (string, bool, error) {
	switch val := v.(type) {
	case int:
		return strconv.FormatInt(int64(val), 10), true, nil
	case int8:
		return strconv.FormatInt(int64(val), 10), true, nil
	case int16:
		return strconv.FormatInt(int64(val), 10), true, nil
	case int32:
		return strconv.FormatInt(int64(val), 10), true, nil
	case int64:
		return strconv.FormatInt(val, 10), true, nil
	case uint:
		return strconv.FormatUint(uint64(val), 10), true, nil
	case uint8:
		return strconv.FormatUint(uint64(val), 10), true, nil
	case uint16:
		return strconv.FormatUint(uint64(val), 10), true, nil
	case uint32:
		return strconv.FormatUint(uint64(val), 10), true, nil
	case uint64:
		return strconv.FormatUint(val, 10), true, nil
	case float32:
		lit, err := floatLiteral(float64(val), 32)
		return lit, true, err
	case float64:
		lit, err := floatLiteral(val, 64)
		return lit, true, err
	default:
		return "", false, nil
	}
}

// timeLiteral renders moment as a quoted SQL timestamp literal, as the
// dialect's descriptor spells it (descriptor.Literals.TimestampOffset and
// TimestampPrefix). The layout uses a space separator, fractional seconds
// (trailing zeros trimmed), and the numeric UTC offset for dialects whose
// timestamp literals accept one, so both
// timestamp-with-time-zone and plain-timestamp columns round-trip: PostgreSQL and
// SQLite preserve the instant from the offset (verified against the driver), a
// PostgreSQL "timestamp without time zone" simply ignores the offset, and SQLite
// stores the text verbatim. For MySQL, MariaDB, ClickHouse, SQL Server, and any
// unrecognized dialect the offset is omitted, because their DATETIME/datetime2
// literal grammars do not accept a trailing offset; the wall-clock rendering
// round-trips within the connection's session time zone. The layout produces only
// digits and the punctuation ' - : . + space', so no escaping is required and the
// value can never break out of its literal.
//
// Oracle takes the offset form behind the TIMESTAMP keyword. A bare string is
// read through NLS_TIMESTAMP_FORMAT, DD-MON-RR by default, and refused with
// ORA-01843; the typed literal is accepted by DATE, TIMESTAMP and TIMESTAMP WITH
// TIME ZONE columns. Measured on 23.26: a TIMESTAMP column keeps the wall clock
// and drops the offset, a TIMESTAMP WITH TIME ZONE column keeps both, and the
// driver hands either back as the time.Time this renders, so the value
// round-trips.
func timeLiteral(t target, moment time.Time) string {
	layout := "2006-01-02 15:04:05.999999999"
	if t.literals.TimestampOffset {
		layout += "-07:00"
	}
	return t.literals.TimestampPrefix + "'" + moment.Format(layout) + "'"
}

// floatLiteral renders f using the shortest representation that round-trips at
// bitSize (32 for float32, 64 for float64). NaN and infinities have no SQL
// literal and are rejected.
func floatLiteral(f float64, bitSize int) (string, error) {
	if math.IsNaN(f) {
		return "", errors.New("datadiff: NaN has no SQL literal representation")
	}
	if math.IsInf(f, 0) {
		return "", errors.New("datadiff: infinity has no SQL literal representation")
	}
	return strconv.FormatFloat(f, 'g', -1, bitSize), nil
}

// stringLiteral renders s as a single-quoted SQL literal, escaping per the rules
// documented on [renderLiteral].
func stringLiteral(t target, s string) (string, error) {
	if strings.IndexByte(s, 0) >= 0 {
		return "", errors.New("datadiff: string value contains a NUL byte, which has no portable SQL literal representation")
	}
	if t.literals.BackslashEscapes {
		s = strings.ReplaceAll(s, `\`, `\\`)
	}
	s = strings.ReplaceAll(s, "'", "''")
	return "'" + s + "'", nil
}

// binaryLiteral renders b as a hexadecimal binary literal for dialect.
//
// A character-string literal cannot carry arbitrary bytes into a binary column.
// PostgreSQL refuses a byte that is not valid UTF-8 (SQLSTATE 22021), its bytea
// input reads a backslash as an escape and stores a different value without an
// error, and SQL Server refuses to convert a varchar literal to varbinary at
// all. A literal spelled in hex digits holds none of those bytes:
//
//   - PostgreSQL, CockroachDB, YugabyteDB, Spanner -> '\x<hex>'::bytea
//   - MySQL, MariaDB, SQLite, ClickHouse          -> X'<hex>'
//   - SQL Server                                  -> 0x<hex>
//   - Oracle                                      -> HEXTORAW('<hex>')
//
// The dialect's descriptor holds the spelling (descriptor.Literals.BinaryPrefix
// and BinarySuffix). The PostgreSQL spelling relies on
// standard_conforming_strings, as [stringLiteral] does. A dialect whose
// descriptor names none is refused rather than given a guess.
func binaryLiteral(t target, b []byte) (string, error) {
	if t.literals.BinaryPrefix == "" {
		return "", fmt.Errorf("datadiff: no binary literal is known for dialect %q, so a []byte value cannot be rendered", t.dialect)
	}
	return t.literals.BinaryPrefix + hex.EncodeToString(b) + t.literals.BinarySuffix, nil
}
