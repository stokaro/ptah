package datadiff

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/sqlident"
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
// or empty strings when diff carries no changes (a no-op is valid). Identifiers
// are quoted for dialect via sqlident.Quote; values are rendered as
// safely-escaped literals (see [renderLiteral]) so a string value can never
// terminate its literal or inject SQL.
//
// Render returns an error for a nil diff, for a non-empty diff with no key
// columns, for a row that is missing a key column or has no columns at all, and
// for any value that [renderLiteral] cannot represent safely.
//
// # Table name
//
// A DataDiff carries no schema, so the table is quoted as a single identifier
// via sqlident.Qualified(dialect, "", diff.Table). A dotted name such as
// "app.regions" is therefore quoted whole (for PostgreSQL: "app.regions")
// rather than as schema.table; schema-qualified managed tables are a known
// follow-up.
//
// # Limitations
//
// The inverse of a DELETE re-inserts only the managed columns captured in the
// live row; columns the database held but that were not read back are not
// reconstructed. Likewise the inverse of an UPDATE restores exactly the non-key
// columns present in RowUpdate.Live, so a forward UPDATE that introduces a value
// for a column absent from the live row cannot be rolled back to that absence.
// Binary blobs are out of scope for this phase: a []byte value is treated as
// UTF-8 text (see [renderLiteral]).
func Render(diff *DataDiff, dialect string) (up string, down string, err error) {
	if diff == nil {
		return "", "", errors.New("datadiff: nil diff")
	}

	if len(diff.Inserts) == 0 && len(diff.Updates) == 0 && len(diff.Deletes) == 0 {
		return "", "", nil
	}

	if len(diff.Keys) == 0 {
		return "", "", errors.New("datadiff: keys must be non-empty to render a non-empty diff")
	}

	table := sqlident.Qualified(dialect, "", diff.Table)

	upStmts, err := renderUp(dialect, table, diff)
	if err != nil {
		return "", "", err
	}
	downStmts, err := renderDown(dialect, table, diff)
	if err != nil {
		return "", "", err
	}
	return joinStatements(upStmts), joinStatements(downStmts), nil
}

// renderUp builds the forward statements in Inserts, Updates, Deletes order.
func renderUp(dialect, table string, diff *DataDiff) ([]string, error) {
	stmts := make([]string, 0, len(diff.Inserts)+len(diff.Updates)+len(diff.Deletes))
	for _, row := range diff.Inserts {
		s, err := insertStmt(dialect, table, row)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
	}
	for _, u := range diff.Updates {
		s, err := updateStmt(dialect, table, diff.Keys, u.Desired, u.Key)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
	}
	for _, row := range diff.Deletes {
		s, err := deleteStmt(dialect, table, diff.Keys, row)
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
func renderDown(dialect, table string, diff *DataDiff) ([]string, error) {
	stmts := make([]string, 0, len(diff.Inserts)+len(diff.Updates)+len(diff.Deletes))
	for _, row := range slices.Backward(diff.Deletes) {
		s, err := insertStmt(dialect, table, row)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
	}
	for _, u := range slices.Backward(diff.Updates) {
		s, err := updateStmt(dialect, table, diff.Keys, u.Live, u.Key)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
	}
	for _, row := range slices.Backward(diff.Inserts) {
		s, err := deleteStmt(dialect, table, diff.Keys, row)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
	}
	return stmts, nil
}

// insertStmt renders an INSERT for every column of row, columns sorted so the
// output is deterministic. A row with no columns is an error.
func insertStmt(dialect, table string, row Row) (string, error) {
	cols := sortedColumns(row)
	if len(cols) == 0 {
		return "", errors.New("datadiff: cannot render INSERT for a row with no columns")
	}
	quotedCols := make([]string, len(cols))
	literals := make([]string, len(cols))
	for i, col := range cols {
		lit, err := renderLiteral(dialect, row[col])
		if err != nil {
			return "", fmt.Errorf("datadiff: column %q: %w", col, err)
		}
		quotedCols[i] = sqlident.Quote(dialect, col)
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
func updateStmt(dialect, table string, keys []string, setRow, keyRow Row) (string, error) {
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
		lit, err := renderLiteral(dialect, setRow[col])
		if err != nil {
			return "", fmt.Errorf("datadiff: column %q: %w", col, err)
		}
		assignments[i] = sqlident.Quote(dialect, col) + " = " + lit
	}

	where, err := keyPredicate(dialect, keys, keyRow)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s;", table, strings.Join(assignments, ", "), where), nil
}

// deleteStmt renders a DELETE that matches rows on the key columns, whose values
// are taken from row.
func deleteStmt(dialect, table string, keys []string, row Row) (string, error) {
	where, err := keyPredicate(dialect, keys, row)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s;", table, where), nil
}

// keyPredicate builds a "<key> = <literal>" predicate for every key column,
// sorted and joined with AND. Values are taken from values; a missing key column
// is an error.
func keyPredicate(dialect string, keys []string, values Row) (string, error) {
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	parts := make([]string, len(sorted))
	for i, k := range sorted {
		v, ok := values[k]
		if !ok {
			return "", fmt.Errorf("datadiff: missing key column %q", k)
		}
		lit, err := renderLiteral(dialect, v)
		if err != nil {
			return "", fmt.Errorf("datadiff: key column %q: %w", k, err)
		}
		parts[i] = sqlident.Quote(dialect, k) + " = " + lit
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

// renderLiteral renders v as a SQL value literal that is safe to embed directly
// in a statement for dialect. It is the security-sensitive core of the renderer:
// a string value can never terminate its literal or inject SQL.
//
// Mapping:
//
//   - nil            -> NULL
//   - bool           -> TRUE/FALSE, except 1/0 for MySQL, MariaDB, ClickHouse,
//     and SQL Server (see [usesNumericBool])
//   - signed ints    -> decimal digits
//   - unsigned ints  -> decimal digits
//   - float32/float64 -> strconv.FormatFloat(f, 'g', -1, bitSize); NaN and
//     infinities are rejected as they have no SQL literal
//   - string, []byte -> a single-quoted literal (see below)
//
// Any other Go type is rejected with an error naming the type rather than being
// formatted into SQL, which would be an injection risk. A []byte is treated as
// UTF-8 text and escaped like a string; binary blob encoding is out of scope.
//
// # String escaping (per dialect)
//
// The single quote is always doubled (” ) per the SQL standard. For MySQL,
// MariaDB, and ClickHouse the backslash is ALSO escaped (\ -> \\) because those
// dialects process C-style backslash escapes inside string literals by default;
// for the PostgreSQL family, SQLite, SQL Server, and any unrecognized dialect
// the backslash is left untouched, as standard SQL treats it literally. A string
// containing a NUL byte is rejected because it is not portably representable as a
// SQL literal.
func renderLiteral(dialect string, v any) (string, error) {
	switch val := v.(type) {
	case nil:
		return "NULL", nil
	case bool:
		if usesNumericBool(dialect) {
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
		return stringLiteral(dialect, val)
	case []byte:
		return stringLiteral(dialect, string(val))
	case int:
		return strconv.FormatInt(int64(val), 10), nil
	case int8:
		return strconv.FormatInt(int64(val), 10), nil
	case int16:
		return strconv.FormatInt(int64(val), 10), nil
	case int32:
		return strconv.FormatInt(int64(val), 10), nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	case uint:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint64:
		return strconv.FormatUint(val, 10), nil
	case float32:
		return floatLiteral(float64(val), 32)
	case float64:
		return floatLiteral(val, 64)
	default:
		return "", fmt.Errorf("datadiff: unsupported value type %T for SQL literal", v)
	}
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
func stringLiteral(dialect, s string) (string, error) {
	if strings.IndexByte(s, 0) >= 0 {
		return "", errors.New("datadiff: string value contains a NUL byte, which has no portable SQL literal representation")
	}
	if usesBackslashEscapes(dialect) {
		s = strings.ReplaceAll(s, `\`, `\\`)
	}
	s = strings.ReplaceAll(s, "'", "''")
	return "'" + s + "'", nil
}

// usesBackslashEscapes reports whether dialect processes C-style backslash
// escapes inside string literals by default, requiring backslashes to be
// doubled.
func usesBackslashEscapes(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB, platform.ClickHouse:
		return true
	default:
		return false
	}
}

// usesNumericBool reports whether dialect spells boolean literals as 1/0 rather
// than TRUE/FALSE.
func usesNumericBool(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB, platform.ClickHouse, platform.SQLServer:
		return true
	default:
		return false
	}
}
