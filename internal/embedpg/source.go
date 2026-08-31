package embedpg

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/internal/embedengine"
	"go.5x5.cz/ptah/internal/embedgen"
)

// Source is embedengine.Source over a PostgreSQL table.
//
// It reads by keyset, which is what makes a scan taking hours safe over a table
// that changes while it runs: an OFFSET counts rows the query has already
// passed, so a row inserted behind the scan shifts everything after it and the
// scan skips one -- silently, and in a way no count afterwards can see.
type Source struct {
	db   *sql.DB
	spec embedgen.Spec
}

// NewSource returns a source for a specification.
func NewSource(db *sql.DB, spec embedgen.Spec) (*Source, error) {
	if err := validateSource(spec); err != nil {
		return nil, err
	}
	return &Source{db: db, spec: spec}, nil
}

// validateSource refuses a specification that cannot be scanned.
func validateSource(spec embedgen.Spec) error {
	switch {
	case strings.TrimSpace(spec.Source.Table) == "":
		return fmt.Errorf("the specification names no source table")
	case len(spec.Source.KeyFields) == 0:
		// Without a key there is no keyset, and without a keyset a resumed run
		// has nothing to resume after.
		return fmt.Errorf("the specification names no key fields, so a scan has nothing to resume after")
	case len(spec.Source.InputFields) == 0:
		return fmt.Errorf("the specification names no input fields, so there is nothing to embed")
	}
	return nil
}

// Scan returns the rows after a cursor.
func (s *Source) Scan(ctx context.Context, after []string, limit int) (embedengine.Page, error) {
	query, arguments, err := s.scanQuery(after, limit)
	if err != nil {
		return embedengine.Page{}, err
	}
	rows, err := s.db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return embedengine.Page{}, fmt.Errorf("scan %s: %w", s.spec.Source.Table, err)
	}
	defer rows.Close()

	page, err := s.readPage(rows, limit)
	if err != nil {
		return embedengine.Page{}, err
	}
	if err := rows.Err(); err != nil {
		return embedengine.Page{}, fmt.Errorf("scan %s: %w", s.spec.Source.Table, err)
	}
	return page, nil
}

// readPage turns the result set into a page.
func (s *Source) readPage(rows *sql.Rows, limit int) (embedengine.Page, error) {
	keyCount := len(s.spec.Source.KeyFields)
	inputCount := len(s.spec.Source.InputFields)
	columns := keyCount + inputCount + len(s.versionColumns())

	var page embedengine.Page
	for rows.Next() {
		values := make([]sql.NullString, columns)
		targets := make([]any, len(values))
		for index := range values {
			targets[index] = &values[index]
		}
		if err := rows.Scan(targets...); err != nil {
			return embedengine.Page{}, fmt.Errorf("read row from %s: %w", s.spec.Source.Table, err)
		}
		row, version, err := s.buildRow(values, keyCount, inputCount)
		if err != nil {
			return embedengine.Page{}, err
		}
		page.Rows = append(page.Rows, row)
		page.Versions = append(page.Versions, version)
		page.Cursor = row.Key
	}
	page.Done = len(page.Rows) < limit
	return page, nil
}

// buildRow turns one scanned result into a source row.
//
// A NULL key is refused rather than folded to an empty string: it would make
// two different rows share a cursor, and the scan would then either repeat one
// forever or skip past both.
func (s *Source) buildRow(values []sql.NullString, keyCount, inputCount int) (embedgen.Row, string, error) {
	row := embedgen.Row{
		Key:    make([]string, keyCount),
		Fields: make([]*string, inputCount),
	}
	for index := range keyCount {
		if !values[index].Valid {
			return embedgen.Row{}, "", fmt.Errorf(
				"key column %s is NULL in %s, and a keyset scan cannot resume after a row it cannot name",
				s.spec.Source.KeyFields[index], s.spec.Source.Table)
		}
		row.Key[index] = values[index].String
	}
	for index := range inputCount {
		value := values[keyCount+index]
		if value.Valid {
			row.Fields[index] = &value.String
		}
	}
	if len(s.versionColumns()) == 0 {
		return row, "", nil
	}
	return row, values[keyCount+inputCount].String, nil
}

// scanQuery renders the keyset query and its arguments.
func (s *Source) scanQuery(after []string, limit int) (string, []any, error) {
	if limit <= 0 {
		return "", nil, fmt.Errorf("a scan limit of %d would read the whole table into memory", limit)
	}
	keys := quoteAll(s.spec.Source.KeyFields)
	columns := append([]string(nil), keys...)
	columns = append(columns, quoteAll(s.spec.Source.InputFields)...)
	columns = append(columns, quoteAll(s.versionColumns())...)

	var conditions []string
	var arguments []any
	if filter := strings.TrimSpace(s.spec.Source.Filter); filter != "" {
		conditions = append(conditions, "("+filter+")")
	}
	if len(after) > 0 {
		if len(after) != len(keys) {
			return "", nil, fmt.Errorf("the cursor has %d components and the key has %d",
				len(after), len(keys))
		}
		placeholders := make([]string, len(after))
		for index, value := range after {
			arguments = append(arguments, value)
			placeholders[index] = fmt.Sprintf("$%d", len(arguments))
		}
		// A row comparison rather than a chain of ORs: PostgreSQL compares the
		// tuples lexicographically in one expression, which is both what the
		// index supports and what a hand-written chain gets wrong when the key
		// has more than two parts.
		conditions = append(conditions, fmt.Sprintf("(%s) > (%s)",
			strings.Join(keys, ", "), strings.Join(placeholders, ", ")))
	}

	arguments = append(arguments, limit)
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), s.qualifiedTable())
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += fmt.Sprintf(" ORDER BY %s LIMIT $%d", strings.Join(keys, ", "), len(arguments))
	return query, arguments, nil
}

// versionColumns names the column the source version is read from, and is
// empty under a strategy that establishes none.
//
// A list of nought or one rather than a string and a flag: the caller needs to
// know how many columns the scan returns, and a name plus a boolean is two
// answers to that question.
func (s *Source) versionColumns() []string {
	if field := strings.TrimSpace(s.spec.Source.VersionField); field != "" {
		return []string{field}
	}
	return nil
}

// qualifiedTable renders the table with its schema when it has one.
func (s *Source) qualifiedTable() string {
	return qualify(s.spec.Source.Schema, s.spec.Source.Table)
}

// quoteAll quotes every identifier.
func quoteAll(names []string) []string {
	quoted := make([]string, len(names))
	for index, name := range names {
		quoted[index] = quoteIdentifier(name)
	}
	return quoted
}

// quoteIdentifier renders an identifier PostgreSQL will read as one name.
//
// The doubling is not decoration: an identifier from a specification is a
// string somebody wrote, and a table called `a"; DROP TABLE b; --` is a valid
// PostgreSQL table name.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// Current returns the rows still present out of a set of keys, and their
// versions.
//
// A key that is absent from the answer is a row that is gone, which is what
// catch-up turns into a tombstone. The absence is the answer rather than an
// error: a row deleted between the change event and the reread is the ordinary
// case, not a fault.
func (s *Source) Current(ctx context.Context, keys [][]string) ([]embedgen.Row, []string, error) {
	if len(keys) == 0 {
		return nil, nil, nil
	}
	query, arguments, err := s.currentQuery(keys)
	if err != nil {
		return nil, nil, err
	}
	rows, err := s.db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, nil, fmt.Errorf("reread %s: %w", s.spec.Source.Table, err)
	}
	defer rows.Close()

	page, err := s.readPage(rows, len(keys)+1)
	if err != nil {
		return nil, nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("reread %s: %w", s.spec.Source.Table, err)
	}
	return page.Rows, page.Versions, nil
}

// currentQuery renders the reread.
//
// A row comparison per key rather than an IN over one column, because the key
// may have several parts and `id IN (...) AND tenant IN (...)` is a different
// query: it matches every combination of the two lists, which for two tenants
// and two ids is four rows where two were asked for.
//
// It applies spec.Source.Filter, and the scan is not the only reason. Without
// it a row the specification excludes was re-read on the strength of its outbox
// event, its text was sent to the provider, and the generation's vector was
// written onto it -- so the corpus the index covers held rows the operator had
// excluded and `plan`'s disclosure ("the text of title, body / for 5 rows") was
// a count of a different set than the one that left (stokaro/ptah#2638).
//
// The filter also decides what a changed row that no longer qualifies BECOMES.
// [tombstonesFor] derives tombstones from what the reread found rather than
// from the event, so a key that stops matching returns no row and is tombstoned
// -- which is the same answer this query already gave for a key that was
// deleted, and the right one: in both cases the generation should not carry it.
func (s *Source) currentQuery(keys [][]string) (string, []any, error) {
	keyColumns := quoteAll(s.spec.Source.KeyFields)
	columns := append([]string(nil), keyColumns...)
	columns = append(columns, quoteAll(s.spec.Source.InputFields)...)
	columns = append(columns, quoteAll(s.versionColumns())...)

	var arguments []any
	tuples := make([]string, 0, len(keys))
	for _, key := range keys {
		if len(key) != len(keyColumns) {
			return "", nil, fmt.Errorf("a key has %d components and the key has %d",
				len(key), len(keyColumns))
		}
		placeholders := make([]string, len(key))
		for index, value := range key {
			arguments = append(arguments, value)
			placeholders[index] = fmt.Sprintf("$%d", len(arguments))
		}
		tuples = append(tuples, "("+strings.Join(placeholders, ", ")+")")
	}
	conditions := []string{fmt.Sprintf("(%s) IN (%s)",
		strings.Join(castToText(keyColumns), ", "), strings.Join(tuples, ", "))}
	if filter := strings.TrimSpace(s.spec.Source.Filter); filter != "" {
		conditions = append(conditions, "("+filter+")")
	}
	// #nosec G201 -- PostgreSQL takes no bind parameter for a relation or column
	// name. The identifiers come from the specification and go through
	// quoteIdentifier; the key VALUES are all placeholders. The filter is a SQL
	// fragment the specification carries, interpolated here exactly as the scan
	// interpolates it.
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s",
		strings.Join(columns, ", "), s.qualifiedTable(),
		strings.Join(conditions, " AND "), strings.Join(keyColumns, ", "))
	return query, arguments, nil
}

// castToText renders the key columns as text, so a key carried as strings
// compares against a column of any type.
func castToText(columns []string) []string {
	cast := make([]string, len(columns))
	for index, column := range columns {
		cast[index] = column + "::text"
	}
	return cast
}
