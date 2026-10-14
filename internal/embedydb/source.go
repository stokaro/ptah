package embedydb

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedengine"
	"ptah.run/internal/embedgen"
)

// Source is [embedbackend.Source] over a YDB row table.
//
// It reads by keyset over the primary key, so a page is a range read and a
// scan taking hours over a table that changes while it runs neither skips
// nor repeats a row.
type Source struct {
	backend *Backend
	spec    embedgen.Spec

	once sync.Once
	key  []keyColumn
	err  error
}

// NewSource returns a source for a specification. The table is described on
// the first read, which is the first moment a context exists to do it in.
func (b *Backend) NewSource(spec embedgen.Spec) (embedbackend.Source, error) {
	if err := validateSource(spec); err != nil {
		return nil, err
	}
	return &Source{backend: b, spec: spec}, nil
}

// validateSource refuses a specification that cannot be scanned.
func validateSource(spec embedgen.Spec) error {
	switch {
	case strings.TrimSpace(spec.Source.Table) == "":
		return fmt.Errorf("the specification names no source table")
	case len(spec.Source.KeyFields) == 0:
		return fmt.Errorf("the specification names no key fields, so a scan has nothing to resume after")
	case len(spec.Source.InputFields) == 0:
		return fmt.Errorf("the specification names no input fields, so there is nothing to embed")
	}
	return nil
}

// sourceKey describes the source once.
func (s *Source) sourceKey(ctx context.Context) ([]keyColumn, error) {
	s.once.Do(func() {
		s.key, s.err = s.backend.sourceKey(ctx, s.spec)
	})
	return s.key, s.err
}

// Scan returns the rows after a cursor, in key order.
func (s *Source) Scan(ctx context.Context, after []string, limit int) (embedengine.Page, error) {
	key, err := s.sourceKey(ctx)
	if err != nil {
		return embedengine.Page{}, err
	}
	query, arguments, err := s.scanQuery(key, after, limit)
	if err != nil {
		return embedengine.Page{}, err
	}
	rows, err := s.backend.db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return embedengine.Page{}, fmt.Errorf("scan %s: %w", s.spec.Source.Table, err)
	}
	defer func() { _ = rows.Close() }()
	page, err := s.readPage(rows, limit)
	if err != nil {
		return embedengine.Page{}, err
	}
	if err := rows.Err(); err != nil {
		return embedengine.Page{}, fmt.Errorf("scan %s: %w", s.spec.Source.Table, err)
	}
	return page, nil
}

// columns is what a read selects: the key, the input fields and the version,
// each as text.
func (s *Source) columns(alias string) []string {
	selected := make([]string, 0, len(s.spec.Source.KeyFields)+len(s.spec.Source.InputFields)+1)
	for _, field := range s.spec.Source.KeyFields {
		selected = append(selected, asText(alias, field))
	}
	for _, field := range s.spec.Source.InputFields {
		selected = append(selected, asText(alias, field))
	}
	for _, field := range versionColumns(s.spec) {
		selected = append(selected, asText(alias, field))
	}
	return selected
}

// scanQuery renders the keyset read and its arguments.
//
// The cursor is compared as a tuple with the key, in key order, which is the
// order a YDB table keeps its rows in; each cursor component is cast back to
// its column's type.
func (s *Source) scanQuery(key []keyColumn, after []string, limit int) (string, []any, error) {
	if limit <= 0 {
		return "", nil, fmt.Errorf("a scan limit of %d would read the whole table into memory", limit)
	}
	var conditions []string
	var arguments []any
	if filter := strings.TrimSpace(s.spec.Source.Filter); filter != "" {
		conditions = append(conditions, "("+filter+")")
	}
	if len(after) > 0 {
		if len(after) != len(key) {
			return "", nil, fmt.Errorf("the cursor has %d components and the key has %d", len(after), len(key))
		}
		names := make([]string, len(key))
		casts := make([]string, len(key))
		for index, column := range key {
			arguments = append(arguments, after[index])
			names[index] = quote(column.name)
			casts[index] = column.cast(len(arguments))
		}
		conditions = append(conditions, fmt.Sprintf("(%s) > (%s)",
			strings.Join(names, ", "), strings.Join(casts, ", ")))
	}
	order := make([]string, len(key))
	for index, column := range key {
		order[index] = quote(column.name)
	}
	// #nosec G201 -- YQL takes no parameter for a path or a column. The names
	// come from the specification and go through quote; the filter is the
	// specification's own YQL, which is what the field is for; the cursor
	// values are parameters; the limit is an integer this function was given.
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(s.columns(""), ", "),
		quoteTable(s.spec.Source.Schema, s.spec.Source.Table))
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY " + strings.Join(order, ", ") + " LIMIT " + strconv.Itoa(limit)
	return query, arguments, nil
}

// readPage turns a result set into a page.
func (s *Source) readPage(rows *sql.Rows, limit int) (embedengine.Page, error) {
	keyCount := len(s.spec.Source.KeyFields)
	inputCount := len(s.spec.Source.InputFields)
	width := keyCount + inputCount + len(versionColumns(s.spec))
	var page embedengine.Page
	for rows.Next() {
		values := make([]sql.NullString, width)
		destinations := make([]any, width)
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
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

// buildRow turns one result into a source row. A NULL key is refused: it would
// make two rows share a cursor.
func (s *Source) buildRow(values []sql.NullString, keyCount, inputCount int) (embedgen.Row, string, error) {
	row := embedgen.Row{Key: make([]string, keyCount), Fields: make([]*string, inputCount)}
	for index := range keyCount {
		if !values[index].Valid {
			return embedgen.Row{}, "", fmt.Errorf(
				"key column %s is NULL in %s, and a keyset scan cannot resume after a row it cannot name",
				s.spec.Source.KeyFields[index], s.spec.Source.Table)
		}
		row.Key[index] = values[index].String
	}
	for index := range inputCount {
		if value := values[keyCount+index]; value.Valid {
			row.Fields[index] = &value.String
		}
	}
	if len(versionColumns(s.spec)) == 0 {
		return row, "", nil
	}
	return row, values[keyCount+inputCount].String, nil
}

// Current returns the rows still present out of a set of keys. A key absent
// from the answer is a row that is gone. The specification's filter applies,
// so a row that stopped qualifying reads as gone too.
func (s *Source) Current(ctx context.Context, keys [][]string) ([]embedgen.Row, []string, error) {
	if len(keys) == 0 {
		return nil, nil, nil
	}
	key, err := s.sourceKey(ctx)
	if err != nil {
		return nil, nil, err
	}
	var arguments []any
	tuples := make([]string, 0, len(keys))
	for _, values := range keys {
		if len(values) != len(key) {
			return nil, nil, fmt.Errorf("a key has %d components and the key has %d", len(values), len(key))
		}
		casts := make([]string, len(key))
		for index, column := range key {
			arguments = append(arguments, values[index])
			casts[index] = column.cast(len(arguments))
		}
		tuples = append(tuples, "("+strings.Join(casts, ", ")+")")
	}
	names := make([]string, len(key))
	for index, column := range key {
		names[index] = quote(column.name)
	}
	conditions := []string{fmt.Sprintf("(%s) IN (%s)", strings.Join(names, ", "), strings.Join(tuples, ", "))}
	if filter := strings.TrimSpace(s.spec.Source.Filter); filter != "" {
		conditions = append(conditions, "("+filter+")")
	}
	// #nosec G201 -- as scanQuery.
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s",
		strings.Join(s.columns(""), ", "), quoteTable(s.spec.Source.Schema, s.spec.Source.Table),
		strings.Join(conditions, " AND "), strings.Join(names, ", "))
	rows, err := s.backend.db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, nil, fmt.Errorf("reread %s: %w", s.spec.Source.Table, err)
	}
	defer func() { _ = rows.Close() }()
	page, err := s.readPage(rows, len(keys)+1)
	if err != nil {
		return nil, nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("reread %s: %w", s.spec.Source.Table, err)
	}
	return page.Rows, page.Versions, nil
}

// versionColumns names the column the source version is read from, and is
// empty under a strategy that establishes none.
func versionColumns(spec embedgen.Spec) []string {
	if field := strings.TrimSpace(spec.Source.VersionField); field != "" {
		return []string{field}
	}
	return nil
}
