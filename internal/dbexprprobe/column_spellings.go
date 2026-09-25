package dbexprprobe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ptah.run/config"
	"ptah.run/dbschema"
)

// ColumnSpellingProbe is one declared table whose column types and defaults
// need the target server's own spelling before they can be compared.
type ColumnSpellingProbe struct {
	// Table is the probe table the statements create, as a regclass the server
	// resolves: a pg_temp name, so the table is temporary.
	Table string
	// Statement creates Table with every column below, as the renderer writes
	// a CREATE TABLE for them.
	Statement string
	// Columns are the probe's columns. Each carries a statement of its own,
	// which is tried when the table's statement is refused, so one column the
	// server cannot create does not leave the others unresolved.
	Columns []ColumnSpellingColumn
}

// ColumnSpellingColumn is one column of a [ColumnSpellingProbe].
type ColumnSpellingColumn struct {
	// Key identifies the column to the caller. It is returned unchanged as the
	// map key and is never sent to the server.
	Key string
	// Name is the column's name as the statements spell it.
	Name string
	// Table and Statement create a probe table holding this column alone.
	Table     string
	Statement string
}

// ResolveColumnSpellings asks the connected server to spell each declared
// column's type and default the way its catalog reports them.
//
// PostgreSQL stores a type and a default, not the text that declared them, and
// prints both back: `varchar(10)[]` as `character varying(10)[]`, a timestamp
// literal with its time and zone filled in, a chain of casts with parentheses.
// Compared as text, a column the plan had just created planned a change of
// type and default on every run (stokaro/ptah#3617).
//
// The declaration is put through the same rewrite: a temporary table with the
// declared columns is created, format_type and pg_get_expr read its columns
// back, and the transaction is rolled back. A column the server refuses is
// returned with Resolved false. Other dialects, and a connection pinned to a
// session, return nil, for the reasons [ResolveCheckExpressions] gives.
func ResolveColumnSpellings(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	probes []ColumnSpellingProbe,
) (map[string]config.ColumnSpelling, error) {
	if conn == nil {
		return nil, fmt.Errorf("resolve column spellings: database connection is nil")
	}
	if len(probes) == 0 || !isPostgresFamily(conn.Info().Dialect) {
		return nil, nil
	}
	resolved := make(map[string]config.ColumnSpelling)
	ran, err := conn.WithRolledBackTransaction(ctx, "resolve column spellings", func(ctx context.Context, tx *sql.Tx) error {
		for _, probe := range probes {
			if err := resolveColumnSpellingProbe(ctx, tx, probe, resolved); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil || !ran {
		return nil, err
	}
	return resolved, nil
}

// resolveColumnSpellingProbe answers every column of one probe, from the
// table's statement when the server takes it and column by column when it
// does not.
func resolveColumnSpellingProbe(
	ctx context.Context,
	tx *sql.Tx,
	probe ColumnSpellingProbe,
	resolved map[string]config.ColumnSpelling,
) error {
	keys := make(map[string]string, len(probe.Columns))
	for _, column := range probe.Columns {
		keys[column.Name] = column.Key
	}
	answered, err := readColumnSpellings(ctx, tx, probe.Table, probe.Statement, keys, resolved)
	if err != nil || answered {
		return err
	}
	for _, column := range probe.Columns {
		answered, err := readColumnSpellings(
			ctx, tx, column.Table, column.Statement, map[string]string{column.Name: column.Key}, resolved)
		if err != nil {
			return err
		}
		if !answered {
			resolved[column.Key] = config.ColumnSpelling{}
		}
	}
	return nil
}

// readColumnSpellings creates one probe table and reads its columns back into
// resolved, under keys. It reports false, with no error, when the server
// refused the statement.
func readColumnSpellings(
	ctx context.Context,
	tx *sql.Tx,
	table, statement string,
	keys map[string]string,
	resolved map[string]config.ColumnSpelling,
) (bool, error) {
	const query = `
		SELECT a.attname, format_type(a.atttypid, a.atttypmod), COALESCE(pg_get_expr(d.adbin, d.adrelid), '')
		FROM pg_attribute a
		LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped`
	read := func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, query, table)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var name, columnType, columnDefault string
			if err := rows.Scan(&name, &columnType, &columnDefault); err != nil {
				return err
			}
			if key, ok := keys[name]; ok {
				resolved[key] = config.ColumnSpelling{
					Type: strings.TrimSpace(columnType), Default: strings.TrimSpace(columnDefault), Resolved: true,
				}
			}
		}
		return rows.Err()
	}
	return runProbe(ctx, tx, "resolve column spellings", table, "ptah_column_probe",
		postgresSavepoints, []string{statement}, read)
}
