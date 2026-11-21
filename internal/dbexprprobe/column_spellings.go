package dbexprprobe

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/config"
	"ptah.run/internal/probetext"
	"ptah.run/internal/probetx"
)

// ColumnSpellingProbe is one declared table whose column types and defaults
// need the target server's own spelling before they can be compared.
type ColumnSpellingProbe struct {
	// Table is the probe table the statements create, as a regclass the server
	// resolves: a pg_temp name, so the table is temporary.
	Table string
	// Schema and Declared name the declared table the probe stands for. They
	// are never sent to the server; a refusal carries them.
	Schema, Declared string
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

// SupportsColumnSpellings reports whether this target can answer column probes.
// Rendering and execution share this predicate: an unsupported probe must not
// require a renderer for statements that its resolver will never execute.
func SupportsColumnSpellings(dialect string) bool {
	return isPostgresFamily(dialect)
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
// returned with Resolved false, and the server's answer is returned among the
// refusals, so the caller can say why the column is compared as written.
//
// A server that offers no temporary table at all, as CockroachDB by default,
// answers every probe with the same refusal. That answer is about the server,
// not about a column, so its columns are returned with Resolved false and none
// of them is a refusal: on such a server every default is compared by its
// value, as on a server that spells none. Other dialects, and a connection
// pinned to a session with a transaction open, return nil, for the reasons
// [ResolveCheckExpressions] gives.
func ResolveColumnSpellings(
	ctx context.Context,
	conn Session,
	probes []ColumnSpellingProbe,
) (map[string]config.ColumnSpelling, []ColumnSpellingRefusal, error) {
	if conn == nil {
		return nil, nil, fmt.Errorf("resolve column spellings: database connection is nil")
	}
	if len(probes) == 0 || !SupportsColumnSpellings(conn.Info().Dialect) {
		return nil, nil, nil
	}
	dialect := conn.Info().Dialect
	resolved := make(map[string]config.ColumnSpelling)
	var refusals []ColumnSpellingRefusal
	ran, err := conn.WithRolledBackTransaction(ctx, "resolve column spellings", func(ctx context.Context, tx *sql.Tx) error {
		for _, probe := range probes {
			refused, err := resolveColumnSpellingProbe(ctx, tx, dialect, probe, resolved)
			if err != nil {
				return err
			}
			if err := probetx.Check(ctx); err != nil {
				return fmt.Errorf("resolve column spellings: table %s: %w", qualifiedName(probe.Schema, probe.Declared), err)
			}
			refusals = append(refusals, refused...)
		}
		return nil
	})
	if err != nil || !ran {
		return nil, nil, err
	}
	return resolved, refusals, nil
}

// ColumnSpellingRefusal is a column whose probe the server refused.
type ColumnSpellingRefusal struct {
	// Key is the column's key.
	Key string
	// Schema, Table and Column name the declared column.
	Schema, Table, Column string
	// Reason is the server's answer to the statement it refused.
	Reason error
}

// resolveColumnSpellingProbe answers every column of one probe, from the
// table's statement when the server takes it and column by column when it
// does not, and returns the columns the server refused.
func resolveColumnSpellingProbe(
	ctx context.Context,
	tx *sql.Tx,
	dialect string,
	probe ColumnSpellingProbe,
	resolved map[string]config.ColumnSpelling,
) ([]ColumnSpellingRefusal, error) {
	keys := make(map[string]string, len(probe.Columns))
	for _, column := range probe.Columns {
		keys[column.Name] = column.Key
	}
	// A table statement the check refuses is not run, and each column's own
	// statement is checked and tried instead, so one column whose text could
	// reach past its place leaves only itself unresolved.
	if probetext.Statement(dialect, probe.Statement) == nil {
		refused, err := readColumnSpellings(ctx, tx, probe.Table, probe.Statement, keys, resolved)
		if err != nil || refused == nil {
			return nil, err
		}
		if temporaryTablesUnavailable(refused) {
			for _, column := range probe.Columns {
				resolved[column.Key] = config.ColumnSpelling{}
			}
			return nil, nil
		}
	}
	var refusals []ColumnSpellingRefusal
	for _, column := range probe.Columns {
		if refusal := probetext.Statement(dialect, column.Statement); refusal != nil {
			resolved[column.Key] = config.ColumnSpelling{}
			probetext.Warn(ctx, column.Key, "column "+qualifiedName(probe.Schema, probe.Declared, column.Name), refusal)
			continue
		}
		refused, err := readColumnSpellings(
			ctx, tx, column.Table, column.Statement, map[string]string{column.Name: column.Key}, resolved)
		if err != nil {
			return nil, err
		}
		if refused != nil {
			resolved[column.Key] = config.ColumnSpelling{}
			refusals = append(refusals, ColumnSpellingRefusal{
				Key: column.Key, Schema: probe.Schema, Table: probe.Declared, Column: column.Name, Reason: refused,
			})
		}
	}
	return refusals, nil
}

// readColumnSpellings creates one probe table and reads its columns back into
// resolved, under keys. refused is the server's answer when it refused the
// statement, and nil when the probe ran; err is a failure of the transaction.
func readColumnSpellings(
	ctx context.Context,
	tx *sql.Tx,
	table, statement string,
	keys map[string]string,
	resolved map[string]config.ColumnSpelling,
) (refused, err error) {
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
	return runRefusableProbe(ctx, tx, "resolve column spellings", table, "ptah_column_probe",
		postgresSavepoints, []string{statement}, read)
}

// experimentalFeature is the SQLSTATE CockroachDB answers with for a feature
// a cluster setting has not turned on, such as a temporary table:
// `temporary tables are only supported experimentally`.
const experimentalFeature = "XCEXF"

// temporaryTablesUnavailable reports that refused is the server saying it
// offers no temporary table, rather than refusing one declaration or one role.
func temporaryTablesUnavailable(refused error) bool {
	var state interface{ SQLState() string }
	return errors.As(refused, &state) && state.SQLState() == experimentalFeature
}

// qualifiedName joins the parts of a name that are not empty, for a message.
func qualifiedName(parts ...string) string {
	return strings.Join(slices.DeleteFunc(parts, func(part string) bool { return part == "" }), ".")
}
