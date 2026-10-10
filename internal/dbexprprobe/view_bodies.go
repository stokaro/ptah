package dbexprprobe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
)

// ViewBodyProbe is one declared view or materialized view whose SELECT needs
// the target server's own spelling before it can be compared.
type ViewBodyProbe struct {
	// Key identifies the body to the caller. It is returned unchanged as the
	// map key and is never sent to the server.
	Key string
	// Body is the declared SELECT, without the CREATE prefix.
	Body string
}

// ResolveViewBodies asks the connected server to spell each declared view body
// the way pg_get_viewdef prints it, which is how the catalog reads a view.
//
// PostgreSQL stores a view's parse tree and prints it back, and the print
// differs from the declaration in ways no fold over the text can produce: a
// function in FROM gains its whole result as a column alias list, `FROM
// all_items()` reads back as `FROM all_items() all_items(id, title, tags)`,
// naming columns the body never mentions (stokaro/ptah#4057).
//
// The declaration is put through the same print: a view with the declared body
// is created inside the rolled-back transaction, its definition is read back,
// and the view is dropped. A materialized view's body is spelled through a
// plain view, which prints the same: measured on PostgreSQL 18.6, CockroachDB
// v26.3.2 and YugabyteDB 2026.1.2, the probe view's definition equals the
// real materialized view's.
//
// Where the probe view lives depends on the server. On PostgreSQL and
// YugabyteDB it is a temporary view, so the probe needs no privilege on any
// schema, with pg_temp last on the search path so a leftover temporary
// relation cannot stand in for a name the body reads. CockroachDB refuses
// temporary views by default, `temporary tables are only supported
// experimentally`, so there the probe view goes in the session's current
// schema, inside the same transaction. The explicit drop is for YugabyteDB:
// there a view created in pg_temp outlives the rollback to the savepoint, the
// transaction's rollback and the session, so without it every comparison
// would leave one view behind.
//
// A body that selects `*`, see [SelectsStar], is returned unresolved without
// asking. The server expands the star against the columns the database holds
// now, while the declaration asks for the columns the desired tables declare,
// and an answer from the current columns would hide a column the plan adds.
// A body the server refuses -- one that reads a column or a table the plan has
// not created yet, for one -- is returned unresolved too. Other dialects, and a
// connection pinned to a session with a transaction open, return nil, for the
// reasons [ResolveCheckExpressions] gives.
func ResolveViewBodies(
	ctx context.Context,
	conn Session,
	probes []ViewBodyProbe,
) (map[string]config.ViewBody, error) {
	if conn == nil {
		return nil, fmt.Errorf("resolve view bodies: database connection is nil")
	}
	if len(probes) == 0 || !isPostgresFamily(conn.Info().Dialect) {
		return nil, nil
	}
	place := temporaryProbeView
	if platform.NormalizeDialect(conn.Info().Dialect) == platform.CockroachDB {
		place = schemaProbeView
	}
	return resolveProbes(ctx, conn, "resolve view bodies", probes,
		func(probe ViewBodyProbe) string { return probe.Key },
		func(ctx context.Context, tx *sql.Tx, index int, probe ViewBodyProbe) (config.ViewBody, error) {
			return resolveOneViewBody(ctx, tx, index, probe, place)
		})
}

// probeViewPlacement names the probe view one server takes, and the statement
// that creates it with body.
type probeViewPlacement func(name, body string) (probeRelation, string)

// temporaryProbeView places the probe view in pg_temp, with pg_temp last on
// the search path for the reason [newProbeRelation] gives.
func temporaryProbeView(name, body string) (probeRelation, string) {
	view := probeRelation{name: name, regclass: "pg_temp." + name, setup: []string{searchPathWithTempLast}}
	return view, "CREATE TEMPORARY VIEW " + name + " AS\n" + body
}

// schemaProbeView places the probe view in the session's current schema, for a
// server that refuses temporary views.
func schemaProbeView(name, body string) (probeRelation, string) {
	return probeRelation{name: name, regclass: name}, "CREATE VIEW " + name + " AS\n" + body
}

// resolveOneViewBody creates one probe view, reads its definition back and
// drops it.
func resolveOneViewBody(
	ctx context.Context,
	tx *sql.Tx,
	index int,
	probe ViewBodyProbe,
	place probeViewPlacement,
) (config.ViewBody, error) {
	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(probe.Body), ";"))
	if body == "" || SelectsStar(body) {
		return config.ViewBody{}, nil
	}
	name := fmt.Sprintf("ptah_view_probe_%d", index)
	view, create := place(name, body)
	var answer config.ViewBody
	read := func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, "SELECT pg_get_viewdef($1::regclass, true)", view.regclass).Scan(&answer.Body); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "DROP VIEW "+view.regclass)
		return err
	}
	answered, err := runProbe(ctx, tx, "resolve view bodies", name, "ptah_view_probe",
		postgresSavepoints, view.statements(create), read)
	if err != nil || !answered {
		return config.ViewBody{}, err
	}
	answer.Body = strings.TrimSpace(answer.Body)
	answer.Resolved = true
	return answer, nil
}

// SelectsStar reports whether a view body selects `*` or `name.*` anywhere,
// in its own select list or in a subquery's.
//
// A star is told from a multiplication by the token before it: a star follows
// SELECT, DISTINCT, ALL, a comma or a dot, and a multiplication follows an
// operand. A star after a closing parenthesis counts as a star, which covers
// `SELECT DISTINCT ON (a) *` and also reads `(a + b) * 2` as one; that only
// leaves such a body to the text comparison. `count(*)` is not a star. Text
// inside quotes, dollar quotes and comments is never read.
func SelectsStar(body string) bool {
	lex := lexer.NewLexerWithOptions(body, lexer.Options{StandardStrings: true, PostgreSQLEscapeStrings: true})
	var previous lexer.Token
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return false
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		}
		if token.Type == lexer.TokenOperator && token.Value == "*" && startsSelectItem(previous) {
			return true
		}
		previous = token
	}
}

// startsSelectItem reports whether a `*` after token is a select item rather
// than an operand of a multiplication or the argument of count(*).
func startsSelectItem(token lexer.Token) bool {
	switch token.Type {
	case lexer.TokenOperator:
		return token.Value == "," || token.Value == "." || token.Value == ")"
	case lexer.TokenIdentifier:
		switch strings.ToLower(token.Value) {
		case "select", "distinct", "all":
			return true
		}
	}
	return false
}
