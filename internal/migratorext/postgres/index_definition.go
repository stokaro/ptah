package postgres

import (
	"context"
	"errors"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/lexer"
	"ptah.run/internal/migratorext"
)

const (
	definitionProbeTable = "pg_temp.ptah_index_probe"
	definitionProbeIndex = "pg_temp.ptah_index_probe_idx"

	insufficientPrivilege = "42501"
)

// SameDefinition builds the index statement declares on an empty LIKE copy of
// the target and compares the two pg_get_indexdef renderings from USING on,
// where the index and table names no longer appear. PostgreSQL renders both,
// so spellings it treats as equal compare equal. The copy has no rows, so the
// build costs nothing, and it is dropped before returning.
func (indexGuard) SameDefinition(
	ctx context.Context,
	q migratorext.RowsQueryer,
	statement string,
	state migratorext.IndexState,
) (same bool, err error) {
	unique, tail, ok := declaredIndex(statement)
	if !ok {
		return false, fmt.Errorf("cannot read the definition the migration declares for index %s", quotedName(state))
	}
	if err := execDefinitionProbe(ctx, q, "CREATE TEMP TABLE ptah_index_probe (LIKE "+quotedTarget(state)+")"); err != nil {
		var stateErr interface{ SQLState() string }
		if errors.As(err, &stateErr) && stateErr.SQLState() == insufficientPrivilege {
			return false, fmt.Errorf("cannot compare the definition of index %s with the one the migration declares: "+
				"the check builds the declared index on a temporary table, so the role needs the TEMPORARY "+
				"privilege on the database: %w", quotedName(state), err)
		}
		return false, fmt.Errorf("failed to compare the definition of index %s: %w", quotedName(state), err)
	}
	// The drop does not inherit the cancelation: outside a transaction a probe
	// table left behind stays in the session, and the next probe fails on it.
	defer func() {
		_ = execDefinitionProbe(context.WithoutCancel(ctx), q, "DROP TABLE IF EXISTS "+definitionProbeTable)
	}()
	create := "CREATE INDEX ptah_index_probe_idx ON " + definitionProbeTable + tail
	if unique {
		create = "CREATE UNIQUE INDEX ptah_index_probe_idx ON " + definitionProbeTable + tail
	}
	if err := execDefinitionProbe(ctx, q, create); err != nil {
		return false, fmt.Errorf("failed to compare the definition of index %s: %w", quotedName(state), err)
	}
	rows, err := q.QueryContext(ctx, sqlutil.Rebind(platform.Postgres, `
		SELECT e.indisunique = p.indisunique
		   AND substr(pg_get_indexdef(e.indexrelid), strpos(pg_get_indexdef(e.indexrelid), ' USING '))
		     = substr(pg_get_indexdef(p.indexrelid), strpos(pg_get_indexdef(p.indexrelid), ' USING '))
		FROM pg_index e, pg_index p
		WHERE e.indexrelid = ?::regclass AND p.indexrelid = ?::regclass`),
		quotedName(state), definitionProbeIndex)
	if err != nil {
		return false, fmt.Errorf("failed to compare the definition of index %s: %w", quotedName(state), err)
	}
	defer rows.Close()
	if rows.Next() {
		err = rows.Scan(&same)
	}
	if err == nil {
		err = rows.Err()
	}
	return same, err
}

func execDefinitionProbe(ctx context.Context, q migratorext.RowsQueryer, statement string) error {
	rows, err := q.QueryContext(ctx, statement)
	if err != nil {
		return err
	}
	closeErr := rows.Close()
	return errors.Join(rows.Err(), closeErr)
}

// declaredIndex reads a CREATE [UNIQUE] INDEX statement: whether it is unique,
// and the source text after its target table -- the method, keys, INCLUDE,
// WITH and WHERE clauses as written. The probe executes that text, so a tail
// with a semicolon before its end, which would end the probe's statement and
// run whatever follows, is refused.
func declaredIndex(statement string) (unique bool, tail string, ok bool) {
	tokens := significantTokens(statement)
	if len(tokens) < 2 || !tokens[0].MatchIdentifierValue("CREATE") {
		return false, "", false
	}
	unique = tokens[1].MatchIdentifierValue("UNIQUE")
	prefix, ok := consumeCreateIndexPrefix(tokens[1:])
	if !ok || len(prefix.tokens) < 3 || !prefix.tokens[1].MatchIdentifierValue("ON") {
		return false, "", false
	}
	rest := skipKeywordToken(prefix.tokens[2:], "ONLY")
	if len(rest) == 0 {
		return false, "", false
	}
	tableEnd := rest[0].End
	rest = rest[1:]
	if len(rest) >= 2 && rest[0].MatchOperatorValue(".") {
		tableEnd = rest[1].End
		rest = rest[2:]
	}
	last := len(rest) - 1
	for last >= 0 && rest[last].Type == lexer.TokenSemicolon {
		last--
	}
	if last < 0 {
		return false, "", false
	}
	for _, token := range rest[:last] {
		if token.Type == lexer.TokenSemicolon {
			return false, "", false
		}
	}
	return unique, statement[tableEnd:rest[last].End], true
}
