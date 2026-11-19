package dbexprprobe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ptah.run/config"
)

// GeneratedColumnProbe is one declared generated column whose expression needs
// the target server's own spelling before it can be compared with what the
// catalog holds.
type GeneratedColumnProbe struct {
	// Key identifies the column to the caller. It is returned unchanged as the
	// map key and is never sent to the server.
	Key string
	// Table is the bare name of the table the column is on, as the server
	// stores it. The probe table takes that name, so an expression naming its
	// own table resolves; see [newProbeRelation]. Empty keeps a numbered name.
	Table string
	// Schema is the schema that holds Table, as the server stores it. The
	// probe takes it out where the expression writes it in front of Table and
	// a column; see [ownSchemaStripped]. Empty strips nothing.
	Schema string
	// Columns are the other columns of the table, taken from the LIVE read:
	// the expression is parsed against their types, and the stored form
	// depends on them. Over a varchar column, `coalesce(title, 'x')` is stored
	// as `(COALESCE(title, 'x'::character varying))::text`, and over a text
	// column as `COALESCE(title, 'x'::text)`.
	Columns []CheckProbeColumn
	// Column is the generated column's name, quoted by the resolver.
	Column string
	// Type is the generated column's type as the catalog spells it.
	Type string
	// Expression is the declared expression, without GENERATED ALWAYS AS and
	// without its enclosing parentheses.
	Expression string
}

// ResolveGeneratedColumns asks the connected server how it stores each
// declared generated column's expression, so a comparison holds the same
// spelling on both sides.
//
// PostgreSQL stores a generated column's expression parsed, as it stores a
// CHECK, and prints it back from the parse tree: literals gain casts, function
// names are upper-cased where the grammar spells them so, and a value whose
// type the call does not take gains an explicit cast. Measured on PostgreSQL
// 16 and 18, the declaration `to_tsvector('simple', coalesce(title, 'x'))`
// over a `varchar(100)` column is stored as
// `to_tsvector('simple'::regconfig, (COALESCE(title, 'x'::character varying))::text)`.
// No textual rule can place that cast without knowing the column's type, and
// a comparison of the two texts planned `SET EXPRESSION` for a column nobody
// changed, which rewrites the whole table.
//
// The declaration is put through the same parse the catalog form went through:
// a TEMPORARY table carrying the live table's columns and the declared
// generated column is created, its stored expression is read back with the
// function the reader uses, and the transaction is rolled back. The target
// table is never touched.
//
// A probe the server refuses -- an expression that does not parse, a type the
// server does not have, a role that may not create a temporary table -- is
// returned with Resolved false rather than omitted, for the reason
// [ResolveCheckExpressions] gives, and the server's answer is returned among
// the refusals, so the caller can say why the column is compared as written.
//
// Only a server whose descriptor names the pg-catalog probes is asked; every
// other dialect returns nil. Oracle's generated columns are resolved by
// [ResolveGeneratedExpressions], since Oracle commits the probe's DDL itself.
// A connection pinned to a session with a transaction open also returns nil,
// for the reason the package documentation gives.
func ResolveGeneratedColumns(
	ctx context.Context,
	conn Session,
	probes []GeneratedColumnProbe,
) (map[string]config.GeneratedExpression, []GeneratedColumnRefusal, error) {
	if conn == nil {
		return nil, nil, fmt.Errorf("resolve generated expressions: database connection is nil")
	}
	if len(probes) == 0 || !isPostgresFamily(conn.Info().Dialect) {
		return nil, nil, nil
	}
	answers, err := resolveProbes(ctx, conn, "resolve generated expressions", probes,
		func(probe GeneratedColumnProbe) string { return probe.Key },
		resolveOnePostgresGeneratedColumn)
	if err != nil || answers == nil {
		return nil, nil, err
	}
	resolved := make(map[string]config.GeneratedExpression, len(answers))
	var refusals []GeneratedColumnRefusal
	for _, probe := range probes {
		answer, asked := answers[probe.Key]
		if !asked {
			continue
		}
		resolved[probe.Key] = answer.expression
		if answer.refusal != nil {
			refusals = append(refusals, GeneratedColumnRefusal{
				Key: probe.Key, Schema: probe.Schema, Table: probe.Table, Column: probe.Column, Reason: answer.refusal,
			})
		}
	}
	return resolved, refusals, nil
}

// SpellsGeneratedColumns reports that [ResolveGeneratedColumns] asks a server
// of dialect, so a caller can tell whether a connection is worth opening.
func SpellsGeneratedColumns(dialect string) bool {
	return isPostgresFamily(dialect)
}

// GeneratedColumnRefusal is a generated column whose probe the server refused.
type GeneratedColumnRefusal struct {
	// Key is the probe's key.
	Key string
	// Schema, Table and Column name the column as the probe did.
	Schema, Table, Column string
	// Reason is the server's answer to the statement it refused.
	Reason error
}

// generatedColumnAnswer is what one probe answered: the stored expression,
// and the server's refusal where there is none.
type generatedColumnAnswer struct {
	expression config.GeneratedExpression
	refusal    error
}

// resolveOnePostgresGeneratedColumn creates one probe table and reads the
// stored expression of its generated column back.
//
// The probe column is STORED whatever the declaration says. The stored form of
// an expression does not depend on the kind, and PostgreSQL before 18 has no
// VIRTUAL generated column to probe with.
func resolveOnePostgresGeneratedColumn(
	ctx context.Context,
	tx *sql.Tx,
	index int,
	probe GeneratedColumnProbe,
) (generatedColumnAnswer, error) {
	expression := strings.TrimSpace(probe.Expression)
	if expression == "" || strings.TrimSpace(probe.Column) == "" || strings.TrimSpace(probe.Type) == "" {
		return generatedColumnAnswer{}, nil
	}

	columns := make([]string, 0, len(probe.Columns)+1)
	if len(probe.Columns) > 0 {
		columns = append(columns, checkProbeColumnList(probe.Columns))
	}
	columns = append(columns, fmt.Sprintf("%s %s GENERATED ALWAYS AS (%s) STORED",
		quoteCheckProbeIdentifier(probe.Column), probe.Type,
		ownSchemaStripped(expression, probe.Schema, probe.Table)))
	relation := newProbeRelation(probe.Table, "ptah_generated_probe", index)
	statements := relation.statements(fmt.Sprintf(
		"CREATE TEMPORARY TABLE %s (%s)", relation.name, strings.Join(columns, ", ")))

	// pg_get_expr over pg_attrdef is what the reader asks of a live generated
	// column, so both sides of the comparison are printed by the same function.
	const query = `
		SELECT COALESCE(pg_get_expr(d.adbin, d.adrelid), '')
		FROM pg_attrdef d
		JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
		WHERE d.adrelid = $1::regclass AND a.attname = $2`

	var stored string
	refusal, err := runRefusableProbe(ctx, tx, "resolve generated expressions", probe.Key, "ptah_generated_probe",
		postgresSavepoints, statements, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, query, relation.regclass, probe.Column).Scan(&stored)
		})
	if err != nil || refusal != nil {
		return generatedColumnAnswer{refusal: refusal}, err
	}
	return generatedColumnAnswer{expression: config.GeneratedExpression{Expression: strings.TrimSpace(stored), Resolved: true}}, nil
}
