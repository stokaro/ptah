package dbschema

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/platform"
)

// GeneratedExpressionProbe is one declared table whose generated columns need
// the server's own spelling.
//
// The columns arrive already rendered, as the target's own DDL. Rendering them
// here would put the renderer under every caller of this package, and the
// declaration the caller holds is what has to be normalized -- a probe built
// from anything else would answer for a table nobody declared.
type GeneratedExpressionProbe struct {
	// Schema and Table qualify the declared table, and are what
	// [config.CompareOptions.GeneratedExpressions] is keyed by. Schema may be
	// empty.
	Schema string
	Table  string
	// ProbeTable is the throwaway table Create makes, and the one this package
	// reads back and drops. The caller names it because the caller renders the
	// statement.
	ProbeTable string
	// Create is the rendered CREATE TABLE for ProbeTable, carrying the declared
	// table's WHOLE column list. The whole list, because a generated expression
	// references its siblings: `"size" * 2` needs a `size` column to reference,
	// and a probe table carrying only the generated column is refused.
	Create string
	// Generated names the columns whose expression is wanted back.
	Generated []string
}

// ResolveGeneratedExpressions asks a dev database how it spells each declared
// generated expression.
//
// It exists because Oracle does not store the text of a generated column's
// expression: it stores a rewrite, with every column reference quoted and
// upper-cased, the spaces around operators gone, and parentheses the
// declaration did not carry. Comparing a declaration against that is comparing
// two languages, so the declaration is put through the same server and the two
// stored forms are compared instead (stokaro/ptah#1915).
//
// The connection must be a DEV database, and the name of the parameter is the
// whole contract. Oracle commits its own DDL -- a CREATE TABLE inside an
// explicit transaction survives ROLLBACK, measured on 23.26 and 21.3 -- and a
// virtual column cannot live on a temporary table at all, ORA-54010 on both
// spellings. So the probe is a permanent table that has to be dropped
// afterwards, and running that against the schema a comparison is only supposed
// to read would create and drop objects in it, and leak one if the process died
// in between.
//
// A target that stores what it was given needs none of this and gets an empty
// map, which leaves every such comparison exactly as it was.
func ResolveGeneratedExpressions(
	ctx context.Context,
	dev *DatabaseConnection,
	probes []GeneratedExpressionProbe,
) (map[string]config.GeneratedExpression, error) {
	if dev == nil || len(probes) == 0 {
		return nil, nil
	}
	if platform.NormalizeDialect(dev.Info().Dialect) != platform.Oracle {
		return nil, nil
	}

	resolved := make(map[string]config.GeneratedExpression)
	for _, probe := range probes {
		if err := resolveOneGeneratedProbe(ctx, dev, probe, resolved); err != nil {
			return nil, err
		}
	}
	return resolved, nil
}

// GeneratedExpressionProbeTable names the throwaway table for the nth probe.
//
// It is here rather than in the caller so that the name the caller renders and
// the name this package drops cannot come apart. The index keeps two tables of
// one schema apart within a run, and the prefix keeps the probe's tables apart
// from anything a person made. A dev database is Ptah's to write in, which is
// what makes a permanent table acceptable here and nowhere else.
func GeneratedExpressionProbeTable(index int) string {
	return fmt.Sprintf("ptah_genexpr_probe_%d", index)
}

func resolveOneGeneratedProbe(
	ctx context.Context,
	dev *DatabaseConnection,
	probe GeneratedExpressionProbe,
	into map[string]config.GeneratedExpression,
) (resultErr error) {
	if probe.Create == "" || probe.ProbeTable == "" || len(probe.Generated) == 0 {
		return nil
	}
	table := probe.ProbeTable

	if _, err := dev.ExecContext(ctx, probe.Create); err != nil {
		// A declaration the server refuses has no stored form, and saying so is
		// the point: an unresolved entry leaves the expression uncompared
		// rather than reported, which is what a comparison that cannot judge
		// must do.
		for _, column := range probe.Generated {
			into[generatedExpressionKey(probe.Schema, probe.Table, column)] = config.GeneratedExpression{}
		}
		return nil
	}
	defer func() {
		// The drop is the point of the probe table's existence, not its error
		// path: nothing here is meant to outlive this call.
		if _, err := dev.ExecContext(context.WithoutCancel(ctx), "DROP TABLE "+table); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf(
				"resolve generated expressions: drop probe table %s: %w", table, err))
		}
	}()

	stored, err := readOracleVirtualColumnExpressions(ctx, dev, table)
	if err != nil {
		return err
	}
	for _, column := range probe.Generated {
		expression, found := stored[strings.ToUpper(strings.TrimSpace(column))]
		into[generatedExpressionKey(probe.Schema, probe.Table, column)] = config.GeneratedExpression{
			Expression: expression,
			Resolved:   found,
		}
	}
	return nil
}

// oracleVirtualColumnQuery reads back what the server stored for each virtual
// column of one table.
//
// DATA_DEFAULT is a LONG, so it is selected bare: wrapping it in anything --
// NVL included -- answers ORA-00932.
const oracleVirtualColumnQuery = `
	SELECT column_name, data_default
	FROM all_tab_cols
	WHERE owner = USER AND table_name = :1 AND virtual_column = 'YES'`

func readOracleVirtualColumnExpressions(
	ctx context.Context,
	dev *DatabaseConnection,
	table string,
) (map[string]string, error) {
	rows, err := dev.QueryContext(ctx, oracleVirtualColumnQuery, strings.ToUpper(table))
	if err != nil {
		return nil, fmt.Errorf("resolve generated expressions: read %s: %w", table, err)
	}
	defer rows.Close()

	stored := make(map[string]string)
	for rows.Next() {
		var name string
		var expression *string
		if err := rows.Scan(&name, &expression); err != nil {
			return nil, fmt.Errorf("resolve generated expressions: scan %s: %w", table, err)
		}
		value := ""
		if expression != nil {
			value = *expression
		}
		stored[strings.ToUpper(strings.TrimSpace(name))] = value
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("resolve generated expressions: read %s: %w", table, err)
	}
	return stored, nil
}

// generatedExpressionKey is the key
// [config.CompareOptions.GeneratedExpressions] is read by, and has to agree
// with the comparison's own spelling of it.
func generatedExpressionKey(schema, table, column string) string {
	name := table + "." + column
	if schema != "" {
		name = schema + "." + name
	}
	return strings.ToLower(name)
}
