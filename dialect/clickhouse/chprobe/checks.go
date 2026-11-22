package chprobe

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/internal/rundiag"
)

// checkQuery formats a CHECK condition as the server formats the one it
// stores, or answers NULL for text the server cannot parse. The renderer
// writes `CHECK (<condition>)`, and the probe puts the same text in
// parentheses, because 26.10 keeps parentheses the whole condition is written
// in and 24.10 drops them. Measured on both, `formatQuerySingleLineOrNull`
// answers NULL for `v >` and formats everything formatQuerySingleLine does.
const checkQuery = `SELECT formatQuerySingleLineOrNull(concat('SELECT (', ?, ')'))`

// formatterQuery asks whether the server has the function checkQuery calls.
// A release without it answers checkQuery with an unknown function, which
// would fail the comparison rather than leave it to the text.
const formatterQuery = `SELECT count() FROM system.functions WHERE name = 'formatQuerySingleLineOrNull'`

// CheckService decides whether a declared CHECK and the CHECK a table holds
// are one constraint, by asking the server to format both conditions and
// comparing what it prints ([schemacomparison.Checks]). Its zero value is
// ready for concurrent use.
//
// ClickHouse stores a CHECK reformatted, and each release line in its own
// way. Measured on 24.10.4.191 and 26.10.3.7: `CAST(v AS Int64) > 0` and
// `v::Int64 > 0` read back as `CAST(v, 'Int64') > 0` on both, `equals(v, v)`
// as `v = v` on 24.10 and as written on 26.10, and 26.10 keeps the
// parentheses of `CHECK (v > 0)` where 24.10 drops them. Each line formats
// its own spelling of a condition and the condition as written to the same
// text, so no table of rewrites has to follow the releases. The formatter
// keeps the case of a name, and `V > 0` is another CHECK from `v > 0` on a
// table with both columns.
type CheckService struct{}

// EquivalentChecks answers, for each pair of request, whether the server
// formats the declared condition and the held one to the same text.
//
// A request with no Session, and a session that cannot run the queries,
// decide nothing: the answer is nil, and the comparison compares the
// conditions as text. A server without formatQuerySingleLineOrNull decides
// nothing either, and the run it serves warns that each held CHECK of the
// request is compared as written. A declared condition the server cannot
// parse is unlike the held one, so the plan adds it and the server refuses the
// statement with its own reason. Any other failure of a query fails the
// comparison.
func (CheckService) EquivalentChecks(ctx context.Context, request schemacomparison.ChecksRequest) ([]bool, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: CHECK comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Target != platform.ClickHouse {
		return nil, fmt.Errorf("%w: ClickHouse CHECK comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if request.Session == nil || len(request.Pairs) == 0 {
		return nil, nil
	}
	answers := make([]bool, len(request.Pairs))
	formats := false
	ran, err := request.Session.WithRolledBackTransaction(ctx, "compare CHECK constraints", func(ctx context.Context, tx *sql.Tx) error {
		var found uint64
		if err := tx.QueryRowContext(ctx, formatterQuery).Scan(&found); err != nil {
			return fmt.Errorf("look up formatQuerySingleLineOrNull: %w", err)
		}
		if formats = found > 0; !formats {
			return nil
		}
		formatter := conditionFormatter{tx: tx, formatted: make(map[string]formattedCondition)}
		for i, pair := range request.Pairs {
			if pair.Held.CheckClause == nil {
				return fmt.Errorf("%w: CHECK %s on %s has no condition to compare", schemaext.ErrInvalidValue, pair.Held.Name, pair.Held.TableName)
			}
			declared, err := formatter.format(ctx, pair.Declared)
			if err != nil {
				return err
			}
			held, err := formatter.format(ctx, *pair.Held.CheckClause)
			if err != nil {
				return err
			}
			if !held.parsed {
				return fmt.Errorf("ClickHouse cannot format CHECK %s on %s, which it holds as %s", pair.Held.Name, pair.Held.TableName, *pair.Held.CheckClause)
			}
			answers[i] = declared.parsed && declared.text == held.text
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !ran {
		return nil, nil
	}
	if !formats {
		warnUnformatted(ctx, request.Pairs)
		return nil, nil
	}
	return answers, ctx.Err()
}

// warnUnformatted raises, for the run ctx carries, that each held CHECK of
// pairs is compared with its declaration as written.
func warnUnformatted(ctx context.Context, pairs []schemacomparison.CheckPair) {
	stream := rundiag.Stream(ctx)
	for _, pair := range pairs {
		check := pair.Held.TableName + "." + pair.Held.Name
		_ = rundiag.Warn(ctx, stream, rundiag.UnformattedChecks, rundiag.Record{
			Key: check,
			Line: fmt.Sprintf("CHECK %s on %s is compared as written, not in the form the server stores: "+
				"the server has no formatQuerySingleLineOrNull function", pair.Held.Name, pair.Held.TableName),
			Attrs: []slog.Attr{slog.String("check", check)},
		})
	}
}

// formattedCondition is what the server printed for one condition, and
// whether it could parse it.
type formattedCondition struct {
	text   string
	parsed bool
}

// conditionFormatter formats conditions in one transaction, each distinct
// text once.
type conditionFormatter struct {
	tx        *sql.Tx
	formatted map[string]formattedCondition
}

// format returns the server's formatting of condition. A condition the
// server cannot parse is returned unparsed; a failed query is an error.
func (f conditionFormatter) format(ctx context.Context, condition string) (formattedCondition, error) {
	condition = strings.TrimSpace(condition)
	if known, found := f.formatted[condition]; found {
		return known, nil
	}
	var printed sql.NullString
	if err := f.tx.QueryRowContext(ctx, checkQuery, condition).Scan(&printed); err != nil {
		return formattedCondition{}, fmt.Errorf("format CHECK condition %s: %w", condition, err)
	}
	result := formattedCondition{text: strings.TrimPrefix(printed.String, "SELECT "), parsed: printed.Valid}
	f.formatted[condition] = result
	return result, nil
}
