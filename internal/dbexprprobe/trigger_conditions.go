package dbexprprobe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ptah.run/config"
	"ptah.run/dbschema"
	"ptah.run/internal/triggerdef"
)

// TriggerConditionProbe is one declared trigger whose WHEN condition needs the
// target server's own spelling before it can be compared.
type TriggerConditionProbe struct {
	// Key identifies the trigger to the caller and is never sent to the server.
	Key string
	// Table is the bare name of the table the trigger is on, as the server
	// stores it. The probe table takes that name; see [newProbeRelation].
	Table string
	// Columns are the columns of the table the trigger is on, from the LIVE
	// read: the condition has to parse against the table it will guard.
	Columns []CheckProbeColumn
	// Timing, Event and ForEach are the declared clauses. The condition is
	// resolved against them because they decide what it may read: OLD does not
	// exist for an INSERT, and a statement-level trigger has neither row.
	Timing  string
	Event   string
	ForEach string
	// When is the declared condition, without its parentheses.
	When string
}

// ResolveTriggerConditions asks the connected server to print each declared
// trigger's WHEN condition.
//
// The probe is a temporary table with the declared trigger on it, executing a
// temporary function, inside a transaction that is rolled back; the condition
// is read out of pg_get_triggerdef the same way the schema reader reads the
// real one, through [triggerdef.When]. A connection pinned to a session with a
// transaction open returns nil, for the reason the package documentation
// gives.
func ResolveTriggerConditions(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	probes []TriggerConditionProbe,
) (map[string]config.TriggerCondition, error) {
	if conn == nil {
		return nil, fmt.Errorf("resolve trigger conditions: database connection is nil")
	}
	if len(probes) == 0 {
		return nil, nil
	}
	if !isPostgresFamily(conn.Info().Dialect) {
		return nil, nil
	}
	return resolveProbes(ctx, conn, "resolve trigger conditions", probes,
		func(probe TriggerConditionProbe) string { return probe.Key },
		resolveOneTriggerCondition)
}

func resolveOneTriggerCondition(
	ctx context.Context,
	tx *sql.Tx,
	index int,
	probe TriggerConditionProbe,
) (config.TriggerCondition, error) {
	when := strings.TrimSpace(probe.When)
	if when == "" || len(probe.Columns) == 0 {
		return config.TriggerCondition{}, nil
	}
	forEach := strings.TrimSpace(probe.ForEach)
	if forEach == "" {
		forEach = "ROW"
	}

	relation := newProbeRelation(probe.Table, "ptah_trigger_probe", index)
	function := fmt.Sprintf("pg_temp.ptah_trigger_probe_fn_%d", index)
	statements := relation.statements(
		fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s)", relation.name, checkProbeColumnList(probe.Columns)),
		fmt.Sprintf("CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $ptah$BEGIN RETURN NULL; END$ptah$", function),
		fmt.Sprintf("CREATE TRIGGER ptah_trigger_probe %s %s ON %s FOR EACH %s WHEN (%s) EXECUTE FUNCTION %s()",
			probe.Timing, probe.Event, relation.name, forEach, when, function),
	)

	const query = `
		SELECT pg_get_triggerdef(t.oid)
		FROM pg_trigger t
		WHERE t.tgrelid = $1::regclass AND t.tgname = 'ptah_trigger_probe'`

	var definition string
	ok, err := runProbe(ctx, tx, "resolve trigger conditions", probe.Key, "ptah_trigger_probe", postgresSavepoints,
		statements, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, query, relation.regclass).Scan(&definition)
		})
	if err != nil || !ok {
		return config.TriggerCondition{}, err
	}
	condition := triggerdef.When(definition)
	if condition == "" {
		return config.TriggerCondition{}, nil
	}
	return config.TriggerCondition{Condition: condition, Resolved: true}, nil
}
