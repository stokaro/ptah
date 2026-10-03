package dbexprprobe

import (
	"context"
	"database/sql"
	"fmt"

	"ptah.run/config"
	"ptah.run/dbschema"
	"ptah.run/internal/routineargs"
)

// RoutineArgumentsProbe is one declared routine whose argument list and result
// need the target server's own spelling before they can be compared.
type RoutineArgumentsProbe struct {
	// Key identifies the argument list to the caller. It is returned unchanged
	// as the map key and is never sent to the server.
	Key string
	// Name is the probe routine's name inside pg_temp, unquoted, as pg_proc
	// holds it.
	Name string
	// Statement creates the probe routine in pg_temp with the declared kind,
	// argument list and result, as the renderer writes a CREATE for them.
	Statement string
	// Drop removes the probe routine once it is read. On PostgreSQL the
	// rollback to the probe's savepoint undoes the CREATE anyway. On
	// YugabyteDB it does not: measured on 2026.1.2, a routine created in
	// pg_temp outlives the savepoint rollback, the transaction's rollback and
	// the session, and a result type that names a table keeps that table
	// from being dropped by anyone.
	Drop string
	// InCurrentSchema reports that Statement creates the probe routine in the
	// session's current schema rather than in pg_temp. CockroachDB refuses a
	// routine in pg_temp, so its probe goes where its view probe goes; see
	// [ResolveViewBodies]. The rollback to the probe's savepoint takes it back.
	InCurrentSchema bool
	// ReadsBody reports that Statement carries the declared language and body,
	// so the body the server stored is part of the answer. See
	// [config.RoutineArguments.Body].
	ReadsBody bool
}

// ResolveRoutineArguments asks the connected server to spell each declared
// argument list the way pg_get_function_arguments prints it, and each declared
// result the way pg_get_function_result prints it.
//
// PostgreSQL stores a routine's arguments, not the text that declared them: a
// default comes back with its cast, `=` as DEFAULT, a type without its
// modifier. Compared as text, a routine with a default argument differs from
// its own read-back, and every plan drops and creates it again
// (stokaro/ptah#3673). A result type loses its schema where the session's
// search path reaches it, so `SETOF public.items` reads back as `SETOF items`
// and differs from its own declaration the same way (stokaro/ptah#4038).
//
// The declaration is put through the same rewrite: a routine with the declared
// arguments and result is created in pg_temp, both are read back, and the
// transaction is rolled back. Its body is not the declared one and is never
// checked, because the signature does not depend on it and a body that names
// objects the plan has not created yet would refuse the probe for no reason.
// A declaration the server refuses -- a result type that does not exist yet,
// for one -- is returned with Resolved false. Other dialects, and a connection
// pinned to a session with a transaction open, return nil, for the reasons
// [ResolveCheckExpressions] gives.
//
// A probe that reads its body creates the routine with the declared language
// and body, for a server that rewrites a body when it stores it: measured on
// CockroachDB v26.3.2, `SELECT * FROM public.items` is stored as `SELECT
// public.items.id, public.items.title FROM f1.public.items;`, the database
// named and the star expanded. Only the server can produce that from the
// declaration, so the body it stores for the probe is the declaration's form,
// and a body naming something the server does not hold refuses the probe, which
// then compares as text (stokaro/ptah#4058).
func ResolveRoutineArguments(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	probes []RoutineArgumentsProbe,
) (map[string]config.RoutineArguments, error) {
	if conn == nil {
		return nil, fmt.Errorf("resolve routine arguments: database connection is nil")
	}
	if len(probes) == 0 || !isPostgresFamily(conn.Info().Dialect) {
		return nil, nil
	}
	return resolveProbes(ctx, conn, "resolve routine arguments", probes,
		func(probe RoutineArgumentsProbe) string { return probe.Key },
		resolveOneRoutineArguments)
}

// uncheckedBodies turns off body validation for the rest of the probe. The
// third argument makes the setting local to the transaction, and the savepoint
// the probe rolls back to undoes it with the routine.
const uncheckedBodies = `SELECT set_config('check_function_bodies', 'off', true)`

// resolveOneRoutineArguments creates one probe routine and reads its arguments
// and result back.
func resolveOneRoutineArguments(
	ctx context.Context,
	tx *sql.Tx,
	_ int,
	probe RoutineArgumentsProbe,
) (config.RoutineArguments, error) {
	var answer config.RoutineArguments
	read := func(ctx context.Context, tx *sql.Tx) error {
		var result, body string
		var returnsSet bool
		if err := tx.QueryRowContext(ctx, routineProbeQuery, probe.Name, probe.InCurrentSchema).
			Scan(&answer.Arguments, &result, &returnsSet, &body); err != nil {
			return err
		}
		// The catalog read restores the set the same way, so the two sides
		// agree on CockroachDB, which leaves SETOF out of the result.
		answer.Result = result
		if returnsSet {
			answer.Result = routineargs.AsSet(result)
		}
		if probe.ReadsBody {
			answer.Body = body
			answer.BodyResolved = true
		}
		_, err := tx.ExecContext(ctx, probe.Drop)
		return err
	}
	answered, err := runProbe(ctx, tx, "resolve routine arguments", probe.Name, "ptah_routine_probe",
		postgresSavepoints, []string{uncheckedBodies, probe.Statement}, read)
	if err != nil || !answered {
		return config.RoutineArguments{}, err
	}
	answer.Resolved = true
	return answer, nil
}

// routineProbeQuery reads a probe routine back from the schema it was created
// in: the session's current schema when $2 is true, pg_temp otherwise.
// COALESCE for the reason the catalog read gives: pg_get_function_result is
// NULL for a procedure.
const routineProbeQuery = `
		SELECT pg_get_function_arguments(p.oid), COALESCE(pg_get_function_result(p.oid), ''),
			p.proretset, p.prosrc
		FROM pg_proc p
		WHERE p.proname = $1
		AND p.pronamespace = CASE
			WHEN $2::bool THEN (SELECT n.oid FROM pg_namespace n WHERE n.nspname = current_schema())
			ELSE pg_my_temp_schema()
		END`
