package postgres

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// CockroachDB plans a drop without running it under EXPLAIN (DDL), and the
// plan of a RESTRICT drop of a type, a function or a procedure is refused with
// SQLSTATE 2BP01 and a list of every object that depends on it:
//
//	cannot drop type "e" because other objects ([db.public.t db.other.v]) still depend on it
//	cannot drop function "fn" because other objects ([db.public.iv, db.other.ov]) still depend on it
//
// Measured over 64 fixtures on v26.4.1, v26.3.6, v26.2.12 and v25.4.21, the
// list holds the objects the cleanup drops as well as the others, a type's
// list is separated by a space and a routine's by a comma and a space, and the
// lines before v26.4.1 call a procedure a function there. A table, a view or a
// sequence gets no such list: the refusal names one view, one foreign key or
// nothing, so it cannot tell a dependent the cleanup drops from one it does
// not, and those kinds are not asked.
//
// The plan takes no lock another session waits on: with the transaction held
// open after the rollback to the savepoint, crdb_internal.cluster_locks lists
// nothing, and DDL on the object and on its dependents runs as fast as beside
// a transaction that ran SELECT 1.

// serverDependentsSavepoint is the savepoint serverReportedDependents asks
// under inside a transaction, so the refusal of one EXPLAIN does not abort the
// transaction the cleanup runs in.
const serverDependentsSavepoint = "ptah_dependents"

// serverQuestion is the EXPLAIN (DDL) of the drop of one object the cleanup
// drops, and the object.
type serverQuestion struct {
	source  cleanupKey
	explain string
}

// serverQuestions are the questions to ask about objects: one for each type,
// function and procedure, in the drop the cleanup sends, which names a
// routine by its arguments and so one overload of it. They are ordered by
// object and then by statement, so the cleanup asks them in the same order on
// every run.
func serverQuestions(objects []postgresCleanupObject) []serverQuestion {
	var questions []serverQuestion
	for _, object := range objects {
		switch object.Kind {
		case "type", "function", "procedure":
		default:
			continue
		}
		family, _ := familyOfCleanupKind(object.Kind)
		questions = append(questions, serverQuestion{
			source:  cleanupKey{family, object.Schema, object.Name},
			explain: "EXPLAIN (DDL) " + object.Statement,
		})
	}
	slices.SortFunc(questions, func(a, b serverQuestion) int {
		return cmp.Or(compareCleanupKeys(a.source, b.source), strings.Compare(a.explain, b.explain))
	})
	return questions
}

// serverReportedDependents asks the server, for each type, function and
// procedure of objects, which objects depend on it, and describes for each
// one the dependents that are neither in dropped nor named for it by known.
// The objects are named as the server writes them, without the database,
// because the server names a dependent by its qualified name alone and does
// not quote it. The overloads of a routine share one sentence.
func serverReportedDependents(
	ctx context.Context,
	q cleanupConn,
	objects []postgresCleanupObject,
	dropped map[cleanupKey]string,
	database string,
	known []outsideDependency,
) ([]outsideDependency, error) {
	outside := make(map[cleanupKey][]string)
	var sources []cleanupKey
	for _, question := range serverQuestions(objects) {
		list, err := serverDependentsList(ctx, q, question, dropped[question.source])
		if err != nil {
			return nil, err
		}
		inside := slices.Collect(maps.Keys(dropped))
		for _, dependency := range known {
			if dependency.source == question.source {
				inside = append(inside, dependency.dependent)
			}
		}
		for _, name := range serverDependentsOutside(list, database, inside) {
			if !slices.Contains(sources, question.source) {
				sources = append(sources, question.source)
			}
			if !slices.Contains(outside[question.source], name) {
				outside[question.source] = append(outside[question.source], name)
			}
		}
	}
	reported := make([]outsideDependency, 0, len(sources))
	for _, source := range sources {
		reported = append(reported, outsideDependency{
			source: source,
			sentence: fmt.Sprintf("CockroachDB records that %s is used by %s",
				source.describe(dropped[source]), strings.Join(outside[source], " ")),
		})
	}
	return reported, nil
}

// serverDependentsList answers the objects the server lists as depending on
// the object question asks about, of kind, as it writes the list, or "" when
// it lists none.
func serverDependentsList(ctx context.Context, q cleanupConn, question serverQuestion, kind string) (string, error) {
	err := explainInSavepoint(ctx, q, question.explain)
	if err == nil {
		return "", nil
	}
	serverErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || serverErr.Code != dependentObjectsStillExist {
		return "", fmt.Errorf("failed to ask the server what depends on %s: %w", question.source.describe(kind), err)
	}
	_, after, found := strings.Cut(serverErr.Message, "([")
	list, _, closed := strings.Cut(after, "])")
	if !found || !closed {
		return "", fmt.Errorf("failed to read what depends on %s from the server's refusal: %w",
			question.source.describe(kind), err)
	}
	return list, nil
}

// explainInSavepoint runs explain, and inside a transaction runs it under
// serverDependentsSavepoint and rolls back to it when the server refuses, so
// the transaction stays open.
func explainInSavepoint(ctx context.Context, q cleanupConn, explain string) error {
	if _, inTransaction := q.(*sql.Tx); !inTransaction {
		_, err := q.ExecContext(ctx, explain)
		return err
	}
	if _, err := q.ExecContext(ctx, "SAVEPOINT "+serverDependentsSavepoint); err != nil {
		return fmt.Errorf("failed to open a savepoint to ask the server what depends on an object: %w", err)
	}
	_, explainErr := q.ExecContext(ctx, explain)
	finish := "RELEASE SAVEPOINT " + serverDependentsSavepoint
	if explainErr != nil {
		finish = "ROLLBACK TO SAVEPOINT " + serverDependentsSavepoint
	}
	if _, err := q.ExecContext(ctx, finish); err != nil {
		return fmt.Errorf("failed to close the savepoint after asking the server what depends on an object: %w", err)
	}
	return explainErr
}

// serverDependentsOutside removes from list, the objects the server names as
// db.schema.name separated by a space or a comma and a space, every object of
// inside, and returns the rest without the database prefix.
func serverDependentsOutside(list, database string, inside []cleanupKey) []string {
	rest := " " + strings.ReplaceAll(list, ", ", " ") + " "
	for _, key := range inside {
		token := " " + database + "." + key.schema + "." + key.name + " "
		for strings.Contains(rest, token) {
			rest = strings.Replace(rest, token, " ", 1)
		}
	}
	rest = strings.ReplaceAll(rest, " "+database+".", " ")
	return strings.Fields(rest)
}
