package postgres

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
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
//
// The plan is the declarative schema changer's. In a session with
// use_declarative_schema_changer off, the drop of an object something depends
// on is still refused with the list, and the drop of one nothing depends on is
// refused with SQLSTATE 0A000. Measured on the four lines, setting the variable
// on inside the transaction, with SET LOCAL or SET, under the savepoint or
// before it, does not change that answer, so the cleanup refuses and names the
// variable.

// serverDependentsSavepoint is the savepoint serverReportedDependents asks
// under inside a transaction, so the refusal of one EXPLAIN does not abort the
// transaction the cleanup runs in.
const serverDependentsSavepoint = "ptah_dependents"

// featureNotSupported is the SQLSTATE of an EXPLAIN (DDL) the declarative
// schema changer does not plan.
const featureNotSupported = "0A000"

// serverQuestion is the EXPLAIN (DDL) of the drop of one object the cleanup
// drops, and the object.
type serverQuestion struct {
	subject serverSubject
	explain string
}

// serverSubject is an object the server is asked about: its key, and for a
// routine the argument list that names one overload of it, as "(INT8)".
type serverSubject struct {
	source    cleanupKey
	arguments string
}

// describe names the subject as kind, with the arguments of a routine.
func (s serverSubject) describe(kind string) string {
	return s.source.describe(kind) + s.arguments
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
		var arguments string
		if family == routineFamily {
			arguments = routineArguments(object.Statement)
		}
		questions = append(questions, serverQuestion{
			subject: serverSubject{cleanupKey{family, object.Schema, object.Name}, arguments},
			explain: "EXPLAIN (DDL) " + object.Statement,
		})
	}
	slices.SortFunc(questions, func(a, b serverQuestion) int {
		return cmp.Or(compareCleanupKeys(a.subject.source, b.subject.source), strings.Compare(a.explain, b.explain))
	})
	return questions
}

// routineArguments is the argument list of a routine's drop statement, the
// text after the routine's qualified name: `DROP FUNCTION IF EXISTS
// public.fn(INT8, STRING) RESTRICT` gives "(INT8, STRING)". The list is taken
// whole, because a type in it may be written unquoted with a parenthesis in
// its name, as v25.4.21 writes `we(ird`. A statement it cannot read gives "".
func routineArguments(statement string) string {
	_, rest, ok := strings.Cut(statement, " IF EXISTS ")
	if ok {
		_, rest, ok = cutIdentifier(rest)
	}
	if ok {
		rest, ok = strings.CutPrefix(rest, ".")
	}
	if ok {
		_, rest, ok = cutIdentifier(rest)
	}
	if !ok || !strings.HasPrefix(rest, "(") {
		return ""
	}
	return strings.TrimSuffix(rest, " RESTRICT")
}

// serverReportedDependents asks the server, for each type, function and
// procedure of objects, which objects depend on it, and describes for each
// one the dependents that are neither in dropped nor named for it by known.
// Each overload of a routine is named by its arguments in a sentence of its
// own, and each dependent by its schema and name, without the database.
func serverReportedDependents(
	ctx context.Context,
	q cleanupConn,
	objects []postgresCleanupObject,
	dropped map[cleanupKey]string,
	database string,
	known []outsideDependency,
) ([]outsideDependency, error) {
	var reported []outsideDependency
	for _, question := range serverQuestions(objects) {
		source := question.subject.source
		list, err := serverDependentsList(ctx, q, question, dropped[source])
		if err != nil {
			return nil, err
		}
		inside := slices.Collect(maps.Keys(dropped))
		for _, dependency := range known {
			if dependency.source == source {
				inside = append(inside, dependency.dependent)
			}
		}
		names := serverDependentsOutside(list, database, inside)
		if len(names) == 0 {
			continue
		}
		reported = append(reported, outsideDependency{
			source: source,
			sentence: fmt.Sprintf("CockroachDB records that %s is used by %s",
				question.subject.describe(dropped[source]), strings.Join(names, ", ")),
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
	if ok && serverErr.Code == featureNotSupported {
		return "", fmt.Errorf("failed to ask the server what depends on %s: CockroachDB plans the drop only with "+
			"use_declarative_schema_changer on, and the cleanup refuses rather than drop without the answer: %w",
			question.subject.describe(kind), err)
	}
	if !ok || serverErr.Code != dependentObjectsStillExist {
		return "", fmt.Errorf("failed to ask the server what depends on %s: %w", question.subject.describe(kind), err)
	}
	_, after, found := strings.Cut(serverErr.Message, "([")
	list, _, closed := strings.Cut(after, "])")
	if !found || !closed {
		return "", fmt.Errorf("failed to read what depends on %s from the server's refusal: %w",
			question.subject.describe(kind), err)
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

// serverDependentsOutside reads list, the objects the server names as
// db.schema.name separated by a space or a comma and a space, and returns
// each one that is not in inside, quoted as schema and name. The server
// quotes nothing, so a name is cut at the database prefix of the next one and
// its schema at the first dot; a name of another database stays whole.
func serverDependentsOutside(list, database string, inside []cleanupKey) []string {
	var names []string
	pieces := strings.Split(" "+strings.ReplaceAll(list, ", ", " "), " "+database+".")
	for i, piece := range pieces {
		piece = strings.TrimSpace(piece)
		if piece == "" {
			continue
		}
		name := strconv.Quote(piece)
		schema, object, qualified := strings.Cut(piece, ".")
		if i > 0 && qualified {
			if slices.ContainsFunc(inside, func(key cleanupKey) bool { return key.schema == schema && key.name == object }) {
				continue
			}
			name = strconv.Quote(schema) + "." + strconv.Quote(object)
		}
		if !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	return names
}
