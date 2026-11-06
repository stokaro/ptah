package mysql

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/internal/readscope"
)

const (
	// unmodeledEventKind is the kind a report names an event by.
	unmodeledEventKind = "event"
	// unmodeledEventReason is why nothing compares an event a read found.
	unmodeledEventReason = "Ptah does not model events, so nothing compares or plans this one"
	// eventPrivilege shows a database's events in information_schema.EVENTS, at
	// the database level, on a pattern or globally. Measured one privilege at a
	// time on MySQL 8.4 and 26.10 and on MariaDB 11.8 and 12.3, against a database
	// holding one event: every other privilege, global SELECT and SELECT on the
	// mysql database included, answers no rows and no error.
	eventPrivilege = "EVENT"
)

// readEvents names the events of database for a report, and says so where it
// cannot list them. Ptah does not model an event: no schema source can declare
// one, and a dev database replay refuses CREATE EVENT because the event's effects
// escape the database. Events take no part in a comparison, and this read exists
// so that a report does not read as covering them.
//
// Nothing the run does depends on the answer, so a read that cannot list them
// records the whole kind and goes on, where a kind the comparison reads is
// refused. An account without EVENT sees no rows and no error, so the privilege is
// asked for by name. A server can refuse the catalog as well: MariaDB answers
// error 1577 for every account when it runs with --event-scheduler=DISABLED.
//
// A read of what this run created, and a run that left events out, have nothing
// to report.
func (r *Reader) readEvents(ctx context.Context, account sessionAccount, database string) ([]catalog.UnmodeledObject, error) {
	if readscope.IsAuthored(ctx) || !readscope.Needs(ctx, readscope.Events) {
		return nil, nil
	}
	var objects []catalog.UnmodeledObject
	if !account.grants.OnDatabase(database).Holds(eventPrivilege) {
		objects = append(objects, catalog.UnmodeledObject{
			Kind: unmodeledEventKind,
			Reason: fmt.Sprintf(
				"the account %s holds no EVENT privilege on `%s`.*, and information_schema.EVENTS lists only what an account holds it on",
				account.name, strings.ReplaceAll(database, "`", "``"),
			),
		})
	}
	names, err := r.listEvents(ctx, database)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("failed to read events: %w", err)
		}
		return []catalog.UnmodeledObject{{
			Kind:   unmodeledEventKind,
			Reason: fmt.Sprintf("the server refused to list them: %v", err),
		}}, nil
	}
	for _, name := range names {
		objects = append(objects, catalog.UnmodeledObject{Kind: unmodeledEventKind, Name: name, Reason: unmodeledEventReason})
	}
	return objects, nil
}

// listEvents lists the names of the events of database that the account may see.
func (r *Reader) listEvents(ctx context.Context, database string) ([]string, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT EVENT_NAME FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ? ORDER BY EVENT_NAME`, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}
