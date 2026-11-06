package mysql_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/readscope"
)

// The two reasons a report prints, for the objects a row expects.
const (
	notModeled = "Ptah does not model events, so nothing compares or plans this one"
	noEvent    = "the account app@% holds no EVENT privilege on `fx`.*, and information_schema.EVENTS lists only what an account holds it on"
)

func event(name string) catalog.UnmodeledObject {
	return catalog.UnmodeledObject{Kind: "event", Name: name, Reason: notModeled}
}

// TestReader_NamesTheEventsItFinds_HappyPath holds each answer of
// information_schema.EVENTS to what the read records. The grants are the ones
// measured on MySQL 8.4 and 26.10 and on MariaDB 11.8 and 12.3: only EVENT lists an
// event, at the database level, on a pattern or globally, and every other privilege
// answers no rows and no error.
func TestReader_NamesTheEventsItFinds_HappyPath(t *testing.T) {
	const reads = "GRANT SELECT, SHOW VIEW, TRIGGER, EXECUTE ON `fx`.* TO `app`@`%`"
	routine := "GRANT SHOW_ROUTINE ON *.* TO `app`@`%`"
	tests := []struct {
		name    string
		version string
		grants  []string
		events  []string
		want    []catalog.UnmodeledObject
	}{
		{
			name:    "MySQL, EVENT on the database and none to find",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT SELECT, SHOW VIEW, TRIGGER, EXECUTE, EVENT ON `fx`.* TO `app`@`%`", routine},
		},
		{
			name:    "MySQL, EVENT on the database",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT SELECT, SHOW VIEW, TRIGGER, EXECUTE, EVENT ON `fx`.* TO `app`@`%`", routine},
			events:  []string{"e_a", "e_b"},
			want:    []catalog.UnmodeledObject{event("e_a"), event("e_b")},
		},
		{
			name:    "MariaDB, EVENT on the database",
			version: dbtest.MariaDBVersion,
			grants:  []string{"GRANT SELECT, SHOW VIEW, TRIGGER, EXECUTE, EVENT ON `fx`.* TO `app`@`%`", "GRANT SELECT ON `mysql`.`proc` TO `app`@`%`"},
			events:  []string{"e_a"},
			want:    []catalog.UnmodeledObject{event("e_a")},
		},
		{
			name:    "EVENT globally",
			version: dbtest.MySQLVersion,
			grants:  []string{reads, routine, "GRANT EVENT ON *.* TO `app`@`%`"},
			events:  []string{"e_a"},
			want:    []catalog.UnmodeledObject{event("e_a")},
		},
		{
			name:    "EVENT on a pattern",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT SELECT, SHOW VIEW, TRIGGER, EXECUTE, EVENT ON `f%`.* TO `app`@`%`", routine},
			events:  []string{"e_a"},
			want:    []catalog.UnmodeledObject{event("e_a")},
		},
		{
			name:    "ALL PRIVILEGES on the database, with none to find",
			version: dbtest.MySQLVersion,
			grants:  []string{"GRANT ALL PRIVILEGES ON `fx`.* TO `app`@`%`", routine},
		},
		{
			name:    "MySQL, no EVENT, where no row can say whether one exists",
			version: dbtest.MySQLVersion,
			grants:  []string{reads, routine},
			want:    []catalog.UnmodeledObject{{Kind: "event", Reason: noEvent}},
		},
		{
			name:    "MariaDB, no EVENT, where no row can say whether one exists",
			version: dbtest.MariaDBVersion,
			grants:  []string{reads, "GRANT SELECT ON `mysql`.`proc` TO `app`@`%`"},
			want:    []catalog.UnmodeledObject{{Kind: "event", Reason: noEvent}},
		},
		{
			name:    "a row the grants did not predict is still named",
			version: dbtest.MySQLVersion,
			grants:  []string{reads, routine},
			events:  []string{"e_a"},
			want:    []catalog.UnmodeledObject{{Kind: "event", Reason: noEvent}, event("e_a")},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			visible := fullCatalog()
			visible.events = map[string][]string{"fx": test.events}
			db := dbtest.Open(t, dbtest.MySQLSession(test.version, test.grants, visible.answer))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(got.UnmodeledObjects, qt.DeepEquals, test.want)
			// An event is a record for a report: it reaches nothing a comparison reads.
			c.Assert(got.NotDescribed.IsZero(), qt.IsTrue)
		})
	}
}

// TestReader_WholeServerSaysWhichDatabaseAnEventIsIn_HappyPath is a read of two
// databases, one the account may list events in and one it may not.
func TestReader_WholeServerSaysWhichDatabaseAnEventIsIn_HappyPath(t *testing.T) {
	c := qt.New(t)
	visible := visibleCatalog{selected: nil, databases: []string{"r1", "r2"}, events: map[string][]string{"r1": {"e_a"}}}
	grants := []string{"GRANT ALL PRIVILEGES ON `r1`.* TO `app`@`%`", "GRANT SELECT, SHOW VIEW, TRIGGER, EXECUTE ON `r2`.* TO `app`@`%`", "GRANT SHOW_ROUTINE ON *.* TO `app`@`%`"}
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, grants, visible.answer))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(got.UnmodeledObjects, qt.DeepEquals, []catalog.UnmodeledObject{
		{Kind: "event", Schema: "r1", Name: "e_a", Reason: notModeled},
		{Kind: "event", Schema: "r2", Reason: strings.ReplaceAll(noEvent, "`fx`", "`r2`")},
	})
}

// TestReader_LeavesEventsOutOfWhatItDoesNotAnswerFor_HappyPath is a read with
// nothing to report. A catalog that holds an event shows it was not asked.
func TestReader_LeavesEventsOutOfWhatItDoesNotAnswerFor_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		ctx  func(context.Context) context.Context
	}{
		{name: "a read of what the run created", ctx: readscope.Authored},
		{name: "a run that left events out", ctx: func(ctx context.Context) context.Context {
			return readscope.Without(ctx, readscope.Events)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			visible := fullCatalog()
			visible.events = map[string][]string{"fx": {"e_a"}}
			db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, visible.answer))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(test.ctx(t.Context()))

			c.Assert(err, qt.IsNil)
			c.Assert(got.UnmodeledObjects, qt.HasLen, 0)
		})
	}
}

// refusingEvents answers the catalog query with err and every other with visible.
func refusingEvents(visible visibleCatalog, err error, before func()) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, "FROM information_schema.EVENTS") {
			before()
			return dbtest.QueryResult{}, err
		}
		return visible.answer(query, args)
	}
}

// TestReader_AServerThatRefusesEventsDoesNotFailTheRead_HappyPath is what MariaDB
// answers for every account when it runs with --event-scheduler=DISABLED, measured
// on 12.3. MySQL 8.4 answers the same question with the rows.
func TestReader_AServerThatRefusesEventsDoesNotFailTheRead_HappyPath(t *testing.T) {
	c := qt.New(t)
	refused := errors.New("Error 1577 (HY000): Cannot proceed, because event scheduler is disabled")
	handler := refusingEvents(fullCatalog(), refused, func() {})
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MariaDBVersion, dbtest.MySQLAllGrants, handler))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(got.Triggers, qt.HasLen, 1)
	c.Assert(got.UnmodeledObjects, qt.DeepEquals, []catalog.UnmodeledObject{{
		Kind:   "event",
		Reason: "the server refused to list them: Error 1577 (HY000): Cannot proceed, because event scheduler is disabled",
	}})
}

// TestReader_ACanceledRunStopsAtTheEventRead_FailurePath is the limit of that: a
// run that is over is not a server refusing a catalog.
func TestReader_ACanceledRunStopsAtTheEventRead_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	handler := refusingEvents(fullCatalog(), context.Canceled, cancel)
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, handler))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(ctx)

	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta("failed to read events: ")+".*")
	c.Assert(got, qt.IsNil)
}
