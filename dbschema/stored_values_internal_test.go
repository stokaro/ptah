package dbschema

// White-box testing required: the window is an unexported field that
// WithStoredValuesKept sets only on a live MySQL or MariaDB session, and
// runInWindow is the one place every path a statement takes to the server
// meets it. A black-box test needs a server; the live test in
// integration/dbschema drives the same paths there, and these tests pin, on a
// window whose answers they choose, that a statement the window cannot read
// is never sent and that a batch is read whole before any of it is.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/connector"
)

// errUnreadable is the error the fake window gives for a statement it cannot
// read.
var errUnreadable = errors.New("unreadable statement")

// readingWindow answers StoredWith from its tables and records each
// statement it is asked about.
type readingWindow struct {
	unreadable map[string]bool
	keeps      map[string]bool
	asked      []string
}

func (w *readingWindow) Restore(context.Context) error { return nil }

func (w *readingWindow) StoredWith(statement string) (*connector.StoredSettings, error) {
	w.asked = append(w.asked, statement)
	if w.unreadable[statement] {
		return nil, errUnreadable
	}
	if !w.keeps[statement] {
		return nil, nil
	}
	return &connector.StoredSettings{
		Own:    connector.SessionStatement{SQL: "SET own"},
		Window: connector.SessionStatement{SQL: "SET window"},
	}, nil
}

// sent runs statement through runInWindow on window and returns the
// statements that reached the server, settings included, in order.
func sent(window connector.StoredValuesWindow, statement string) ([]string, error) {
	var server []string
	err := runInWindow(context.Background(), window, statement,
		func(_ context.Context, settings connector.SessionStatement) error {
			server = append(server, settings.SQL)
			return nil
		},
		func(context.Context) error {
			server = append(server, statement)
			return nil
		},
	)
	return server, err
}

func TestRunInWindow_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      []string
	}{
		{name: "a statement that keeps no settings", statement: "CREATE TABLE t (id INT)", want: []string{"CREATE TABLE t (id INT)"}},
		{name: "a statement that keeps them", statement: "CREATE TRIGGER tr", want: []string{"SET own", "CREATE TRIGGER tr", "SET window"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			window := &readingWindow{keeps: map[string]bool{"CREATE TRIGGER tr": true}}

			server, err := sent(window, test.statement)

			c.Assert(err, qt.IsNil)
			c.Assert(server, qt.DeepEquals, test.want)
		})
	}
}

// A statement the window cannot read reaches the server neither as itself
// nor with settings around it: without the refusal it would run under the
// window's settings and keep them.
func TestRunInWindow_FailurePath(t *testing.T) {
	c := qt.New(t)
	window := &readingWindow{unreadable: map[string]bool{"CREATE #\rTRIGGER": true}}

	server, err := sent(window, "CREATE #\rTRIGGER")

	c.Assert(err, qt.ErrorIs, errUnreadable)
	c.Assert(err, qt.ErrorMatches, `read whether the statement defines an object that keeps the session's settings: unreadable statement`)
	c.Assert(server, qt.IsNil)
}

// A connection outside the window, and a batch the window reads whole, pass
// the preflight; the window is asked about every statement.
func TestPreflightStoredValues_HappyPath(t *testing.T) {
	c := qt.New(t)
	batch := []string{"CREATE TABLE t (id INT)", "CREATE TRIGGER tr", ""}
	window := &readingWindow{keeps: map[string]bool{"CREATE TRIGGER tr": true}}

	c.Assert((*DatabaseConnection)(nil).PreflightStoredValues(batch), qt.IsNil)
	c.Assert((&DatabaseConnection{}).PreflightStoredValues(batch), qt.IsNil)
	c.Assert((&DatabaseConnection{storedValues: window}).PreflightStoredValues(batch), qt.IsNil)
	c.Assert(window.asked, qt.DeepEquals, batch)
}

// The preflight names the first statement the window cannot read, and stops
// there.
func TestPreflightStoredValues_FailurePath(t *testing.T) {
	c := qt.New(t)
	window := &readingWindow{unreadable: map[string]bool{"CREATE #\rTRIGGER": true, "ALTER #\rEVENT": true}}
	conn := &DatabaseConnection{storedValues: window}

	err := conn.PreflightStoredValues([]string{"CREATE TABLE t (id INT)", "CREATE #\rTRIGGER", "ALTER #\rEVENT"})

	c.Assert(err, qt.ErrorIs, errUnreadable)
	c.Assert(err, qt.ErrorMatches, `statement 2 of 3: read whether the statement defines an object that keeps the session's settings: unreadable statement`)
	c.Assert(window.asked, qt.DeepEquals, []string{"CREATE TABLE t (id INT)", "CREATE #\rTRIGGER"})
}

// The session WithStoredValuesKept gives its callback carries the window, so
// the preflight reads through it.
func TestInStoredValuesWindow_CarriesTheWindow(t *testing.T) {
	c := qt.New(t)
	window := &readingWindow{unreadable: map[string]bool{"CREATE #\rTRIGGER": true}}

	scoped := (&DatabaseConnection{}).inStoredValuesWindow(window)

	c.Assert(scoped.PreflightStoredValues([]string{"CREATE #\rTRIGGER"}), qt.ErrorIs, errUnreadable)
}
