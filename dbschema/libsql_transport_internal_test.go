package dbschema

// White-box testing required: the driver and DSN a URL resolves to are chosen
// by an unexported function, and no exported call reports which transport a
// connection would use without opening one.

import (
	"errors"
	"fmt"
	"io"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
)

// libSQLURLRow is one authored URL and the transport the pinned community
// binary v1.3.0 reaches for it.
type libSQLURLRow struct {
	name       string
	url        string
	wantDriver string
	wantDSN    string
	wantRemote bool
}

func TestLibSQLDriverConfigMatchesTheMeasuredTransports(t *testing.T) {
	rows := []libSQLURLRow{{
		// Measured: the binary answers `Post "https://host/v2/pipeline"`, so
		// the client takes this spelling unchanged.
		name:       "the http pipeline form",
		url:        "libsql://db.example.com",
		wantDriver: "libsql",
		wantDSN:    "libsql://db.example.com",
		wantRemote: true,
	}, {
		// Measured: the binary answers `failed to WebSocket dial`. The client
		// library spells that transport `ws://`.
		name:       "the websocket form",
		url:        "libsql+ws://127.0.0.1:8080",
		wantDriver: "libsql",
		wantDSN:    "ws://127.0.0.1:8080",
		wantRemote: true,
	}, {
		name:       "a local sqlite file is not a libsql url",
		url:        "sqlite://local.db",
		wantRemote: false,
	}, {
		// The binary refuses this spelling as an unknown driver, so it must
		// not be silently accepted here either.
		name:       "libsql+http is not a driver the binary has",
		url:        "libsql+http://host",
		wantRemote: false,
	}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			driver, dsn, remote := libSQLDriverConfig(row.url)

			c.Assert(remote, qt.Equals, row.wantRemote)
			c.Assert(driver, qt.Equals, row.wantDriver)
			c.Assert(dsn, qt.Equals, row.wantDSN)
		})
	}
}

func TestLibSQLResolvesToTheSQLiteDialect(t *testing.T) {
	c := qt.New(t)

	// libsql is a transport, not a dialect: the schema it serves is SQLite's,
	// and the binary's own errors on these URLs are prefixed `sqlite:`.
	c.Assert(platform.NormalizeDialect("libsql"), qt.Equals, platform.SQLite)
	c.Assert(platform.NormalizeDialect("libsql+ws"), qt.Equals, platform.SQLite)
	// Not a driver the binary has, so not a dialect Ptah invents.
	c.Assert(platform.NormalizeDialect("libsql+http"), qt.Equals, "")
}

// closeRow is one connection shape and whether Close must report its error.
type closeRow struct {
	name    string
	remote  bool
	err     error
	wantErr bool
}

func TestCloseSwallowsOnlyTheLibSQLTeardown(t *testing.T) {
	rows := []closeRow{{
		// Measured against libsql-server v0.24.32: every WebSocket connection
		// that ran a statement reports this on Close, and one that ran none
		// closes cleanly. Reporting it would put a warning after every
		// successful command, which the binary does not.
		name:    "the measured teardown is not an error",
		remote:  true,
		err:     fmt.Errorf("failed to close WebSocket: failed to read frame header: %w", io.EOF),
		wantErr: false,
	}, {
		// The inverse control: a real failure on the same transport must still
		// reach the caller, or this would be a blanket silencer.
		name:    "a genuine failure on the same transport still reports",
		remote:  true,
		err:     errors.New("connection reset by peer"),
		wantErr: true,
	}, {
		// And an EOF from any other driver is not covered by this at all.
		name:    "an EOF from another driver still reports",
		remote:  false,
		err:     fmt.Errorf("closing pool: %w", io.EOF),
		wantErr: true,
	}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(libSQLTeardownIsBenign(row.remote, row.err), qt.Equals, !row.wantErr)
		})
	}
}
