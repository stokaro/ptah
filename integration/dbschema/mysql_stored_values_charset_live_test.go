//go:build integration

package dbschema_test

import (
	"net/url"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// latin1Parameters are the URL parameters that give a session the latin1
// client character set.
var latin1Parameters = []struct {
	name, parameter, value string
}{
	{name: "charset", parameter: "charset", value: "latin1"},
	{name: "collation", parameter: "collation", value: "latin1_swedish_ci"},
}

// connectWithParameter connects to engine's database with parameter set to
// value in the URL, every session under mode.
func connectWithParameter(c *qt.C, engine dbtarget.Engine, parameter, value, mode string) *dbschema.DatabaseConnection {
	c.Helper()
	address, rawQuery, _ := strings.Cut(dbtarget.URL(c, engine), "?")
	query, err := url.ParseQuery(rawQuery)
	c.Assert(err, qt.IsNil)
	query.Set(parameter, value)
	return connectURLWithMode(c, address+"?"+query.Encode(), mode)
}

// A loose session whose URL gives it the latin1 client character set reads a
// statement's bytes by other rules than the window does, so the window
// refuses to open: the callback does not run, and the session keeps its own
// mode. Measured on MySQL 8.4.13 and MariaDB 11.8.9, under latin1 the byte
// 0xA0 is white space, and a trigger written CREATE<0xA0>TRIGGER is created.
func TestWithStoredValuesKept_LiveMySQLFamilyRefusesALatin1LooseSession(t *testing.T) {
	for _, row := range storedSpellingEngines {
		for _, parameter := range latin1Parameters {
			t.Run(row.name+"/"+parameter.name, func(t *testing.T) {
				c := qt.New(t)
				conn := connectWithParameter(c, row.engine, parameter.parameter, parameter.value, looseSessionMode)
				ran := 0

				err := conn.WithStoredValuesKept(c.Context(), func(*dbschema.DatabaseConnection) error {
					ran++
					return nil
				})

				c.Assert(err, qt.ErrorMatches, `keep stored values on the session: the session's client character set is "latin1", .*`)
				c.Assert(ran, qt.Equals, 0)
				c.Assert(sessionMode(c, conn), qt.Equals, ownSessionMode)
			})
		}
	}
}

// The control: a strict session needs no window and reads no statement, so
// the same URL runs the callback.
func TestWithStoredValuesKept_LiveMySQLFamilyRunsALatin1StrictSession(t *testing.T) {
	for _, row := range storedSpellingEngines {
		for _, parameter := range latin1Parameters {
			t.Run(row.name+"/"+parameter.name, func(t *testing.T) {
				c := qt.New(t)
				conn := connectWithParameter(c, row.engine, parameter.parameter, parameter.value, "'STRICT_ALL_TABLES'")
				ran := 0

				err := conn.WithStoredValuesKept(c.Context(), func(*dbschema.DatabaseConnection) error {
					ran++
					return nil
				})

				c.Assert(err, qt.IsNil)
				c.Assert(ran, qt.Equals, 1)
			})
		}
	}
}
