package oracleconnect

// White-box testing required: the data source go-ora receives is built by an unexported converter, and no exported result reports it without a live server.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/connector"
)

// TestOracleDataSourceName_TurnsFastLoginOffUnlessTheCallerSpokeFirst pins the
// one option Ptah adds to an Oracle URL, and the one case where it does not.
//
// go-ora v3 caches the server's negotiation in a package-level store and
// replays it on the next connection to the same server. Oracle 23.26.2.0.0
// closes the socket on the replay, so the second connection a process opens
// answers EOF, and the driver clears the cached cookie only for
// driver.ErrBadConn -- so every later connection in that process is refused
// too. Measured: with the option the second handle opens, without it the
// second handle is EOF (stokaro/ptah#1875).
func TestOracleDataSourceName_TurnsFastLoginOffUnlessTheCallerSpokeFirst(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "a URL with no query gets one",
			url:  "oracle://ptah:secret@127.0.0.1:1521/FREEPDB1",
			want: "oracle://ptah:secret@127.0.0.1:1521/FREEPDB1?FAST+LOGIN=FALSE",
		},
		{
			// The space in the option name is escaped rather than passed
			// through: go-ora reads the query with net/url, and an unescaped
			// space ends the URL at the parser rather than at the driver.
			name: "a URL with a query gets another parameter",
			url:  "oracle://ptah:secret@127.0.0.1:1521/FREEPDB1?TIMEOUT=30",
			want: "oracle://ptah:secret@127.0.0.1:1521/FREEPDB1?FAST+LOGIN=FALSE&TIMEOUT=30",
		},
		{
			// The default is Ptah's, not a prohibition: a caller who wants the
			// optimization back says so and keeps it.
			name: "a caller who set it keeps their answer",
			url:  "oracle://ptah:secret@127.0.0.1:1521/FREEPDB1?FAST+LOGIN=TRUE",
			want: "oracle://ptah:secret@127.0.0.1:1521/FREEPDB1?FAST+LOGIN=TRUE",
		},
		{
			name: "and the unescaped spelling counts as having spoken",
			url:  "oracle://ptah:secret@127.0.0.1:1521/FREEPDB1?FAST LOGIN=ENABLE",
			want: "oracle://ptah:secret@127.0.0.1:1521/FREEPDB1?FAST LOGIN=ENABLE",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(oracleDataSourceName(test.url), qt.Equals, test.want)
		})
	}
}

// TestDatabaseDriverConfig_OracleCarriesTheOption is the wiring half.
//
// oracleDataSourceName passing its own table proves the helper works, not that
// the data source a connection opens comes from it, and dataSource is the only
// place Open reads one from.
func TestDatabaseDriverConfig_OracleCarriesTheOption(t *testing.T) {
	c := qt.New(t)

	described, known := platform.Lookup(platform.Oracle)
	c.Assert(known, qt.IsTrue)

	driverName, dataSourceName := dataSource(connector.Request{
		Dialect: described,
		URL:     "oracle://ptah:secret@127.0.0.1:1521/FREEPDB1",
	})

	c.Assert(driverName, qt.Equals, "oracle")
	c.Assert(dataSourceName, qt.Contains, "FAST+LOGIN=FALSE")
}
