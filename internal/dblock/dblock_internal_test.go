package dblock

// White-box testing required: the GET_LOCK and sp_getapplock timeout
// conversions are only observable through SQL sent to live MySQL, MariaDB,
// and SQL Server servers, so their numeric edge cases (rounding up, MariaDB's
// finite stand-in for an infinite wait, and the SQL Server int cap) cannot be
// asserted through the public Acquire API without those servers.

import (
	"math"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestMySQLLockTimeoutSeconds(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
		timeout time.Duration
		want    float64
	}{
		{name: "mysql default uses native infinite timeout", dialect: "mysql", want: -1},
		{name: "mariadb default avoids unsupported negative timeout", dialect: "mariadb", want: mariaDBDefaultTimeoutSeconds},
		{name: "mysql explicit subsecond rounds up", dialect: "mysql", timeout: 500 * time.Millisecond, want: 1},
		{name: "mariadb explicit duration", dialect: "mariadb", timeout: 2 * time.Second, want: 2},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(mySQLLockTimeoutSeconds(test.dialect, test.timeout), qt.Equals, test.want)
		})
	}
}

func TestSQLServerLockTimeoutMilliseconds(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		timeout time.Duration
		want    int
	}{
		{name: "default waits indefinitely", want: -1},
		{name: "submillisecond rounds up", timeout: time.Nanosecond, want: 1},
		{name: "explicit duration", timeout: 1500 * time.Millisecond, want: 1500},
		{name: "caps at SQL Server int maximum", timeout: time.Duration(math.MaxInt32+1) * time.Millisecond, want: math.MaxInt32},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(sqlServerLockTimeoutMilliseconds(test.timeout), qt.Equals, test.want)
		})
	}
}
