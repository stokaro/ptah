package atlasurl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
)

// TestParseMySQLURL_HappyPath reads each form of a MySQL-family URL. The
// expected socket-form values are what the pinned community binary v1.3.0 was
// measured to connect with against MariaDB 11.8.9 and MySQL 8.4.11: the path
// is the socket, a host is ignored, an absent path dials the driver's default
// socket, the first `database` parameter names the database, and the other
// parameters reach the driver.
func TestParseMySQLURL_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		rawURL       string
		wantDialect  string
		wantNetwork  string
		wantAddress  string
		wantDatabase string
		wantUser     string
		wantPassword string
		wantDSN      string
	}{
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:        "URL form",
			rawURL:      "mysql://app:p%40ss@db.internal:3307/shop?parseTime=true",
			wantDialect: "mysql", wantNetwork: "tcp", wantAddress: "db.internal:3307", wantDatabase: "shop",
			wantUser: "app", wantPassword: "p@ss",
			wantDSN: "app:p@ss@tcp(db.internal:3307)/shop?parseTime=true",
		},
		{
			name:        "URL form with the driver's default port",
			rawURL:      "mariadb://app@db.internal/shop",
			wantDialect: "mariadb", wantNetwork: "tcp", wantAddress: "db.internal:3306", wantDatabase: "shop",
			wantUser: "app", wantPassword: "",
			wantDSN: "app:@tcp(db.internal)/shop",
		},
		{
			name:        "URL form with a database that needs escaping",
			rawURL:      "mysql://app@db.internal/my%2Fshop",
			wantDialect: "mysql", wantNetwork: "tcp", wantAddress: "db.internal:3306", wantDatabase: "my/shop",
			wantUser: "app", wantPassword: "",
			wantDSN: "app:@tcp(db.internal)/my%2Fshop",
		},
		{
			name:        "driver TCP form is handed on as written",
			rawURL:      "maria://app:pa)ss@tcp(127.0.0.1:3306)/shop",
			wantDialect: "mariadb", wantNetwork: "tcp", wantAddress: "127.0.0.1:3306", wantDatabase: "shop",
			wantUser: "app", wantPassword: "pa)ss",
			wantDSN: "app:pa)ss@tcp(127.0.0.1:3306)/shop",
		},
		{
			name:        "driver socket form",
			rawURL:      "mysql://app:pw@unix(/tmp/mysql.sock)/shop",
			wantDialect: "mysql", wantNetwork: "unix", wantAddress: "/tmp/mysql.sock", wantDatabase: "shop",
			wantUser: "app", wantPassword: "pw",
			wantDSN: "app:pw@unix(/tmp/mysql.sock)/shop",
		},
		{
			name:        "driver form without credentials",
			rawURL:      "mysql://tcp(localhost:3306)/shop",
			wantDialect: "mysql", wantNetwork: "tcp", wantAddress: "localhost:3306", wantDatabase: "shop",
			wantUser: "", wantPassword: "",
			wantDSN: "tcp(localhost:3306)/shop",
		},
		{
			name:        "socket form",
			rawURL:      "mysql+unix://app:pw@/run/mysqld/mysqld.sock?database=shop",
			wantDialect: "mysql", wantNetwork: "unix", wantAddress: "/run/mysqld/mysqld.sock", wantDatabase: "shop",
			wantUser: "app", wantPassword: "pw",
			wantDSN: "app:pw@unix(/run/mysqld/mysqld.sock)/shop",
		},
		{
			name:        "socket form keeps the other parameters for the driver",
			rawURL:      "maria+unix://app:pw@/run/mysqld/mysqld.sock?database=shop&parseTime=true",
			wantDialect: "mariadb", wantNetwork: "unix", wantAddress: "/run/mysqld/mysqld.sock", wantDatabase: "shop",
			wantUser: "app", wantPassword: "pw",
			wantDSN: "app:pw@unix(/run/mysqld/mysqld.sock)/shop?parseTime=true",
		},
		{
			name:        "socket form without a database selects none",
			rawURL:      "mariadb+unix://app:pw@/run/mysqld/mysqld.sock",
			wantDialect: "mariadb", wantNetwork: "unix", wantAddress: "/run/mysqld/mysqld.sock", wantDatabase: "",
			wantUser: "app", wantPassword: "pw",
			wantDSN: "app:pw@unix(/run/mysqld/mysqld.sock)/",
		},
		{
			name:        "socket form ignores a host",
			rawURL:      "mysql+unix://app@localhost/run/mysqld/mysqld.sock?database=shop",
			wantDialect: "mysql", wantNetwork: "unix", wantAddress: "/run/mysqld/mysqld.sock", wantDatabase: "shop",
			wantUser: "app", wantPassword: "",
			wantDSN: "app@unix(/run/mysqld/mysqld.sock)/shop",
		},
		{
			name:        "socket form without a path dials the driver's default socket",
			rawURL:      "mysql+unix://app:pw@localhost?database=shop",
			wantDialect: "mysql", wantNetwork: "unix", wantAddress: "/tmp/mysql.sock", wantDatabase: "shop",
			wantUser: "app", wantPassword: "pw",
			wantDSN: "app:pw@unix()/shop",
		},
		{
			name:        "socket form in upper case, without a password",
			rawURL:      "MARIA+UNIX://app@/run/mysqld/mysqld.sock?database=shop",
			wantDialect: "mariadb", wantNetwork: "unix", wantAddress: "/run/mysqld/mysqld.sock", wantDatabase: "shop",
			wantUser: "app", wantPassword: "",
			wantDSN: "app@unix(/run/mysqld/mysqld.sock)/shop",
		},
		{
			name:        "socket form decodes the credentials and the database",
			rawURL:      "mysql+unix://app:p%40ss@/run/x.sock?database=my%2Fshop",
			wantDialect: "mysql", wantNetwork: "unix", wantAddress: "/run/x.sock", wantDatabase: "my/shop",
			wantUser: "app", wantPassword: "p@ss",
			wantDSN: "app:p@ss@unix(/run/x.sock)/my%2Fshop",
		},
		{
			name:        "socket form reads the first database parameter",
			rawURL:      "mysql+unix://app@/run/x.sock?database=shop&database=other",
			wantDialect: "mysql", wantNetwork: "unix", wantAddress: "/run/x.sock", wantDatabase: "shop",
			wantUser: "app", wantPassword: "",
			wantDSN: "app@unix(/run/x.sock)/shop",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasurl.ParseMySQLURL(test.rawURL)

			c.Assert(err, qt.IsNil)
			c.Assert(got.Dialect(), qt.Equals, test.wantDialect)
			c.Assert(got.Network(), qt.Equals, test.wantNetwork)
			c.Assert(got.Address(), qt.Equals, test.wantAddress)
			c.Assert(got.Database(), qt.Equals, test.wantDatabase)
			c.Assert(got.User(), qt.Equals, test.wantUser)
			c.Assert(got.Password(), qt.Equals, test.wantPassword)
			c.Assert(got.DSN(), qt.Equals, test.wantDSN)
		})
	}
}

func TestParseMySQLURL_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		wantErr string
	}{
		{
			// The pinned community binary hands an empty database on, and the
			// server answers with a syntax error. Reading it as no database
			// would open the whole server for a URL that asked for one.
			name:    "an empty database parameter",
			rawURL:  "mysql+unix://app:secret@/run/x.sock?database=",
			wantErr: `invalid mysql\+unix URL: the database parameter is empty; name a database, or leave the parameter out`,
		},
		{
			name:    "an unterminated driver address",
			rawURL:  "mysql://app:secret@tcp(localhost:3306/shop",
			wantErr: `invalid mysql URL: invalid DSN: network address not terminated \(missing closing brace\)`,
		},
		{
			name:    "a driver address with no database separator",
			rawURL:  "maria://app:secret@tcp(localhost:3306)",
			wantErr: `invalid maria URL: invalid DSN: missing the slash separating the database name`,
		},
		{
			// The URL is not repeated, because it carries a password.
			name:    "a URL net/url refuses",
			rawURL:  "mysql://app:secret@db:notaport/shop",
			wantErr: `invalid mysql URL: invalid port ":notaport" after host`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasurl.ParseMySQLURL(test.rawURL)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, atlasurl.MySQLURL{})
		})
	}
}

func TestParseMySQLURL_RefusesAnotherDialect(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
	}{
		{name: "PostgreSQL", rawURL: "postgres://app@localhost/shop"},
		{name: "a transport the community binary refuses", rawURL: "maria+tcp://app@localhost/shop"},
		{name: "a docker dev URL", rawURL: "docker://mariadb/11/dev"},
		{name: "a driver DSN with no scheme", rawURL: "app:pw@tcp(localhost:3306)/shop"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasurl.ParseMySQLURL(test.rawURL)

			c.Assert(err, qt.ErrorIs, atlasurl.ErrNotMySQLURL)
			c.Assert(got, qt.Equals, atlasurl.MySQLURL{})
		})
	}
}

// TestMySQLURL_URL renders each form as net/url carries it. The go-sql-driver
// form is the one net/url refuses, so it is the one rendered anew.
func TestMySQLURL_URL(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{
			name:   "URL form as written",
			rawURL: "mysql://app@db:3307/shop?parseTime=true",
			want:   "mysql://app@db:3307/shop?parseTime=true",
		},
		{
			name:   "socket form as written",
			rawURL: "mysql+unix://app:pw@/run/x.sock?database=shop",
			want:   "mysql+unix://app:pw@/run/x.sock?database=shop",
		},
		{
			name:   "driver TCP form as the URL form",
			rawURL: "MARIA://app@tcp(db:3307)/shop?parseTime=true",
			want:   "maria://app@db:3307/shop?parseTime=true",
		},
		{
			name:   "driver socket form as the socket form",
			rawURL: "mysql://app@unix(/run/x.sock)/shop",
			want:   "mysql+unix://app@/run/x.sock?database=shop",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := atlasurl.ParseMySQLURL(test.rawURL)

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.URL().String(), qt.Equals, test.want)
		})
	}
}
