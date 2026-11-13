package dburldisplay_test

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/internal/dburldisplay"
)

func TestFormat(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "PostgreSQL URL with password",
			input:    "postgres://user:secret123@localhost:5432/mydb",
			expected: "postgres://user:***@localhost:5432/mydb",
		},
		{
			name:     "PostgreSQL URL with query password",
			input:    "postgres://user:secret123@localhost:5432/mydb?sslmode=disable&sslpassword=querysecret",
			expected: "postgres://user:***@localhost:5432/mydb?sslmode=disable&sslpassword=redacted",
		},
		{
			name:     "PostgreSQL URL with query password and no user password",
			input:    "postgres://user@localhost:5432/mydb?password=querysecret",
			expected: "postgres://user@localhost:5432/mydb?password=redacted",
		},
		{
			name:     "PostgreSQL URL preserves fragment while redacting query",
			input:    "postgres://user:secret123@localhost:5432/mydb?password=querysecret#frag",
			expected: "postgres://user:***@localhost:5432/mydb?password=redacted#frag",
		},
		{
			name:     "PostgreSQL URL without password",
			input:    "postgres://user@localhost:5432/mydb",
			expected: "postgres://user@localhost:5432/mydb",
		},
		{
			name:     "Invalid URL",
			input:    "not-a-url",
			expected: "not-a-url",
		},
		{
			// net/url reads the rest of the password as a fragment and refuses
			// the port it is left with.
			name:     "PostgreSQL URL with a # in its password",
			input:    "postgres://user:p#ss@localhost:5432/mydb",
			expected: "postgres://user:***@localhost:5432/mydb",
		},
		{
			name:     "PostgreSQL URL with a % that starts no escape in its password",
			input:    "postgres://user:p%zz@localhost:5432/mydb",
			expected: "postgres://user:***@localhost:5432/mydb",
		},
		{
			// Parsed, this is host user, port 5 and a path holding the rest
			// of the password; the @ in the path is what gives it away.
			name:     "PostgreSQL URL with a / in its password",
			input:    "postgres://user:5/ss@localhost/mydb",
			expected: "postgres://user:***@localhost/mydb",
		},
		{
			name:     "PostgreSQL URL with a ? in its password and a secret in its query",
			input:    "postgres://user:p?ss@localhost/mydb?sslmode=disable&sslpassword=querysecret",
			expected: "postgres://user:***@localhost/mydb?sslmode=disable&sslpassword=redacted",
		},
		{
			// Parsed, this is user p, host ss and a fragment holding the
			// rest; the first @ is part of the password.
			name:     "PostgreSQL URL with an @ and a # in its password",
			input:    "postgres://user:p@ss#word@localhost/mydb",
			expected: "postgres://user:***@localhost/mydb",
		},
		{
			// Parsed, this is host user, port 5 and a query whose key holds
			// the rest of the password.
			name:     "PostgreSQL URL with a ? after a digit in its password",
			input:    "postgres://user:5?ss@localhost/mydb",
			expected: "postgres://user:***@localhost/mydb",
		},
		{
			// Parsed, this is host user, port 5 and a query whose key holds the
			// rest of the password, and the query itself does not parse.
			name:     "PostgreSQL URL with a ? after a digit in its password and a query that does not parse",
			input:    "postgres://user:5?ss@localhost/mydb?x=%zz",
			expected: "postgres://user:***@localhost/mydb",
		},
		{
			name:     "PostgreSQL URL with an escaped @ in a query key",
			input:    "postgres://user:5?ss%40x=1@localhost/mydb",
			expected: "postgres://user:***@localhost/mydb",
		},
		{
			// An @ in the query is a value, not the end of credentials.
			name:     "PostgreSQL URL with an @ in a query value",
			input:    "postgres://localhost:5432/mydb?application_name=ops@example",
			expected: "postgres://localhost:5432/mydb?application_name=ops%40example",
		},
		{
			name:     "keyword/value string with a password",
			input:    "host=127.0.0.1 port=5432 user=app password=s3cret dbname=mydb",
			expected: "host=127.0.0.1 port=5432 user=app password=redacted dbname=mydb",
		},
		{
			// A quoted value, a backslash-escaped one, and the spacing around
			// the = signs are read the way libpq reads them and kept as written.
			name:     "keyword/value string with quoted and escaped secrets",
			input:    `host = db  password = 'it\'s a secret'  sslpassword=a\ b dbname=mydb`,
			expected: `host = db  password = redacted  sslpassword=redacted dbname=mydb`,
		},
		{
			name:     "keyword/value string without a secret",
			input:    "host=/tmp dbname=mydb sslmode=disable",
			expected: "host=/tmp dbname=mydb sslmode=disable",
		},
		{
			// The YDB SDK reads a static user and password from the
			// authority and an access token from the token parameter; it
			// reads no other credential from a URL.
			name:     "YDB URL with a user, a password and a token",
			input:    "ydbs://user:secret123@ydb.example:2135/local?token=t0k3n",
			expected: "ydbs://user:***@ydb.example:2135/local?token=redacted",
		},
		{
			name:     "YDB URL with its database in a parameter",
			input:    "ydb://localhost:2136/?database=%2Flocal&token=t0k3n",
			expected: "ydb://localhost:2136/?database=%2Flocal&token=redacted",
		},
		{
			// The YDB provider refuses a user in the monitoring endpoint, and
			// a command prints the URL before it is parsed: the line naming
			// the target must not carry the password the refusal is about.
			name:     "YDB URL with credentials in its monitoring endpoint",
			input:    "ydb://h:2136/local?monitoring=http://viewer:s3cret@mon.example:8765&go_balancer=disable",
			expected: "ydb://h:2136/local?go_balancer=disable&monitoring=http%3A%2F%2Fredacted%40mon.example%3A8765",
		},
		{
			// A user name alone can be a token, and the endpoint takes none of its own.
			name:     "YDB URL with a user name alone in its monitoring endpoint",
			input:    "ydbs://h/local?monitoring=https://t0k3n@mon.example:8765",
			expected: "ydbs://h/local?monitoring=https%3A%2F%2Fredacted%40mon.example%3A8765",
		},
		{
			name:     "YDB URL with credentials in a monitoring parameter spelled in capitals",
			input:    "ydb://h/local?MONITORING=http://viewer:s3cret@mon.example:8765",
			expected: "ydb://h/local?MONITORING=http%3A%2F%2Fredacted%40mon.example%3A8765",
		},
		{
			// No scheme, so no user info can be read: the value goes whole.
			name:     "YDB URL with credentials in a monitoring endpoint that names no scheme",
			input:    "ydb://h/local?monitoring=viewer:s3cret@mon.example:8765",
			expected: "ydb://h/local?monitoring=redacted",
		},
		{
			// The port does not parse, so neither does the endpoint.
			name:     "YDB URL with a monitoring endpoint that does not parse",
			input:    "ydb://h/local?monitoring=http://viewer:s3cret@mon.example:87x5",
			expected: "ydb://h/local?monitoring=redacted",
		},
		{
			// Parsed, this is host viewer, port 5 and a path holding the rest of
			// the password.
			name:     "YDB URL with a / in a monitoring endpoint's password",
			input:    "ydb://h/local?monitoring=http://viewer:5/s3cret@mon.example:8765",
			expected: "ydb://h/local?monitoring=redacted",
		},
		{
			// Parsed, this is user viewer, password p and a path with an @ in it.
			name:     "YDB URL with an @ and a / in a monitoring endpoint's password",
			input:    "ydb://h/local?monitoring=http://viewer:p@ss/s3cret@mon.example:8765",
			expected: "ydb://h/local?monitoring=redacted",
		},
		{
			name:     "YDB URL with a monitoring endpoint and no credentials",
			input:    "ydb://h/local?monitoring=http://mon.example:8765",
			expected: "ydb://h/local?monitoring=http%3A%2F%2Fmon.example%3A8765",
		},
		{
			// The parameter is YDB's; another scheme's URL is left as it is,
			// as the MySQL rows below leave a URL-shaped value alone.
			name:     "PostgreSQL URL with a monitoring parameter",
			input:    "postgres://user@localhost:5432/mydb?monitoring=http://viewer:s3cret@mon.example:8765",
			expected: "postgres://user@localhost:5432/mydb?monitoring=http%3A%2F%2Fviewer%3As3cret%40mon.example%3A8765",
		},
		{
			name:     "MySQL URL with password",
			input:    "mysql://root:password@localhost:3306/testdb",
			expected: "mysql://root:***@localhost:3306/testdb",
		},
		{
			name:     "MySQL tcp URL with query secret",
			input:    "mysql://root:password@tcp(localhost:3306)/testdb?parseTime=true&sslpassword=querysecret",
			expected: "mysql://root:***@tcp(localhost:3306)/testdb?parseTime=true&sslpassword=redacted",
		},
		{
			name:     "MySQL tcp URL does not redact embedded query URL credentials",
			input:    "mysql://root@tcp(localhost:3306)/testdb?callback=https%3A%2F%2Fx%3Ay%40example.test&password=querysecret",
			expected: "mysql://root@tcp(localhost:3306)/testdb?callback=https%3A%2F%2Fx%3Ay%40example.test&password=redacted",
		},
		{
			// net/url refuses the tcp() form, so only the MySQL-family branch
			// can redact it: a scheme the connector opens and that branch did
			// not recognize would print the password whole.
			name:     "maria tcp URL with password",
			input:    "maria://root:password@tcp(localhost:3306)/testdb",
			expected: "maria://root:***@tcp(localhost:3306)/testdb",
		},
		{
			name:     "upper-case MariaDB tcp URL with password",
			input:    "MARIADB://root:password@tcp(localhost:3306)/testdb",
			expected: "MARIADB://root:***@tcp(localhost:3306)/testdb",
		},
		{
			name:     "maria URL with password",
			input:    "maria://root:password@localhost:3306/testdb",
			expected: "maria://root:***@localhost:3306/testdb",
		},
		{
			name:     "socket URL with password",
			input:    "mysql+unix://root:password@/run/mysqld/mysqld.sock?database=testdb",
			expected: "mysql+unix://root:***@/run/mysqld/mysqld.sock?database=testdb",
		},
		{
			name:     "SQL Server URL with password and query secret",
			input:    "sqlserver://sa:VerySecret@localhost:1433?database=ptah&password=querysecret&encrypt=disable",
			expected: "sqlserver://sa:***@localhost:1433?database=ptah&encrypt=disable&password=redacted",
		},
		{
			// go-sql-driver reads the password up to the last @ before the
			// last /, so an @ and a / in it are part of it.
			name:     "MySQL tcp URL with an @ and a / in its password",
			input:    "mysql://root:p@ss/w0rd@tcp(localhost:3306)/testdb",
			expected: "mysql://root:***@tcp(localhost:3306)/testdb",
		},
		{
			name:     "MySQL tcp URL with a ? in its password",
			input:    "mysql://root:p?ss@tcp(localhost:3306)/testdb?parseTime=true",
			expected: "mysql://root:***@tcp(localhost:3306)/testdb?parseTime=true",
		},
		{
			name:     "MySQL tcp URL with a # in its password",
			input:    "mysql://root:p#ss@tcp(localhost:3306)/testdb",
			expected: "mysql://root:***@tcp(localhost:3306)/testdb",
		},
		{
			name:     "MySQL tcp URL with an @ in a parameter",
			input:    "mysql://root:password@tcp(localhost:3306)/testdb?connectionAttributes=owner:ops@example",
			expected: "mysql://root:***@tcp(localhost:3306)/testdb?connectionAttributes=owner%3Aops%40example",
		},
		{
			// net/url reads the socket's parenthesis as a host and the @ in
			// the database as the end of the credentials.
			name:     "MySQL unix URL with an @ in its database",
			input:    "mysql://root:password@unix(/run/mysqld/mysqld.sock)/sh@p",
			expected: "mysql://root:***@unix(/run/mysqld/mysqld.sock)/sh@p",
		},
		{
			name:     "MySQL unix URL with password",
			input:    "mysql://root:password@unix(/run/mysqld/mysqld.sock)/testdb?parseTime=true",
			expected: "mysql://root:***@unix(/run/mysqld/mysqld.sock)/testdb?parseTime=true",
		},
		{
			// The driver reads the last @ before the last /, so everything
			// before this one is what it would send as a password.
			name:     "MySQL tcp URL with an @ the driver reads after the address",
			input:    "mysql://root:s3cret@tcp(localhost:3306)/x@y/testdb",
			expected: "mysql://root:***@y/testdb",
		},
		{
			name:     "MySQL address with no credentials",
			input:    "mysql://tcp(localhost:3306)/testdb?parseTime=true",
			expected: "mysql://tcp(localhost:3306)/testdb?parseTime=true",
		},
		{
			// The driver reads the first / it finds from the end as the
			// database's, so with none after the address it reads this one.
			name:     "MySQL tcp URL with a / in its password and no database",
			input:    "mysql://root:p/ss@tcp(localhost:3306)",
			expected: "mysql://root:***@tcp(localhost:3306)",
		},
		{
			name:     "MySQL tcp URL whose address does not close",
			input:    "mysql://root:p/ss@tcp(localhost:3306/testdb",
			expected: "mysql://root:***@tcp(localhost:3306/testdb",
		},
		{
			name:     "a scheme no driver reads, in front of the tcp form",
			input:    "tidb://root:p@ss/word@tcp(localhost:3306)/testdb",
			expected: "tidb://root:***@tcp(localhost:3306)/testdb",
		},
		{
			name:     "driver DSN with no database separator",
			input:    "app:s3cretpw@tcp(localhost:3306)",
			expected: "app:***@tcp(localhost:3306)",
		},
		{
			// The driver reads no credentials here, since no @ comes before
			// the last /, but the text before the last @ reads as a password.
			name:     "driver DSN with a / in its password and no database separator",
			input:    "app:s3c/retpw@tcp(localhost:3306)",
			expected: "app:***@tcp(localhost:3306)",
		},
		{
			// An empty password is hidden as a URL's is: the display does
			// not say whether a password is empty.
			name:     "driver DSN with an empty password",
			input:    "app:@tcp(localhost:3306)/shop",
			expected: "app:***@tcp(localhost:3306)/shop",
		},
		{
			name:     "driver DSN with no password and a secret parameter",
			input:    "app@tcp(localhost:3306)/shop?password=querysecret",
			expected: "app@tcp(localhost:3306)/shop?password=redacted",
		},
		{
			name:     "driver DSN with a :// in its password",
			input:    "app:p://w@tcp(localhost:3306)/shop",
			expected: "app:***@tcp(localhost:3306)/shop",
		},
		{
			name:     "driver DSN with a host and no network",
			input:    "app:s3cretpw@localhost:3306/shop",
			expected: "app:***@localhost:3306/shop",
		},
		{
			name:     "address with no scheme and a leading //",
			input:    "//app:s3cretpw@localhost:5432/shop",
			expected: "//app:***@localhost:5432/shop",
		},
		{
			// With nothing after the password, what net/url refused may be all
			// of it: this is the sentence that printed `invalid port ":s3cretpw"`.
			name:     "PostgreSQL URL with a password and no host",
			input:    "postgres://app:s3cret#pw",
			expected: "postgres://app:***",
		},
		{
			name:     "PostgreSQL URL with a password and no end to it",
			input:    "postgres://app:s3cr@et/w#or%zzd",
			expected: "postgres://app:***@***",
		},
		{
			name:     "PostgreSQL URL with a port that is a number and a path net/url refuses",
			input:    "postgres://localhost:5432/sh%zzop",
			expected: "postgres://localhost:5432/sh%zzop",
		},
		{
			name:     "PostgreSQL URL with a leading space",
			input:    " postgres://user:p/ss@localhost/mydb",
			expected: " postgres://user:***@localhost/mydb",
		},
		{
			// The value is what the parameter holds, not where the URL ends.
			name:     "PostgreSQL URL with a # in a password and a parameter that holds a URL",
			input:    "postgres://user:p#ss@localhost/mydb?sslpassword=querysecret&application_name=x",
			expected: "postgres://user:***@localhost/mydb?application_name=x&sslpassword=redacted",
		},
		{
			name:     "libsql URL with an auth token",
			input:    "libsql://db.example?authToken=t0k3n",
			expected: "libsql://db.example?authToken=redacted",
		},
		{
			name:     "libsql URL with an auth token spelled in snake case",
			input:    "libsql://db.example?auth_token=t0k3n",
			expected: "libsql://db.example?auth_token=redacted",
		},
		{
			name:     "libsql URL with a JWT",
			input:    "libsql://db.example?jwt=t0k3n",
			expected: "libsql://db.example?jwt=redacted",
		},
		{
			name:     "SQL Server URL with a login password to change",
			input:    "sqlserver://sa@localhost?change%20password=s3cret",
			expected: "sqlserver://sa@localhost?change+password=redacted",
		},
		{
			name:     "Oracle URL with a wallet password",
			input:    "oracle://app@localhost:1521/svc?WALLET%20PASSWORD=s3cret",
			expected: "oracle://app@localhost:1521/svc?WALLET+PASSWORD=redacted",
		},
		{
			name:     "Oracle URL with an OS password",
			input:    "oracle://app@localhost:1521/svc?OS%20PASS=s3cret",
			expected: "oracle://app@localhost:1521/svc?OS+PASS=redacted",
		},
		{
			name:     "Oracle URL with an OS password hash",
			input:    "oracle://app@localhost:1521/svc?OS%20HASH=s3cret",
			expected: "oracle://app@localhost:1521/svc?OS+HASH=redacted",
		},
		{
			name:     "Oracle URL with a token",
			input:    "oracle://app@localhost:1521/svc?TOKEN=s3cret",
			expected: "oracle://app@localhost:1521/svc?TOKEN=redacted",
		},
		{
			name:     "ClickHouse URL with a password in its query",
			input:    "clickhouse://localhost:9000/db?password=s3cret",
			expected: "clickhouse://localhost:9000/db?password=redacted",
		},
		{
			// The driver reads a proxy, and its credentials, from this parameter.
			name:     "ClickHouse URL with credentials in its proxy",
			input:    "clickhouse://localhost:9000/db?http_proxy=http://u:s3cret@proxy:3128",
			expected: "clickhouse://localhost:9000/db?http_proxy=http%3A%2F%2Fredacted%40proxy%3A3128",
		},
		{
			name:     "SQLite URL with a key",
			input:    "sqlite:file:app.db?_key=s3cret&mode=ro",
			expected: "sqlite:file:app.db?_key=redacted&mode=ro",
		},
		{
			name:     "SQLite URL with an @ in its file name",
			input:    "sqlite:file:/tmp/a@b.db",
			expected: "sqlite:file:/tmp/a@b.db",
		},
		{
			name:     "SQLite URL with an @ in its file name and a secret",
			input:    "sqlite:file:/tmp/a@b.db?_auth_pass=s3cret",
			expected: "sqlite:file:/tmp/a@b.db?_auth_pass=redacted",
		},
		{
			name:     "SQLite URL with a Windows path",
			input:    `sqlite://C:\dir\app.db?mode=ro`,
			expected: `sqlite://C:\dir\app.db?mode=ro`,
		},
		{
			name:     "a local path with an @ and a :",
			input:    "/tmp/a:b@c/schema.hcl",
			expected: "/tmp/a:b@c/schema.hcl",
		},
		{
			name:     "a Windows path with an @",
			input:    `C:\Users\me@corp\schema.hcl`,
			expected: `C:\Users\me@corp\schema.hcl`,
		},
		{
			name:     "a local path with a space",
			input:    "my dir/schema.hcl",
			expected: "my dir/schema.hcl",
		},
		{
			name:     "the in-memory SQLite name",
			input:    ":memory:",
			expected: ":memory:",
		},
		{
			name:     "an env reference",
			input:    "env://schema.src",
			expected: "env://schema.src",
		},
		{
			name:     "ADO string",
			input:    "server=localhost;user id=sa;password=s3cret pw;database=ptah",
			expected: "server=localhost;user id=sa;password=redacted;database=ptah",
		},
		{
			name:     "ADO string with a colon in its server and an @ in its password",
			input:    "server=tcp:localhost,1433;uid=sa;pwd=s3@ret;database=ptah",
			expected: "server=tcp:localhost,1433;uid=sa;pwd=redacted;database=ptah",
		},
		{
			name:     "ODBC string with a password in braces",
			input:    "odbc:server=localhost;uid=sa;pwd={s3;cr}}et};database=ptah",
			expected: "odbc:server=localhost;uid=sa;pwd=redacted;database=ptah",
		},
		{
			name:     "ODBC string with a brace that never closes",
			input:    "odbc:server=localhost;pwd={s3;cret",
			expected: "odbc:server=localhost;pwd=redacted",
		},
		{
			// Nothing says where the value stops, so all of what follows goes.
			name:     "keyword/value string with a quote that never closes",
			input:    "host=localhost password='s3cret dbname=mydb",
			expected: "host=localhost password=redacted",
		},
		{
			name:     "keyword/value string with a :// in its password",
			input:    "host=localhost password=s3://cret dbname=mydb",
			expected: "host=localhost password=redacted dbname=mydb",
		},
		{
			name:     "keyword/value string with a ; in its password",
			input:    "host=localhost password=s3;cret dbname=mydb",
			expected: "host=localhost password=redacted dbname=mydb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			result := dburldisplay.Format(tt.input)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

// A password is hidden where go-sql-driver reads it, whatever it holds, and the
// user, the network, the address, the database and the parameters stay.
func TestFormat_DriverDSNWithPassword(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		expected string
	}{
		{
			name:     "tcp",
			dsn:      "app:Pa55wordX9@tcp(127.0.0.1:3306)/shop",
			expected: "app:***@tcp(127.0.0.1:3306)/shop",
		},
		{
			name:     "tcp with parameters",
			dsn:      "app:Pa55wordX9@tcp(db.internal:3307)/shop?parseTime=true&loc=Local",
			expected: "app:***@tcp(db.internal:3307)/shop?loc=Local&parseTime=true",
		},
		{
			name:     "unix socket",
			dsn:      "app:Pa55wordX9@unix(/run/mysqld/mysqld.sock)/shop",
			expected: "app:***@unix(/run/mysqld/mysqld.sock)/shop",
		},
		{
			name:     "unix socket with parameters",
			dsn:      "app:Pa55wordX9@unix(/tmp/mysql.sock)/shop?charset=utf8mb4&parseTime=true",
			expected: "app:***@unix(/tmp/mysql.sock)/shop?charset=utf8mb4&parseTime=true",
		},
		{
			// No network: the driver dials tcp to localhost:3306.
			name:     "no network",
			dsn:      "app:Pa55wordX9@/shop",
			expected: "app:***@/shop",
		},
		{
			name:     "no database",
			dsn:      "app:Pa55wordX9@tcp(127.0.0.1:3306)/",
			expected: "app:***@tcp(127.0.0.1:3306)/",
		},
		{
			name:     "an @ in the password",
			dsn:      "app:Pa55@wordX9@tcp(127.0.0.1:3306)/shop",
			expected: "app:***@tcp(127.0.0.1:3306)/shop",
		},
		{
			name:     "a : in the password",
			dsn:      "app:Pa55:wordX9@tcp(127.0.0.1:3306)/shop",
			expected: "app:***@tcp(127.0.0.1:3306)/shop",
		},
		{
			name:     "an @, a : and a / in the password",
			dsn:      "app:P@:5/wordX9@tcp(127.0.0.1:3306)/shop",
			expected: "app:***@tcp(127.0.0.1:3306)/shop",
		},
		{
			name:     "every reserved character in the password",
			dsn:      "app:Zq7Kp@Xk9Lm/Wm3Nt#Vp8Qr%Ty4Hs:Rn6Jd?Lh2Fb@tcp(127.0.0.1:3306)/shop",
			expected: "app:***@tcp(127.0.0.1:3306)/shop",
		},
		{
			// An @ after the last / is the database's or a parameter's.
			name:     "an @ in a parameter",
			dsn:      "app:Pa55wordX9@tcp(127.0.0.1:3306)/shop?connectionAttributes=owner:ops@example",
			expected: "app:***@tcp(127.0.0.1:3306)/shop?connectionAttributes=owner%3Aops%40example",
		},
		{
			name:     "an @ in the database",
			dsn:      "app:Pa55wordX9@tcp(127.0.0.1:3306)/sh@p",
			expected: "app:***@tcp(127.0.0.1:3306)/sh@p",
		},
		{
			name:     "a secret parameter",
			dsn:      "app:Pa55wordX9@tcp(127.0.0.1:3306)/shop?password=querysecret",
			expected: "app:***@tcp(127.0.0.1:3306)/shop?password=redacted",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			config, err := mysqldriver.ParseDSN(test.dsn)
			c.Assert(err, qt.IsNil)
			c.Assert(config.Passwd, qt.Not(qt.Equals), "")

			got := dburldisplay.Format(test.dsn)

			c.Assert(got, qt.Equals, test.expected)
			c.Assert(got, qt.Not(qt.Contains), config.Passwd)
		})
	}
}

// A DSN with no password keeps its text, the user included.
func TestFormat_DriverDSNWithoutPassword(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
	}{
		{name: "a user", dsn: "app@tcp(127.0.0.1:3306)/shop"},
		{name: "a user over a unix socket", dsn: "app@unix(/tmp/mysql.sock)/shop"},
		{name: "no credentials", dsn: "tcp(127.0.0.1:3306)/shop"},
		{name: "a database alone", dsn: "/shop"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			config, err := mysqldriver.ParseDSN(test.dsn)
			c.Assert(err, qt.IsNil)
			c.Assert(config.Passwd, qt.Equals, "")

			c.Assert(dburldisplay.Format(test.dsn), qt.Equals, test.dsn)
		})
	}
}

// passwordsWithReservedCharacters each name the parts a reader could recognize,
// whichever of them a parser stops at.
var passwordsWithReservedCharacters = []struct {
	password string
	parts    []string
}{
	{password: "Zq7KpXk9Lm", parts: []string{"Zq7KpXk9Lm"}},
	{password: "Zq7Kp@Xk9Lm", parts: []string{"Zq7Kp", "Xk9Lm"}},
	{password: "Zq7Kp/Xk9Lm", parts: []string{"Zq7Kp", "Xk9Lm"}},
	{password: "Zq7Kp#Xk9Lm", parts: []string{"Zq7Kp", "Xk9Lm"}},
	{password: "Zq7Kp%Xk9Lm", parts: []string{"Zq7Kp", "Xk9Lm"}},
	{password: "Zq7Kp:Xk9Lm", parts: []string{"Zq7Kp", "Xk9Lm"}},
	{password: "Zq7Kp?Xk9Lm", parts: []string{"Zq7Kp", "Xk9Lm"}},
	{password: "Zq7Kp%zzXk9Lm", parts: []string{"Zq7Kp", "zzXk9Lm"}},
	{password: "12345/Xk9Lm", parts: []string{"12345", "Xk9Lm"}},
	{password: "12345?Xk9Lm", parts: []string{"12345", "Xk9Lm"}},
	{password: "12345#Xk9Lm", parts: []string{"12345", "Xk9Lm"}},
	{password: "Zq7Kp@Xk9Lm#Vp8Qr", parts: []string{"Zq7Kp", "Xk9Lm", "Vp8Qr"}},
	{password: "Zq7Kp@Xk9Lm/Vp8Qr", parts: []string{"Zq7Kp", "Xk9Lm", "Vp8Qr"}},
	{password: "Zq7Kp@Xk9Lm?Vp8Qr", parts: []string{"Zq7Kp", "Xk9Lm", "Vp8Qr"}},
	{password: "Zq7Kp@Xk9Lm/Wm3Nt#Vp8Qr%Ty4Hs:Rn6Jd?Lh2Fb", parts: []string{"Zq7Kp", "Xk9Lm", "Wm3Nt", "Vp8Qr", "Ty4Hs", "Rn6Jd", "Lh2Fb"}},
}

// addressForms are the shapes an address can hold a password in, with PASSWORD
// where it goes and the text that must still be there around it.
var addressForms = []struct {
	name    string
	address string
	keeps   string
}{
	{name: "postgres", address: "postgres://app:PASSWORD@db.internal:5432/shop", keeps: "db.internal:5432/shop"},
	{name: "postgresql", address: "postgresql://app:PASSWORD@db.internal:5432/shop", keeps: "db.internal:5432/shop"},
	{name: "cockroachdb", address: "cockroachdb://app:PASSWORD@db.internal:26257/shop", keeps: "db.internal:26257/shop"},
	{name: "yugabytedb", address: "yugabytedb://app:PASSWORD@db.internal:5433/shop", keeps: "db.internal:5433/shop"},
	{name: "spanner", address: "spanner://app:PASSWORD@db.internal:5432/shop", keeps: "db.internal:5432/shop"},
	{name: "sqlserver", address: "sqlserver://app:PASSWORD@db.internal:1433?database=shop", keeps: "db.internal:1433?database=shop"},
	{name: "oracle", address: "oracle://app:PASSWORD@db.internal:1521/shop", keeps: "db.internal:1521/shop"},
	{name: "clickhouse", address: "clickhouse://app:PASSWORD@db.internal:9000/shop", keeps: "db.internal:9000/shop"},
	{name: "libsql", address: "libsql://app:PASSWORD@db.internal:8080", keeps: "db.internal:8080"},
	{name: "ydb", address: "ydb://app:PASSWORD@db.internal:2136/local", keeps: "db.internal:2136/local"},
	{name: "a scheme nothing serves", address: "unknown://app:PASSWORD@db.internal/shop", keeps: "db.internal/shop"},
	{name: "mysql URL", address: "mysql://app:PASSWORD@db.internal:3306/shop", keeps: "db.internal:3306/shop"},
	{name: "mariadb URL", address: "mariadb://app:PASSWORD@db.internal:3306/shop", keeps: "db.internal:3306/shop"},
	{name: "maria URL", address: "maria://app:PASSWORD@db.internal:3306/shop", keeps: "db.internal:3306/shop"},
	{name: "mysql socket URL", address: "mysql+unix://app:PASSWORD@/tmp/m.sock?database=shop", keeps: "/tmp/m.sock?database=shop"},
	{name: "mysql tcp", address: "mysql://app:PASSWORD@tcp(db.internal:3306)/shop", keeps: "tcp(db.internal:3306)/shop"},
	{name: "mariadb tcp", address: "mariadb://app:PASSWORD@tcp(db.internal:3306)/shop", keeps: "tcp(db.internal:3306)/shop"},
	{name: "maria tcp", address: "maria://app:PASSWORD@tcp(db.internal:3306)/shop", keeps: "tcp(db.internal:3306)/shop"},
	{name: "upper case tcp", address: "MARIADB://app:PASSWORD@tcp(db.internal:3306)/shop", keeps: "tcp(db.internal:3306)/shop"},
	{name: "mysql unix", address: "mysql://app:PASSWORD@unix(/tmp/m.sock)/shop", keeps: "unix(/tmp/m.sock)/shop"},
	{name: "mysql tcp with no database", address: "mysql://app:PASSWORD@tcp(db.internal:3306)", keeps: "tcp(db.internal:3306)"},
	{name: "mysql tcp with parameters", address: "mysql://app:PASSWORD@tcp(db.internal:3306)/shop?parseTime=true", keeps: "tcp(db.internal:3306)/shop?parseTime=true"},
	{name: "DSN tcp", address: "app:PASSWORD@tcp(db.internal:3306)/shop", keeps: "tcp(db.internal:3306)/shop"},
	{name: "DSN with a user net/url refuses as a scheme", address: "my_app:PASSWORD@tcp(db.internal:3306)/shop", keeps: "tcp(db.internal:3306)/shop"},
	{name: "DSN unix", address: "app:PASSWORD@unix(/tmp/m.sock)/shop", keeps: "unix(/tmp/m.sock)/shop"},
	{name: "DSN with no network", address: "app:PASSWORD@/shop", keeps: "/shop"},
	{name: "DSN with a host", address: "app:PASSWORD@db.internal:3306/shop", keeps: "db.internal:3306/shop"},
	{name: "DSN with no database", address: "app:PASSWORD@tcp(db.internal:3306)", keeps: "tcp(db.internal:3306)"},
	{name: "a port that is not a number", address: "postgres://app:PASSWORD@db.internal:54x2/shop", keeps: "db.internal:54x2/shop"},
	{name: "a host net/url refuses", address: "postgres://app:PASSWORD@bad^host/shop", keeps: "bad^host/shop"},
	{name: "a leading space", address: " postgres://app:PASSWORD@db.internal/shop", keeps: "db.internal/shop"},
	{name: "a scheme net/url refuses", address: "1pg://app:PASSWORD@db.internal/shop", keeps: "db.internal/shop"},
	{name: "a // and no scheme", address: "//app:PASSWORD@db.internal/shop", keeps: "db.internal/shop"},
}

// Whatever a password holds and whichever form holds it, no part of it reaches
// the display, and the address around it does.
func TestFormat_HidesEveryPartOfThePassword(t *testing.T) {
	for _, form := range addressForms {
		for _, password := range passwordsWithReservedCharacters {
			t.Run(form.name+"/"+password.password, func(t *testing.T) {
				c := qt.New(t)
				address := strings.ReplaceAll(form.address, "PASSWORD", password.password)

				got := dburldisplay.Format(address)

				for _, part := range password.parts {
					c.Assert(got, qt.Not(qt.Contains), part)
				}
				c.Assert(got, qt.Contains, form.keeps)
				c.Assert(got, qt.Contains, "app")
			})
		}
	}
}

// secretSettings are the ways an engine takes a secret by name, with VALUE where
// it goes.
var secretSettings = []struct {
	name    string
	address string
	keeps   string
}{
	{name: "PostgreSQL password", address: "postgres://db.internal/shop?password=VALUE", keeps: "db.internal/shop"},
	{name: "PostgreSQL sslpassword", address: "postgres://db.internal/shop?sslpassword=VALUE", keeps: "db.internal/shop"},
	{name: "PostgreSQL sslkey", address: "postgres://db.internal/shop?sslkey=VALUE", keeps: "db.internal/shop"},
	{name: "a parameter in capitals", address: "postgres://db.internal/shop?PASSWORD=VALUE", keeps: "db.internal/shop"},
	{name: "SQL Server password", address: "sqlserver://db.internal?database=shop&password=VALUE", keeps: "database=shop"},
	{name: "SQL Server pwd", address: "sqlserver://db.internal?database=shop&pwd=VALUE", keeps: "database=shop"},
	{name: "SQL Server change password", address: "sqlserver://app@db.internal?database=shop&change%20password=VALUE", keeps: "database=shop"},
	{name: "ClickHouse password", address: "clickhouse://db.internal/shop?password=VALUE", keeps: "db.internal/shop"},
	{name: "ClickHouse proxy", address: "clickhouse://db.internal/shop?http_proxy=http://u:VALUE@proxy:3128", keeps: "db.internal/shop"},
	{name: "libsql authToken", address: "libsql://db.internal?authToken=VALUE", keeps: "db.internal"},
	{name: "libsql auth_token", address: "libsql://db.internal?auth_token=VALUE", keeps: "db.internal"},
	{name: "libsql jwt", address: "libsql://db.internal?jwt=VALUE", keeps: "db.internal"},
	{name: "Oracle wallet password", address: "oracle://db.internal/shop?WALLET%20PASSWORD=VALUE", keeps: "db.internal/shop"},
	{name: "Oracle OS pass", address: "oracle://db.internal/shop?OS%20PASS=VALUE", keeps: "db.internal/shop"},
	{name: "Oracle OS password", address: "oracle://db.internal/shop?OS%20PASSWORD=VALUE", keeps: "db.internal/shop"},
	{name: "Oracle OS hash", address: "oracle://db.internal/shop?OS%20HASH=VALUE", keeps: "db.internal/shop"},
	{name: "Oracle OS passhash", address: "oracle://db.internal/shop?OS%20PASSHASH=VALUE", keeps: "db.internal/shop"},
	{name: "Oracle token", address: "oracle://db.internal/shop?TOKEN=VALUE", keeps: "db.internal/shop"},
	{name: "Spanner credentials", address: "spanner://projects/p/instances/i/databases/d?credentials=VALUE", keeps: "databases/d"},
	{name: "YDB token", address: "ydb://db.internal/local?token=VALUE", keeps: "db.internal/local"},
	{name: "YDB password", address: "ydb://db.internal/local?password=VALUE", keeps: "db.internal/local"},
	{name: "a provider's parameter that holds a URL", address: "ydb://db.internal/local?monitoring=http://u:VALUE@mon:8765", keeps: "db.internal/local"},
	{name: "SQLite auth", address: "sqlite://app.db?_auth_pass=VALUE", keeps: "app.db"},
	{name: "SQLite key", address: "sqlite:file:app.db?_key=VALUE", keeps: "app.db"},
	{name: "a DSN's parameter", address: "mysql://app@tcp(db.internal:3306)/shop?password=VALUE", keeps: "tcp(db.internal:3306)/shop"},
	{name: "libpq keyword", address: "host=db.internal password=VALUE dbname=shop", keeps: "host=db.internal"},
	{name: "libpq sslpassword", address: "host=db.internal sslpassword=VALUE dbname=shop", keeps: "host=db.internal"},
	{name: "a server option", address: "postgres://db.internal/shop?options=-c%20app.jwt_secret%3DVALUE", keeps: "db.internal/shop"},
	{
		name:    "a server option after one that is no secret",
		address: "postgres://db.internal/shop?options=-c%20search_path%3Dapp%20--app.api_key%3DVALUE",
		keeps:   "search_path%3Dapp",
	},
	{name: "a libpq server option", address: "host=db.internal options='-c app.jwt_secret=VALUE' dbname=shop", keeps: "host=db.internal"},
	{name: "ADO password", address: "server=db.internal;user id=app;password=VALUE;database=shop", keeps: "server=db.internal"},
	{name: "ODBC pwd", address: "odbc:server=db.internal;uid=app;pwd={VALUE};database=shop", keeps: "server=db.internal"},
}

// A secret an engine takes by name is hidden whether the value is written with
// its reserved characters escaped or not.
func TestFormat_HidesASecretSetting(t *testing.T) {
	const value = "Zq7Kp@Xk9Lm/Wm3Nt#Vp8Qr%Ty4Hs:Rn6Jd?Lh2Fb"
	parts := []string{"Zq7Kp", "Xk9Lm", "Wm3Nt", "Vp8Qr", "Ty4Hs", "Rn6Jd", "Lh2Fb"}
	for _, setting := range secretSettings {
		t.Run(setting.name, func(t *testing.T) {
			c := qt.New(t)
			address := strings.ReplaceAll(setting.address, "VALUE", url.QueryEscape(value))

			got := dburldisplay.Format(address)

			for _, part := range parts {
				c.Assert(got, qt.Not(qt.Contains), part)
			}
			c.Assert(got, qt.Contains, setting.keeps)
		})
	}
}

// A parameter that holds no secret keeps its value.
func TestFormat_KeepsAParameterThatIsNoSecret(t *testing.T) {
	tests := []struct {
		name    string
		address string
	}{
		{name: "sslmode", address: "postgres://db.internal/shop?sslmode=require"},
		{name: "a file of passwords, not a password", address: "postgres://db.internal/shop?passfile=.pgpass"},
		{name: "a user", address: "clickhouse://db.internal/shop?username=app"},
		{name: "a database", address: "sqlserver://db.internal?database=shop"},
		{name: "parseTime", address: "mysql://app@tcp(db.internal:3306)/shop?parseTime=true"},
		{name: "a server option", address: "postgres://db.internal/shop?options=-c+statement_timeout%3D5s"},
		{name: "a libpq server option", address: "host=db.internal options='-c statement_timeout=5s' dbname=shop"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(dburldisplay.Format(test.address), qt.Equals, test.address)
		})
	}
}

// ParseURL names the URL as Format does, and keeps net/url's reason where the
// reason is about what Format shows: net/url quotes the URL it refused and often
// part of it, which for a password holding a "#" or a stray "%" is part of the
// password.
func TestParseURL_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{
			name:   "a password with a # in it",
			rawURL: "postgres://user:Pa5#5wordX9@localhost:5432/mydb",
			want: `parse "postgres://user:***@localhost:5432/mydb": the user name or password cannot be read as written; ` +
				`percent-encode the characters a URL reserves, such as @ : / ? # and %`,
		},
		{
			name:   "a password with a % that starts no escape",
			rawURL: "postgres://user:Pa%zz5wordX9@localhost:5432/mydb",
			want: `parse "postgres://user:***@localhost:5432/mydb": the user name or password cannot be read as written; ` +
				`percent-encode the characters a URL reserves, such as @ : / ? # and %`,
		},
		{
			name:   "a driver DSN whose user net/url refuses as a scheme",
			rawURL: "my_app:Pa55wordX9@tcp(127.0.0.1:3306)/shop",
			want:   `parse "my_app:***@tcp(127.0.0.1:3306)/shop": first path segment in URL cannot contain colon`,
		},
		{
			// The port is outside what Format hides, so its reason stays.
			name:   "a port that is not a number, behind a password",
			rawURL: "postgres://user:Pa55wordX9@localhost:54x2/mydb",
			want:   `parse "postgres://user:***@localhost:54x2/mydb": invalid port ":54x2" after host`,
		},
		{
			name:   "a host net/url refuses, behind a password",
			rawURL: "postgres://user:Pa55wordX9@bad^host/mydb",
			want:   `parse "postgres://user:***@bad^host/mydb": invalid character "^" in host name`,
		},
		{
			name:   "a scheme net/url refuses",
			rawURL: "1pg://user:Pa55wordX9@localhost/mydb",
			want:   `parse "1pg://user:***@localhost/mydb": first path segment in URL cannot contain colon`,
		},
		{
			name:   "a password and no host",
			rawURL: "postgres://user:Pa55wordX9",
			want:   `parse "postgres://user:***": invalid port ":***" after host`,
		},
		{
			// Nothing to hide: net/url's own error, as it was.
			name:   "a URL with no password and a port that is not a number",
			rawURL: "postgres://localhost/my%zzdb",
			want:   `parse "postgres://localhost/my%zzdb": invalid URL escape "%zz"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := dburldisplay.ParseURL(test.rawURL)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.want))
			c.Assert(err, qt.ErrorAs, new(*url.Error))
			c.Assert(err, qt.Not(qt.ErrorMatches), `.*(Pa5|wordX9|5wo).*`)
			c.Assert(got, qt.IsNil)
		})
	}
}

// A URL net/url reads comes back as net/url reads it.
func TestParseURL_HappyPath(t *testing.T) {
	c := qt.New(t)

	got, err := dburldisplay.ParseURL("postgres://user:Pa55wordX9@localhost:5432/mydb")

	c.Assert(err, qt.IsNil)
	password, hasPassword := got.User.Password()
	c.Assert(hasPassword, qt.IsTrue)
	c.Assert(password, qt.Equals, "Pa55wordX9")
	c.Assert(got.Host, qt.Equals, "localhost:5432")
}

// errFromDriver stands for the error of a driver that was handed the address.
var errFromDriver = errors.New("the driver refused it")

// RedactError takes out of a driver's error what Format hides, and each part of
// it, since a driver reports what it read: a host and a database that are pieces
// of a password. The error it wraps still answers errors.Is.
func TestRedactError_HidesWhatTheDriverRead(t *testing.T) {
	tests := []struct {
		name    string
		address string
		message string
		want    string
	}{
		{
			name:    "the password as it is written",
			address: "postgres://app:Pa55wordX9@localhost/shop",
			message: "connect to postgres://app:Pa55wordX9@localhost/shop failed",
			want:    "connect to postgres://app:***@localhost/shop failed",
		},
		{
			name:    "the password unescaped",
			address: "postgres://app:Pa55%40wordX9@localhost/shop",
			message: "login as app with Pa55@wordX9 failed",
			want:    "login as app with *** failed",
		},
		{
			// net/url reads host app, port 12345 and this database.
			name:    "a part of the password read as a database",
			address: "postgres://app:12345/Xk9Lm@localhost/shop",
			message: "failed to connect to `user=app database=Xk9Lm@localhost/shop`",
			want:    "failed to connect to `user=app database=***@localhost/shop`",
		},
		{
			// net/url reads user app, password Zq7Kp and this host.
			name:    "a part of the password read as a host",
			address: "postgres://app:Zq7Kp@Xk9Lm#Vp8Qr@localhost/shop",
			message: "lookup Xk9Lm: no such host",
			want:    "lookup ***: no such host",
		},
		{
			name:    "the value of a secret parameter",
			address: "postgres://localhost/shop?sslkey=%2Ftmp%2FXk9Lm",
			message: "unable to read sslkey: open /tmp/Xk9Lm: no such file or directory",
			want:    "unable to read sslkey: open ***: no such file or directory",
		},
		{
			name:    "a DSN's password",
			address: "app:Zq7Kp/Xk9Lm@tcp(localhost:3306)/shop",
			message: "cannot parse `app:xxxxxx/Xk9Lm@tcp(localhost:3306)/shop`",
			want:    "cannot parse `app:xxxxxx/***@tcp(localhost:3306)/shop`",
		},
		{
			// A part this short would take letters out of every other word.
			name:    "a part too short to look for",
			address: "postgres://app:a/b@localhost/shop",
			message: "a bad database",
			want:    "a bad database",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := fmtError(test.message)

			got := dburldisplay.RedactError(test.address, err)

			c.Assert(got.Error(), qt.Equals, test.want)
			c.Assert(got, qt.ErrorIs, errFromDriver)
		})
	}
}

// fmtError is an error reading message that wraps errFromDriver without showing
// it.
func fmtError(message string) error {
	return wrappedDriverError{message: message}
}

type wrappedDriverError struct{ message string }

func (e wrappedDriverError) Error() string { return e.message }
func (e wrappedDriverError) Unwrap() error { return errFromDriver }

// An error that holds nothing of the address comes back as the very value, and
// no error is no error.
func TestRedactError_KeepsAnErrorWithNothingToHide(t *testing.T) {
	c := qt.New(t)
	c.Assert(dburldisplay.RedactError("postgres://app:Pa55wordX9@localhost/shop", errFromDriver), qt.Equals, errFromDriver)
	c.Assert(dburldisplay.RedactError("postgres://localhost/shop", errFromDriver), qt.Equals, errFromDriver)
	c.Assert(dburldisplay.RedactError("postgres://app:Pa55wordX9@localhost/shop", nil), qt.IsNil)
}

func TestSecrets(t *testing.T) {
	tests := []struct {
		name    string
		address string
		want    []string
	}{
		{name: "none", address: "postgres://app@localhost/shop"},
		{name: "a password", address: "postgres://app:Pa55wordX9@localhost/shop", want: []string{"Pa55wordX9"}},
		{name: "a DSN's password", address: "app:P@5s/w0rd@tcp(localhost:3306)/shop", want: []string{"P@5s/w0rd"}},
		{name: "a parameter, escaped", address: "postgres://localhost/shop?password=a%40b", want: []string{"a@b"}},
		{name: "two secrets, the longer first", address: "postgres://app:ab@localhost/shop?password=abcd", want: []string{"abcd", "ab"}},
		{name: "an escaped password", address: "mysql+unix://app:p%2Fss@/tmp/m.sock", want: []string{"p/ss"}},
		{name: "a server option", address: "postgres://localhost/shop?options=-c%20app.jwt_secret%3Dabc123", want: []string{"abc123"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := dburldisplay.Secrets(test.address)
			c.Assert(got, qt.HasLen, len(test.want))
			for i, secret := range test.want {
				c.Assert(got[i], qt.Equals, secret)
			}
		})
	}
}

func TestIsSecretParameter(t *testing.T) {
	tests := []struct {
		key  string
		want bool
	}{
		{key: "password", want: true},
		{key: "PASSWORD", want: true},
		{key: "passwd", want: true},
		{key: "pwd", want: true},
		{key: "sslpassword", want: true},
		{key: "sslkey", want: true},
		{key: "sslcert", want: true},
		{key: "token", want: true},
		{key: "authToken", want: true},
		{key: "auth_token", want: true},
		{key: "jwt", want: true},
		{key: "api_key", want: true},
		{key: "apikey", want: true},
		{key: "client_secret", want: true},
		{key: "aws_secret_access_key", want: true},
		{key: "credentials", want: true},
		{key: "credentialsJson", want: true},
		{key: "WALLET PASSWORD", want: true},
		{key: "OS PASS", want: true},
		{key: "OS PASSHASH", want: true},
		{key: "OS HASH", want: true},
		{key: "change password", want: true},
		{key: "_auth_pass", want: true},
		{key: "_key", want: true},
		{key: "sslmode", want: false},
		{key: "passfile", want: false},
		{key: "user", want: false},
		{key: "username", want: false},
		{key: "database", want: false},
		{key: "application_name", want: false},
		{key: "parseTime", want: false},
		{key: "monitoring", want: false},
	}

	for _, test := range tests {
		t.Run(test.key, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(dburldisplay.IsSecretParameter(test.key), qt.Equals, test.want)
		})
	}
}
