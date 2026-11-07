//go:build integration

package passworddisplay_test

import (
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"ptah.run/internal/clirun"
)

// refused is the address nothing listens on. A run against it reaches every
// message a connection can fail with.
const refused = "127.0.0.1:1"

// addressForm is one way to write an address that holds the password.
type addressForm struct {
	name    string
	address string
}

// addressForms are the forms an address can hold the password in, each reaching a
// different reader of it: net/url, the driver's DSN parser, or a driver that
// connects and fails.
var addressForms = []addressForm{
	{name: "PostgreSQL URL", address: "postgres://app:" + password + "@" + refused + "/shop"},
	{name: "PostgreSQL URL, escaped", address: "postgres://app:" + url.QueryEscape(password) + "@" + refused + "/shop"},
	{name: "MySQL URL", address: "mysql://app:" + password + "@" + refused + "/shop"},
	{name: "MariaDB URL", address: "mariadb://app:" + password + "@" + refused + "/shop"},
	{name: "SQL Server URL", address: "sqlserver://app:" + password + "@" + refused + "?database=shop"},
	{name: "Oracle URL", address: "oracle://app:" + password + "@" + refused + "/shop"},
	{name: "ClickHouse URL", address: "clickhouse://app:" + password + "@" + refused + "/shop"},
	{name: "libsql URL", address: "libsql://app:" + password + "@" + refused},
	{name: "Spanner URL", address: "spanner://app:" + password + "@" + refused + "/shop"},
	{name: "YDB URL", address: "ydb://app:" + password + "@" + refused + "/local"},
	{name: "a scheme nothing serves", address: "db2://app:" + password + "@" + refused + "/shop"},
	{name: "MySQL tcp", address: "mysql://app:" + password + "@tcp(" + refused + ")/shop"},
	{name: "MariaDB tcp", address: "mariadb://app:" + password + "@tcp(" + refused + ")/shop"},
	{name: "maria tcp", address: "maria://app:" + password + "@tcp(" + refused + ")/shop"},
	{name: "MySQL unix", address: "mysql://app:" + password + "@unix(/tmp/absent.sock)/shop"},
	{name: "MySQL socket URL", address: "mysql+unix://app:" + password + "@/tmp/absent.sock?database=shop"},
	{name: "MySQL tcp with no database", address: "mysql://app:" + password + "@tcp(" + refused + ")"},
	{name: "MySQL tcp that does not close", address: "mysql://app:" + password + "@tcp(" + refused + "/shop"},
	{name: "MySQL tcp with a parameter the driver refuses", address: "mysql://app:" + password + "@tcp(" + refused + ")/shop?parseTime=maybe"},
	{name: "DSN", address: "app:" + password + "@tcp(" + refused + ")/shop"},
	{name: "DSN with a user net/url refuses as a scheme", address: "my_app:" + password + "@tcp(" + refused + ")/shop"},
	{name: "DSN with no network", address: "app:" + password + "@/shop"},
	{name: "a port that is not a number", address: "postgres://app:" + password + "@127.0.0.1:54x2/shop"},
	{name: "a host net/url refuses", address: "postgres://app:" + password + "@bad^host/shop"},
	{name: "a query that does not parse", address: "postgres://app:" + password + "@" + refused + "/shop?x=%zz"},
	{name: "a password and no host", address: "postgres://app:" + password},
	{name: "PostgreSQL password parameter", address: "postgres://" + refused + "/shop?password=" + url.QueryEscape(password)},
	{name: "PostgreSQL sslpassword and a mode pgx refuses", address: "postgres://" + refused + "/shop?sslpassword=" + url.QueryEscape(password) + "&sslmode=bogus"},
	{name: "PostgreSQL sslkey pgx cannot open", address: "postgres://" + refused + "/shop?sslcert=/absent&sslkey=" + url.QueryEscape(password)},
	{name: "SQL Server password parameter", address: "sqlserver://" + refused + "?database=shop&password=" + url.QueryEscape(password)},
	{name: "ClickHouse password parameter", address: "clickhouse://" + refused + "/shop?password=" + url.QueryEscape(password)},
	{name: "libsql authToken", address: "libsql://" + refused + "?authToken=" + url.QueryEscape(password)},
	{name: "Oracle wallet password", address: "oracle://" + refused + "/shop?WALLET%20PASSWORD=" + url.QueryEscape(password)},
	{name: "Oracle token", address: "oracle://" + refused + "/shop?TOKEN=" + url.QueryEscape(password)},
	{name: "YDB token", address: "ydb://" + refused + "/local?token=" + url.QueryEscape(password)},
	{name: "libpq keywords", address: "host=127.0.0.1 port=1 password='" + password + "' dbname=shop"},
	{name: "libpq keywords with a quote that never closes", address: "host=127.0.0.1 password='" + password},
	{name: "ADO string", address: "server=127.0.0.1;port=1;user id=app;password=" + password + ";database=shop"},
	{name: "ODBC string", address: "odbc:server=127.0.0.1;port=1;uid=app;pwd={" + password + "};database=shop"},
}

// verbs are the commands that name an address, each in the output it can report
// an error in. ADDRESS stands where the address under test goes.
var verbs = []struct {
	name   string
	target clirun.Target
	args   []string
}{
	{name: "db read", target: clirun.Ptah, args: []string{"db", "read", "--db-url", "ADDRESS"}},
	{name: "db capabilities", target: clirun.Ptah, args: []string{"db", "capabilities", "--db-url", "ADDRESS"}},
	{name: "db drop-all", target: clirun.Ptah, args: []string{"db", "drop-all", "--db-url", "ADDRESS", "--dry-run"}},
	{name: "migrations up", target: clirun.Ptah, args: []string{"migrations", "up", "--db-url", "ADDRESS", "--migrations-dir", "native", "--verbose"}},
	{name: "migrations up, JSON", target: clirun.Ptah, args: []string{"migrations", "up", "--db-url", "ADDRESS", "--migrations-dir", "native", "--json"}},
	{name: "migrations status, JSON", target: clirun.Ptah, args: []string{"migrations", "status", "--db-url", "ADDRESS", "--migrations-dir", "native", "--json"}},
	{name: "migrations down", target: clirun.Ptah, args: []string{"migrations", "down", "--db-url", "ADDRESS", "--migrations-dir", "native", "--verbose"}},
	{name: "migrations log", target: clirun.Ptah, args: []string{"migrations", "log", "--db-url", "ADDRESS"}},
	{name: "migrations lint", target: clirun.Ptah, args: []string{"migrations", "lint", "--dev-url", "ADDRESS", "--dir", "migrations", "--latest", "1", "--format", "json"}},
	{name: "migrations validate", target: clirun.Ptah, args: []string{"migrations", "validate", "--dev-url", "ADDRESS", "--dir", "native"}},
	{name: "migrations generate", target: clirun.Ptah, args: []string{"migrations", "generate", "--db-url", "ADDRESS", "--root-dir", "models", "--migrations-dir", "native"}},
	{name: "schema apply", target: clirun.Ptah, args: []string{"schema", "apply", "--db-url", "ADDRESS", "--root-dir", "models", "--auto-approve", "--json"}},
	{name: "schema apply, --to", target: clirun.Ptah, args: []string{"schema", "apply", "--db-url", "sqlite://t.db", "--to", "ADDRESS", "--auto-approve"}},
	{name: "schema plan", target: clirun.Ptah, args: []string{"schema", "plan", "--db-url", "ADDRESS", "--root-dir", "models", "--output", "p.json"}},
	{name: "schema drift, JSON", target: clirun.Ptah, args: []string{"schema", "drift", "--db-url", "ADDRESS", "--root-dir", "models", "--format", "json"}},
	{name: "schema diff", target: clirun.Ptah, args: []string{"schema", "diff", "--from", "ADDRESS", "--to", "schema.sql"}},
	{name: "schema inspect", target: clirun.Ptah, args: []string{"schema", "inspect", "--db-url", "ADDRESS"}},
	{name: "schema test, source", target: clirun.Ptah, args: []string{"schema", "test", "--source-db-url", "ADDRESS", "--dir", "tests"}},
	{name: "seed", target: clirun.Ptah, args: []string{"seed", "--db-url", "ADDRESS", "--env", "dev", "--seeds-dir", "seeds"}},
	{name: "inference status", target: clirun.Ptah, args: []string{"inference", "status", "--db-url", "ADDRESS", "--release", "r1", "--run-id", "r1"}},
	{name: "mcp, until its input ends", target: clirun.Ptah, args: []string{"mcp", "--workspace", ".", "--dialect", "postgres", "--schema-dir", "models", "--migrations-dir", "native", "--allow-database-inspect", "allow", "--database-url", "ADDRESS"}},
	{name: "compat migrate apply", target: clirun.Compat, args: []string{"migrate", "apply", "--url", "ADDRESS", "--dir", "file://migrations"}},
	{name: "compat migrate status", target: clirun.Compat, args: []string{"migrate", "status", "--url", "ADDRESS", "--dir", "file://migrations", "--format", "{{ json . }}"}},
	{name: "compat migrate diff", target: clirun.Compat, args: []string{"migrate", "diff", "x", "--to", "file://schema.sql", "--dev-url", "ADDRESS", "--dir", "file://migrations"}},
	{name: "compat migrate lint", target: clirun.Compat, args: []string{"migrate", "lint", "--dev-url", "ADDRESS", "--latest", "1", "--dir", "file://migrations", "--format", "{{ json . }}"}},
	{name: "compat schema inspect", target: clirun.Compat, args: []string{"schema", "inspect", "--url", "ADDRESS"}},
	{name: "compat schema apply", target: clirun.Compat, args: []string{"schema", "apply", "--url", "ADDRESS", "--to", "file://schema.sql", "--auto-approve"}},
	{name: "compat schema diff", target: clirun.Compat, args: []string{"schema", "diff", "--from", "ADDRESS", "--to", "file://schema.sql"}},
	{name: "compat schema clean", target: clirun.Compat, args: []string{"schema", "clean", "--url", "ADDRESS", "--auto-approve"}},
	{name: "compat schema plan new", target: clirun.Compat, args: []string{"schema", "plan", "new", "--from", "ADDRESS", "--to", "file://schema.sql"}},
	{name: "compat schema plan new, --to", target: clirun.Compat, args: []string{"schema", "plan", "new", "--from", "sqlite://t.db", "--to", "ADDRESS"}},
	{name: "compat schema test", target: clirun.Compat, args: []string{"schema", "test", "--dev-url", "sqlite://d?mode=memory", "--url", "ADDRESS", "tests"}},
	{name: "compat schema stats", target: clirun.Compat, args: []string{"schema", "stats", "inspect", "--db-url", "ADDRESS"}},
}

// Whichever verb reads an address and whichever form holds the password, no part
// of it reaches the standard output, the standard error or a file the run wrote.
// Each verb has a directory of its own, so the verbs run side by side: this is a
// little over a thousand processes, and nothing they reach listens.
func TestNoVerbPrintsAPasswordE2E(t *testing.T) {
	for _, verb := range verbs {
		t.Run(verb.name, func(t *testing.T) {
			t.Parallel()
			dir := newWorkspace(qt.New(t))
			for _, form := range addressForms {
				t.Run(form.name, func(t *testing.T) {
					c := qt.New(t)

					got := clirun.Run(c, verb.target, clirun.Options{Dir: dir, Timeout: 30 * time.Second}, withAddress(verb.args, form.address)...)

					assertNoPassword(c, got.Stdout)
					assertNoPassword(c, got.Stderr)
					assertNoPasswordInFiles(c, dir)
				})
			}
		})
	}
}

// A refusal still says what is wrong with the address, and the address it names
// stays readable: an error that said nothing would pass every absence check above.
func TestARefusalSaysWhatIsWrongE2E(t *testing.T) {
	tests := []struct {
		name    string
		target  clirun.Target
		args    []string
		address string
		want    string
	}{
		{
			name:    "a reserved character in a URL's password",
			target:  clirun.Ptah,
			args:    []string{"db", "capabilities", "--db-url", "ADDRESS"},
			address: "postgres://app:" + password + "@" + refused + "/shop",
			want:    `invalid database URL: parse "postgres://app:***@127.0.0.1:1/shop": the user name or password cannot be read as written; percent-encode the characters a URL reserves, such as @ : / ? # and %`,
		},
		{
			name:    "a reserved character in a MySQL URL's password",
			target:  clirun.Ptah,
			args:    []string{"db", "capabilities", "--db-url", "ADDRESS"},
			address: "mysql://app:" + password + "@" + refused + "/shop",
			want:    `invalid database URL: invalid mysql URL: the user name or password cannot be read as written; percent-encode`,
		},
		{
			name:    "a DSN with no scheme and a user net/url refuses as a scheme",
			target:  clirun.Ptah,
			args:    []string{"db", "capabilities", "--db-url", "ADDRESS"},
			address: "my_app:" + password + "@tcp(" + refused + ")/shop",
			want:    `invalid database URL: parse "my_app:***@tcp(127.0.0.1:1)/shop": first path segment in URL cannot contain colon`,
		},
		{
			name:    "a DSN with no scheme",
			target:  clirun.Ptah,
			args:    []string{"db", "capabilities", "--db-url", "ADDRESS"},
			address: "app:" + password + "@tcp(" + refused + ")/shop",
			want:    `invalid database URL: parse "app:***@tcp(127.0.0.1:1)/shop": the user name or password cannot be read as written`,
		},
		{
			name:    "a port that is not a number, behind a password",
			target:  clirun.Ptah,
			args:    []string{"db", "capabilities", "--db-url", "ADDRESS"},
			address: "postgres://app:" + password + "@127.0.0.1:54x2/shop",
			want:    `invalid port ":54x2" after host`,
		},
		{
			name:    "a host net/url refuses, behind a password",
			target:  clirun.Ptah,
			args:    []string{"db", "capabilities", "--db-url", "ADDRESS"},
			address: "postgres://app:" + password + "@bad^host/shop",
			want:    `invalid character "^" in host name`,
		},
		{
			name:    "a MySQL DSN with no database",
			target:  clirun.Ptah,
			args:    []string{"db", "capabilities", "--db-url", "ADDRESS"},
			address: "mysql://app:" + password + "@tcp(" + refused + ")",
			want:    `invalid mysql URL: invalid DSN: missing the slash separating the database name`,
		},
		{
			name:    "a parameter the driver refuses, behind a password",
			target:  clirun.Ptah,
			args:    []string{"db", "capabilities", "--db-url", "ADDRESS"},
			address: "mysql://app:" + password + "@tcp(" + refused + ")/shop?parseTime=maybe",
			want:    `invalid mysql URL: invalid bool value: maybe`,
		},
		{
			name:    "a server that does not answer, behind a password",
			target:  clirun.Ptah,
			args:    []string{"db", "capabilities", "--db-url", "ADDRESS"},
			address: "mysql://app:" + password + "@tcp(" + refused + ")/shop",
			want:    `dial tcp 127.0.0.1:1`,
		},
		{
			name:    "a dev URL of a dialect nothing serves",
			target:  clirun.Ptah,
			args:    []string{"migrations", "lint", "--dev-url", "ADDRESS", "--dir", "migrations", "--latest", "1"},
			address: "db2://app:" + password + "@" + refused + "/dev",
			want:    `db2://app:***@127.0.0.1:1/dev`,
		},
		{
			name:    "a source of a kind the flag does not take",
			target:  clirun.Compat,
			args:    []string{"schema", "plan", "new", "--from", "ADDRESS", "--to", "file://schema.sql"},
			address: "db2://app:" + password + "@" + refused + "/shop",
			want:    `(got "db2://app:***@127.0.0.1:1/shop")`,
		},
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:    "a reserved character on the compatibility surface",
			target:  clirun.Compat,
			args:    []string{"schema", "inspect", "--url", "ADDRESS"},
			address: "postgres://app:" + password + "@" + refused + "/shop",
			want:    `parse "postgres://app:***@127.0.0.1:1/shop": the user name or password cannot be read as written`,
		},
	}

	dir := newWorkspace(qt.New(t))
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := clirun.Run(c, test.target, clirun.Options{Dir: dir, Timeout: 30 * time.Second}, withAddress(test.args, test.address)...)

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(got.Stderr, qt.Contains, test.want)
			assertNoPassword(c, got.Stdout)
			assertNoPassword(c, got.Stderr)
		})
	}
}

// What a run prints about the address it was given keeps the address, with only
// the password gone, in every form that has one. An address with no password
// comes through as it was written.
func TestTheDisplayKeepsTheAddressE2E(t *testing.T) {
	tests := []struct {
		name    string
		address string
		want    string
	}{
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{name: "a URL", address: "postgres://app:" + url.QueryEscape(password) + "@" + refused + "/shop", want: "postgres://app:***@127.0.0.1:1/shop"},
		{name: "a URL with no password", address: "postgres://app@" + refused + "/shop", want: "postgres://app@127.0.0.1:1/shop"},
		{name: "a URL with no user", address: "postgres://" + refused + "/shop", want: "postgres://127.0.0.1:1/shop"},
		{name: "a DSN with a scheme", address: "mysql://app:" + password + "@tcp(" + refused + ")/shop", want: "mysql://app:***@tcp(127.0.0.1:1)/shop"},
		{name: "a DSN with no scheme", address: "app:" + password + "@tcp(" + refused + ")/shop", want: "app:***@tcp(127.0.0.1:1)/shop"},
		{name: "a DSN with no password", address: "mysql://app@tcp(" + refused + ")/shop", want: "mysql://app@tcp(127.0.0.1:1)/shop"},
		{name: "a socket", address: "mysql://app:" + password + "@unix(/tmp/absent.sock)/shop", want: "mysql://app:***@unix(/tmp/absent.sock)/shop"},
		{name: "keywords", address: "host=127.0.0.1 port=1 password='" + password + "' dbname=shop", want: "host=127.0.0.1 port=1 password=redacted dbname=shop"},
		{name: "a secret parameter", address: "postgres://" + refused + "/shop?sslpassword=" + url.QueryEscape(password) + "&sslmode=disable", want: "postgres://127.0.0.1:1/shop?sslmode=disable&sslpassword=redacted"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: 30 * time.Second}, "db", "read", "--db-url", test.address)

			c.Assert(got.Stdout+got.Stderr, qt.Contains, "Reading schema from database: "+test.want)
			assertNoPassword(c, got.Stdout)
			assertNoPassword(c, got.Stderr)
		})
	}
}

// project inspect reads an address out of a file and reports it, with no flag or
// connection in between.
func TestProjectInspectPrintsNoPasswordE2E(t *testing.T) {
	for _, format := range []string{"text", "json"} {
		t.Run(format, func(t *testing.T) {
			c := qt.New(t)
			dir := newWorkspace(c)
			address := "mysql://app:" + password + "@tcp(db.internal:3306)/shop"
			writeProjectFile(c, dir, address)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, "project", "inspect", "--env", "local", "--format", format)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("%s", got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "mysql://app:***@tcp(db.internal:3306)/shop")
			assertNoPassword(c, got.Stdout)
			assertNoPassword(c, got.Stderr)
		})
	}
}

// formsTheServerStartsWith are the address forms of an engine this binary serves
// by itself: for one only a provider serves, `ptah mcp` refuses to start, which
// the verb table above measures.
func formsTheServerStartsWith() []addressForm {
	return slices.DeleteFunc(slices.Clone(addressForms), func(form addressForm) bool {
		return strings.HasPrefix(form.address, "ydb://") || strings.HasPrefix(form.address, "spanner://")
	})
}

// ptah mcp hands what a database refused to a model, which is the one reader that
// must not see an address at all, and it writes an audit record too.
func TestMCPPrintsNoPasswordE2E(t *testing.T) {
	binary := clirun.Build(qt.New(t), clirun.Ptah)
	for _, form := range formsTheServerStartsWith() {
		t.Run(form.name, func(t *testing.T) {
			c := qt.New(t)
			dir := newWorkspace(c)
			// #nosec G204 -- the test runs the ptah binary clirun built from this checkout
			cmd := exec.Command(binary,
				"mcp", "--workspace", ".", "--dialect", "postgres", "--schema-dir", "models", "--migrations-dir", "native",
				"--allow-database-inspect", "allow", "--database-url", form.address)
			cmd.Dir = dir
			// An exported PTAH_* variable would configure the server beside the flags.
			cmd.Env = slices.DeleteFunc(os.Environ(), func(entry string) bool { return strings.HasPrefix(entry, "PTAH_") })
			client := mcp.NewClient(&mcp.Implementation{Name: "passworddisplay-e2e", Version: "1"}, nil)
			session, err := client.Connect(c.Context(), &mcp.CommandTransport{Command: cmd}, nil)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(session.Close(), qt.IsNil) })

			for _, call := range []struct {
				name      string
				arguments map[string]any
			}{
				{name: "describe_session", arguments: make(map[string]any)},
				{name: "read_database", arguments: make(map[string]any)},
				{name: "inference_status", arguments: map[string]any{"run_id": "r1"}},
			} {
				result, callErr := session.CallTool(c.Context(), &mcp.CallToolParams{Name: call.name, Arguments: call.arguments})
				c.Assert(callErr, qt.IsNil, qt.Commentf("%s", call.name))
				encoded, marshalErr := json.Marshal(result)
				c.Assert(marshalErr, qt.IsNil)
				assertNoPassword(c, string(encoded))
			}
			assertNoPasswordInFiles(c, dir)
		})
	}
}
