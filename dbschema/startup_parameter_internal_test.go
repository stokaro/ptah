package dbschema

// White-box testing required: the explanation is chosen from a driver error
// inside the connect path, and the exported failure carries the same wrapped
// error either way.

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
)

// startupError is what a PostgreSQL driver hands back when the server refused
// the startup packet.
type startupError struct {
	state   string
	message string
}

func (e startupError) Error() string    { return e.message }
func (e startupError) SQLState() string { return e.state }

const refusedSearchPath = "server error: FATAL: unsupported startup parameter: search_path (SQLSTATE 08P01)"

// TestExplainRefusedStartupParameter_ExplainsOnlyWhatItRecognizes holds which
// failures get the explanation and which pass through untouched.
//
// The SQLSTATE alone is not enough: 08P01 covers every protocol violation, and
// answering this message for an unrelated one would explain the wrong thing.
func TestExplainRefusedStartupParameter_ExplainsOnlyWhatItRecognizes(t *testing.T) {
	tests := []struct {
		name     string
		dbURL    string
		dialect  string
		err      error
		wantSaid string
	}{
		{
			name:     "the URL's own search_path",
			dbURL:    "postgres://proxy:6432/app?sslmode=disable&search_path=app",
			dialect:  platform.Postgres,
			err:      startupError{state: "08P01", message: refusedSearchPath},
			wantSaid: "the database URL selects a schema with \"search_path\"",
		},
		{
			// The parameter is refused and the URL did not ask for it, so the
			// advice about removing it would be wrong. It still says which
			// parameter, because the driver's own message is buried.
			name:     "some other refused parameter",
			dbURL:    "postgres://proxy:6432/app?sslmode=disable",
			dialect:  platform.Postgres,
			err:      startupError{state: "08P01", message: "server error: FATAL: unsupported startup parameter: options (SQLSTATE 08P01)"},
			wantSaid: "the database URL carries the startup parameter \"options\"",
		},
		{
			name:     "another protocol violation entirely",
			dbURL:    "postgres://proxy:6432/app?search_path=app",
			dialect:  platform.Postgres,
			err:      startupError{state: "08P01", message: "server error: FATAL: invalid startup packet layout"},
			wantSaid: "invalid startup packet layout",
		},
		{
			name:     "an ordinary connection failure",
			dbURL:    "postgres://proxy:6432/app?search_path=app",
			dialect:  platform.Postgres,
			err:      errors.New("dial tcp: connection refused"),
			wantSaid: "dial tcp: connection refused",
		},
		{
			// The message names PgBouncer and a PostgreSQL parameter. A MySQL
			// failure that happened to carry the words must not receive it.
			name:     "another dialect",
			dbURL:    "mysql://proxy:6432/app",
			dialect:  platform.MySQL,
			err:      startupError{state: "08P01", message: refusedSearchPath},
			wantSaid: refusedSearchPath,
		},
		{
			name:     "no error at all",
			dbURL:    "postgres://proxy:6432/app?search_path=app",
			dialect:  platform.Postgres,
			err:      nil,
			wantSaid: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			explained := explainRefusedStartupParameter(test.dbURL, test.dialect, test.err)

			c.Assert(errorText(explained), qt.Contains, test.wantSaid)
			// The driver's own error survives in every arm, so the operator
			// never loses what the server actually said.
			c.Assert(errors.Is(explained, test.err) || explained == nil, qt.IsTrue)
		})
	}
}

// TestExplainRefusedStartupParameter_NamesNoFlag keeps stokaro/ptah#1924's
// shape out of this message.
//
// It is produced by the connection layer, which does not know which verb
// asked, and the schema option is not on every one: `ptah schema inspect` has
// no --schema while the compatibility surface does. A message that named one
// would be advice the caller cannot follow.
func TestExplainRefusedStartupParameter_NamesNoFlag(t *testing.T) {
	c := qt.New(t)
	explained := explainRefusedStartupParameter(
		"postgres://proxy:6432/app?search_path=app",
		platform.Postgres,
		startupError{state: "08P01", message: refusedSearchPath})

	c.Assert(regexp.MustCompile(`--[a-z][a-z-]+`).FindAllString(explained.Error(), -1),
		qt.HasLen, 0)
}

// TestRefusedStartupParameter_ReadsTheNameOutOfTheMessage pins the extraction,
// because the parameter name appears nowhere else on the wire.
func TestRefusedStartupParameter_ReadsTheNameOutOfTheMessage(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "search_path", err: startupError{state: "08P01", message: refusedSearchPath}, want: "search_path"},
		{
			name: "wrapped, because database/sql wraps",
			err:  fmt.Errorf("ping: %w", startupError{state: "08P01", message: refusedSearchPath}),
			want: "search_path",
		},
		{
			name: "another parameter",
			err:  startupError{state: "08P01", message: "FATAL: unsupported startup parameter: options"},
			want: "options",
		},
		{
			name: "the right words under the wrong SQLSTATE",
			err:  startupError{state: "42501", message: refusedSearchPath},
			want: "",
		},
		{
			name: "the right SQLSTATE with other words",
			err:  startupError{state: "08P01", message: "FATAL: invalid startup packet layout"},
			want: "",
		},
		{name: "no code at all", err: errors.New(refusedSearchPath), want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(refusedStartupParameter(test.err), qt.Equals, test.want)
		})
	}
}

// errorText renders an error without a branch in a test body. The empty string
// is only ever compared against the row that expects no error, and that row is
// the only one whose want is empty, so qt.Contains cannot pass vacuously
// anywhere else.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}

// TestURLWithoutSearchPath_RetriesOnlyWhatItCanCarry holds which failures earn
// a second connection and which are reported as they are.
//
// The retry drops a parameter the operator wrote, so it may only fire where
// Ptah knows what the parameter was for. The schema selection is that one: it
// is Ptah's own spelling of "work in this schema", and the value survives the
// drop because the URL still carries it. Every other startup parameter belongs
// to the operator, and running their command without one would be running it
// under settings they did not ask for.
//
// The expected URL is written out rather than checked for the absence of
// `search_path`, because dropping the database name or `sslmode` would pass
// that check and connect somewhere else, or over a channel nobody chose.
func TestURLWithoutSearchPath_RetriesOnlyWhatItCanCarry(t *testing.T) {
	const pooled = "postgres://proxy:6432/app?sslmode=disable"

	tests := []struct {
		name      string
		dbURL     string
		dialect   string
		err       error
		wantRetry bool
		wantURL   string
	}{
		{
			name:      "the URL's own search_path",
			dbURL:     pooled + "&search_path=app",
			dialect:   platform.Postgres,
			err:       startupError{state: "08P01", message: refusedSearchPath},
			wantRetry: true,
			wantURL:   pooled,
		},
		{
			// The parameter is the operator's, so the failure stands.
			name:    "another refused parameter",
			dbURL:   pooled + "&statement_timeout=5000",
			dialect: platform.Postgres,
			err: startupError{
				state:   "08P01",
				message: "server error: FATAL: unsupported startup parameter: statement_timeout (SQLSTATE 08P01)",
			},
			wantRetry: false,
		},
		{
			// The URL carries a selection AND the parameter that was refused.
			// Dropping the selection would retry a connection that fails the
			// same way, and then report the SECOND failure -- so the operator
			// is told about a parameter Ptah removed rather than about the one
			// the server named.
			name:    "a refused parameter beside a selection",
			dbURL:   pooled + "&search_path=app&statement_timeout=5000",
			dialect: platform.Postgres,
			err: startupError{
				state:   "08P01",
				message: "server error: FATAL: unsupported startup parameter: statement_timeout (SQLSTATE 08P01)",
			},
			wantRetry: false,
		},
		{
			// Nothing to drop: the refusal names search_path, and this URL does
			// not carry one. Retrying would open the same connection twice.
			name:      "no selection on the URL",
			dbURL:     pooled,
			dialect:   platform.Postgres,
			err:       startupError{state: "08P01", message: refusedSearchPath},
			wantRetry: false,
		},
		{
			name:      "another dialect",
			dbURL:     "mysql://host:3306/app?search_path=app",
			dialect:   platform.MySQL,
			err:       startupError{state: "08P01", message: refusedSearchPath},
			wantRetry: false,
		},
		{
			name:      "an unrelated failure",
			dbURL:     pooled + "&search_path=app",
			dialect:   platform.Postgres,
			err:       errors.New("dial tcp: connection refused"),
			wantRetry: false,
		},
		{
			name:      "no failure at all",
			dbURL:     pooled + "&search_path=app",
			dialect:   platform.Postgres,
			err:       nil,
			wantRetry: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			retryURL, retryable := urlWithoutSearchPath(test.dbURL, test.dialect, test.err)

			c.Assert(retryable, qt.Equals, test.wantRetry)
			c.Assert(retryURL, qt.Equals, test.wantURL)
		})
	}
}

// TestURLWithoutSearchPath_KeepsEverythingElse pins that the retry drops one
// parameter rather than rewriting the URL.
//
// A retry that lost `sslmode` would connect over a channel the operator did not
// choose, and one that lost the database name would connect somewhere else
// entirely. Both would still pass a test that only asked whether `search_path`
// was gone.
func TestURLWithoutSearchPath_KeepsEverythingElse(t *testing.T) {
	c := qt.New(t)

	retryURL, retryable := urlWithoutSearchPath(
		"postgres://proxy:6432/app?sslmode=verify-full&search_path=reporting&application_name=ptah",
		platform.Postgres,
		startupError{state: "08P01", message: refusedSearchPath},
	)

	c.Assert(retryable, qt.IsTrue)
	c.Assert(retryURL, qt.Contains, "sslmode=verify-full")
	c.Assert(retryURL, qt.Contains, "application_name=ptah")
	c.Assert(retryURL, qt.Contains, "//proxy:6432/app")
	c.Assert(retryURL, qt.Not(qt.Contains), "search_path")
}
