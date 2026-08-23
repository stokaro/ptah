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
