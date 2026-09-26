package preflight_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/preflight"
)

// failingCommandRunner answers every hook with the output it holds and a
// failure, so the output reaches the caller inside the hook's error.
type failingCommandRunner struct {
	output string
}

func (r failingCommandRunner) Run(_ context.Context, _ string, _, _ []string) (string, error) {
	return r.output, errors.New("exit status 42")
}

// TestCommandHookOutputRedactsEveryMySQLFamilyScheme holds the hook output
// redaction to the schemes the connector opens.
//
// net/url refuses the tcp() form, so the password is found only by the
// MySQL-family parser, and that parser is reached only for a scheme it
// recognizes. With `mysql://` and `mariadb://` written out there, a `maria://`
// URL the migration connects with is printed with its password, twice: in the
// echoed URL and as MYSQL_PWD (stokaro/ptah#3744).
func TestCommandHookOutputRedactsEveryMySQLFamilyScheme(t *testing.T) {
	tests := []struct {
		name        string
		databaseURL string
		wantURL     string
	}{
		{
			name:        "maria",
			databaseURL: "maria://app:pw-value@tcp(db.internal:3307)/shop",
			wantURL:     "PTAH_DB_URL=maria://app@db.internal:3307/shop",
		},
		{
			name:        "upper-case mariadb",
			databaseURL: "MARIADB://app:pw-value@tcp(db.internal:3307)/shop",
			wantURL:     "PTAH_DB_URL=mariadb://app@db.internal:3307/shop",
		},
		{
			name:        "mysql",
			databaseURL: "mysql://app:pw-value@tcp(db.internal:3307)/shop",
			wantURL:     "PTAH_DB_URL=mysql://app@db.internal:3307/shop",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runner := failingCommandRunner{
				output: "PTAH_DB_URL=" + test.databaseURL + "\nMYSQL_PWD=pw-value\n",
			}

			_, err := preflight.Runner{CommandRunner: runner}.Execute(context.Background(), preflight.Options{
				Direction:   preflight.DirectionUp,
				DatabaseURL: test.databaseURL,
				Command:     "backup",
			})

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Not(qt.Contains), "pw-value")
			c.Assert(err.Error(), qt.Contains, test.wantURL)
			c.Assert(err.Error(), qt.Contains, "MYSQL_PWD=redacted")
		})
	}
}
