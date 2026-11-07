package embedpg_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedpg"
)

// Open opens no connection it can report on without naming the address, and pgx
// quotes what it could not read of it. Neither repeats the password, and the
// error still says which of the two went wrong.
func TestOpen_FailurePath_RepeatsNoPartOfThePassword(t *testing.T) {
	tests := []struct {
		name    string
		dbURL   string
		wantErr string
	}{
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:    "a password pgx cannot parse",
			dbURL:   "postgres://app:Pa5%zz5wordX9@127.0.0.1:1/shop",
			wantErr: `(?s)connect to postgres://app:\*\*\*@127\.0\.0\.1:1/shop: cannot parse .*failed to parse as URL.*`,
		},
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name:    "a server that does not answer",
			dbURL:   "postgres://app:Pa55wordX9@127.0.0.1:1/shop?connect_timeout=1",
			wantErr: `(?s)connect to postgres://app:\*\*\*@127\.0\.0\.1:1/shop\?connect_timeout=1: .*connect.*`,
		},
		{
			name:    "a secret parameter pgx repeats",
			dbURL:   "postgres://127.0.0.1:1/shop?sslkey=Pa5/5wordX9&sslcert=/absent",
			wantErr: `(?s)connect to postgres://127\.0\.0\.1:1/shop\?.*unable to read sslkey.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			backend, err := embedpg.Open(c.Context(), test.dbURL)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*(Pa5|wordX9).*`)
			c.Assert(backend, qt.IsNil)
		})
	}
}
