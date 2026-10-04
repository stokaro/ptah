package inference_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A connection error repeats the URL, so the operator can see which database
// failed, and never the password in it: not in the user info, not in a query
// parameter, not in a keyword/value string, and not when a reserved character
// in the password keeps net/url from reading the URL. Nothing listens on port
// 1, so each row fails at the connection.
func TestOpen_AConnectionErrorHidesThePassword(t *testing.T) {
	for _, test := range []struct {
		name  string
		dbURL string
		shown string
	}{
		// #nosec G101 -- a made-up password for a port nothing listens on
		{
			name:  "in the user info",
			dbURL: "postgres://app:s3cret@127.0.0.1:1/x?connect_timeout=1&sslmode=disable",
			shown: "connect to postgres://app:***@127.0.0.1:1/x?connect_timeout=1&sslmode=disable: ",
		},
		{
			name:  "in a query parameter",
			dbURL: "postgres://app@127.0.0.1:1/x?connect_timeout=1&password=s3cret&sslmode=disable",
			shown: "connect to postgres://app@127.0.0.1:1/x?connect_timeout=1&password=redacted&sslmode=disable: ",
		},
		{
			name:  "with an @ of its own",
			dbURL: "postgres://app:s3c@ret@127.0.0.1:1/x?connect_timeout=1&sslmode=disable",
			shown: "connect to postgres://app:***@127.0.0.1:1/x?connect_timeout=1&sslmode=disable: ",
		},
		{
			name:  "with a # net/url reads as a fragment",
			dbURL: "postgres://app:s3c#ret@127.0.0.1:1/x?connect_timeout=1",
			shown: "connect to postgres://app:***@127.0.0.1:1/x?connect_timeout=1: ",
		},
		// #nosec G101 -- a made-up password for a port nothing listens on
		{
			name:  "with a % that starts no escape",
			dbURL: "postgres://app:s3c%zz@127.0.0.1:1/x?connect_timeout=1",
			shown: "connect to postgres://app:***@127.0.0.1:1/x?connect_timeout=1: ",
		},
		{
			name:  "in a keyword/value string",
			dbURL: "host=127.0.0.1 port=1 user=app password='s3c ret' dbname=x connect_timeout=1 sslmode=disable",
			shown: "connect to host=127.0.0.1 port=1 user=app password=redacted dbname=x connect_timeout=1 sslmode=disable: ",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := runInference(c, "plan", "--spec", writeSpec(c), "--db-url", test.dbURL)

			c.Assert(err, qt.ErrorMatches, `(?s)`+regexp.QuoteMeta(test.shown)+`.*`)
			c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*s3c.*`)
		})
	}
}
