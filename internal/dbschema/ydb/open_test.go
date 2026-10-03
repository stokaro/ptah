package ydb_test

import (
	"context"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	ydbschema "ptah.run/internal/dbschema/ydb"
)

// Every refusal here is decided from the URL and the environment, before the
// SDK dials anything, so none of them needs a server.
func TestOpen_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr string
	}{
		{
			name:    "no database",
			url:     "ydb://localhost:2136",
			wantErr: "invalid YDB URL: name the database in the path (ydb://host:2136/local) or in the database parameter",
		},
		{
			name:    "the path and the parameter disagree",
			url:     "ydb://localhost:2136/local?database=/dev",
			wantErr: "invalid YDB URL: the URL names database /local in its path and /dev in the database parameter; name it once",
		},
		{
			// The SDK ignores a parameter it does not know without a word.
			name: "an unknown parameter",
			url:  "ydb://localhost:2136/local?sslmode=disable",
			wantErr: `invalid YDB URL: parameter "sslmode" is not one Ptah reads on a YDB URL; accepted: ` +
				"database, monitoring, token, use_env_credentials, go_balancer, balancer, go_query_mode, query_mode, " +
				"go_default_idempotent, prefetch_query_result_parts",
		},
		{
			// The SDK falls back to its default balancer on a value it cannot
			// parse and says nothing.
			name: "a balancer the SDK does not know",
			url:  "ydb://localhost:2136/local?go_balancer=disabled",
			wantErr: `invalid YDB URL: parameter go_balancer="disabled" is not a balancer ydb-go-sdk knows: ` +
				"use disable, single, random_choice, round_robin or a JSON balancer config",
		},
		{
			name:    "two balancer spellings",
			url:     "ydb://localhost:2136/local?go_balancer=disable&balancer=single",
			wantErr: "invalid YDB URL: parameters go_balancer and balancer both choose a balancer; give one",
		},
		{
			name:    "a parameter given twice",
			url:     "ydb://localhost:2136/local?go_balancer=disable&go_balancer=single",
			wantErr: "invalid YDB URL: parameter go_balancer is given 2 times",
		},
		{
			name: "the SDK's own binder",
			url:  "ydb://localhost:2136/local?go_query_bind=positional",
			wantErr: "invalid YDB URL: parameter go_query_bind is refused: Ptah binds parameters itself, " +
				"and ydb-go-sdk's binders rewrite a ? inside a YQL literal",
		},
		{
			name: "a faked transaction",
			url:  "ydb://localhost:2136/local?go_fake_tx=query",
			wantErr: "invalid YDB URL: parameter go_fake_tx is refused: Ptah reads a schema in a snapshot " +
				"read-only transaction, and a faked transaction would read outside it",
		},
		{
			name: "a table-service query mode",
			url:  "ydb://localhost:2136/local?go_query_mode=scripting",
			wantErr: `invalid YDB URL: parameter go_query_mode="scripting" is refused: Ptah runs its reads and DDL ` +
				"through the query service, and the table-service modes run one kind of statement each; " +
				"use go_query_mode=query or leave it out",
		},
		{
			name:    "an idempotence flag that is not a boolean",
			url:     "ydb://localhost:2136/local?go_default_idempotent=sometimes",
			wantErr: `invalid YDB URL: parameter go_default_idempotent="sometimes" is not a boolean`,
		},
		{
			name:    "a negative prefetch",
			url:     "ydb://localhost:2136/local?prefetch_query_result_parts=-1",
			wantErr: `invalid YDB URL: parameter prefetch_query_result_parts="-1" is not a non-negative integer`,
		},
		// #nosec G101 -- a fixture with a made-up password, not a credential
		{
			name: "a password and a token",
			url:  "ydb://alice:secret@localhost:2136/local?token=t",
			wantErr: "invalid YDB URL: the URL names more than one credential source " +
				"(the URL's user, the token parameter); give one",
		},
		{
			name: "a token and the environment",
			url:  "ydb://localhost:2136/local?token=t&use_env_credentials",
			wantErr: "invalid YDB URL: the URL names more than one credential source " +
				"(the token parameter, use_env_credentials); give one",
		},
		{
			name:    "an empty token",
			url:     "ydb://localhost:2136/local?token=",
			wantErr: "invalid YDB URL: parameter token is empty",
		},
		{
			name:    "an environment switch that is not a boolean",
			url:     "ydb://localhost:2136/local?use_env_credentials=maybe",
			wantErr: `invalid YDB URL: parameter use_env_credentials="maybe" is not a boolean`,
		},
		{
			name: "a monitoring endpoint without a scheme",
			url:  "ydb://localhost:2136/local?monitoring=localhost:8765",
			wantErr: `invalid YDB URL: the monitoring parameter "localhost:8765" names no http:// or https:// ` +
				"endpoint: write monitoring=http://host:8765",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ydbschema.Open(t.Context(), test.url)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(got, qt.IsNil)
		})
	}
}

// use_env_credentials reads the variables ydb-go-sdk-auth-environ defines, and
// that package connects anonymously, without a word, from an environment that
// sets none of them or only part of one source. Each of those is refused.
func TestOpen_FailurePath_EnvironmentCredentials(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{
			name: "no credential variable",
			wantErr: "invalid YDB URL: use_env_credentials is set and no credential variable is: set one of " +
				"YDB_SERVICE_ACCOUNT_KEY_CREDENTIALS, YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS, " +
				"YDB_METADATA_CREDENTIALS, YDB_ACCESS_TOKEN_CREDENTIALS, YDB_STATIC_CREDENTIALS_USER, " +
				"YDB_OAUTH2_KEY_FILE, YDB_ANONYMOUS_CREDENTIALS",
		},
		{
			name: "a static user without the password and the endpoint",
			env:  map[string]string{"YDB_STATIC_CREDENTIALS_USER": "alice"},
			wantErr: "invalid YDB URL: YDB_STATIC_CREDENTIALS_USER is set without YDB_STATIC_CREDENTIALS_PASSWORD " +
				"and YDB_STATIC_CREDENTIALS_ENDPOINT, and the YDB SDK would connect anonymously instead",
		},
		{
			name: "a static user without the endpoint",
			env: map[string]string{
				"YDB_STATIC_CREDENTIALS_USER":     "alice",
				"YDB_STATIC_CREDENTIALS_PASSWORD": "secret",
			},
			wantErr: "invalid YDB URL: YDB_STATIC_CREDENTIALS_USER is set without YDB_STATIC_CREDENTIALS_ENDPOINT, " +
				"and the YDB SDK would connect anonymously instead",
		},
		{
			name:    "a metadata switch the SDK ignores",
			env:     map[string]string{"YDB_METADATA_CREDENTIALS": "true"},
			wantErr: `invalid YDB URL: YDB_METADATA_CREDENTIALS="true" is ignored by the YDB SDK, which reads only 1`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			for _, name := range []string{
				"YDB_SERVICE_ACCOUNT_KEY_CREDENTIALS", "YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS",
				"YDB_METADATA_CREDENTIALS", "YDB_ACCESS_TOKEN_CREDENTIALS", "YDB_STATIC_CREDENTIALS_USER",
				"YDB_STATIC_CREDENTIALS_PASSWORD", "YDB_STATIC_CREDENTIALS_ENDPOINT", "YDB_OAUTH2_KEY_FILE",
				"YDB_ANONYMOUS_CREDENTIALS",
			} {
				c.Unsetenv(name)
			}
			for name, value := range test.env {
				c.Setenv(name, value)
			}

			got, err := ydbschema.Open(t.Context(), "ydb://localhost:2136/local?use_env_credentials")

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(got, qt.IsNil)
		})
	}
}

// The control for the refusals above: each URL here passes every check Open
// makes from the URL and the environment, so the error comes from the SDK,
// which is handed a context that has already ended. A check that refused one
// of them would answer `invalid YDB URL` instead.
func TestOpen_FailurePath_AcceptedURLsReachTheDriver(t *testing.T) {
	tests := []struct {
		name string
		url  string
		env  map[string]string
	}{
		{name: "the database in the path", url: "ydb://localhost:2136/local"},
		{name: "the database parameter", url: "ydbs://localhost?database=/ru-central1/b1g/etn"},
		{name: "a disabled balancer", url: "ydb://localhost:2136/local?go_balancer=disable"},
		{name: "the older balancer spelling", url: "ydb://localhost:2136/local?balancer=random_choice"},
		{name: "the query service mode", url: "ydb://localhost:2136/local?go_query_mode=query"},
		{name: "the older query mode spelling", url: "ydb://localhost:2136/local?query_mode=query"},
		{name: "default idempotence", url: "ydb://localhost:2136/local?go_default_idempotent=true"},
		{name: "no prefetch", url: "ydb://localhost:2136/local?prefetch_query_result_parts=0"},
		// Ptah reads it; internal/ydburl takes it out before the SDK sees the
		// URL.
		{name: "a monitoring endpoint", url: "ydb://localhost:2136/local?monitoring=http://localhost:8765"},
		// #nosec G101 -- a fixture with a made-up password, not a credential
		{name: "a user and a password", url: "ydb://alice:secret@localhost:2136/local"},
		{name: "a token", url: "ydb://localhost:2136/local?token=t1.abc"},
		{
			name: "a token and the environment switched off",
			url:  "ydb://localhost:2136/local?token=t1.abc&use_env_credentials=false",
		},
		{
			name: "the environment with an access token",
			url:  "ydb://localhost:2136/local?use_env_credentials",
			env:  map[string]string{"YDB_ACCESS_TOKEN_CREDENTIALS": "t1.abc"},
		},
		{
			name: "the environment switched on by value",
			url:  "ydb://localhost:2136/local?use_env_credentials=true",
			env:  map[string]string{"YDB_ANONYMOUS_CREDENTIALS": "1"},
		},
		{
			name: "the environment with a complete static user",
			url:  "ydb://localhost:2136/local?use_env_credentials",
			// #nosec G101 -- a fixture with a made-up password, not a credential
			env: map[string]string{
				"YDB_STATIC_CREDENTIALS_USER":     "alice",
				"YDB_STATIC_CREDENTIALS_PASSWORD": "secret",
				"YDB_STATIC_CREDENTIALS_ENDPOINT": "localhost:2136",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			for _, name := range []string{
				"YDB_SERVICE_ACCOUNT_KEY_CREDENTIALS", "YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS",
				"YDB_METADATA_CREDENTIALS", "YDB_ACCESS_TOKEN_CREDENTIALS", "YDB_STATIC_CREDENTIALS_USER",
				"YDB_STATIC_CREDENTIALS_PASSWORD", "YDB_STATIC_CREDENTIALS_ENDPOINT", "YDB_OAUTH2_KEY_FILE",
				"YDB_ANONYMOUS_CREDENTIALS",
			} {
				c.Unsetenv(name)
			}
			for name, value := range test.env {
				c.Setenv(name, value)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			got, err := ydbschema.Open(ctx, test.url)

			c.Assert(err, qt.ErrorMatches, `(?s)open YDB driver: .*`)
			c.Assert(got, qt.IsNil)
		})
	}
}
