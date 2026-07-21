package preflight

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

type commandCall struct {
	Name string
	Args []string
	Env  []string
}

type fakeCommandRunner struct {
	calls  []commandCall
	output string
	err    error
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func (r *fakeCommandRunner) Run(_ context.Context, name string, args []string, env []string) (string, error) {
	r.calls = append(r.calls, commandCall{
		Name: name,
		Args: append([]string(nil), args...),
		Env:  append([]string(nil), env...),
	})
	return r.output, r.err
}

func TestExecuteCommandHookSetsRequiredEnvironment(t *testing.T) {
	c := qt.New(t)
	runner := &fakeCommandRunner{}

	results, err := Runner{CommandRunner: runner}.Execute(context.Background(), Options{
		Direction:      DirectionUp,
		DatabaseURL:    "postgres://app@db/prod",
		Dialect:        "postgres",
		CurrentVersion: 7,
		TargetVersion:  9,
		Command:        "backup --before-migration",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(results, qt.DeepEquals, []Result{{Name: "custom command"}})
	c.Assert(runner.calls, qt.HasLen, 1)
	c.Assert(runner.calls[0].Name, qt.Equals, "/bin/sh")
	c.Assert(runner.calls[0].Args, qt.DeepEquals, []string{"-c", "backup --before-migration"})
	c.Assert(runner.calls[0].Env, qt.Contains, "PTAH_DB_URL=postgres://app@db/prod")
	c.Assert(runner.calls[0].Env, qt.Contains, "PTAH_DIALECT=postgres")
	c.Assert(runner.calls[0].Env, qt.Contains, "PTAH_CURRENT_VERSION=7")
	c.Assert(runner.calls[0].Env, qt.Contains, "PTAH_TARGET_VERSION=9")
}

func TestExecuteCommandHookFailureIncludesOutput(t *testing.T) {
	c := qt.New(t)
	runner := &fakeCommandRunner{
		output: "snapshot refused\n",
		err:    errors.New("exit status 42"),
	}

	_, err := Runner{CommandRunner: runner}.Execute(context.Background(), Options{
		Direction: DirectionDown,
		Command:   "false",
	})

	c.Assert(err, qt.ErrorMatches, "(?s)down pre-flight custom command hook failed: exit status 42\nsnapshot refused")
}

func TestCommandHookOutputRedactsDatabaseSecrets(t *testing.T) {
	c := qt.New(t)
	runner := &fakeCommandRunner{
		output: "PTAH_DB_URL=mysql://app:pw-value@tcp(db.internal:3307)/shop\nMYSQL_PWD=pw-value\npassword=pw-value\n",
		err:    errors.New("exit status 42"),
	}

	_, err := Runner{CommandRunner: runner}.Execute(context.Background(), Options{
		Direction:          DirectionUp,
		DatabaseURL:        "mysql://app:pw-value@tcp(db.internal:3307)/shop",
		DisplayDatabaseURL: "mysql://app@db.internal:3307/shop",
		Command:            "backup",
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "pw-value")
	c.Assert(err.Error(), qt.Contains, "PTAH_DB_URL=mysql://app@db.internal:3307/shop")
	c.Assert(err.Error(), qt.Contains, "MYSQL_PWD=redacted")
}

func TestCommandHookOutputFallbackRedactsURLQuerySecrets(t *testing.T) {
	c := qt.New(t)
	dbPassword := "pg-" + "pass"
	queryToken := "query-" + "token"
	dbURL := "postgres://app:" + dbPassword + "@db.internal/prod?sslmode=require&token=" + queryToken
	runner := &fakeCommandRunner{
		output: "PTAH_DB_URL=" + dbURL + "\n",
		err:    errors.New("exit status 42"),
	}

	_, err := Runner{CommandRunner: runner}.Execute(context.Background(), Options{
		Direction:   DirectionUp,
		DatabaseURL: dbURL,
		Command:     "backup",
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), dbPassword)
	c.Assert(err.Error(), qt.Not(qt.Contains), queryToken)
	c.Assert(err.Error(), qt.Contains, "PTAH_DB_URL=postgres://app@db.internal/prod?sslmode=require&token=redacted")
}

func TestPostgresDumpUsesCustomFormatAndStableFilename(t *testing.T) {
	c := qt.New(t)
	runner := &fakeCommandRunner{}
	dir := t.TempDir()
	dbURL := "postgres://app:" + "pg-pass" + "@db/prod"

	results, err := Runner{
		CommandRunner: runner,
		Now:           func() time.Time { return time.Date(2026, 7, 21, 12, 13, 14, 0, time.FixedZone("UTC+2", 2*60*60)) },
	}.Execute(context.Background(), Options{
		Direction:       DirectionUp,
		DatabaseURL:     dbURL,
		Dialect:         "postgres",
		CurrentVersion:  1,
		TargetVersion:   3,
		PostgresDumpDir: dir,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(results, qt.HasLen, 1)
	c.Assert(results[0].Name, qt.Equals, "pg_dump")
	c.Assert(results[0].Artifact, qt.Matches, `.*/ptah_pre_v1_to_v3_20260721T101314\.000000000Z\.dump`)
	c.Assert(runner.calls, qt.HasLen, 1)
	c.Assert(runner.calls[0].Name, qt.Equals, "pg_dump")
	c.Assert(runner.calls[0].Args, qt.DeepEquals, []string{
		"--format=custom",
		"--file",
		results[0].Artifact,
		"postgres://app@db/prod",
	})
	c.Assert(strings.Join(runner.calls[0].Args, " "), qt.Not(qt.Contains), "pg-pass")
	c.Assert(runner.calls[0].Env, qt.Contains, "PGPASSWORD=pg-pass")
	c.Assert(strings.Join(runner.calls[0].Env, "\n"), qt.Not(qt.Contains), "PTAH_DB_URL=")
}

func TestPostgresDumpRejectsNonPostgresDialect(t *testing.T) {
	c := qt.New(t)

	_, err := Runner{}.Execute(context.Background(), Options{
		Direction:       DirectionUp,
		Dialect:         "sqlite",
		PostgresDumpDir: t.TempDir(),
	})

	c.Assert(err, qt.ErrorMatches, `up pre-flight pg_dump hook requires a PostgreSQL-compatible dialect, got sqlite`)
}

func TestMySQLDumpParsesTCPURLAndKeepsPasswordOutOfArgs(t *testing.T) {
	c := qt.New(t)
	runner := &fakeCommandRunner{}

	results, err := Runner{
		CommandRunner: runner,
		Now:           func() time.Time { return time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC) },
	}.Execute(context.Background(), Options{
		Direction:      DirectionUp,
		DatabaseURL:    "mysql://app:secret@tcp(db.internal:3307)/shop?parseTime=true",
		Dialect:        "mysql",
		CurrentVersion: 2,
		TargetVersion:  5,
		MySQLDumpDir:   t.TempDir(),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(results[0].Artifact, qt.Matches, `.*/ptah_pre_v2_to_v5_20260721T000000\.000000000Z\.sql`)
	c.Assert(runner.calls, qt.HasLen, 1)
	c.Assert(runner.calls[0].Name, qt.Equals, "mysqldump")
	c.Assert(runner.calls[0].Args, qt.DeepEquals, []string{
		"--result-file",
		results[0].Artifact,
		"--protocol=TCP",
		"--host",
		"db.internal",
		"--port",
		"3307",
		"--user",
		"app",
		"shop",
	})
	c.Assert(strings.Join(runner.calls[0].Args, " "), qt.Not(qt.Contains), "secret")
	c.Assert(runner.calls[0].Env, qt.Contains, "MYSQL_PWD=secret")
	c.Assert(strings.Join(runner.calls[0].Env, "\n"), qt.Not(qt.Contains), "PTAH_DB_URL=")
}

func TestMySQLDumpAcceptsURLWithoutUserInfo(t *testing.T) {
	c := qt.New(t)
	runner := &fakeCommandRunner{}

	results, err := Runner{
		CommandRunner: runner,
		Now:           func() time.Time { return time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC) },
	}.Execute(context.Background(), Options{
		Direction:      DirectionUp,
		DatabaseURL:    "mysql://db.internal/shop",
		Dialect:        "mysql",
		CurrentVersion: 2,
		TargetVersion:  5,
		MySQLDumpDir:   t.TempDir(),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(results, qt.HasLen, 1)
	c.Assert(runner.calls, qt.HasLen, 1)
	c.Assert(runner.calls[0].Args, qt.DeepEquals, []string{
		"--result-file",
		results[0].Artifact,
		"--protocol=TCP",
		"--host",
		"db.internal",
		"shop",
	})
	c.Assert(runner.calls[0].Env, qt.HasLen, 0)
}

func TestPostgresDumpNormalizesPostgresWireSchemes(t *testing.T) {
	c := qt.New(t)

	args, env := postgresDumpCommand("cockroachdb://app:pw@db.internal:26257/defaultdb?sslmode=disable", "/tmp/pre.dump")
	c.Assert(args, qt.DeepEquals, []string{
		"--format=custom",
		"--file",
		"/tmp/pre.dump",
		"postgres://app@db.internal:26257/defaultdb?sslmode=disable",
	})
	c.Assert(env, qt.DeepEquals, []string{"PGPASSWORD=pw"})

	args, env = postgresDumpCommand("yugabytedb://yugabyte:pw@db.internal:5433/yugabyte", "/tmp/pre.dump")
	c.Assert(args[3], qt.Equals, "postgres://yugabyte@db.internal:5433/yugabyte")
	c.Assert(env, qt.DeepEquals, []string{"PGPASSWORD=pw"})
}

func TestPostgresDumpStripsSecretQueryParamsFromArgs(t *testing.T) {
	c := qt.New(t)

	args, env := postgresDumpCommand(
		"postgres://app:pg-pass@db.internal/prod?sslmode=require&sslpassword=query-secret&token=api-token",
		"/tmp/pre.dump",
	)

	c.Assert(args, qt.DeepEquals, []string{
		"--format=custom",
		"--file",
		"/tmp/pre.dump",
		"postgres://app@db.internal/prod?sslmode=require",
	})
	c.Assert(strings.Join(args, " "), qt.Not(qt.Contains), "query-secret")
	c.Assert(strings.Join(args, " "), qt.Not(qt.Contains), "api-token")
	c.Assert(env, qt.DeepEquals, []string{"PGPASSWORD=pg-pass"})
}

func TestWebhookPostsMetadataAndRequiresHTTP200(t *testing.T) {
	c := qt.New(t)
	var payload webhookPayload
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.Header.Get("Content-Type"), qt.Equals, "application/json")
		c.Assert(json.NewDecoder(r.Body).Decode(&payload), qt.IsNil)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	})}

	results, err := Runner{HTTPClient: client}.Execute(context.Background(), Options{
		Direction:          DirectionUp,
		DatabaseURL:        "postgres://app@db/prod",
		DisplayDatabaseURL: "postgres://app@db/prod",
		Dialect:            "postgres",
		CurrentVersion:     1,
		TargetVersion:      2,
		WebhookURL:         "https://ops.example/hooks/ptah",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(results, qt.DeepEquals, []Result{{Name: "webhook"}})
	c.Assert(payload, qt.DeepEquals, webhookPayload{
		Direction:          DirectionUp,
		Dialect:            "postgres",
		CurrentVersion:     1,
		TargetVersion:      2,
		DisplayDatabaseURL: "postgres://app@db/prod",
	})
}

func TestWebhookRejectsNonOKResponse(t *testing.T) {
	c := qt.New(t)
	client := &http.Client{Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusAccepted,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	})}

	_, err := Runner{HTTPClient: client}.Execute(context.Background(), Options{
		Direction:  DirectionUp,
		WebhookURL: "https://ops.example/hooks/ptah",
	})

	c.Assert(err, qt.ErrorMatches, `up pre-flight webhook failed: expected HTTP 200, got 202`)
}

func TestWebhookNetworkErrorRedactsSecretURLParams(t *testing.T) {
	c := qt.New(t)
	client := &http.Client{Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
		return nil, &url.Error{
			Op:  "Post",
			URL: "https://ops.example/hooks/ptah?token=secret-token&env=prod",
			Err: errors.New("dial tcp: no route to host"),
		}
	})}

	_, err := Runner{HTTPClient: client}.Execute(context.Background(), Options{
		Direction:  DirectionUp,
		WebhookURL: "https://ops.example/hooks/ptah?token=secret-token&env=prod",
	})

	c.Assert(err, qt.ErrorMatches, `up pre-flight webhook failed for https://ops.example/hooks/ptah\?env=prod&token=redacted: dial tcp: no route to host`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "secret-token")
}

func TestDefaultHTTPClientDoesNotFollowRedirects(t *testing.T) {
	c := qt.New(t)
	requests := 0
	client := defaultHTTPClient()
	c.Assert(client.Timeout, qt.Equals, defaultWebhookTimeout)
	client.Transport = roundTripFunc(func(_ *http.Request) (*http.Response, error) {
		requests++
		return &http.Response{
			StatusCode: http.StatusFound,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     http.Header{"Location": []string{"https://ops.example/redirected"}},
		}, nil
	})

	_, err := Runner{HTTPClient: client}.Execute(context.Background(), Options{
		Direction:  DirectionUp,
		WebhookURL: "https://ops.example/hooks/ptah",
	})

	c.Assert(err, qt.ErrorMatches, `up pre-flight webhook failed: expected HTTP 200, got 302`)
	c.Assert(requests, qt.Equals, 1)
}
