package preflight_test

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/preflight"
)

// recordingRunner records the environment of every command it is asked to run.
type recordingRunner struct {
	env [][]string
}

func (r *recordingRunner) Run(_ context.Context, _ string, _, env []string) (string, error) {
	r.env = append(r.env, append([]string(nil), env...))
	return "", nil
}

// versions is the version part of preflight.Options a row varies.
type versions struct {
	current, target       int64
	currentKey, targetKey string
	keyed                 bool
}

func (v versions) options(opts preflight.Options) preflight.Options {
	opts.CurrentVersion, opts.TargetVersion = v.current, v.target
	opts.CurrentVersionKey, opts.TargetVersionKey = v.currentKey, v.targetKey
	opts.KeyedByPosition = v.keyed
	return opts
}

// The rows share their numbers and keys: 1_x, 2_x, 10_x and v1_x run as
// 10_x, 1_x, 2_x and v1_x, so position 4 is v1_x, and a hook that is told 4
// is told a number no file carries.
var keyedRows = []struct {
	name     string
	versions versions
	wantEnv  []string
	wantDump string
	wantBody string
}{
	{
		name:     "numbered",
		versions: versions{current: 2, target: 4, currentKey: "2", targetKey: "4"},
		wantEnv:  []string{"PTAH_CURRENT_VERSION=2", "PTAH_TARGET_VERSION=4"},
		wantDump: "ptah_pre_v2_to_v4_20260721T101314.000000000Z.dump",
		wantBody: `{"direction":"up","dialect":"postgres","current_version":2,"target_version":4}`,
	},
	{
		name:     "positions",
		versions: versions{current: 3, target: 4, currentKey: "2", targetKey: "v1", keyed: true},
		wantEnv:  []string{"PTAH_CURRENT_VERSION_KEY=2", "PTAH_TARGET_VERSION_KEY=v1"},
		wantDump: "ptah_pre_2_to_v1_20260721T101314.000000000Z.dump",
		wantBody: `{"direction":"up","dialect":"postgres","current_version_key":"2","target_version_key":"v1"}`,
	},
	{
		name:     "positions with nothing applied",
		versions: versions{current: 0, target: 2, targetKey: "1", keyed: true},
		wantEnv:  []string{"PTAH_CURRENT_VERSION_KEY=", "PTAH_TARGET_VERSION_KEY=1"},
		wantDump: "ptah_pre_none_to_1_20260721T101314.000000000Z.dump",
		wantBody: `{"direction":"up","dialect":"postgres","current_version_key":"","target_version_key":"1"}`,
	},
}

// TestExecuteVersionsInTheCommandEnvironment_HappyPath gives a command hook
// the numbers in a numbered directory and the keys alone where the numbers
// are positions.
func TestExecuteVersionsInTheCommandEnvironment_HappyPath(t *testing.T) {
	for _, row := range keyedRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			runner := &recordingRunner{}

			_, err := preflight.Runner{CommandRunner: runner}.Execute(context.Background(), row.versions.options(preflight.Options{
				Direction:   preflight.DirectionUp,
				DatabaseURL: "postgres://app@db/prod",
				Dialect:     "postgres",
				Command:     "backup",
			}))

			c.Assert(err, qt.IsNil)
			c.Assert(runner.env, qt.DeepEquals, [][]string{
				append([]string{"PTAH_DB_URL=postgres://app@db/prod", "PTAH_DIALECT=postgres"}, row.wantEnv...),
			})
		})
	}
}

// TestExecuteVersionsInTheDumpName_HappyPath names a dump by the numbers in
// a numbered directory and by the keys where the numbers are positions.
func TestExecuteVersionsInTheDumpName_HappyPath(t *testing.T) {
	for _, row := range keyedRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()

			results, err := preflight.Runner{
				CommandRunner: &recordingRunner{},
				Now:           func() time.Time { return time.Date(2026, 7, 21, 10, 13, 14, 0, time.UTC) },
			}.Execute(context.Background(), row.versions.options(preflight.Options{
				Direction:       preflight.DirectionUp,
				DatabaseURL:     "postgres://app@db/prod",
				Dialect:         "postgres",
				PostgresDumpDir: dir,
			}))

			c.Assert(err, qt.IsNil)
			c.Assert(results, qt.DeepEquals, []preflight.Result{{Name: "pg_dump", Artifact: filepath.Join(dir, row.wantDump)}})
		})
	}
}

// TestExecuteVersionsInTheWebhookPayload_HappyPath posts the numbers in a
// numbered directory and the keys alone where the numbers are positions. The
// body is compared as bytes, so a member left in beside the keys reddens it.
func TestExecuteVersionsInTheWebhookPayload_HappyPath(t *testing.T) {
	for _, row := range keyedRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			var body []byte
			client := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
				read, err := io.ReadAll(r.Body)
				body = read
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, err
			})}

			_, err := preflight.Runner{HTTPClient: client}.Execute(context.Background(), row.versions.options(preflight.Options{
				Direction:   preflight.DirectionUp,
				DatabaseURL: "postgres://app@db/prod",
				Dialect:     "postgres",
				WebhookURL:  "https://ops.example/hooks/ptah",
			}))

			c.Assert(err, qt.IsNil)
			c.Assert(string(body), qt.Equals, row.wantBody)
		})
	}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
