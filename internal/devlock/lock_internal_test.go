package devlock

// White-box testing required: local-file-lock URL eligibility and the
// post-acquisition cancellation handoff are safety boundaries that cannot be
// exercised deterministically through exported API without live databases.

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
)

func TestFinishAcquire_CanceledContextReleasesFileLock(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "realm.lock")
	file, err := tryAcquireFile(path)
	c.Assert(err, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	lock, err := finishAcquire(ctx, &Lock{file: file})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(lock, qt.IsNil)
	reacquired, err := tryAcquireFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(errors.Join(unlockFile(reacquired), reacquired.Close()), qt.IsNil)
}

func TestValidateLocalFileLockURL_HappyPath(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		dialect string
		rawURL  string
	}{
		{
			name:    "clickhouse localhost",
			dialect: platform.ClickHouse,
			rawURL:  "clickhouse://user:password@localhost:9000/dev",
		},
		{
			name:    "clickhouse localhost absolute",
			dialect: platform.ClickHouse,
			rawURL:  "clickhouse://user:password@localhost.:9000/dev",
		},
		{
			name:    "clickhouse local socket",
			dialect: platform.ClickHouse,
			rawURL:  "clickhouse:///dev?address=%2Ftmp%2Fclickhouse.sock",
		},
		{
			name:    "cockroachdb ipv4 loopback",
			dialect: platform.CockroachDB,
			rawURL:  "cockroachdb://root@127.0.0.2:26257/defaultdb",
		},
		{
			name:    "cockroachdb ipv6 loopback",
			dialect: platform.CockroachDB,
			rawURL:  "cockroachdb://root@[::1]:26257/defaultdb",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(validateLocalFileLockURL(test.dialect, test.rawURL), qt.IsNil)
		})
	}
}

func TestValidateLocalFileLockURL_FailurePath(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		dialect string
		rawURL  string
		wantErr string
	}{
		{
			name:    "clickhouse remote DNS name",
			dialect: platform.ClickHouse,
			rawURL:  "clickhouse://user:password@clickhouse.example.com:9000/dev",
			wantErr: `clickhouse replay cannot safely serialize non-local dev database host "clickhouse.example.com" with a local file lock`,
		},
		{
			name:    "clickhouse container DNS name",
			dialect: platform.ClickHouse,
			rawURL:  "clickhouse://user:password@clickhouse:9000/dev",
			wantErr: `clickhouse replay cannot safely serialize non-local dev database host "clickhouse" with a local file lock`,
		},
		{
			name:    "cockroachdb private address",
			dialect: platform.CockroachDB,
			rawURL:  "cockroachdb://root@10.0.0.12:26257/defaultdb",
			wantErr: `cockroachdb replay cannot safely serialize non-local dev database host "10.0.0.12" with a local file lock`,
		},
		{
			name:    "malformed URL",
			dialect: platform.ClickHouse,
			rawURL:  "clickhouse://local%zzhost/dev",
			wantErr: `parse clickhouse dev database URL for local locking: .*invalid URL escape.*`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(
				validateLocalFileLockURL(test.dialect, test.rawURL),
				qt.ErrorMatches,
				test.wantErr,
			)
		})
	}
}
