package devlock

// White-box testing required: local-file-lock URL eligibility, the
// post-acquisition cancellation handoff and the advisory lock's name are
// safety boundaries that cannot be exercised deterministically through
// exported API without live databases.

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(validateLocalFileLockURL(test.dialect, test.rawURL), qt.IsNil)
		})
	}
}

func TestValidateLocalFileLockURL_FailurePath(t *testing.T) {
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(
				validateLocalFileLockURL(test.dialect, test.rawURL),
				qt.ErrorMatches,
				test.wantErr,
			)
		})
	}
}

// A readable lock name is kept up to 64 bytes, the bound MySQL's GET_LOCK
// holds in characters. Past it the name is a digest of the dialect and the
// identity, pinned literally so that two Ptah builds name one realm alike, and
// distinct for identities that share every byte the readable form would keep.
func TestRealmLockName(t *testing.T) {
	tests := []struct {
		name     string
		identity string
		want     string
	}{
		{
			name:     "exactly 64 bytes stays readable",
			identity: strings.Repeat("x", 42),
			want:     "ptah-dev-replay:mysql:" + strings.Repeat("x", 42),
		},
		{
			name:     "65 bytes is digested",
			identity: strings.Repeat("x", 43),
			want:     "ptah-dev-replay:#e0b1b154db9488909225b1e14c3a67f1",
		},
		{
			name:     "a shared prefix gets its own digest",
			identity: strings.Repeat("x", 42) + "y",
			want:     "ptah-dev-replay:#5478bfd385ede11c173d5ea5850ed81c",
		},
		{
			name:     "21 two-byte characters fit in 64 bytes",
			identity: strings.Repeat("д", 21),
			want:     "ptah-dev-replay:mysql:" + strings.Repeat("д", 21),
		},
		{
			name:     "22 two-byte characters are counted in bytes",
			identity: strings.Repeat("д", 22),
			want:     "ptah-dev-replay:#eedb56cf99fcf9d56fec5ba797a88901",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := realmLockName("mysql", tt.identity)
			c.Assert(got, qt.Equals, tt.want)
			c.Assert(len(got) <= maxLockNameBytes, qt.IsTrue)
		})
	}
}
