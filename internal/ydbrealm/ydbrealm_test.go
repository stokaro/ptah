package ydbrealm_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbrealm"
)

// A ydb:// or ydbs:// URL, in any case and with space around it, names a
// database whose dev databases are realms.
func TestApplies_HappyPath(t *testing.T) {
	for _, rawURL := range []string{
		"ydb://localhost:2136/local",
		"ydbs://ydb.example/ru-central1/b1g/etn",
		" YDB://localhost/local?dev_realm=r1 ",
	} {
		t.Run(rawURL, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbrealm.Applies(rawURL), qt.IsTrue)
		})
	}
}

// Another engine's URL, a docker URL that starts YDB, and a URL with no
// scheme get no realm.
func TestApplies_FailurePath(t *testing.T) {
	for _, rawURL := range []string{
		"postgres://localhost/app",
		"docker://ydb/26.2.1.14/local",
		"grpc://localhost:2136/local",
		"localhost:2136/local",
		"",
	} {
		t.Run(rawURL, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbrealm.Applies(rawURL), qt.IsFalse)
		})
	}
}

// A URL that names no database, or is not a YDB URL, is refused before
// anything connects, with no realm and a release that does nothing.
func TestEnter_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		wantErr string
	}{
		{name: "no database", rawURL: "ydb://localhost:2136",
			wantErr: "invalid YDB URL: name the database the dev realm is created in"},
		{name: "another engine", rawURL: "postgres://localhost/app",
			wantErr: `invalid YDB URL: not a ydb:// or ydbs:// URL: "postgres"`},
		{name: "a malformed realm", rawURL: "ydb://localhost:2136/local?dev_realm=A",
			wantErr: `invalid YDB URL: the dev_realm parameter "A" is not a realm name: .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, release, err := ydbrealm.Enter(context.Background(), test.rawURL)
			release()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}
