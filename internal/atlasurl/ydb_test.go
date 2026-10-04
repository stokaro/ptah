package atlasurl_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
)

// A YDB database is a path, and the URL may carry it in its path or in a
// database parameter. Two URLs on one endpoint that name different databases
// must never compare as one, because a dev or scratch run cleans the database
// it is handed.
func TestSameDatabaseEndpoint_YDB_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{
			name:  "the plaintext default port is 2136, and a user and options change nothing",
			left:  "ydb://localhost/local",
			right: "ydb://reader@localhost:2136/local?go_query_mode=query",
			want:  true,
		},
		{
			name:  "the TLS default port is 2135",
			left:  "ydbs://localhost/local",
			right: "ydbs://localhost:2135/local",
			want:  true,
		},
		{
			name:  "the two schemes default to different ports",
			left:  "ydb://localhost/local",
			right: "ydbs://localhost/local",
			want:  false,
		},
		{
			name:  "the path and the parameter name one database",
			left:  "ydb://localhost:2136/local",
			right: "ydb://localhost:2136/?database=/local",
			want:  true,
		},
		{
			name:  "slashes around the path do not change the database",
			left:  "ydb://localhost:2136/local/",
			right: "ydb://localhost:2136/?database=local",
			want:  true,
		},
		{
			name:  "one endpoint, two databases in the parameter",
			left:  "ydb://localhost:2136/?database=/local",
			right: "ydb://localhost:2136/?database=/dev",
			want:  false,
		},
		{
			name:  "one endpoint, two databases in the path",
			left:  "ydb://localhost:2136/local",
			right: "ydb://localhost:2136/dev",
			want:  false,
		},
		{
			name:  "the path and the parameter may both name the database",
			left:  "ydb://localhost:2136/local?database=/local",
			right: "ydb://localhost:2136/dev?database=dev/",
			want:  false,
		},
		{
			name:  "a nested database path",
			left:  "ydbs://ydb.example:2135/ru-central1/b1g/etn",
			right: "ydbs://ydb.example:2135/ru-central1/b1g/other",
			want:  false,
		},
		{
			name:  "no database proves nothing",
			left:  "ydb://localhost:2136",
			right: "ydb://localhost:2136/",
			want:  false,
		},
		{
			name:  "a dev realm is not the database that holds it",
			left:  "ydb://localhost:2136/local?dev_realm=r1",
			right: "ydb://localhost:2136/local",
			want:  false,
		},
		{
			name:  "two dev realms in one database",
			left:  "ydb://localhost:2136/local?dev_realm=r1",
			right: "ydb://localhost:2136/local?dev_realm=r2",
			want:  false,
		},
		{
			name:  "one dev realm spelled twice",
			left:  "ydb://localhost:2136/local?dev_realm=r1",
			right: "ydb://localhost/?database=/local&dev_realm=r1",
			want:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasurl.SameDatabaseEndpoint(test.left, test.right)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// The YDB SDK silently takes the parameter when the path and the parameter
// name different databases, so a reader of the path would describe a database
// the command does not open. Such a URL is refused rather than compared.
func TestSameDatabaseEndpoint_YDB_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		left    string
		wantErr string
	}{
		{
			name:    "the path and the parameter disagree",
			left:    "ydb://localhost:2136/local?database=/dev",
			wantErr: `invalid YDB database URL: the URL names database /local in its path and /dev in the database parameter; name it once`,
		},
		{
			name:    "the parameter is given twice",
			left:    "ydb://localhost:2136/?database=/a&database=/b",
			wantErr: `invalid YDB database URL: the database parameter is given more than once`,
		},
		{
			name:    "the parameter is empty",
			left:    "ydb://localhost:2136/local?database=",
			wantErr: `invalid YDB database URL: the database parameter is empty`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasurl.SameDatabaseEndpoint(test.left, "ydb://localhost:2136/local")
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(got, qt.IsFalse)
		})
	}
}

// Hosts are not compared here, so the database alone decides, and an unknown
// database fails closed.
func TestMayAddressSameDatabase_YDB_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{
			name:  "different databases in the parameter prove distinct realms",
			left:  "ydb://localhost:2136/?database=/local",
			right: "ydb://localhost:2136/?database=/dev",
			want:  false,
		},
		{
			name:  "one database spelled in the path and the parameter fails closed",
			left:  "ydb://db-a:2136/?database=/local",
			right: "ydbs://db-b:2135/local",
			want:  true,
		},
		{
			name:  "an unspecified database fails closed",
			left:  "ydb://localhost:2136",
			right: "ydb://localhost:2136/dev",
			want:  true,
		},
		{
			name:  "YDB and another dialect are distinct realms",
			left:  "ydb://localhost:2136/local",
			right: "postgres://localhost/local",
			want:  false,
		},
		{
			name:  "a dev realm and the database that holds it are distinct realms",
			left:  "ydb://db-a:2136/local?dev_realm=r1",
			right: "ydb://db-b:2136/local",
			want:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasurl.MayAddressSameDatabase(test.left, test.right)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// SQL cannot create a YDB database, so a URL renamed to a new one would name
// a database nothing makes. The refusal comes before the path is rewritten,
// which would also leave a database parameter naming the old database.
func TestWithDatabaseName_RefusesYDB(t *testing.T) {
	for _, rawURL := range []string{"ydb://localhost:2136/local", "ydbs://localhost:2135/?database=/local"} {
		t.Run(rawURL, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasurl.WithDatabaseName(rawURL, "scratch")

			c.Assert(err, qt.ErrorIs, atlasurl.ErrYDBDatabaseName)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestDialectFromURL_YDBSchemes(t *testing.T) {
	for _, rawURL := range []string{"ydb://localhost:2136/local", "ydbs://localhost:2135/local"} {
		t.Run(rawURL, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasurl.DialectFromURL(rawURL)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, "ydb")
		})
	}
}
