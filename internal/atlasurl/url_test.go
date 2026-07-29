package atlasurl_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasurl"
)

func TestDialectFromURL_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{name: "empty", rawURL: "", want: ""},
		{name: "postgres", rawURL: "postgres://localhost/dev", want: "postgres"},
		{name: "postgresql alias", rawURL: "postgresql://localhost/dev", want: "postgres"},
		{name: "sqlserver", rawURL: "sqlserver://localhost/dev", want: "sqlserver"},
		{name: "mysql TCP spelling", rawURL: "mysql://root@tcp(localhost:3306)/dev", want: "mysql"},
		{name: "mysql TCP spelling with closing parenthesis in password", rawURL: "mysql://root:pa)ss@tcp(localhost:3306)/dev", want: "mysql"},
		{name: "mariadb TCP spelling", rawURL: "mariadb://root@tcp(localhost:3306)/dev", want: "mariadb"},
		{name: "docker postgres", rawURL: "docker://postgres/16/dev", want: "postgres"},
		{name: "docker postgres port", rawURL: "docker://postgres:16/dev", want: "postgres"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasurl.DialectFromURL(test.rawURL)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestDialectFromURL_FailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		rawURL  string
		wantErr string
	}{
		{name: "missing docker engine", rawURL: "docker:///dev", wantErr: `docker --dev-url is missing database engine`},
		{name: "unsupported", rawURL: "spanner://localhost/dev", wantErr: `unsupported --dev-url dialect "spanner://localhost/dev"`},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasurl.DialectFromURL(test.rawURL)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestValidateDialectMatch_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name          string
		rawURL        string
		targetDialect string
	}{
		{name: "empty dev url", rawURL: "", targetDialect: "postgres"},
		{name: "exact dialect", rawURL: "mysql://localhost/dev", targetDialect: "mysql"},
		{name: "target alias", rawURL: "postgres://localhost/dev", targetDialect: "postgresql"},
		{name: "docker dialect", rawURL: "docker://mariadb/11/dev", targetDialect: "mariadb"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := atlasurl.ValidateDialectMatch(test.rawURL, test.targetDialect)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidateDialectMatch_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("unsupported dev url", func(c *qt.C) {
		err := atlasurl.ValidateDialectMatch("spanner://localhost/dev", "postgres")
		c.Assert(err, qt.ErrorMatches, `unsupported --dev-url dialect "spanner://localhost/dev"`)
	})

	c.Run("mismatched dialect", func(c *qt.C) {
		err := atlasurl.ValidateDialectMatch("mysql://localhost/dev", "postgres")
		c.Assert(err, qt.ErrorMatches, `--dev-url dialect "mysql" does not match --url dialect "postgres"`)
	})
}

func TestSameDatabase_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	sqlitePath := filepath.Join(dir, "dev.db")
	sqliteHardLink := filepath.Join(dir, "dev-hard-link.db")
	c.Assert(os.WriteFile(sqlitePath, nil, 0o600), qt.IsNil)
	c.Assert(os.Link(sqlitePath, sqliteHardLink), qt.IsNil)

	tests := []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{
			name:  "postgres credentials and options do not change identity",
			left:  "postgres://writer@localhost/app?sslmode=disable",
			right: "postgresql://reader@localhost:5432/app?sslmode=require",
			want:  true,
		},
		{
			name:  "mysql TCP spelling and options do not change identity",
			left:  "mysql://root:pa)ss@tcp(localhost:3306)/app?parseTime=true",
			right: "mysql://reader@localhost/app?tls=false",
			want:  true,
		},
		{
			name:  "sqlite relative and absolute paths identify the same file",
			left:  atlasurl.SQLiteURLFromPath(sqlitePath),
			right: atlasurl.SQLiteURLFromPath(filepath.Join(dir, ".", "dev.db")) + "?mode=rwc",
			want:  true,
		},
		{
			name:  "sqlite hard links identify the same file",
			left:  atlasurl.SQLiteURLFromPath(sqlitePath),
			right: atlasurl.SQLiteURLFromPath(sqliteHardLink),
			want:  true,
		},
		{
			name:  "loopback host aliases identify the same server",
			left:  "postgres://localhost/app",
			right: "postgres://127.0.0.1:5432/app",
			want:  true,
		},
		{
			name:  "expanded and compressed ipv6 identify the same server",
			left:  "postgres://[2001:0db8:0000:0000:0000:ff00:0042:8329]/app",
			right: "postgres://[2001:db8::ff00:42:8329]:5432/app",
			want:  true,
		},
		{
			name:  "expanded ipv6 loopback and localhost identify the same server",
			left:  "postgres://[0:0:0:0:0:0:0:1]/app",
			right: "postgres://localhost:5432/app",
			want:  true,
		},
		{
			name:  "different database names",
			left:  "postgres://localhost/source",
			right: "postgres://localhost/dev",
			want:  false,
		},
		{
			name:  "different hosts",
			left:  "postgres://db-a/app",
			right: "postgres://db-b/app",
			want:  false,
		},
		{
			name:  "different sqlite files",
			left:  atlasurl.SQLiteURLFromPath(sqlitePath),
			right: atlasurl.SQLiteURLFromPath(filepath.Join(dir, "other.db")),
			want:  false,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasurl.SameDatabase(test.left, test.right)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestSameDatabase_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("unsupported dialect", func(c *qt.C) {
		got, err := atlasurl.SameDatabase("oracle://localhost/source", "postgres://localhost/dev")
		c.Assert(err, qt.ErrorMatches, "unsupported database URL dialect")
		c.Assert(got, qt.IsFalse)
	})

	c.Run("invalid URL", func(c *qt.C) {
		got, err := atlasurl.SameDatabase("postgres://%zz", "postgres://localhost/dev")
		c.Assert(err, qt.ErrorMatches, "invalid database URL")
		c.Assert(got, qt.IsFalse)
	})
}
