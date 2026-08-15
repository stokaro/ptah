package atlasurl_test

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasurl"
)

func TestDialectFromURL_HappyPath(t *testing.T) {
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
		{name: "sqlite3 opaque drive path alias", rawURL: "sqlite3:C:/work/app.db", want: "sqlite"},
		{name: "docker postgres", rawURL: "docker://postgres/16/dev", want: "postgres"},
		{name: "docker postgres port", rawURL: "docker://postgres:16/dev", want: "postgres"},
		{name: "docker mariadb", rawURL: "docker://mariadb/11/dev", want: "mariadb"},
		{
			// The alias the pinned binary accepts and this resolver did not.
			// devdocker starts a MariaDB container for it, but the dialect
			// preflight runs first and refused `unsupported docker --dev-url
			// engine "maria"`, so no consumer ever reached the provisioner.
			name:   "docker maria alias",
			rawURL: "docker://maria/11/dev",
			want:   "mariadb",
		},
		{name: "docker maria alias with a tag", rawURL: "docker://maria:11/dev", want: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasurl.DialectFromURL(test.rawURL)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestDialectFromURL_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		wantErr string
	}{
		{name: "missing docker engine", rawURL: "docker:///dev", wantErr: `docker --dev-url is missing database engine`},
		{name: "unsupported", rawURL: "spanner://localhost/dev", wantErr: `unsupported --dev-url dialect "spanner://localhost/dev"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasurl.DialectFromURL(test.rawURL)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestValidateDialectMatch_HappyPath(t *testing.T) {
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := atlasurl.ValidateDialectMatch(test.rawURL, test.targetDialect)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidateDialectMatch_FailurePath(t *testing.T) {
	t.Run("unsupported dev url", func(t *testing.T) {
		c := qt.New(t)
		err := atlasurl.ValidateDialectMatch("spanner://localhost/dev", "postgres")
		c.Assert(err, qt.ErrorMatches, `unsupported --dev-url dialect "spanner://localhost/dev"`)
	})

	t.Run("mismatched dialect", func(t *testing.T) {
		c := qt.New(t)
		err := atlasurl.ValidateDialectMatch("mysql://localhost/dev", "postgres")
		c.Assert(err, qt.ErrorMatches, `--dev-url dialect "mysql" does not match --url dialect "postgres"`)
	})
}

func TestSameDatabaseEndpoint_HappyPath(t *testing.T) {
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
			name:  "sqlite percent-encoded file URI identifies the same file",
			left:  atlasurl.SQLiteURLFromPath(sqlitePath),
			right: "sqlite:file:" + url.PathEscape(filepath.ToSlash(sqlitePath)) + "?mode=rwc",
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
			name:  "postgres query parameters override endpoint and database",
			left:  "postgres://ignored.invalid/ignored?host=localhost&port=5432&dbname=app",
			right: "postgres://localhost/app",
			want:  true,
		},
		{
			name:  "sqlserver database query parameter is case insensitive",
			left:  "sqlserver://localhost:1433?DATABASE=app",
			right: "mssql://localhost?database=app",
			want:  true,
		},
		{
			name:  "sqlserver named instances remain distinct",
			left:  `sqlserver://localhost/instance-a?database=app`,
			right: `sqlserver://localhost/instance-b?database=app`,
			want:  false,
		},
		{
			name:  "clickhouse database query parameter overrides path",
			left:  "clickhouse://localhost:9000/ignored?database=app",
			right: "clickhouse://localhost/app",
			want:  true,
		},
		{
			name:  "different database names",
			left:  "postgres://localhost/source",
			right: "postgres://localhost/dev",
			want:  false,
		},
		{
			name:  "unspecified sqlserver databases are not proven identical",
			left:  "sqlserver://cleanup@localhost",
			right: "sqlserver://scenario@localhost",
			want:  false,
		},
		{
			name:  "case-distinct database names are not assumed identical",
			left:  "postgres://localhost/app",
			right: "postgres://localhost/App",
			want:  false,
		},
		{
			name:  "different hosts defer alias detection to live realm identity",
			left:  "postgres://db-a/app",
			right: "postgres://db-b/app",
			want:  false,
		},
		{
			name:  "different postgres fallback routes are not proven identical",
			left:  "postgres://ignored/app?host=db-a,db-b&port=5432,5432",
			right: "postgres://ignored/app?host=db-a,db-c&port=5432,5432",
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasurl.SameDatabaseEndpoint(test.left, test.right)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestSameDatabaseEndpoint_FailurePath(t *testing.T) {
	t.Run("unsupported dialect", func(t *testing.T) {
		c := qt.New(t)
		got, err := atlasurl.SameDatabaseEndpoint("oracle://localhost/source", "postgres://localhost/dev")
		c.Assert(err, qt.ErrorMatches, "unsupported database URL dialect")
		c.Assert(got, qt.IsFalse)
	})

	t.Run("invalid URL", func(t *testing.T) {
		c := qt.New(t)
		got, err := atlasurl.SameDatabaseEndpoint("postgres://%zz", "postgres://localhost/dev")
		c.Assert(err, qt.ErrorMatches, "invalid database URL")
		c.Assert(got, qt.IsFalse)
	})
}

func TestMayAddressSameDatabase_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{
			name:  "same database name across hosts fails closed",
			left:  "postgres://db-a/app",
			right: "postgres://db-b/app",
			want:  true,
		},
		{
			name:  "case-only database difference fails closed",
			left:  "postgres://db-a/app",
			right: "postgres://db-b/App",
			want:  true,
		},
		{
			name:  "different database names prove distinct realms",
			left:  "postgres://db-a/source",
			right: "postgres://db-a/dev",
			want:  false,
		},
		{
			name:  "different dialects prove distinct realms",
			left:  "postgres://localhost/app",
			right: "mysql://localhost/app",
			want:  false,
		},
		{
			name:  "unspecified database fails closed",
			left:  "sqlserver://localhost",
			right: "sqlserver://localhost/app?database=dev",
			want:  true,
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

// TestDatabaseIdentity_ReadsAWindowsAddressEverywhereThisPackageParsesOne pins
// that the Windows rule this package exports applies to the addresses it reads
// itself.
//
// [Parse] carries a drive letter's colon as opaque because net/url reads it as
// a port separator and refuses the whole address. The endpoint comparison
// reached for net/url directly, so it refused every Windows SQLite address it
// was given -- and the callers that ask it whether two URLs address one
// database reported that refusal as `invalid database URL` from `schema
// apply`, `migrate diff` and the rollback verification alike. 46 unit tests
// answered that on windows-latest, for paths the operating system they ran on
// considers ordinary.
//
// The rows run on every operating system because the defect is in string
// parsing: nothing here opens the path.
func TestDatabaseIdentity_ReadsAWindowsAddressEverywhereThisPackageParsesOne(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
		same  bool
	}{
		{
			name:  "one drive path is the same database as itself",
			left:  `sqlite://C:\Users\runner\AppData\Local\Temp\dev.db`,
			right: `sqlite://C:\Users\runner\AppData\Local\Temp\dev.db`,
			same:  true,
		},
		{
			name:  "connection options do not change which database it is",
			left:  `sqlite://C:\Users\runner\AppData\Local\Temp\dev.db`,
			right: `sqlite://C:\Users\runner\AppData\Local\Temp\dev.db?_fk=1`,
			same:  true,
		},
		{
			name:  "two drive paths are still told apart",
			left:  `sqlite://C:\Users\runner\AppData\Local\Temp\dev.db`,
			right: `sqlite://C:\Users\runner\AppData\Local\Temp\other.db`,
			same:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			same, sameErr := atlasurl.SameDatabaseEndpoint(test.left, test.right)
			may, mayErr := atlasurl.MayAddressSameDatabase(test.left, test.right)

			c.Assert(sameErr, qt.IsNil)
			c.Assert(mayErr, qt.IsNil)
			c.Assert(same, qt.Equals, test.same)
			c.Assert(may, qt.Equals, test.same)
		})
	}
}
