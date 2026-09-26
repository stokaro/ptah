package atlasurl_test

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
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
		// The pinned binary v1.3.0 accepts `maria://` wherever it opens a
		// database, as the MariaDB spelling it also takes as a docker engine.
		{name: "maria", rawURL: "maria://root@localhost:3306/dev", want: "mariadb"},
		{name: "maria TCP spelling", rawURL: "maria://root:pa)ss@tcp(localhost:3306)/dev", want: "mariadb"},
		{name: "maria in upper case", rawURL: "MARIA://root@localhost/dev", want: "mariadb"},
		// The socket spellings the pinned binary v1.3.0 opens. Only the
		// transport differs from the plain scheme.
		{name: "mysql socket", rawURL: "mysql+unix://root@/run/mysqld/mysqld.sock?database=dev", want: "mysql"},
		{name: "mariadb socket", rawURL: "mariadb+unix://root@/run/mysqld/mysqld.sock?database=dev", want: "mariadb"},
		{name: "maria socket", rawURL: "maria+unix://root@/run/mysqld/mysqld.sock", want: "mariadb"},
		{name: "sqlite3 opaque drive path alias", rawURL: "sqlite3:C:/work/app.db", want: "sqlite"},
		{name: "docker postgres", rawURL: "docker://postgres/16/dev", want: "postgres"},
		{name: "docker postgres port", rawURL: "docker://postgres:16/dev", want: "postgres"},
		{name: "docker mariadb", rawURL: "docker://mariadb/11/dev", want: "mariadb"},
		{
			// The engine the pinned binary provisions for this name is its
			// MariaDB image, as it is for `docker://mariadb/...`. devdocker
			// starts a MariaDB container for it too, and it is reached only if
			// the dialect preflight in front of it resolves `maria`.
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
		// "db2" rather than "spanner": Spanner lands here only when a
		// hand-written scheme list has drifted from NormalizeDialect, not
		// because a Spanner dev database is refused -- internal/devclean and
		// internal/devlock both put it in the PostgreSQL family, and the
		// dev-database page documents the URL form.
		{name: "unsupported", rawURL: "db2://localhost/dev", wantErr: `unsupported --dev-url dialect "db2://localhost/dev"`},
		// The pinned binary v1.3.0 answers `unknown driver "maria+tcp"`: the
		// MariaDB spelling is a scheme of its own, not a prefix.
		{name: "maria with a transport the community binary refuses", rawURL: "maria+tcp://localhost/dev", wantErr: `unsupported --dev-url dialect "maria\+tcp://localhost/dev"`},
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
		// A MySQL-family scheme does not say whether its server is MySQL or
		// MariaDB, so it names a server of either.
		{name: "a mysql dev URL for a MariaDB target", rawURL: "mysql://localhost/dev", targetDialect: "mariadb"},
		{name: "a maria dev URL for a MySQL target", rawURL: "maria://localhost/dev", targetDialect: "mysql"},
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
		err := atlasurl.ValidateDialectMatch("db2://localhost/dev", "postgres")
		c.Assert(err, qt.ErrorMatches, `unsupported --dev-url dialect "db2://localhost/dev"`)
	})

	t.Run("mismatched dialect", func(t *testing.T) {
		c := qt.New(t)
		err := atlasurl.ValidateDialectMatch("mysql://localhost/dev", "postgres")
		c.Assert(err, qt.ErrorMatches, `--dev-url dialect "mysql" does not match --url dialect "postgres"`)
	})

	t.Run("a MariaDB dev URL for a PostgreSQL target", func(t *testing.T) {
		c := qt.New(t)
		err := atlasurl.ValidateDialectMatch("mariadb://localhost/dev", "postgres")
		c.Assert(err, qt.ErrorMatches, `--dev-url dialect "mariadb" does not match --url dialect "postgres"`)
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
			name:  "a socket and its database",
			left:  "mysql+unix://writer@/run/mysqld/mysqld.sock?database=app",
			right: "mysql+unix://reader@/run/mysqld/../mysqld/mysqld.sock?database=app&parseTime=true",
			want:  true,
		},
		{
			name:  "one socket, two databases",
			left:  "mysql+unix://root@/run/mysqld/mysqld.sock?database=app",
			right: "mysql+unix://root@/run/mysqld/mysqld.sock?database=dev",
			want:  false,
		},
		{
			name:  "a socket is not proven to be a TCP address",
			left:  "mysql+unix://root@/run/mysqld/mysqld.sock?database=app",
			right: "mysql://root@127.0.0.1:3306/app",
			want:  false,
		},
		{
			name:  "the driver's socket form and the socket URL",
			left:  "mariadb://root@unix(/run/mysqld/mysqld.sock)/app",
			right: "mariadb+unix://root@/run/mysqld/mysqld.sock?database=app",
			want:  true,
		},
		{
			name:  "maria and mariadb spell one dialect and one server",
			left:  "maria://root:pa)ss@tcp(localhost:3306)/app",
			right: "mariadb://reader@localhost/app",
			want:  true,
		},
		{
			name:  "mysql and mariadb spellings of one server and database",
			left:  "mysql://writer@localhost:3306/app",
			right: "mariadb://reader@127.0.0.1/app",
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
		got, err := atlasurl.SameDatabaseEndpoint("db2://localhost/source", "postgres://localhost/dev")
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
		// A socket URL's path is the socket. Read as the database, the target
		// below is `run/mysqld/mysqld.sock`, the two look distinct, and the
		// dev-database cleanup this guards would run against the target.
		{
			name:  "a socket URL and a TCP URL naming one database fail closed",
			left:  "mysql+unix://root@/run/mysqld/mysqld.sock?database=app",
			right: "mysql://root@127.0.0.1:3306/app",
			want:  true,
		},
		{
			name:  "two socket URLs naming two databases prove distinct realms",
			left:  "mariadb+unix://root@/run/mysqld/mysqld.sock?database=app",
			right: "mariadb+unix://root@/run/mysqld/mysqld.sock?database=dev",
			want:  false,
		},
		{
			name:  "a socket URL naming no database fails closed",
			left:  "mysql+unix://root@/run/mysqld/mysqld.sock",
			right: "mysql://root@127.0.0.1:3306/dev",
			want:  true,
		},
		// The schemes cannot tell MySQL from MariaDB, so two spellings naming
		// one database name are not proven distinct. Proven distinct, the dev
		// URL below is reset as a dev database while it is the target.
		{
			name:  "mysql and mariadb spellings naming one database fail closed",
			left:  "mysql://root@localhost/app",
			right: "mariadb://root@localhost/app",
			want:  true,
		},
		{
			name:  "maria and mysql spellings naming two databases prove distinct realms",
			left:  "maria://root@localhost/app",
			right: "mysql://root@localhost/dev",
			want:  false,
		},
		{
			name:  "a mariadb socket URL and a mysql TCP URL naming one database fail closed",
			left:  "mariadb+unix://root@/run/mysqld/mysqld.sock?database=app",
			right: "mysql://root@127.0.0.1:3306/app",
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

// TestIsDockerURL_HappyPath names the URLs that ask Ptah to start a dev
// database. The engine, the port and the trailing path vary and none of them
// changes the answer.
func TestIsDockerURL_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
	}{
		{name: "sqlite", rawURL: "docker://sqlite/dev"},
		{name: "postgres", rawURL: "docker://postgres/16/dev"},
		{name: "mysql with tag", rawURL: "docker://mysql/8/dev"},
		{name: "no path", rawURL: "docker://postgres"},
		{name: "surrounding space", rawURL: "  docker://postgres/16/dev  "},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasurl.IsDockerURL(test.rawURL), qt.IsTrue)
		})
	}
}

// TestIsDockerURL_FailurePath covers the URLs a target may legitimately carry,
// including the SQLite spellings whose opaque form has no host at all.
func TestIsDockerURL_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
	}{
		{name: "empty", rawURL: ""},
		{name: "postgres", rawURL: "postgres://user@localhost:5432/db"},
		{name: "sqlite path", rawURL: "sqlite://app.db"},
		{name: "sqlite opaque", rawURL: "sqlite:file:app.db"},
		{name: "a host named docker", rawURL: "postgres://user@docker:5432/db"},
		{name: "scheme prefix only", rawURL: "dockerish://postgres/dev"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasurl.IsDockerURL(test.rawURL), qt.IsFalse)
		})
	}
}

func TestCutMySQLScheme_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{name: "mysql", rawURL: "mysql://root@localhost:3306/app", want: "root@localhost:3306/app"},
		{name: "mariadb", rawURL: "mariadb://root@tcp(localhost:3306)/app", want: "root@tcp(localhost:3306)/app"},
		{name: "maria", rawURL: "maria://root@localhost/app", want: "root@localhost/app"},
		{name: "maria TCP spelling", rawURL: "maria://root:pa)ss@tcp(localhost:3306)/app", want: "root:pa)ss@tcp(localhost:3306)/app"},
		{name: "maria socket spelling", rawURL: "maria://unix(/tmp/mysql.sock)/app", want: "unix(/tmp/mysql.sock)/app"},
		// net/url lowercases a scheme, so the connector reads this one as
		// MariaDB; a scheme test that did not would pass it to the driver
		// with the scheme attached.
		{name: "upper case", rawURL: "MARIADB://root:pass@tcp(localhost:3306)/app", want: "root:pass@tcp(localhost:3306)/app"},
		{name: "mixed case", rawURL: "Maria://root@localhost/app", want: "root@localhost/app"},
		{name: "only the first separator is the scheme's", rawURL: "maria://root:a://b@localhost/app", want: "root:a://b@localhost/app"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rest, ok := atlasurl.CutMySQLScheme(test.rawURL)
			c.Assert(ok, qt.IsTrue)
			c.Assert(rest, qt.Equals, test.want)
		})
	}
}

func TestCutMySQLScheme_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
	}{
		{name: "another dialect", rawURL: "postgres://localhost/app"},
		{name: "a transport the community binary refuses", rawURL: "maria+tcp://localhost/app"},
		{name: "a longer name that begins with a family spelling", rawURL: "mariadbx://localhost/app"},
		{name: "a docker dev URL naming the engine", rawURL: "docker://maria/11/dev"},
		{name: "no separator", rawURL: "maria:root@localhost/app"},
		{name: "a driver DSN with no scheme", rawURL: "root:pass@tcp(localhost:3306)/app"},
		// net/url refuses a scheme with a space in it, and so does this: a
		// trimmed comparison would read a value the connector rejects as one
		// it accepts.
		{name: "leading space", rawURL: " maria://localhost/app"},
		{name: "space before the separator", rawURL: "maria ://localhost/app"},
		{name: "empty", rawURL: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rest, ok := atlasurl.CutMySQLScheme(test.rawURL)
			c.Assert(ok, qt.IsFalse)
			c.Assert(rest, qt.Equals, test.rawURL)
		})
	}
}

// TestWithDatabaseName_KeepsTheMySQLForm names another database in each form a
// MySQL-family URL takes, leaving the rest as written. In a +unix URL the path
// is the socket, so replacing the path would name a socket after the database.
func TestWithDatabaseName_KeepsTheMySQLForm(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{
			name:   "URL form",
			rawURL: "mysql://app@db:3307/shop?parseTime=true",
			want:   "mysql://app@db:3307/scratch?parseTime=true",
		},
		{
			name:   "driver form",
			rawURL: "maria://app:pw@tcp(db:3307)/shop?parseTime=true",
			want:   "maria://app:pw@tcp(db:3307)/scratch?parseTime=true",
		},
		{
			name:   "socket form",
			rawURL: "mysql+unix://app:pw@/run/x.sock?database=shop&parseTime=true",
			want:   "mysql+unix://app:pw@/run/x.sock?database=scratch&parseTime=true",
		},
		{
			name:   "socket form naming no database",
			rawURL: "mariadb+unix://app:pw@/run/x.sock",
			want:   "mariadb+unix://app:pw@/run/x.sock?database=scratch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasurl.WithDatabaseName(test.rawURL, "scratch")

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestSchemeDialectMatches_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		schemeDialect string
		other         string
	}{
		{name: "one dialect", schemeDialect: "postgres", other: "postgres"},
		{name: "an alias of the other", schemeDialect: "postgresql", other: "postgres"},
		{name: "mysql names a MariaDB server", schemeDialect: "mysql", other: "mariadb"},
		{name: "mariadb names a MySQL server", schemeDialect: "mariadb", other: "mysql"},
		{name: "maria names a MySQL server", schemeDialect: "maria", other: "mysql"},
		{name: "letter case", schemeDialect: "MySQL", other: "MariaDB"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasurl.SchemeDialectMatches(test.schemeDialect, test.other), qt.IsTrue)
		})
	}
}

// TestSchemeDialectMatches_FailurePath keeps the MySQL family a family of two.
// The PostgreSQL-wire dialects are not joined: a CockroachDB or Spanner server
// may announce itself as PostgreSQL, so there the scheme is evidence.
func TestSchemeDialectMatches_FailurePath(t *testing.T) {
	tests := []struct {
		name          string
		schemeDialect string
		other         string
	}{
		{name: "MySQL and PostgreSQL", schemeDialect: "mysql", other: "postgres"},
		{name: "MariaDB and SQLite", schemeDialect: "mariadb", other: "sqlite"},
		{name: "two PostgreSQL-wire dialects", schemeDialect: "cockroachdb", other: "postgres"},
		{name: "a dialect and nothing", schemeDialect: "mysql", other: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasurl.SchemeDialectMatches(test.schemeDialect, test.other), qt.IsFalse)
		})
	}
}
