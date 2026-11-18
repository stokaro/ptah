//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// keySelectorTarget is a database built from a row's live schema, and a way to
// read one text value back from it.
type keySelectorTarget struct {
	url  string
	read func(c *qt.C, query string) string
}

func postgresKeySelectorTarget(c *qt.C, live string) keySelectorTarget {
	c.Helper()
	url := postgresScratchDevURL(c, "ptah_key_selector")
	execPostgresFamilySQL(c, c.Context(), url, live)
	return keySelectorTarget{url: url, read: func(c *qt.C, query string) string {
		c.Helper()
		var value string
		c.Assert(openReadScopeDatabase(c, url).QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil)
		return value
	}}
}

// mySQLFamilyKeySelectorTarget builds the database on engine's server, and
// names it with the URL scheme that states engine's dialect.
func mySQLFamilyKeySelectorTarget(engine foreignKeySelectorEngine) func(c *qt.C, live string) keySelectorTarget {
	return func(c *qt.C, live string) keySelectorTarget {
		c.Helper()
		scratch := newMySQLFamilyScratch(c, engine.admin)
		name, url := scratch.builtFrom(c, live)
		return keySelectorTarget{url: engine.scheme + strings.TrimPrefix(url, "mysql://"), read: readMySQLFamily(scratch, name)}
	}
}

// keySelectorServers are the builders of a database on each server the rows
// name.
func keySelectorServers() map[string]func(c *qt.C, live string) keySelectorTarget {
	servers := map[string]func(c *qt.C, live string) keySelectorTarget{"postgresql": postgresKeySelectorTarget}
	for _, engine := range foreignKeySelectorEngines {
		servers[engine.name] = mySQLFamilyKeySelectorTarget(engine)
	}
	return servers
}

func readMySQLFamily(scratch mysqlScratch, name string) func(c *qt.C, query string) string {
	return func(c *qt.C, query string) string {
		c.Helper()
		db, err := sql.Open("mysql", mySQLDSNForDatabase(c, scratch.adminDSN, name))
		c.Assert(err, qt.IsNil)
		defer func() { c.Check(db.Close(), qt.IsNil) }()
		var value string
		c.Assert(db.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil)
		return value
	}
}

const (
	postgresKindsIndexes = "SELECT string_agg(indexname, ',' ORDER BY indexname) FROM pg_indexes " +
		"WHERE schemaname = 'public' AND tablename = 'kinds'"
	postgresKindsPrimaryKey = "SELECT conname FROM pg_constraint WHERE contype = 'p' AND conrelid = 'public.kinds'::regclass"
	postgresTypesPrimaryKey = "SELECT conname FROM pg_constraint WHERE contype = 'p' AND conrelid = 'public.types'::regclass"
	mySQLKindsIndexes       = "SELECT GROUP_CONCAT(DISTINCT INDEX_NAME ORDER BY INDEX_NAME) FROM information_schema.STATISTICS " +
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kinds'"
	mySQLKindsID = "SELECT CONCAT_WS(' ', COLUMN_KEY, EXTRA, IS_NULLABLE) FROM information_schema.COLUMNS " +
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kinds' AND COLUMN_NAME = 'id'"
)

// TestExcludeNamesAKeyByTheTargetsNameE2E applies a desired schema with a
// selector, reads the table back, and plans and compares again. A selector
// that names a key by the name the target's server gives it keeps the key out
// of the plan, and the runs that follow find nothing to do.
//
// The UNIQUE rows place an unrelated index under the name the other engine
// would give the column's UNIQUE, and name that index. The UNIQUE the server
// holds is not named, so it stays, under its own name; a desired side that
// read the other engine's name as the UNIQUE's would drop it.
//
// A declaration that writes a key on its column leaves the key unnamed, and
// the server may hold the key under another name: one the author gave it, or
// the name a renamed table kept. A selector names that key by the server's
// name on both sides. A desired side that named its key by the predicted name
// alone kept the key the selector named, and planned ADD PRIMARY KEY, which
// PostgreSQL refuses with 42P16 while the server's key stands.
func TestExcludeNamesAKeyByTheTargetsNameE2E(t *testing.T) {
	type keySelectorRow struct {
		name, server                 string
		live, desired, exclude, read string
		want                         string
	}
	rows := []keySelectorRow{{
		name:   "postgresql/an index named as MySQL names a UNIQUE",
		server: "postgresql",
		live: "CREATE TABLE public.kinds (id int PRIMARY KEY, code text NOT NULL, CONSTRAINT kinds_code_key UNIQUE (code));\n" +
			"CREATE INDEX code ON public.kinds (id, code);\n",
		desired: "CREATE TABLE public.kinds (id int PRIMARY KEY, code text NOT NULL UNIQUE);\n",
		exclude: "public.kinds.code[type=index]",
		read:    postgresKindsIndexes,
		want:    "code,kinds_code_key,kinds_pkey",
	}}
	for _, exclude := range []string{"public.kinds.pk_kinds", "public.kinds.pk_kinds[type=constraint]", "public.kinds.pk_kinds[type=index]"} {
		rows = append(rows, keySelectorRow{
			name:    "postgresql/a primary key the author named, as " + exclude,
			server:  "postgresql",
			live:    "CREATE TABLE public.kinds (id int NOT NULL, code text, CONSTRAINT pk_kinds PRIMARY KEY (id));\n",
			desired: "CREATE TABLE public.kinds (id int NOT NULL PRIMARY KEY, code text);\n",
			exclude: exclude,
			read:    postgresKindsPrimaryKey,
			want:    "pk_kinds",
		})
	}
	for _, exclude := range []string{"public.types.kinds_pkey", "public.types.kinds_pkey[type=constraint]", "public.types.kinds_pkey[type=index]"} {
		rows = append(rows, keySelectorRow{
			name:    "postgresql/the primary key a renamed table kept, as " + exclude,
			server:  "postgresql",
			live:    "CREATE TABLE public.types (id int NOT NULL, CONSTRAINT kinds_pkey PRIMARY KEY (id));\n",
			desired: "CREATE TABLE public.types (id int NOT NULL PRIMARY KEY);\n",
			exclude: exclude,
			read:    postgresTypesPrimaryKey,
			want:    "kinds_pkey",
		})
	}

	for _, engine := range foreignKeySelectorEngines {
		rows = append(rows,
			keySelectorRow{
				name:   engine.name + "/an index named as PostgreSQL names a UNIQUE",
				server: engine.name,
				live: "CREATE TABLE kinds (id int NOT NULL, code varchar(20) NOT NULL, PRIMARY KEY (id), " +
					"UNIQUE KEY code (code), KEY kinds_code_key (id, code));",
				desired: "CREATE TABLE kinds (id int NOT NULL, code varchar(20) NOT NULL UNIQUE, PRIMARY KEY (id));\n",
				exclude: "kinds.kinds_code_key[type=index]",
				read:    mySQLKindsIndexes,
				want:    "code,kinds_code_key,PRIMARY",
			},
			keySelectorRow{
				name:    engine.name + "/the primary key as an index, declared on the column",
				server:  engine.name,
				live:    "CREATE TABLE kinds (id int NOT NULL AUTO_INCREMENT, code varchar(20), PRIMARY KEY (id));",
				desired: "CREATE TABLE kinds (id int AUTO_INCREMENT PRIMARY KEY, code varchar(20));\n",
				exclude: "kinds.PRIMARY[type=index]",
				read:    mySQLKindsID,
				want:    "PRI auto_increment NO",
			},
			keySelectorRow{
				name:    engine.name + "/the primary key as a constraint, declared nowhere",
				server:  engine.name,
				live:    "CREATE TABLE kinds (id int NOT NULL AUTO_INCREMENT, code varchar(20), PRIMARY KEY (id));",
				desired: "CREATE TABLE kinds (id int NOT NULL AUTO_INCREMENT, code varchar(20));\n",
				exclude: "kinds.PRIMARY[type=constraint]",
				read:    mySQLKindsID,
				want:    "PRI auto_increment NO",
			},
		)
	}
	servers := keySelectorServers()
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			target := servers[row.server](c, row.live)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(row.desired), 0o600), qt.IsNil)
			apply := []string{"schema", "apply", "--db-url", target.url, "--schema-file", schema, "--exclude", row.exclude}

			applied := runPtahIn(c, append(apply, "--auto-approve")...)
			read := target.read(c, row.read)
			planned := runPtahIn(c, append(apply, "--dry-run")...)
			compared := runPtahIn(c, "schema", "compare", "--db-url", target.url, "--schema-file", schema, "--exit-code", "--exclude", row.exclude)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(read, qt.Equals, row.want)
			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(compared.ExitCode, qt.Equals, 0, historyComment(compared))
			c.Assert(comparisonBody(c, compared.Stdout), qt.Equals, includeNoDiffs)
		})
	}
}

// TestExcludeNamesNoKeyByTheNameADeclarationPredictsE2E names a primary key
// by the name the declaration predicts for it, where the server holds the key
// under another name. The selector names nothing on either side, so apply
// refuses it as a selection that protects nothing, and the key stays. Allowed
// to run, the comparison pairs the two keys and plans nothing: no DROP
// CONSTRAINT for the server's key and no ADD PRIMARY KEY beside it.
func TestExcludeNamesNoKeyByTheNameADeclarationPredictsE2E(t *testing.T) {
	tests := []struct {
		name, live, desired, exclude, read, want string
	}{
		{
			name:    "a primary key the author named",
			live:    "CREATE TABLE public.kinds (id int NOT NULL, code text, CONSTRAINT pk_kinds PRIMARY KEY (id));\n",
			desired: "CREATE TABLE public.kinds (id int NOT NULL PRIMARY KEY, code text);\n",
			exclude: "public.kinds.kinds_pkey",
			read:    postgresKindsPrimaryKey,
			want:    "pk_kinds",
		},
		{
			name:    "the primary key a renamed table kept",
			live:    "CREATE TABLE public.types (id int NOT NULL, CONSTRAINT kinds_pkey PRIMARY KEY (id));\n",
			desired: "CREATE TABLE public.types (id int NOT NULL PRIMARY KEY);\n",
			exclude: "public.types.types_pkey[type=constraint]",
			read:    postgresTypesPrimaryKey,
			want:    "kinds_pkey",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := postgresKeySelectorTarget(c, test.live)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.desired), 0o600), qt.IsNil)
			apply := []string{"schema", "apply", "--db-url", target.url, "--schema-file", schema, "--exclude", test.exclude}
			allowed := clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1")}

			refused := runPtahIn(c, append(apply, "--auto-approve")...)
			afterRefusal := target.read(c, test.read)
			applied := clirun.Run(c, clirun.Ptah, allowed, append(apply, "--auto-approve")...)
			afterApply := target.read(c, test.read)
			planned := clirun.Run(c, clirun.Ptah, allowed, append(apply, "--dry-run")...)

			c.Assert(refused.ExitCode, qt.Equals, 2, historyComment(refused))
			c.Assert(refused.Stderr, qt.Contains, `the --exclude selection matched no objects: "`+test.exclude+`"`)
			c.Assert(afterRefusal, qt.Equals, test.want)
			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(afterApply, qt.Equals, test.want)
			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestExcludeNamesAUniqueByItsOwnNameE2E excludes one of two UNIQUE
// constraints on the same column: the server's, under the name an author gave
// it, and the declaration's, under the name PostgreSQL gives a column's
// UNIQUE. The comparison pairs a UNIQUE by its name, so the two are different
// constraints and a selector leaves out the one it names. The plan creates or
// drops the other, as the community binary v1.3.0 plans.
func TestExcludeNamesAUniqueByItsOwnNameE2E(t *testing.T) {
	tests := []struct {
		exclude, want string
	}{
		{exclude: "public.kinds.uq_code", want: "Planned schema changes:\n" +
			`ALTER TABLE "public"."kinds" ADD CONSTRAINT "kinds_code_key" UNIQUE ("code");` + "\n"},
		{exclude: "public.kinds.kinds_code_key", want: "Planned schema changes:\n" +
			`ALTER TABLE "public"."kinds" DROP CONSTRAINT "uq_code";` + "\n"},
	}
	for _, test := range tests {
		t.Run(test.exclude, func(t *testing.T) {
			c := qt.New(t)
			target := postgresKeySelectorTarget(c, "CREATE TABLE public.kinds (id int PRIMARY KEY, code text, CONSTRAINT uq_code UNIQUE (code));\n")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte("CREATE TABLE public.kinds (id int PRIMARY KEY, code text UNIQUE);\n"), 0o600), qt.IsNil)

			planned := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--schema-file", schema, "--exclude", test.exclude, "--dry-run")

			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Equals, test.want)
		})
	}
}

// sqliteRewriteLive holds a named primary key, a UNIQUE, a CHECK and an index,
// and sqliteRewriteDesired declares the primary key alone. A plan changes the
// constraints the declaration leaves out by writing a new table.
const (
	sqliteRewriteLive = "CREATE TABLE kinds (id integer NOT NULL, code text, CONSTRAINT kinds_pk PRIMARY KEY (id), " +
		"CONSTRAINT uq_code UNIQUE (code), CONSTRAINT code_len CHECK (length(code) > 0));\nCREATE INDEX idx_code ON kinds (code);\n"
	sqliteRewriteDesired = "CREATE TABLE kinds (id integer NOT NULL, code text, CONSTRAINT kinds_pk PRIMARY KEY (id));\n"
	// sqliteRewriteKept is what sqlite_schema holds for kinds and its index
	// while no plan has rewritten the table.
	sqliteRewriteKept = "CREATE TABLE kinds (id integer NOT NULL, code text, CONSTRAINT kinds_pk PRIMARY KEY (id), " +
		"CONSTRAINT uq_code UNIQUE (code), CONSTRAINT code_len CHECK (length(code) > 0));CREATE INDEX idx_code ON kinds (code)"
)

// sqliteRewriteFixture is a database file built from sqliteRewriteLive and
// the desired schema file.
type sqliteRewriteFixture struct {
	url, schema string
	db          *sql.DB
}

func newSQLiteRewriteFixture(c *qt.C) sqliteRewriteFixture {
	c.Helper()
	dir := c.TempDir()
	path := filepath.Join(dir, "live.db")
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.ExecContext(c.Context(), sqliteRewriteLive)
	c.Assert(err, qt.IsNil)
	schema := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(sqliteRewriteDesired), 0o600), qt.IsNil)
	return sqliteRewriteFixture{url: "sqlite://" + filepath.ToSlash(path), schema: schema, db: db}
}

// stored returns the statements sqlite_schema holds for kinds and its
// indexes, the table first.
func (f sqliteRewriteFixture) stored(c *qt.C) string {
	c.Helper()
	var stored string
	c.Assert(f.db.QueryRowContext(c.Context(), "SELECT group_concat(sql, ';') FROM (SELECT sql FROM sqlite_schema "+
		"WHERE tbl_name = 'kinds' AND sql IS NOT NULL ORDER BY type DESC, name)").Scan(&stored), qt.IsNil)
	return stored
}

func (f sqliteRewriteFixture) apply(c *qt.C, exclude, mode string) clirun.Result {
	c.Helper()
	return runPtahIn(c, "schema", "apply", "--db-url", f.url, "--schema-file", f.schema, "--exclude", exclude, mode)
}

func (f sqliteRewriteFixture) compatDiff(c *qt.C, exclude string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, "schema", "diff",
		"--from", f.url, "--to", "file://"+filepath.ToSlash(f.schema), "--dev-url", "sqlite://dev?mode=memory", "--exclude", exclude)
}

// TestExcludeRefusesAConstraintSQLiteRewritesE2E excludes constraints on
// SQLite, where a desired schema without the UNIQUE and the CHECK plans a new
// table written from the declaration. A constraint the selector keeps out of
// scope would not be in it, so both binaries refuse a selector that names a
// constraint and nothing else, `*[type=constraint]` included, and the table
// keeps every constraint. Excluding the whole table keeps them all.
func TestExcludeRefusesAConstraintSQLiteRewritesE2E(t *testing.T) {
	const remedy = `, and sqlite changes a constraint only by writing a new table from the declaration, ` +
		`which would leave the constraint out; exclude the whole table instead`
	tests := []struct {
		exclude, want string
	}{
		{exclude: "kinds.code_len[type=check]", want: `exclude selector "kinds.code_len[type=check]" names constraint "code_len" of table "kinds"`},
		{exclude: "*[type=constraint]", want: `exclude selector "*[type=constraint]" names constraint "uq_code" of table "kinds"`},
	}
	for _, test := range tests {
		t.Run(test.exclude, func(t *testing.T) {
			c := qt.New(t)
			fixture := newSQLiteRewriteFixture(c)

			applied := fixture.apply(c, test.exclude, "--auto-approve")
			diffed := fixture.compatDiff(c, test.exclude)
			whole := fixture.apply(c, "kinds", "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 2, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, test.want+remedy)
			c.Assert(diffed.ExitCode, qt.Equals, 1, historyComment(diffed))
			c.Assert(diffed.Stderr, qt.Contains, test.want+remedy)
			c.Assert(whole.ExitCode, qt.Equals, 0, historyComment(whole))
			c.Assert(fixture.stored(c), qt.Equals, sqliteRewriteKept)
		})
	}
}

// TestExcludeAnIndexSelectorRewritesASQLiteTableE2E excludes the indexes of a
// table on SQLite. The selector names the live index and, in the declaration,
// the primary key; it was written for the index, so it keeps its meaning there
// and is not refused. The plan writes a new table without the UNIQUE and the
// CHECK the declaration leaves out, as the community binary v1.3.0 plans.
func TestExcludeAnIndexSelectorRewritesASQLiteTableE2E(t *testing.T) {
	for _, exclude := range []string{"*[type=index]", "kinds.*[type=index]"} {
		t.Run(exclude, func(t *testing.T) {
			c := qt.New(t)
			fixture := newSQLiteRewriteFixture(c)

			planned := fixture.apply(c, exclude, "--dry-run")
			diffed := fixture.compatDiff(c, exclude)

			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Contains, `CREATE TABLE "new_kinds"`)
			c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
			c.Assert(diffed.Stdout, qt.Contains, `-- Create "new_kinds" table`)
		})
	}
}

// TestExcludeEveryColumnOfASQLiteTableKeepsItE2E excludes every column of a
// table on SQLite. Each constraint of the table leaves with its columns, the
// CHECK whose columns the read cannot tell included, so nothing is left to
// change and the table is kept as it is, as on the community binary v1.3.0.
func TestExcludeEveryColumnOfASQLiteTableKeepsItE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newSQLiteRewriteFixture(c)

	applied := fixture.apply(c, "kinds.*", "--auto-approve")
	diffed := fixture.compatDiff(c, "kinds.*")

	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	c.Assert(applied.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
	c.Assert(diffed.Stdout, qt.Equals, includeSynced)
	c.Assert(fixture.stored(c), qt.Equals, sqliteRewriteKept)
}
