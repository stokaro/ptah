//go:build integration

package providercli_test

import (
	"database/sql"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"
	// The test reads the fixture's database file back through a driver of its
	// own, so the answer does not pass through the provider the statement used.
	_ "modernc.org/sqlite"

	"ptah.run/core/ptaherr"
	"ptah.run/core/query"
	"ptah.run/dbschema"
	"ptah.run/internal/providerhost"
)

// A statement the query builder renders for an engine whose provider declares
// the dollar-named placeholder style runs through the provider, each named
// argument bound to the parameter its name says. The named-parameters fixture
// binds the Nth argument it receives to $pN by name, as the YDB provider does,
// so an argument the host put at another position binds another parameter.
// The expected rows come from reading the fixture's database file back.

// namedEngine is the engine the named-parameters fixture serves.
const namedEngine = "fixture-named"

// namedStatements starts the named-parameters fixture and opens the operator
// statement handle of a fresh database holding an empty table "regions", and
// returns it with the database file's path.
func namedStatements(c *qt.C) (dbschema.OperatorStatements, string) {
	c.Helper()
	host, err := providerhost.Start(c.Context(), fixtureSpec(c, "named-parameters"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(host.Close)
	path := filepath.Join(c.TempDir(), "named.db")
	address := (&url.URL{Scheme: namedEngine, Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/")}).String()
	operator, release, err := connectTransactionAddress(c, host, address).OperatorStatements()
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(release(), qt.IsNil) })
	_, err = operator.ExecContext(c.Context(), `CREATE TABLE "regions" ("code" TEXT PRIMARY KEY, "name" TEXT, "rank" INTEGER)`)
	c.Assert(err, qt.IsNil)
	return operator, path
}

// storedRegions reads the rows of "regions" back as code, name and rank, in
// code order.
func storedRegions(c *qt.C, path string) [][3]string {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `SELECT "code", "name", CAST("rank" AS TEXT) FROM "regions" ORDER BY "code"`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var stored [][3]string
	for rows.Next() {
		var code, name string
		var rank sql.NullString
		c.Assert(rows.Scan(&code, &name, &rank), qt.IsNil)
		stored = append(stored, [3]string{code, name, rank.String})
	}
	c.Assert(rows.Err(), qt.IsNil)
	return stored
}

// queryRegions runs a statement through the provider and reads the code and
// name of every row it returns.
func queryRegions(c *qt.C, operator dbschema.OperatorStatements, text string, args ...any) [][2]string {
	c.Helper()
	rows, err := operator.QueryContext(c.Context(), text, args...)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var read [][2]string
	for rows.Next() {
		var code, name string
		c.Assert(rows.Scan(&code, &name), qt.IsNil)
		read = append(read, [2]string{code, name})
	}
	c.Assert(rows.Err(), qt.IsNil)
	return read
}

// The builder's INSERT and SELECT carry named arguments, which reach the
// provider at the positions their names give: the rows the INSERT wrote are
// in the file, and the SELECT returns the one row its WHERE and LIMIT select.
// The two SELECT arguments differ, so swapped they would select no row.
func TestProviderNamedArguments_BuilderQuery(t *testing.T) {
	c := qt.New(t)
	operator, path := namedStatements(c)
	insert := query.InsertInto("regions").Columns("code", "name", "rank").Values("NO", "Norway", int64(2)).Values("CZ", "Czechia", int64(1))
	insertText, insertArgs, err := query.RenderInsert(insert.Build(), namedEngine)
	c.Assert(err, qt.IsNil)
	selection := query.Select("code", "name").From("regions").Where(query.Gt("rank", int64(0))).OrderBy(query.Asc("code")).Limit(1)
	selectText, selectArgs, err := query.RenderSelect(selection.Build(), namedEngine)
	c.Assert(err, qt.IsNil)

	_, err = operator.ExecContext(c.Context(), insertText, insertArgs...)

	c.Assert(err, qt.IsNil)
	c.Assert(insertText, qt.Equals, `INSERT INTO "regions" ("code", "name", "rank") VALUES ($p1, $p2, $p3), ($p4, $p5, $p6)`)
	c.Assert(insertArgs, qt.CmpEquals(cmpopts.EquateComparable(sql.NamedArg{})), []any{sql.Named("p1", "NO"), sql.Named("p2", "Norway"), sql.Named("p3", int64(2)),
		sql.Named("p4", "CZ"), sql.Named("p5", "Czechia"), sql.Named("p6", int64(1))})
	c.Assert(storedRegions(c, path), qt.DeepEquals, [][3]string{{"CZ", "Czechia", "1"}, {"NO", "Norway", "2"}})
	c.Assert(selectText, qt.Equals, `SELECT "code", "name" FROM "regions" WHERE "rank" > $p1 ORDER BY "code" ASC LIMIT $p2`)
	c.Assert(queryRegions(c, operator, selectText, selectArgs...), qt.DeepEquals, [][2]string{{"CZ", "Czechia"}})
}

// A named argument binds the parameter its name says wherever the call puts
// it, so arguments given in another order than their names still write each
// value to its own column.
func TestProviderNamedArguments_OutOfOrder(t *testing.T) {
	c := qt.New(t)
	operator, path := namedStatements(c)

	_, err := operator.ExecContext(c.Context(), `INSERT INTO "regions" ("code", "name", "rank") VALUES ($p1, $p2, $p3)`,
		sql.Named("p3", int64(7)), sql.Named("p1", "NO"), sql.Named("p2", "Norway"))

	c.Assert(err, qt.IsNil)
	c.Assert(storedRegions(c, path), qt.DeepEquals, [][3]string{{"NO", "Norway", "7"}})
}

// Named arguments that do not give every position exactly once are refused
// before the provider receives the statement, so the table stays empty, and
// the same statement with exact names runs afterwards on the same handle.
func TestProviderNamedArguments_FailurePath(t *testing.T) {
	const insert = `INSERT INTO "regions" ("code", "name") VALUES ($p1, $p2)`
	tests := []struct {
		name    string
		args    []any
		wantErr string
	}{
		{name: "a gap", args: []any{sql.Named("p1", "NO"), sql.Named("p3", "Norway")},
			wantErr: `the named argument "p3" without one named "p2", since it carries arguments by position`},
		{name: "one name twice", args: []any{sql.Named("p1", "NO"), sql.Named("p1", "Norway")}, wantErr: `the named argument "p1" twice`},
		{name: "a name the style gives no position", args: []any{sql.Named("p1", "NO"), sql.Named("name", "Norway")},
			wantErr: `the named argument "name", since the engine's placeholder style "dollar-named" gives a position only to the names p1, p2 and so on`},
		{name: "a leading zero", args: []any{sql.Named("p1", "NO"), sql.Named("p02", "Norway")},
			wantErr: `the named argument "p02", since the engine's placeholder style "dollar-named" gives a position only to the names p1, p2 and so on`},
		{name: "named and positional arguments", args: []any{sql.Named("p1", "NO"), "Norway"},
			wantErr: `named and positional arguments in one statement`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			operator, path := namedStatements(c)

			_, refused := operator.ExecContext(c.Context(), insert, test.args...)
			stored := storedRegions(c, path)
			_, err := operator.ExecContext(c.Context(), insert, sql.Named("p2", "Norway"), sql.Named("p1", "NO"))

			c.Assert(refused, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(refused, qt.ErrorMatches, `unsupported feature: the experimental provider contract does not support `+test.wantErr)
			c.Assert(stored, qt.HasLen, 0)
			c.Assert(err, qt.IsNil)
			c.Assert(storedRegions(c, path), qt.DeepEquals, [][3]string{{"NO", "Norway", ""}})
		})
	}
}

// The control: an engine whose provider declares no placeholder style that
// names a parameter binds by position alone, so a named argument is refused
// there before anything is sent, and the table stays empty.
func TestProviderNamedArguments_PositionalEngine(t *testing.T) {
	c := qt.New(t)
	operator, path := argumentStatements(c)

	_, err := operator.ExecContext(c.Context(), `INSERT INTO "arguments" ("value") VALUES (?)`, sql.Named("p1", int64(5)))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `unsupported feature: the experimental provider contract does not support the named argument "p1", `+
		`since the engine declares no placeholder style that names a parameter`)
	c.Assert(storedArguments(c, path), qt.HasLen, 0)
}
