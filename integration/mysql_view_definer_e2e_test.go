//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A MySQL or MariaDB CREATE OR REPLACE VIEW that names no DEFINER makes the
// account that runs it the view's definer, and a view under SQL SECURITY
// DEFINER runs with its definer's privileges. So a plan that replaces such a
// view another account defined is refused before the server changes, naming
// the view, its definer and the connected account (stokaro/ptah#6100). A view
// under SQL SECURITY INVOKER after the change runs with its caller's
// privileges, and is replaced.

// viewDefinerTarget is a database holding the table docs and the view my_docs,
// whose definer is either the administrative account or an account of its own.
type viewDefinerTarget struct {
	sqlSecurityTarget
	// owner is the account the view was created for, as information_schema
	// spells it, and empty when the administrative account created it as
	// itself.
	owner string
	// account is the administrative account, as CURRENT_USER() spells it:
	// the one ptah connects as.
	account string
}

// foreignOwner names the account a test creates a view for, so the view's
// definer is not the account ptah connects as. A unique suffix keeps runs
// apart, since an account belongs to the whole server.
const foreignOwner = "ptah_def"

// newViewDefinerTarget creates my_docs with the SQL SECURITY mode given. With
// an owner, it creates an account of that name with a unique suffix and makes
// it the view's definer; without one, the administrative account defines the
// view as itself. The account exists, and may read the table, so the view
// stays valid; it is dropped after the database, since MySQL refuses to drop
// the definer of a view that still exists.
func newViewDefinerTarget(c *qt.C, engine dbtarget.Engine, mode, owner string) viewDefinerTarget {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine)
	var target viewDefinerTarget
	c.Assert(scratch.admin.QueryRowContext(c.Context(), "SELECT CURRENT_USER()").Scan(&target.account), qt.IsNil)
	var user, definer string
	if owner != "" {
		user = fmt.Sprintf("%s_%d", owner, time.Now().UnixNano())
		target.owner = user + "@%"
		definer = "DEFINER = '" + user + "'@'%' "
		_, err := scratch.admin.ExecContext(c.Context(), "CREATE USER '"+user+"'@'%'")
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() {
			_, err := scratch.admin.ExecContext(context.Background(), "DROP USER IF EXISTS '"+user+"'@'%'")
			c.Check(err, qt.IsNil)
		})
	}
	name, _ := scratch.database(c, "view_definer")
	statements := []string{
		"CREATE TABLE `" + name + "`.docs (id int PRIMARY KEY, owner_name varchar(64) NOT NULL)",
		"CREATE " + definer + "SQL SECURITY " + mode + " VIEW `" + name + "`.my_docs AS SELECT id, owner_name FROM `" + name + "`.docs",
	}
	if owner != "" {
		statements = append(statements, "GRANT SELECT ON `"+name+"`.* TO '"+user+"'@'%'")
	}
	for _, statement := range statements {
		_, err := scratch.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	target.sqlSecurityTarget = sqlSecurityTarget{
		db:       scratch.admin,
		database: name,
		url:      replaceMySQLDatabaseName(c, scratch.adminURL, name),
	}
	return target
}

// definer reads back the definer information_schema.VIEWS reports for my_docs.
func (t viewDefinerTarget) definer(c *qt.C) string {
	c.Helper()
	var definer string
	c.Assert(t.db.QueryRowContext(c.Context(),
		"SELECT DEFINER FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'my_docs'",
		t.database).Scan(&definer), qt.IsNil)
	return definer
}

// viewDefinerRefusal is the part of a refused apply document that names the
// refusal.
type viewDefinerRefusal struct {
	Outcome string `json:"outcome"`
	Refusal struct {
		Code         string `json:"code"`
		ViewDefiners []struct {
			View    string `json:"view"`
			Definer string `json:"definer"`
			Account string `json:"account"`
		} `json:"view_definers"`
	} `json:"refusal"`
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}

// TestMySQLViewDefinerRefusedE2E applies a changed body to my_docs, which runs
// under SQL SECURITY DEFINER as an account of its own, declared with no mode
// and with DEFINER. Either plan would make the connected account the definer
// and fail no statement, so the proof the refusal came first is the server:
// the definer and the body are what they were, under --auto-approve.
func TestMySQLViewDefinerRefusedE2E(t *testing.T) {
	tests := []struct {
		name   string
		clause string
	}{
		{name: "no mode", clause: ""},
		{name: "a declared definer", clause: "SQL SECURITY DEFINER "},
	}
	for _, engine := range sqlSecurityEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				target := newViewDefinerTarget(c, engine.engine, "DEFINER", foreignOwner)
				definition := target.definition(c)

				got := runPtahIn(c, "schema", "apply", "--db-url", target.url,
					"--schema-file", target.schemaFile(c, test.clause, sqlSecurityPositive), "--auto-approve", "--json")

				assertRunExited(c, got, 2)
				var document viewDefinerRefusal
				c.Assert(json.Unmarshal([]byte(got.Stdout), &document), qt.IsNil, qt.Commentf("%s", got.Stdout))
				c.Assert(document.Outcome, qt.Equals, "refused")
				c.Assert(document.Refusal.Code, qt.Equals, "view-definer")
				c.Assert(document.Refusal.ViewDefiners, qt.HasLen, 1)
				c.Assert(document.Refusal.ViewDefiners[0].View, qt.Equals, "my_docs")
				c.Assert(document.Refusal.ViewDefiners[0].Definer, qt.Equals, target.owner)
				c.Assert(document.Refusal.ViewDefiners[0].Account, qt.Equals, target.account)
				for _, part := range []string{
					"view my_docs runs with the privileges of its definer " + target.owner + " under SQL SECURITY DEFINER",
					"replacing it as " + target.account + " would make " + target.account + " its definer",
					"Connect as " + target.owner + " to replace it",
				} {
					c.Assert(document.Error.Message, qt.Contains, part)
				}
				c.Assert(target.definer(c), qt.Equals, target.owner)
				c.Assert(target.held(c), qt.Equals, "DEFINER")
				c.Assert(target.definition(c), qt.Equals, definition)
			})
		}
	}
}

// TestMySQLViewDefinerReplacedE2E applies a changed body where the definer
// decides nothing or does not change: a view the connected account defined,
// and a view of an account of its own that runs under SQL SECURITY INVOKER
// after the change. The replacement makes the connected account the definer,
// read back from the server.
func TestMySQLViewDefinerReplacedE2E(t *testing.T) {
	tests := []struct {
		name   string
		held   string
		owner  string
		clause string
		want   string
	}{
		{name: "the connected account's view", held: "DEFINER", owner: "", clause: "", want: "DEFINER"},
		{name: "an invoker view of another account", held: "INVOKER", owner: foreignOwner, clause: "SQL SECURITY INVOKER ", want: "INVOKER"},
		{name: "a definer view of another account declared invoker", held: "DEFINER", owner: foreignOwner, clause: "SQL SECURITY INVOKER ", want: "INVOKER"},
	}
	for _, engine := range sqlSecurityEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				target := newViewDefinerTarget(c, engine.engine, test.held, test.owner)
				definition := target.definition(c)

				got := runPtahIn(c, "schema", "apply", "--db-url", target.url,
					"--schema-file", target.schemaFile(c, test.clause, sqlSecurityPositive), "--auto-approve", "--json")

				assertRunExited(c, got, 0)
				var document sqlSecurityDocument
				c.Assert(json.Unmarshal([]byte(got.Stdout), &document), qt.IsNil, qt.Commentf("%s", got.Stdout))
				c.Assert(document.Outcome, qt.Equals, "applied")
				c.Assert(target.definer(c), qt.Equals, target.account)
				c.Assert(target.held(c), qt.Equals, test.want)
				c.Assert(target.definition(c), qt.Not(qt.Equals), definition)
			})
		}
	}
}

// TestMySQLViewDefinerDriftE2E compares the refused declaration with `schema
// drift`, which plans nothing: it reports the body under views_modified and
// does not refuse, whoever defined the view.
func TestMySQLViewDefinerDriftE2E(t *testing.T) {
	for _, engine := range sqlSecurityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newViewDefinerTarget(c, engine.engine, "DEFINER", foreignOwner)

			got := runPtahIn(c, "schema", "drift", "--db-url", target.url, "--schema-file", target.schemaFile(c, "", sqlSecurityPositive))

			assertRunExited(c, got, 1)
			c.Assert(got.Stdout, qt.Contains, "views_modified")
			c.Assert(got.Stdout+got.Stderr, qt.Not(qt.Contains), "its definer")
			c.Assert(target.definer(c), qt.Equals, target.owner)
		})
	}
}
