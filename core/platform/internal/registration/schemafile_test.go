package registration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// TestRegister_ReadsTheDoBlocksOfAnEngineNobodyShips registers an engine
// whose grammar writes PostgreSQL's routines and whose descriptor says its DO
// blocks are PL/pgSQL. Before the registration the SQL reader reads the
// engine's DO block as an unmodeled statement and declares no role, and
// refuses CockroachDB's, whose descriptor names no language, naming
// PostgreSQL as the one dialect that reads them. After it the block declares
// its role, and the refusal names the engine too, though nothing names it.
func TestRegister_ReadsTheDoBlocksOfAnEngineNobodyShips(t *testing.T) {
	const block = `DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reader') THEN CREATE ROLE Reader LOGIN; END IF;
 END $$;`
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte(block), "bootdb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Roles, qt.HasLen, 0)
	database, _, err = sqlschema.Read([]byte(block), platform.CockroachDB)
	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(err, qt.ErrorMatches, `.*: procedural role declarations require one of the dialects \["postgres"\]`)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})

	engine := described("bootstrapping", "", "bootdb")
	engine.Grammar.Routines.Body = descriptor.RoutineQuotedBody
	engine.Names = descriptor.Names{Unquoted: "ascii_case_insensitive"}
	engine.Statements = descriptor.Statements{DoBlocks: descriptor.DoBlocksPLpgSQL}
	c.Assert(platform.Register(engine), qt.IsNil)

	database, _, err = sqlschema.Read([]byte(block), "bootdb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Roles, qt.DeepEquals, []schemamodel.Role{{Name: "reader", Login: true, Inherit: true}})
	database, _, err = sqlschema.Read([]byte(block), platform.CockroachDB)
	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(err, qt.ErrorMatches, `.*: procedural role declarations require one of the dialects \["postgres" "bootstrapping"\]`)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})
}

// TestRegister_KeepsTheTableOptionOfAnEngineNobodyShipsInItsGroup registers
// an engine that states, as a table option of its own, an option the shared
// parser reports for every dialect: a storage parameter of a WITH clause.
// Before the registration a table read as PostgreSQL keeps the option among
// PostgreSQL's properties, as it keeps any option no dialect owns. After it
// the option is the engine's: a property of the engine's group, and absent
// from PostgreSQL's.
func TestRegister_KeepsTheTableOptionOfAnEngineNobodyShipsInItsGroup(t *testing.T) {
	const table = "CREATE TABLE events (id int) WITH (autovacuum_enabled = true);"
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte(table), platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{
		platform.Postgres: {"autovacuum_enabled": "true"},
	})

	engine := described("vacuuming", "", "vacuumdb")
	engine.TableOptions = []descriptor.TableOption{{Option: "autovacuum_enabled", Property: "autovacuum"}}
	c.Assert(platform.Register(engine), qt.IsNil)

	database, _, err = sqlschema.Read([]byte(table), platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{
		"vacuuming": {"autovacuum": "true"},
	})
}
