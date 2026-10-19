package registration_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

// postgresScript parses a document as PostgreSQL's and leaves every
// statement to the shared conversion, so a load converted with it and no
// target reads a DO block as the first dialect that reads one.
type postgresScript struct{}

func (postgresScript) Parse(_ context.Context, file sqlext.SourceFile) (*ast.StatementList, error) {
	return parser.NewParser(file.Text, parser.WithDialect(platform.Postgres)).Parse()
}

func (postgresScript) Statement(sqlschema.Scope, ast.Node) (bool, error) { return false, nil }

func (postgresScript) AlterOperation(sqlschema.Scope, sqlschema.Table, *ast.ExtensionAlterOperation) (bool, error) {
	return false, nil
}

// TestRegister_ReadsTheDoBlocksOfAnEngineNobodyShips registers an engine
// whose grammar writes PostgreSQL's routines, whose descriptor says its DO
// blocks are PL/pgSQL, and which records a name as written. Before the
// registration the SQL reader reads the engine's DO block as an unmodeled
// statement and declares no role, and refuses CockroachDB's, whose
// descriptor names no language, naming PostgreSQL as the one dialect that
// reads them. After it the block declares its role under the engine's names,
// and the refusal names the engine too, though nothing names it. A load with
// no target reads a block as PostgreSQL, which registers first.
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
	c.Assert(err, qt.ErrorMatches, `.*: procedural role declarations require a dialect that reads DO blocks: postgres`)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})

	untargeted := sqlext.GrammarRequest{Files: []sqlext.SourceFile{{Path: "roles.sql", Text: block}}}
	result, err := sqlschema.ConvertScript(context.Background(), untargeted, postgresScript{})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Database.Roles, qt.DeepEquals, []schemamodel.Role{{Name: "reader", Login: true, Inherit: true}})

	engine := described("bootstrapping", "", "bootdb")
	engine.Grammar.Routines.Body = descriptor.RoutineQuotedBody
	engine.Statements = descriptor.Statements{DoBlocks: descriptor.DoBlocksPLpgSQL}
	c.Assert(platform.Register(engine), qt.IsNil)

	database, _, err = sqlschema.Read([]byte(block), "bootdb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Roles, qt.DeepEquals, []schemamodel.Role{{Name: "Reader", Login: true, Inherit: true}})
	result, err = sqlschema.ConvertScript(context.Background(), untargeted, postgresScript{})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Database.Roles, qt.DeepEquals, []schemamodel.Role{{Name: "reader", Login: true, Inherit: true}})
	database, _, err = sqlschema.Read([]byte(block), platform.CockroachDB)
	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(err, qt.ErrorMatches, `.*: procedural role declarations require a dialect that reads DO blocks: postgres, bootstrapping`)
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
