//go:build integration

package integration_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// modeProgram is an object whose || operator concatenates under ANSI and
// computes logical OR under the ordinary session mode. The table also gives
// a trigger a target and survives the migration that drops the program.
type modeProgram struct {
	kind   string
	create string
	read   string
	drop   string
}

var modePrograms = []modeProgram{
	{
		kind:   "function",
		create: "CREATE FUNCTION program() RETURNS VARCHAR(30) DETERMINISTIC RETURN 'a' || 'b';\n",
		read:   "SELECT program()",
		drop:   "DROP FUNCTION program;\n",
	},
	{
		kind:   "procedure",
		create: "CREATE PROCEDURE program() SELECT 'a' || 'b';\n",
		read:   "CALL program()",
		drop:   "DROP PROCEDURE program;\n",
	},
	{
		kind:   "trigger",
		create: "CREATE TRIGGER program BEFORE INSERT ON mode_result FOR EACH ROW SET NEW.result = 'a' || 'b';\n",
		read:   "SELECT result FROM mode_result LIMIT 1",
		drop:   "DROP TRIGGER program;\n",
	},
}

const modeResultTable = "CREATE TABLE mode_result (result VARCHAR(30));\n"

// modeProgramDDL keeps the mode around CREATE in one session, the way a dump
// restores a stored program. Later connections use the ordinary session mode.
func modeProgramDDL(program modeProgram) string {
	return modeResultTable +
		"SET @ptah_saved_mode = @@SESSION.sql_mode;\nSET SESSION sql_mode = 'ANSI';\n" +
		program.create + "SET SESSION sql_mode = @ptah_saved_mode;\n"
}

func TestStoredProgramModesRefuseSchemaPlansE2E_FailurePath(t *testing.T) {
	for _, engine := range storedValueEngines {
		for _, program := range modePrograms {
			t.Run(engine.name+"/"+program.kind, func(t *testing.T) {
				c := qt.New(t)
				_, target := newMySQLFamilyScratch(c, engine.engine).builtAs(c, "program_mode", modeProgramDDL(program))
				execStoredValues(c, target, "INSERT INTO mode_result VALUES ('seed')")
				c.Assert(readStoredScalar(c, target, program.read), qt.Equals, "ab")
				schema := writeStoredValuesSchema(c, modeResultTable+"CREATE TABLE marker (id INT PRIMARY KEY);\n")
				output := filepath.Join(c.TempDir(), "change.plan.json")

				plan := runPtahIn(c, "schema", "plan", "--db-url", target, "--schema-file", schema, "--output", output)

				c.Assert(plan.ExitCode, qt.Not(qt.Equals), 0, historyComment(plan))
				c.Assert(plan.Stderr, qt.Contains, "cannot read "+program.kind)
				c.Assert(plan.Stderr, qt.Contains, "stored sql_mode includes")
				c.Assert(readStoredScalar(c, target, program.read), qt.Equals, "ab")
				_, err := os.Stat(output)
				c.Assert(err, qt.ErrorIs, fs.ErrNotExist)

				directory := c.TempDir()
				generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema,
					"--migrations-dir", directory, "--name", "change")

				c.Assert(generated.ExitCode, qt.Not(qt.Equals), 0, historyComment(generated))
				c.Assert(generated.Stderr, qt.Contains, "stored sql_mode includes")
				entries, err := os.ReadDir(directory)
				c.Assert(err, qt.IsNil)
				c.Assert(entries, qt.HasLen, 0)
				c.Assert(readStoredScalar(c, target, program.read), qt.Equals, "ab")
			})
		}
	}
}

func TestStoredProgramModesRefuseDerivedRollbackE2E_FailurePath(t *testing.T) {
	for _, engine := range storedValueEngines {
		for _, program := range modePrograms {
			t.Run(engine.name+"/"+program.kind, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.engine)
				_, target := scratch.database(c, "program_mode_target")
				_, shadow := scratch.database(c, "program_mode_shadow")
				directory := c.TempDir()
				for name, body := range map[string]string{
					"0000000000001_init.up.sql":   "-- +ptah no_transaction\n" + modeProgramDDL(program),
					"0000000000001_init.down.sql": program.drop + "DROP TABLE mode_result;\n",
					"0000000000002_drop.up.sql":   program.drop,
					"0000000000002_drop.down.sql": "-- +ptah empty_rollback\n",
				} {
					c.Assert(os.WriteFile(filepath.Join(directory, name), []byte(body), 0o600), qt.IsNil)
				}
				first := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", directory, "--to-version", "1")
				c.Assert(first.ExitCode, qt.Equals, 0, historyComment(first))
				execStoredValues(c, target, "INSERT INTO mode_result VALUES ('seed')")
				c.Assert(readStoredScalar(c, target, program.read), qt.Equals, "ab")
				dropped := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", directory, "--allow-destructive")
				c.Assert(dropped.ExitCode, qt.Equals, 0, historyComment(dropped))

				// The history creates a stored program, which the replay guard
				// refuses on a shadow database the run does not own unless the
				// SQL is trusted. The test is about the plan's reading of it.
				down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", directory,
					"--target", "1", "--plan", "--shadow-db", shadow, "--trust-replay-sql", "--confirm")

				c.Assert(down.ExitCode, qt.Not(qt.Equals), 0, historyComment(down))
				c.Assert(down.Stderr, qt.Contains, "cannot read "+program.kind)
				c.Assert(down.Stderr, qt.Contains, "stored sql_mode includes")
				c.Assert(readStoredScalar(c, target, "SELECT "+
					"(SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'program') + "+
					"(SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = DATABASE() AND TRIGGER_NAME = 'program')"), qt.Equals, "0")
				c.Assert(readStoredScalar(c, target, "SELECT MAX(version) FROM schema_migrations"), qt.Equals, "2")
			})
		}
	}
}

func TestStoredProgramModesAllowOrdinaryProgramsE2E_HappyPath(t *testing.T) {
	for _, engine := range storedValueEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			definition := modeResultTable + "CREATE FUNCTION program() RETURNS VARCHAR(30) DETERMINISTIC RETURN CONCAT('a', 'b');\n"
			_, target := newMySQLFamilyScratch(c, engine.engine).builtAs(c, "program_mode_control", definition)
			output := filepath.Join(c.TempDir(), "change.plan.json")

			plan := runPtahIn(c, "schema", "plan", "--db-url", target,
				"--schema-file", writeStoredValuesSchema(c, definition), "--output", output)

			c.Assert(plan.ExitCode, qt.Equals, 0, historyComment(plan))
			c.Assert(readStoredScalar(c, target, "SELECT program()"), qt.Equals, "ab")
		})
	}
}

func TestStoredProgramModesRefuseAChangedSessionE2E_FailurePath(t *testing.T) {
	for _, engine := range storedValueEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			definition := "CREATE FUNCTION program() RETURNS VARCHAR(30) DETERMINISTIC RETURN '1' || '0';\n"
			_, target := newMySQLFamilyScratch(c, engine.engine).builtAs(c, "program_session_mode", definition)
			c.Assert(readStoredScalar(c, target, "SELECT program()"), qt.Equals, "1")
			changed := withSQLMode(c, target, "'PIPES_AS_CONCAT'")
			output := filepath.Join(c.TempDir(), "change.plan.json")

			plan := runPtahIn(c, "schema", "plan", "--db-url", changed,
				"--schema-file", writeStoredValuesSchema(c, definition), "--output", output)

			c.Assert(plan.ExitCode, qt.Not(qt.Equals), 0, historyComment(plan))
			c.Assert(plan.Stderr, qt.Contains, "session sql_mode includes PIPES_AS_CONCAT")
			c.Assert(readStoredScalar(c, target, "SELECT program()"), qt.Equals, "1")
		})
	}
}
