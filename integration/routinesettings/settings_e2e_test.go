//go:build integration

package routinesettings_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// pinned is what every source below declares, read back from pg_proc.proconfig.
// Each entry needs what a declaration writes and the catalog does not keep: a
// literal for the unit, a quoted name for the time zone and a list element in
// double quotes for `$user`.
const pinned = `pinned: TimeZone=Europe/Prague | search_path="$user", public | work_mem=8MB`

const pinnedHCL = `schema "public" {}

function "pinned" {
  schema = schema.public
  lang   = SQL
  return = integer
  as     = "SELECT 1"
  set = {
    work_mem    = "8MB"
    TimeZone    = "Europe/Prague"
    search_path = "\"$user\", public"
  }
}
`

const pinnedSQL = `CREATE FUNCTION pinned() RETURNS integer LANGUAGE sql
  SET work_mem = '8MB' SET "TimeZone" = 'Europe/Prague' SET search_path = '$user', public
  AS $$ SELECT 1 $$;
`

const pinnedYAML = `functions:
  pinned:
    returns: integer
    language: sql
    settings:
      - work_mem='8MB'
      - '"TimeZone"=''Europe/Prague'''
      - search_path='$user', public
    body: SELECT 1
`

const pinnedGo = `package entities

//ptah:schema:function name="pinned" returns="integer" language="sql" settings="work_mem='8MB';\"TimeZone\"='Europe/Prague';search_path='$user', public" body="SELECT 1"
type Pinned struct{}
`

// TestRoutineSettingsSettleFromEverySourceE2E applies a routine with a unit, a
// quoted time zone and a search_path list from each source that declares one,
// reads what the server stored, and plans again.
//
// Only the server can answer either half. `SET work_mem = 8MB` reads like SQL and
// is refused with "trailing junk after numeric literal", and `SET TimeZone =
// Europe/Prague` with a syntax error at the slash, so a `set` value rendered as
// it stands applies nothing. `"TimeZone"` is stored as TimeZone, so a comparison
// that keeps the quotes plans CREATE OR REPLACE on every run.
//
// YugabyteDB stores settings as PostgreSQL does and takes the same path.
// CockroachDB answers "unimplemented" to a SET clause in CREATE FUNCTION, so it
// has no settings to read back.
func TestRoutineSettingsSettleFromEverySourceE2E(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
	}
	sources := []struct {
		name    string
		flag    string
		file    string
		content string
		path    string
	}{
		{name: "an HCL schema", flag: "--schema-file", file: "schema.hcl", content: pinnedHCL, path: "schema.hcl"},
		{name: "a SQL schema", flag: "--schema-file", file: "schema.sql", content: pinnedSQL, path: "schema.sql"},
		{name: "a YAML schema", flag: "--schema-file", file: "schema.yaml", content: pinnedYAML, path: "schema.yaml"},
		{name: "Go annotations", flag: "--root-dir", file: "entities.go", content: pinnedGo, path: ""},
	}
	for _, engine := range engines {
		for _, source := range sources {
			t.Run(engine.name+"/"+source.name, func(t *testing.T) {
				c := qt.New(t)
				target := scratchDatabase(c, engine.engine)
				dir := c.TempDir()
				writeFile(c, dir, source.file, source.content)
				from := []string{source.flag, filepath.Join(dir, source.path)}

				applySchema(c, target, from...)

				c.Assert(catalogSettings(c, target), qt.Equals, pinned)
				c.Assert(planOutcome(c, target, from...), qt.Equals, noChanges)
			})
		}
	}
}

// TestRoutineSettingsHCLListIsReadAsTheServerKeepsItE2E applies each list a `set`
// can hold and reads the catalog. The server reads a bare element of a list in
// lower case and keeps a double-quoted one as it is, so a document that says
// MySchema bare pins myschema, and only the quotes pin MySchema.
func TestRoutineSettingsHCLListIsReadAsTheServerKeepsItE2E(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		stored string
	}{
		{name: "two names", value: `a, b`, stored: `pinned: search_path=a, b`},
		{name: "one name with a comma", value: `\"a, b\"`, stored: `pinned: search_path="a, b"`},
		{name: "a bare name in upper case", value: `MySchema, public`, stored: `pinned: search_path=myschema, public`},
		{name: "a quoted name in upper case", value: `\"MySchema\", public`, stored: `pinned: search_path="MySchema", public`},
		{name: "the empty value", value: ``, stored: `pinned: search_path=""`},
		{name: "the catalog's empty name", value: `\"\"`, stored: `pinned: search_path=""`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := scratchDatabase(c, dbtarget.PostgreSQL)
			schema := writeFile(c, c.TempDir(), "schema.hcl", `schema "public" {}

function "pinned" {
  schema = schema.public
  lang   = SQL
  return = integer
  as     = "SELECT 1"
  set = {
    search_path = "`+test.value+`"
  }
}
`)
			from := []string{"--schema-file", schema}

			applySchema(c, target, from...)

			c.Assert(catalogSettings(c, target), qt.Equals, test.stored)
			c.Assert(planOutcome(c, target, from...), qt.Equals, noChanges)
		})
	}
}

// TestRoutineSettingsPlanComparesWhatTheServerStoresE2E plans from a routine
// created with nothing of Ptah's in between to a SQL declaration of it. Where the
// server would store something else the plan holds a statement and applying it
// stores what the declaration says; where it would store the same text the plan
// is empty. Either way the next plan is empty.
func TestRoutineSettingsPlanComparesWhatTheServerStoresE2E(t *testing.T) {
	tests := []struct {
		name     string
		live     string
		declared string
		outcome  string
		stored   string
	}{
		{
			name: "one schema with a comma is not two schemas",
			live: "SET search_path = a, b", declared: "SET search_path = 'a, b'",
			outcome: changes, stored: `pinned: search_path="a, b"`,
		},
		{
			name: "two schemas are not one with a comma",
			live: "SET search_path = 'a, b'", declared: "SET search_path = a, b",
			outcome: changes, stored: `pinned: search_path=a, b`,
		},
		{
			name: "two spellings of two schemas",
			live: "SET search_path = a, b", declared: "SET search_path = 'a', 'b'",
			outcome: noChanges, stored: `pinned: search_path=a, b`,
		},
		{
			name: "a bare name and the name it folds to",
			live: `SET search_path = "myschema"`, declared: "SET search_path = MySchema",
			outcome: noChanges, stored: `pinned: search_path=myschema`,
		},
		{
			name: "a bare name and a name that keeps its case",
			live: "SET search_path = MySchema", declared: `SET search_path = "MySchema"`,
			outcome: changes, stored: `pinned: search_path="MySchema"`,
		},
		{
			name: "a quoted name",
			live: `SET "TimeZone" = 'UTC'`, declared: `SET "TimeZone" = 'UTC'`,
			outcome: noChanges, stored: `pinned: TimeZone=UTC`,
		},
		{
			name: "a name in another case",
			live: "SET timezone = 'UTC'", declared: `SET "TimeZone" = 'UTC'`,
			outcome: noChanges, stored: `pinned: TimeZone=UTC`,
		},
		{
			name: "another time zone",
			live: `SET "TimeZone" = 'UTC'`, declared: `SET "TimeZone" = 'Europe/Prague'`,
			outcome: changes, stored: `pinned: TimeZone=Europe/Prague`,
		},
		{
			name: "another order",
			live: "SET work_mem = '8MB' SET search_path = public", declared: "SET search_path = public SET work_mem = '8MB'",
			outcome: noChanges, stored: `pinned: search_path=public | work_mem=8MB`,
		},
		{
			name: "a name set twice is stored once",
			live: "SET work_mem = '8MB'", declared: "SET work_mem = '4MB' SET work_mem = '8MB'",
			outcome: noChanges, stored: `pinned: work_mem=8MB`,
		},
		{
			name: "a name set twice is stored with its last value",
			live: "SET work_mem = '8MB'", declared: "SET work_mem = '8MB' SET work_mem = '4MB'",
			outcome: changes, stored: `pinned: work_mem=4MB`,
		},
		{
			name: "white space around a value",
			live: "SET application_name = ' a '", declared: "SET application_name = ' a '",
			outcome: noChanges, stored: `pinned: application_name= a `,
		},
		{
			name: "a setting taken away",
			live: "SET work_mem = '8MB' SET search_path = public", declared: "SET work_mem = '8MB'",
			outcome: changes, stored: `pinned: work_mem=8MB`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := scratchDatabase(c, dbtarget.PostgreSQL,
				"CREATE FUNCTION pinned() RETURNS integer LANGUAGE sql "+test.live+" AS $$ SELECT 1 $$")
			schema := writeFile(c, c.TempDir(), "schema.sql",
				"CREATE FUNCTION pinned() RETURNS integer LANGUAGE sql "+test.declared+" AS $$ SELECT 1 $$;\n")
			from := []string{"--schema-file", schema}

			outcome := planOutcome(c, target, from...)
			applySchema(c, target, from...)

			c.Assert(outcome, qt.Equals, test.outcome)
			c.Assert(catalogSettings(c, target), qt.Equals, test.stored)
			c.Assert(planOutcome(c, target, from...), qt.Equals, noChanges)
		})
	}
}

// seededRoutines create routines holding the catalog spellings an export has to
// keep apart, with nothing of Ptah's in between.
var seededRoutines = []string{
	"CREATE FUNCTION unit() RETURNS integer LANGUAGE sql SET work_mem = '8MB' AS 'SELECT 1'",
	`CREATE FUNCTION zone() RETURNS integer LANGUAGE sql SET "TimeZone" = 'Europe/Prague' AS 'SELECT 1'`,
	"CREATE FUNCTION two_schemas() RETURNS integer LANGUAGE sql SET search_path = a, b AS 'SELECT 1'",
	"CREATE FUNCTION one_schema() RETURNS integer LANGUAGE sql SET search_path = 'a, b' AS 'SELECT 1'",
	`CREATE FUNCTION odd_schemas() RETURNS integer LANGUAGE sql SET search_path = '$user', "MySchema", 'Weird "q"', 'it''s', 'select', '1a' AS 'SELECT 1'`,
	"CREATE FUNCTION no_schema() RETURNS integer LANGUAGE sql SET search_path = '' AS 'SELECT 1'",
	`CREATE FUNCTION style() RETURNS integer LANGUAGE sql SET "DateStyle" = 'ISO, MDY' AS 'SELECT 1'`,
	"CREATE FUNCTION named() RETURNS integer LANGUAGE sql SET application_name = 'it''s, me' AS 'SELECT 1'",
	"CREATE FUNCTION spaced() RETURNS integer LANGUAGE sql SET application_name = ' a ' AS 'SELECT 1'",
	`CREATE FUNCTION dotted() RETURNS integer LANGUAGE sql SET "myapp.mode" = 'on' AS 'SELECT 1'`,
	"CREATE FUNCTION several() RETURNS integer LANGUAGE sql SET work_mem = '8MB' SET search_path = public AS 'SELECT 1'",
}

// exportedSettings are lines an export of seededRoutines holds: each value as
// pg_proc.proconfig keeps it, with no SQL quotes in it.
var exportedSettings = []string{
	`"work_mem" = "8MB"`,
	`"TimeZone" = "Europe/Prague"`,
	`"search_path" = "a, b"`,
	`"search_path" = "\"a, b\""`,
	`"search_path" = "\"$user\", \"MySchema\", \"Weird \"\"q\"\"\", \"it's\", select, \"1a\""`,
	`"search_path" = "\"\""`,
	`"DateStyle" = "ISO, MDY"`,
	`"application_name" = "it's, me"`,
	`"application_name" = " a "`,
	`"myapp.mode" = "on"`,
}

// TestRoutineSettingsSurviveAnExportRoundTripE2E exports routines the server
// holds with `ptah schema inspect`, and reads the document back two ways: planned
// against the same database it finds nothing to do, and applied to an empty
// database it stores what the first one holds.
func TestRoutineSettingsSurviveAnExportRoundTripE2E(t *testing.T) {
	c := qt.New(t)
	source := scratchDatabase(c, dbtarget.PostgreSQL, seededRoutines...)

	export := runPtah(c, "schema", "inspect", "--db-url", source, "--schemas", "public")

	for _, line := range exportedSettings {
		c.Assert(export, qt.Contains, line)
	}
	exported := []string{"--schema-file", writeFile(c, c.TempDir(), "export.hcl", export)}
	c.Assert(planOutcome(c, source, exported...), qt.Equals, noChanges)

	copied := scratchDatabase(c, dbtarget.PostgreSQL)
	applySchema(c, copied, exported...)

	c.Assert(catalogSettings(c, copied), qt.Equals, catalogSettings(c, source))
}

// TestCompatRoutineSettingsApplyAndExportE2E drives the same document through
// `ptah-compat`, which shares the reader and the renderer with the native
// commands. The pinned community binary accepts a function block, whatever its
// `set` holds, and emits nothing for it, so this surface has no spelling of its
// own to match: the capability behaves as it does natively.
func TestCompatRoutineSettingsApplyAndExportE2E(t *testing.T) {
	c := qt.New(t)
	target := scratchDatabase(c, dbtarget.PostgreSQL)
	schema := "file://" + filepath.ToSlash(writeFile(c, c.TempDir(), "schema.hcl", pinnedHCL))

	runCompat(c, "schema", "apply", "--url", target, "--to", schema, "--auto-approve")

	c.Assert(catalogSettings(c, target), qt.Equals, pinned)
	settled := runCompat(c, "schema", "apply", "--url", target, "--to", schema, "--auto-approve")
	c.Assert(settled, qt.Contains, "Schema is synced, no changes to be made")

	export := runCompat(c, "schema", "inspect", "--url", target)

	c.Assert(export, qt.Contains, `"work_mem" = "8MB"`)
	c.Assert(export, qt.Contains, `"TimeZone" = "Europe/Prague"`)
	c.Assert(export, qt.Contains, `"search_path" = "\"$user\", public"`)
	copied := scratchDatabase(c, dbtarget.PostgreSQL)
	exported := "file://" + filepath.ToSlash(writeFile(c, c.TempDir(), "export.hcl", export))
	runCompat(c, "schema", "apply", "--url", copied, "--to", exported, "--auto-approve")
	c.Assert(catalogSettings(c, copied), qt.Equals, pinned)
}
