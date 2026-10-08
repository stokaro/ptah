package planner_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// outputPinDirectory holds one recorded output per pinned dialect.
const outputPinDirectory = "testdata/output-pin"

// updateOutputPin rewrites the recorded outputs instead of comparing against
// them.
var updateOutputPin = flag.Bool("update-output-pin", false,
	"rewrite testdata/output-pin from the current render and plan")

// outputPinSources are the schemas the pin renders and plans. The current
// schema is a table with one index; the desired one changes that table and
// adds a second, so the plan adds an index to a table that exists, drops one,
// and creates a table together with its own index. The covering variant adds
// an index with an INCLUDE payload, which a target without
// capability.IndexCoveringColumns refuses.
var outputPinSources = map[string]string{
	"current": `package entities

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string
	//ptah:schema:field name="name" type="VARCHAR(100)"
	//ptah:schema:index name="users_name_ix" fields="name"
	Name string
}
`,
	"desired": `package entities

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true"
	//ptah:schema:index name="users_email_uq" fields="email" unique="true"
	Email string
	//ptah:schema:field name="name" type="VARCHAR(100)"
	Name string
	//ptah:schema:field name="created_at" type="TIMESTAMP"
	//ptah:schema:index name="users_created_ix" fields="created_at"
	CreatedAt string
}

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="user_id" type="BIGINT" not_null="true"
	//ptah:schema:index name="orders_user_ix" fields="user_id"
	UserID int64
	//ptah:schema:field name="total" type="DECIMAL(10,2)"
	Total string
}
`,
	"covering": `package entities

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string
	//ptah:schema:field name="name" type="VARCHAR(100)"
	//ptah:schema:index name="users_name_ix" fields="name" include="email"
	Name string
}
`,
}

// parsePinSchema parses one of outputPinSources the way `ptah schema render`
// parses an entity directory. It parses afresh on every call, so no render or
// comparison can see what an earlier one did to the schema.
func parsePinSchema(c *qt.C, name string) *schemamodel.Database {
	c.Helper()
	database, err := goschema.ParseFS(fstest.MapFS{
		"entities/schema.go": &fstest.MapFile{Data: []byte(outputPinSources[name])},
	}, "entities")
	c.Assert(err, qt.IsNil, qt.Commentf("parsing the %s schema", name))
	return database
}

// pinnedDialects are the default dialects whose output the pin holds: every one
// except YDB, which is still being built and whose output moves with it.
func pinnedDialects() []string {
	return slices.DeleteFunc(capability.DefaultDialects(), func(dialect string) bool {
		return dialect == platform.YDB
	})
}

// pinnedOutput renders and plans the pin schemas for one dialect. A refusal is
// output too: a target that stopped refusing what it refused is a change.
func pinnedOutput(c *qt.C, dialect string) string {
	c.Helper()
	var out strings.Builder
	section := func(title, body string, err error) {
		fmt.Fprintf(&out, "-- %s\n", title)
		if err != nil {
			fmt.Fprintf(&out, "-- refused: %v\n", err)
			return
		}
		out.WriteString(strings.TrimRight(body, "\n"))
		out.WriteString("\n")
	}

	statements, err := builtin.GetOrderedCreateStatements(parsePinSchema(c, "desired"), dialect)
	section("render: the desired schema", strings.Join(statements, "\n"), err)

	diff := must.Must(schemadiff.CompareSchemas(c.Context(), parsePinSchema(c, "desired"), parsePinSchema(c, "current"), dialect, must.Must(builtin.New())))
	planned, err := planner.GenerateSchemaDiffSQL(
		context.Background(), must.Must(builtin.New()),
		diff, dialect,
	)
	section("plan: the current schema to the desired one", planned, err)

	statements, err = builtin.GetOrderedCreateStatements(parsePinSchema(c, "covering"), dialect)
	section("render: a covering index", strings.Join(statements, "\n"), err)

	diff = must.Must(schemadiff.CompareSchemas(c.Context(), parsePinSchema(c, "covering"), parsePinSchema(c, "current"), dialect, must.Must(builtin.New())))
	planned, err = planner.GenerateSchemaDiffSQL(
		context.Background(), must.Must(builtin.New()),
		diff, dialect,
	)
	section("plan: the current schema to a covering index", planned, err)

	return out.String()
}

// TestOutputPin_EveryDefaultDialectRendersAndPlansAsRecorded holds the DDL
// every default dialect writes, byte for byte.
//
// Capability keys decide some of that DDL for every target at once: which
// targets take an index's INCLUDE payload, and which write a new table's
// indexes inside its CREATE TABLE. A change to such a decision made for one
// target must leave the others' DDL as it is, and only a recorded output can
// show that it did: an assertion that a statement contains a fragment survives
// a reordering, a new clause, or a second statement. The recorded output
// changes only on purpose, through the update flag, and the diff of the
// testdata is then the review.
func TestOutputPin_EveryDefaultDialectRendersAndPlansAsRecorded(t *testing.T) {
	for _, dialect := range pinnedDialects() {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(outputPinDirectory, dialect+".sql")
			current := pinnedOutput(c, dialect)
			refreshOutputPin(c, path, current)

			recorded, err := os.ReadFile(path)
			c.Assert(err, qt.IsNil, qt.Commentf(
				"regenerate with: go test ./migration/planner/ -run TestOutputPin -update-output-pin"))
			c.Assert(current, qt.Equals, string(recorded), qt.Commentf(
				"the %s output changed. If the change is meant, regenerate %s with "+
					"-update-output-pin and review the diff of the file; if it is not, the change "+
					"that moved it is the defect.", dialect, path))
		})
	}
}

// refreshOutputPin writes the current output over the recorded one when the
// update flag asks for it.
func refreshOutputPin(c *qt.C, path, current string) {
	c.Helper()
	if !*updateOutputPin {
		return
	}
	c.Assert(os.MkdirAll(outputPinDirectory, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(path, []byte(current), 0o600), qt.IsNil)
}

// TestOutputPin_TheRecordedOutputDescribesTheSchemas is the control.
//
// A recording compared with a recording passes whether it holds DDL or not.
// This asserts that every pinned dialect's recording exists, that each one
// creates both tables and adds the index the plan adds to the table that
// exists, and that no file is left for a dialect the pin no longer covers.
func TestOutputPin_TheRecordedOutputDescribesTheSchemas(t *testing.T) {
	c := qt.New(t)
	entries, err := os.ReadDir(outputPinDirectory)
	c.Assert(err, qt.IsNil)
	var recorded []string
	for _, entry := range entries {
		recorded = append(recorded, strings.TrimSuffix(entry.Name(), ".sql"))
	}
	c.Assert(recorded, qt.DeepEquals, pinnedDialects())

	for _, dialect := range pinnedDialects() {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			content, err := os.ReadFile(filepath.Join(outputPinDirectory, dialect+".sql"))
			c.Assert(err, qt.IsNil)
			c.Assert(string(content), qt.Matches, `(?s).*CREATE TABLE \S*users.*`)
			c.Assert(string(content), qt.Matches, `(?s).*CREATE TABLE \S*orders.*`)
			c.Assert(string(content), qt.Matches, `(?s).*users_created_ix.*`)
		})
	}
}
