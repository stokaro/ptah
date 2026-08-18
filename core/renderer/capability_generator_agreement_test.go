package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
)

// TestRender_ObjectKindCapabilitiesAgreeWithTheGenerator generalizes the gate
// that until now guarded exactly one key.
//
// capability.MariaDB1011 once declared Sequences: true while no code path
// emitted, read or planned a sequence for that dialect -- a flag promising a
// capability that did not exist (stokaro/ptah#931 item 8).
// TestRender_SequencesCapabilityAgreesWithTheGenerator was built to stop that
// recurring, and it did, for Sequences. Every other object-kind key had
// nothing: flipping RoleManagement for SQL Server broke no test at all
// (stokaro/ptah#1698), which is how a promise with no path behind it lands
// again.
//
// The rule is one sentence and it is the same for every key: a preset may claim
// an object kind only if rendering a declared object of that kind produces a
// statement the server would execute.
//
// Each kind gets a schema declaring ONLY that kind, and that is not tidiness.
// Several renderers fail closed before emitting anything -- the MySQL family
// refuses a declared role outright, ClickHouse refuses a role carrying
// attributes -- so one kind's refusal in a shared fixture blanks the output and
// every other key reads as "does not emit". The first draft of this test did
// exactly that and reported fifteen disagreements that were all its own.
func TestRender_ObjectKindCapabilitiesAgreeWithTheGenerator(t *testing.T) {
	dialects := []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.ClickHouse,
		platform.SQLite, platform.SQLServer, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
	}

	for _, dialect := range dialects {
		for _, kind := range capabilityProbeKinds() {
			t.Run(dialect+"/"+string(kind.key), func(t *testing.T) {
				c := qt.New(t)

				statements, err := renderer.GetOrderedCreateStatements(kind.schema(), dialect)

				// A target that fails before SQL certainly emitted nothing, and
				// that is the honest reading rather than a reason to skip: the
				// MySQL family refuses a declared role, and RoleManagement is
				// false there, which is agreement.
				emits := err == nil && emitsExecutable(statements, kind.keywords)
				c.Assert(emits, qt.Equals, capability.ForDialect(dialect).Has(kind.key),
					qt.Commentf("%s: the %s capability and the generator disagree (render error: %v)",
						dialect, kind.key, err))
			})
		}
	}
}

// capabilityProbeKind is one key, the schema that exercises it, and the
// statement spellings that count as emission.
type capabilityProbeKind struct {
	key      capability.Capability
	keywords []string
	schema   func() *goschema.Database
}

func capabilityProbeKinds() []capabilityProbeKind {
	return []capabilityProbeKind{
		{
			key:      capability.Sequences,
			keywords: []string{"CREATE SEQUENCE"},
			schema:   func() *goschema.Database { return probeSchema(withSequence) },
		},
		{
			key:      capability.RoleManagement,
			keywords: []string{"CREATE ROLE"},
			schema:   func() *goschema.Database { return probeSchema(withRole) },
		},
		{
			key:      capability.Views,
			keywords: []string{"CREATE VIEW", "CREATE OR REPLACE VIEW", "CREATE OR ALTER VIEW"},
			schema:   func() *goschema.Database { return probeSchema(withTable, withView) },
		},
		{
			key:      capability.Triggers,
			keywords: []string{"CREATE TRIGGER", "CREATE OR REPLACE TRIGGER"},
			schema:   func() *goschema.Database { return probeSchema(withTable, withTrigger) },
		},
		{
			key:      capability.Functions,
			keywords: []string{"CREATE FUNCTION", "CREATE OR REPLACE FUNCTION"},
			schema:   func() *goschema.Database { return probeSchema(withFunction) },
		},
	}
}

func probeSchema(parts ...func(*goschema.Database)) *goschema.Database {
	schema := &goschema.Database{}
	for _, part := range parts {
		part(schema)
	}
	return schema
}

func withSequence(schema *goschema.Database) {
	start := int64(1000)
	schema.Sequences = []goschema.Sequence{{Name: "seq_probe", AsType: "bigint", Start: &start}}
}

// withRole declares a role with no attributes, so the only thing under test is
// whether the target creates one. ClickHouse refuses a role carrying
// PostgreSQL attributes, which would answer a different question.
func withRole(schema *goschema.Database) {
	schema.Roles = []goschema.Role{{Name: "role_probe", Inherit: true}}
}

func withTable(schema *goschema.Database) {
	schema.Tables = []goschema.Table{{StructName: "T", Name: "table_probe"}}
	schema.Fields = []goschema.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}}
}

func withView(schema *goschema.Database) {
	schema.Views = []goschema.View{{StructName: "V", Name: "view_probe", Body: "SELECT id FROM table_probe"}}
}

func withTrigger(schema *goschema.Database) {
	schema.Triggers = []goschema.Trigger{{
		StructName: "TR", Name: "trigger_probe", Table: "table_probe",
		Timing: "AFTER", Event: "INSERT", ForEach: "ROW", Body: "SELECT 1",
	}}
}

func withFunction(schema *goschema.Database) {
	schema.Functions = []goschema.Function{{
		Name: "func_probe", Returns: "integer", Language: "sql", Body: "SELECT 1;",
	}}
}

// emitsExecutable reports whether the rendered statements contain a statement
// the server would execute, as opposed to one named inside a not-supported
// comment.
//
// The distinction is the whole measurement: every renderer writes its refusal
// as a comment repeating the object's DDL keywords, so a plain substring search
// over the output cannot tell the two apart.
func emitsExecutable(statements []string, keywords []string) bool {
	for _, statement := range statements {
		for line := range strings.SplitSeq(statement, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "--") {
				continue
			}
			for _, keyword := range keywords {
				if strings.Contains(trimmed, keyword) {
					return true
				}
			}
		}
	}
	return false
}

// TestRender_ObjectKindPathsExistIndependentOfThePreset asserts the path is
// reachable on its own, rather than only through the preset that happens to
// claim it today.
//
// The agreement gate above asserts `emits == preset.Has(key)`, and its two
// sides are not independent: the renderer consults the same flag, so a preset
// that dropped a claim would make the renderer refuse and the assertion would
// still hold. That is not a hole to be plugged -- a preset may withhold a key
// deliberately, and several do, because the RENDER half exists while the read
// or plan half does not. Equality in both directions would force a claim that
// cannot converge.
//
// What is worth asserting is the weaker, true thing: turning a key on gets real
// DDL rather than nothing. A path that only works because some other preset
// enabled something alongside it would fail here.
func TestRender_ObjectKindPathsExistIndependentOfThePreset(t *testing.T) {
	claimed := map[string][]capability.Capability{
		platform.Postgres:   {capability.Sequences, capability.RoleManagement, capability.Views, capability.Triggers, capability.Functions},
		platform.SQLServer:  {capability.Sequences, capability.RoleManagement, capability.Views, capability.Triggers},
		platform.ClickHouse: {capability.RoleManagement, capability.Views},
	}

	for dialect, keys := range claimed {
		for _, key := range keys {
			t.Run(dialect+"/"+string(key), func(t *testing.T) {
				c := qt.New(t)
				kind := probeKindFor(c, key)

				statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
					kind.schema(), dialect, capability.ForDialect(dialect).With(key, true))

				c.Assert(err, qt.IsNil)
				c.Assert(emitsExecutable(statements, kind.keywords), qt.IsTrue,
					qt.Commentf("%s: %s is claimed but no path emits the object", dialect, key))
			})
		}
	}
}

func probeKindFor(c *qt.C, key capability.Capability) capabilityProbeKind {
	c.Helper()
	for _, kind := range capabilityProbeKinds() {
		if kind.key == key {
			return kind
		}
	}
	c.Fatalf("no probe kind for %s", key)
	return capabilityProbeKind{}
}
