package planner_test

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// normalizeDialectSource is the file that decides which dialect spellings ptah
// accepts. The spelling list below is read out of it rather than copied here,
// so a spelling added to the switch becomes a row of the agreement test without
// anyone editing this file.
const normalizeDialectSource = "../../core/platform/constants.go"

// quotedLiteral deliberately requires a non-empty literal: the switch's default
// arm returns "", which is the one string in the body that is not a spelling.
var quotedLiteral = regexp.MustCompile(`"([^"]+)"`)

// acceptedSpellings returns every dialect spelling that appears as a case in
// platform.NormalizeDialect's switch, read from the switch body itself.
func acceptedSpellings(c *qt.C) []string {
	source, err := os.ReadFile(normalizeDialectSource)
	c.Assert(err, qt.IsNil)

	_, afterSignature, foundSignature := strings.Cut(string(source), "func NormalizeDialect(dialect string) string {")
	c.Assert(foundSignature, qt.IsTrue, qt.Commentf("NormalizeDialect signature moved in %s", normalizeDialectSource))

	body, _, foundEnd := strings.Cut(afterSignature, "\n}")
	c.Assert(foundEnd, qt.IsTrue, qt.Commentf("NormalizeDialect body is unterminated in %s", normalizeDialectSource))

	spellings := make([]string, 0, 24)
	for _, match := range quotedLiteral.FindAllStringSubmatch(body, -1) {
		spellings = append(spellings, match[1])
	}
	slices.Sort(spellings)
	return slices.Compact(spellings)
}

// postgresFamilySpellings is every spelling NormalizeDialect maps onto a
// PostgreSQL-family engine — the canonical names and their documented
// non-canonical spellings alike.
func postgresFamilySpellings(c *qt.C) []string {
	return slices.DeleteFunc(acceptedSpellings(c), func(spelling string) bool {
		return !platform.IsPostgresFamily(spelling)
	})
}

// nonCanonicalSpellings is every accepted spelling that is not already a
// canonical engine name — the documented aliases, and only those.
func nonCanonicalSpellings(c *qt.C) []string {
	return slices.DeleteFunc(acceptedSpellings(c), func(spelling string) bool {
		return spelling == platform.NormalizeDialect(spelling)
	})
}

// objectKindFixture is one desired schema carrying each object kind the
// capability registry can now refuse: a view, a materialized view, a function,
// and a trigger, over a table they can all attach to.
//
// It deliberately carries no role, grant, RLS policy, sequence or enum. Those
// are refused by OTHER keys on some family members, and one of them refusing
// first would decide the render before the four keys under test were consulted.
func objectKindFixture() goschema.Database {
	return goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "User", Name: "updated_at", Type: "TIMESTAMP", Nullable: true},
		},
		Functions: []goschema.Function{{
			StructName: "FunctionMarker",
			Name:       "touch_updated",
			Returns:    "trigger",
			Language:   "plpgsql",
			Body:       "BEGIN NEW.updated_at = NOW(); RETURN NEW; END;",
		}},
		Views: []goschema.View{{
			StructName: "ActiveUsers",
			Name:       "active_users",
			Body:       "SELECT id FROM users",
		}},
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "UserCounts",
			Name:       "user_counts",
			Body:       "SELECT count(*) AS cnt FROM users",
		}},
		Triggers: []goschema.Trigger{{
			StructName: "UsersTouch",
			Name:       "users_touch",
			Table:      "users",
			Timing:     "BEFORE",
			Event:      "UPDATE",
			ForEach:    "ROW",
			Body:       "NEW.updated_at = NOW(); RETURN NEW;",
		}},
	}
}

// renderedSchema is what `ptah schema render --dialect <d>` produces: the
// offline converter's AST for the whole desired schema, rendered.
func renderedSchema(c *qt.C, database goschema.Database, dialect string) string {
	nodes := fromschema.FromDatabase(database, dialect)
	sql, err := renderer.RenderSQL(dialect, nodes.Statements...)
	c.Assert(err, qt.IsNil, qt.Commentf("render path failed for %s", dialect))
	return sql
}

// plannedSchema is what `ptah schema apply` plans for the same desired schema
// against an empty database: the comparator's diff, through the dialect planner
// and the same renderer.
func plannedSchema(c *qt.C, database goschema.Database, dialect string) string {
	diff := schemadiff.CompareWithDialect(&database, &dbschematypes.DBSchema{}, dialect)
	sql, err := planner.GenerateSchemaDiffSQL(diff, &database, dialect)
	c.Assert(err, qt.IsNil, qt.Commentf("plan path failed for %s", dialect))
	return sql
}

// renderedOrRefusal and plannedOrRefusal fold a failure into the compared
// string instead of failing, so an engine that refuses part of the fixture
// still contributes a value both spellings of that engine must agree on. The
// spelling-parity test below compares engines to themselves, never to each
// other, so a refusal is a legitimate answer as long as it is the same answer.
func renderedOrRefusal(database goschema.Database, dialect string) string {
	nodes := fromschema.FromDatabase(database, dialect)
	sql, err := renderer.RenderSQL(dialect, nodes.Statements...)
	return fmt.Sprintf("%s | err=%v", sql, err)
}

func plannedOrRefusal(database goschema.Database, dialect string) string {
	diff := schemadiff.CompareWithDialect(&database, &dbschematypes.DBSchema{}, dialect)
	sql, err := planner.GenerateSchemaDiffSQL(diff, &database, dialect)
	return fmt.Sprintf("%s | err=%v", sql, err)
}

// objectKindGates lists the object kinds the four new keys govern, with the
// words the renderer's skip comment uses and the object each one names in the
// fixture above.
var objectKindGates = []struct {
	name   string
	key    capability.Capability
	kind   string
	object string
}{
	{"view", capability.Views, "view", "active_users"},
	{"materialized view", capability.MaterializedViews, "materialized view", "user_counts"},
	{"function", capability.Functions, "function", "touch_updated"},
	{"trigger", capability.Triggers, "trigger", "users_touch"},
}

// postgresFamily is every canonical dialect registerBuiltInPlanners routes
// through the PostgreSQL planner and renderer. That shared implementation is
// the point: these four must still be allowed to answer differently.
var postgresFamily = []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner}

// TestSpellingExtraction_Controls proves the spelling list the agreement test
// iterates is really NormalizeDialect's own list.
//
// Reverting the extraction — a renamed signature, a regexp that stops matching
// — leaves acceptedSpellings empty, and an empty list would make the agreement
// test below pass while comparing nothing.
func TestSpellingExtraction_Controls(t *testing.T) {
	c := qt.New(t)

	spellings := acceptedSpellings(c)
	family := postgresFamilySpellings(c)
	aliases := nonCanonicalSpellings(c)

	// Positive control: spellings that exist only inside that switch, one per
	// engine family that has one. An extractor that stopped working loses these
	// first, and prints the missing name.
	for _, alias := range []string{"pgx", "ch", "sqlite3", "tsql", "sql-server", "crdb", "ysql", "google_spanner"} {
		c.Assert(spellings, qt.Contains, alias)
		c.Assert(aliases, qt.Contains, alias)
	}
	// Positive control: every canonical family name is a case of its own.
	for _, canonical := range postgresFamily {
		c.Assert(family, qt.Contains, canonical)
		c.Assert(aliases, qt.Not(qt.Contains), canonical)
	}
	// Negative control: the extractor must not reach past the switch body.
	// Every literal it collected has to be a spelling NormalizeDialect really
	// accepts, so a comment word or a neighboring function's literal fails here.
	for _, spelling := range spellings {
		c.Assert(platform.NormalizeDialect(spelling), qt.Not(qt.Equals), "",
			qt.Commentf("collected %q, which is not an accepted spelling", spelling))
	}
	// Negative control: the family filter must not drag in another engine.
	for _, spelling := range family {
		c.Assert(platform.IsPostgresFamily(spelling), qt.IsTrue,
			qt.Commentf("collected %q, which is not a PostgreSQL-family spelling", spelling))
	}
	c.Assert(family, qt.Not(qt.Contains), "mysql")
	c.Assert(family, qt.Not(qt.Contains), "sqlite3")
}

// TestEverySpelling_RendersAndPlansLikeItsCanonicalName is the alias table, one
// row per documented spelling, on BOTH paths.
//
// stokaro/ptah#929 items 2 and 3 are exactly this defect on the render path:
// `--dialect mssql` and `--dialect pgx` produced different DDL from `sqlserver`
// and `postgres`, silently, at exit 0. The offline converter's half of that is
// already pinned in internal/convert/fromschema. This is the half the capability
// gate added: a preset lookup that failed to normalize would hand an alias the
// nil set, and the nil set refuses every object kind — so `pgx` would drop its
// views, functions and triggers while `postgres` kept them, on both paths.
//
// The comparison is a spelling against its OWN canonical name, never against
// another engine, and both sides fold a refusal into the compared string. What
// this asserts is only that the spelling of the name never changes the answer.
//
// The rows come from NormalizeDialect's own switch, so a spelling added there
// is covered without anyone editing this file.
func TestEverySpelling_RendersAndPlansLikeItsCanonicalName(t *testing.T) {
	c := qt.New(t)

	aliases := nonCanonicalSpellings(c)
	c.Assert(len(aliases) > 0, qt.IsTrue, qt.Commentf("the alias table is empty; the extractor is broken"))

	for _, spelling := range aliases {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)

			canonical := platform.NormalizeDialect(spelling)
			database := objectKindFixture()

			c.Assert(renderedOrRefusal(database, spelling), qt.Equals, renderedOrRefusal(database, canonical),
				qt.Commentf("render path: %s must answer like %s", spelling, canonical))
			c.Assert(plannedOrRefusal(database, spelling), qt.Equals, plannedOrRefusal(database, canonical),
				qt.Commentf("plan path: %s must answer like %s", spelling, canonical))
		})
	}
}

// TestObjectKinds_RenderAndPlanAgree is the completion criterion for
// stokaro/ptah#929 item 5, and the check that item 5 did not reopen items 1
// through 4.
//
// `schema render` and `schema apply` used to answer differently about the same
// file for the whole PostgreSQL family: apply planned the sequences, functions,
// views and triggers that render dropped in silence at exit 0. Items 1 and 4
// closed that by making the converter emit for the family. This test holds that
// line while capability refusals arrive: for every family member and every gated
// object kind, the two paths must carry the SAME answer — either both the DDL,
// or both the identical skip comment naming the object.
//
// It iterates the canonical names only, deliberately. Its expectation is read
// from that dialect's own preset, so running it against an alias would compare
// a mis-normalized lookup against itself and pass. Aliases are the subject of
// TestEverySpelling_RendersAndPlansLikeItsCanonicalName above, which compares
// output to output and cannot be fooled that way.
//
// Reading the expectation from the preset is what keeps a preset that changes
// its mind about an object kind covered without anyone editing this test.
func TestObjectKinds_RenderAndPlanAgree(t *testing.T) {
	for _, dialect := range postgresFamily {
		t.Run(dialect, func(t *testing.T) {
			for _, gate := range objectKindGates {
				t.Run(gate.name, func(t *testing.T) {
					c := qt.New(t)

					database := objectKindFixture()
					supported := capability.ForDialect(dialect).Has(gate.key)
					skipped := fmt.Sprintf("-- %s: %s %s is not supported by this target; skipped.",
						strings.ToUpper(dialect), gate.kind, gate.object)

					rendered := renderedSchema(c, database, dialect)
					planned := plannedSchema(c, database, dialect)

					c.Assert(strings.Contains(rendered, skipped), qt.Equals, !supported,
						qt.Commentf("render path for %s:\n%s", dialect, rendered))
					c.Assert(strings.Contains(planned, skipped), qt.Equals, !supported,
						qt.Commentf("plan path for %s:\n%s", dialect, planned))
				})
			}
		})
	}
}

// TestObjectKinds_EveryGateIsExercised is the control for the test above.
//
// Reading the expectation from the preset makes the comparison honest, but it
// also makes it vacuous if no preset ever says no: every assertion would read
// "the skip comment is absent, on both paths", which a renderer that had never
// heard of these keys would satisfy too. This asserts the family really
// exercises both sides of the gates.
func TestObjectKinds_EveryGateIsExercised(t *testing.T) {
	c := qt.New(t)

	// Three of the four gates have a refusing member, or the agreement test
	// would only ever compare two absences.
	c.Assert(refusingDialects(capability.MaterializedViews), qt.DeepEquals, []string{platform.Spanner})
	c.Assert(refusingDialects(capability.Functions), qt.DeepEquals, []string{platform.Spanner})
	c.Assert(refusingDialects(capability.Triggers), qt.DeepEquals, []string{platform.Spanner})

	// And each has accepting members, or "refuses everything" would pass as
	// well as "consults the preset".
	c.Assert(acceptingDialects(capability.MaterializedViews), qt.HasLen, 3)
	c.Assert(acceptingDialects(capability.Functions), qt.HasLen, 3)
	c.Assert(acceptingDialects(capability.Triggers), qt.HasLen, 3)

	// Plain views are the deliberate control: no family member refuses one, so
	// that row asserts the gate stays out of the way of an object every one of
	// these targets hosts.
	c.Assert(refusingDialects(capability.Views), qt.HasLen, 0)
	c.Assert(acceptingDialects(capability.Views), qt.HasLen, len(postgresFamily))
}

// refusingDialects lists the family members whose preset denies key.
func refusingDialects(key capability.Capability) []string {
	return slices.DeleteFunc(slices.Clone(postgresFamily), func(dialect string) bool {
		return capability.ForDialect(dialect).Has(key)
	})
}

// acceptingDialects lists the family members whose preset allows key.
func acceptingDialects(key capability.Capability) []string {
	return slices.DeleteFunc(slices.Clone(postgresFamily), func(dialect string) bool {
		return !capability.ForDialect(dialect).Has(key)
	})
}
