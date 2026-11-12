//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// invokerEffect is what a document says one statement does to security_invoker
// on the view my_docs, in the document's own words.
type invokerEffect struct {
	Version int
	Action  string
	Before  *bool
	After   *bool
	Changed bool
}

// effectsUnit is a statement of a saved plan, or a unit of an apply document,
// read the way a consumer reads it: by the field names the documents publish.
type effectsUnit struct {
	SQL     string `json:"sql"`
	Effects []struct {
		Version int `json:"version"`
		Object  struct {
			Kind   string `json:"kind"`
			Schema string `json:"schema"`
			Name   string `json:"name"`
		} `json:"object"`
		Action     string `json:"action"`
		Properties []struct {
			Name    string `json:"name"`
			Before  *bool  `json:"before"`
			After   *bool  `json:"after"`
			Changed bool   `json:"changed"`
		} `json:"properties"`
	} `json:"effects"`
}

// effectsDocument is the part of a `schema plan --json` or `schema apply
// --json` document, or of a saved plan, that carries effects.
type effectsDocument struct {
	Outcome    string        `json:"outcome"`
	Statements []effectsUnit `json:"statements"`
	Plan       struct {
		Statements []effectsUnit `json:"statements"`
	} `json:"plan"`
	Units []effectsUnit `json:"units"`
}

// readEffectsDocument decodes what a run printed on standard output, or the
// contents of a saved plan.
func readEffectsDocument(c *qt.C, text string) effectsDocument {
	c.Helper()
	var document effectsDocument
	c.Assert(json.Unmarshal([]byte(text), &document), qt.IsNil, qt.Commentf("%s", text))
	return document
}

// invokerEffects reads what each unit says it does to security_invoker on
// public.my_docs, in unit order. A unit that says nothing about it is left out.
func invokerEffects(units []effectsUnit) []invokerEffect {
	var found []invokerEffect
	for _, unit := range units {
		for _, effect := range unit.Effects {
			if effect.Object.Kind != "view" || effect.Object.Schema != "public" || effect.Object.Name != "my_docs" {
				continue
			}
			for _, property := range effect.Properties {
				if property.Name == "security_invoker" {
					found = append(found, invokerEffect{
						Version: effect.Version, Action: effect.Action,
						Before: property.Before, After: property.After, Changed: property.Changed,
					})
				}
			}
		}
	}
	return found
}

// invokerHeld is the security_invoker a view applies, read from what
// pg_class.reloptions holds for it, as [viewSecurityTarget.held] spells it.
func invokerHeld(c *qt.C, held string) bool {
	c.Helper()
	on, known := map[string]bool{"": false, "security_invoker=true": true, "security_invoker=false": false}[held]
	c.Assert(known, qt.IsTrue, qt.Commentf("the view holds %q", held))
	return on
}

// viewEffectPath is a change to the view my_docs, which holds
// security_invoker=true, and what each engine's plan says its statements do to
// the option. CockroachDB drops the view and creates it again for a change to
// its body, where the other engines replace it in place.
type viewEffectPath struct {
	name    string
	body    string
	options string
	effects map[string][]invokerEffect
}

var viewEffectPaths = []viewEffectPath{
	{
		name: "a body change that keeps security_invoker", body: viewSecurityBody + " WHERE id > 0", options: "security_invoker = true",
		effects: map[string][]invokerEffect{
			"postgresql": {{Version: 1, Action: "replace", Before: new(true), After: new(true)}},
			"cockroachdb": {
				{Version: 1, Action: "drop", Before: new(true), Changed: true},
				{Version: 1, Action: "create", After: new(true), Changed: true},
			},
			"yugabytedb": {{Version: 1, Action: "replace", Before: new(true), After: new(true)}},
		},
	},
	{
		name: "security_invoker alone", body: viewSecurityBody, options: "security_invoker = false",
		effects: map[string][]invokerEffect{
			"postgresql":  {{Version: 1, Action: "alter", Before: new(true), After: new(false), Changed: true}},
			"cockroachdb": {{Version: 1, Action: "alter", Before: new(true), After: new(false), Changed: true}},
			"yugabytedb":  {{Version: 1, Action: "alter", Before: new(true), After: new(false), Changed: true}},
		},
	},
	{
		name: "a new column and security_invoker", body: viewSecurityWide, options: "security_invoker = false",
		effects: map[string][]invokerEffect{
			"postgresql": {{Version: 1, Action: "replace", Before: new(true), After: new(false), Changed: true}},
			"cockroachdb": {
				{Version: 1, Action: "drop", Before: new(true), Changed: true},
				{Version: 1, Action: "create", After: new(false), Changed: true},
			},
			"yugabytedb": {{Version: 1, Action: "replace", Before: new(true), After: new(false), Changed: true}},
		},
	},
}

// TestViewSecurityEffectsMatchTheServerE2E plans a change to a view that holds
// security_invoker=true, on each engine and each path the change takes, and
// reads what the plan says each statement does to the option: in the saved
// plan, in the plan and apply documents, and in a dry run computed without the
// plan. The value before the first statement is the one the server held, and
// the value after the last is the one the server holds once the plan ran. A
// body change states the option again and says it is unchanged, which reading
// the statement cannot tell from a change.
func TestViewSecurityEffectsMatchTheServerE2E(t *testing.T) {
	for _, engine := range viewSecurityEngines {
		for _, path := range viewEffectPaths {
			t.Run(engine.name+"/"+path.name, func(t *testing.T) {
				c := qt.New(t)
				target := newViewSecurityTarget(c, engine, "security_invoker = true")
				schema := target.schemaFile(c, path.body, path.options)
				planPath := filepath.Join(c.TempDir(), "my_docs.plan.json")
				want := path.effects[engine.name]
				held := target.held(c)

				planned := runPtahIn(c, "schema", "plan", "--db-url", target.url, "--schema-file", schema, "--output", planPath, "--json")
				saved, err := os.ReadFile(planPath)
				c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
				dryRun := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--schema-file", schema, "--dry-run", "--json")
				applied := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--plan", planPath, "--auto-approve", "--json")

				assertRunExited(c, planned, 0)
				assertRunExited(c, dryRun, 0)
				assertRunExited(c, applied, 0)
				c.Assert(readEffectsDocument(c, planned.Stdout).Outcome, qt.Equals, "changes")
				c.Assert(readEffectsDocument(c, dryRun.Stdout).Outcome, qt.Equals, "dry-run")
				c.Assert(readEffectsDocument(c, applied.Stdout).Outcome, qt.Equals, "applied")
				c.Assert(invokerEffects(readEffectsDocument(c, string(saved)).Statements), qt.DeepEquals, want, qt.Commentf("%s", saved))
				c.Assert(invokerEffects(readEffectsDocument(c, planned.Stdout).Plan.Statements), qt.DeepEquals, want)
				c.Assert(invokerEffects(readEffectsDocument(c, dryRun.Stdout).Units), qt.DeepEquals, want, qt.Commentf("%s", dryRun.Stdout))
				c.Assert(invokerEffects(readEffectsDocument(c, applied.Stdout).Units), qt.DeepEquals, want, qt.Commentf("%s", applied.Stdout))
				c.Assert(invokerHeld(c, held), qt.Equals, *want[0].Before)
				c.Assert(invokerHeld(c, target.held(c)), qt.Equals, *want[len(want)-1].After)
			})
		}
	}
}

// assertRunExited asserts a run's exit status, with what it printed.
func assertRunExited(c *qt.C, got clirun.Result, code int) {
	c.Helper()
	c.Assert(got.ExitCode, qt.Equals, code, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
}
