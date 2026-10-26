//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/engine"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
)

// The scope-selections fixture declares a queue, named in a schema, which a
// TOPIC grant names, and a pool, named across the database. A queue whose name
// ends in "-<pool>" requires that pool, and a scope that keeps the queue keeps
// the pool.
const (
	selectedQueue = schemaext.Kind("example.org/fixture/queue")
	selectedPool  = schemaext.Kind("example.org/fixture/pool")
)

// selectionRead reads the fixture's two queues and two pools through the
// host, with the scope selections of the runtime the host serves.
func selectionRead(c *qt.C) (*catalog.Database, selectext.Set) {
	c.Helper()
	host, runtime := yamlProviderCommand(c, "scope-selections")
	db, err := observedConnection(c, host, "items").Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	return db, runtime.Selections()
}

// selected names the queues and pools objects holds, as kind and name.
func selected(objects schemaext.Objects) []string {
	names := make([]string, 0, objects.Len())
	for _, ref := range objects.Refs() {
		name := ref.Name.Source
		if !ref.Schema.Empty() {
			name = ref.Schema.Source + "." + name
		}
		names = append(names, map[objectidentity.Kind]string{objectidentity.Kind(selectedQueue): "queue ", objectidentity.Kind(selectedPool): "pool "}[ref.Kind]+name)
	}
	slices.Sort(names)
	return names
}

// A selector names a provider's object by the type its engine declares and
// by its schema, and a kept queue keeps the pool it requires unless an
// exclusion names the pool.
func TestProviderScopeSelectionsNameItsObjects(t *testing.T) {
	db, selections := selectionRead(qt.New(t))
	every := []string{"pool fast", "pool slow", "queue main.jobs-fast", "queue main.mail"}
	excluded := []struct {
		name     string
		patterns []string
		want     []string
	}{
		{name: "every queue", patterns: []string{"*[type=queue]"}, want: []string{"pool fast", "pool slow"}},
		{name: "a pool by its name alone", patterns: []string{"fast[type=pool]"}, want: []string{"pool slow", "queue main.jobs-fast", "queue main.mail"}},
		{name: "a queue in its schema", patterns: []string{"main.mail[type=queue]"}, want: []string{"pool fast", "pool slow", "queue main.jobs-fast"}},
		{name: "another type's name", patterns: []string{"mail[type=pool]"}, want: every},
	}
	for _, test := range excluded {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasfilter.ExcludeDatabase(selections, db, test.patterns)
			c.Assert(err, qt.IsNil)
			c.Assert(selected(got.FeatureObjects), qt.DeepEquals, test.want)
			c.Assert(selected(db.FeatureObjects), qt.DeepEquals, every)
		})
	}
	scoped := []struct {
		name  string
		scope atlasfilter.Scope
		want  []string
	}{
		{name: "a kept queue keeps its pool", scope: atlasfilter.Scope{Include: []string{"main.jobs-fast[type=queue]"}}, want: []string{"pool fast", "queue main.jobs-fast"}},
		{name: "a queue on no pool keeps none", scope: atlasfilter.Scope{Include: []string{"main.mail[type=queue]"}}, want: []string{"queue main.mail"}},
		{name: "an exclusion of the pool stands", scope: atlasfilter.Scope{Include: []string{"main.jobs-fast[type=queue]"}, Exclude: []string{"fast[type=pool]"}}, want: []string{"queue main.jobs-fast"}},
		{name: "another schema keeps the pools", scope: atlasfilter.Scope{Schemas: []string{"other"}}, want: []string{"pool fast", "pool slow"}},
	}
	for _, test := range scoped {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.scope.Selections = selections
			got, err := atlasfilter.ScopeDatabase(db, test.scope)
			c.Assert(err, qt.IsNil)
			c.Assert(selected(got.FeatureObjects), qt.DeepEquals, test.want)
		})
	}
}

// declaredSelections declares the fixture's four objects and a grant on each
// queue.
func declaredSelections(c *qt.C, runtime *engine.Runtime) *schemamodel.Database {
	c.Helper()
	queue := func(name string) objectidentity.ID {
		return objectidentity.ID{Kind: objectidentity.Kind(selectedQueue), Schema: objectidentity.Part{Source: "main", Normalized: "main"}, Name: objectidentity.Part{Source: name, Normalized: name}}
	}
	pool := func(name string) objectidentity.ID {
		return objectidentity.ID{Kind: objectidentity.Kind(selectedPool), Name: objectidentity.Part{Source: name, Normalized: name}}
	}
	var objects []schemaext.Object
	for _, ref := range []objectidentity.ID{queue("jobs-fast"), queue("mail"), pool("fast"), pool("slow")} {
		objects = append(objects, schemaext.Object{Ref: ref, Value: comparisonModel(c, runtime, schemaext.Desired, schemaext.Kind(ref.Kind), ref, `{"seconds":60}`)})
	}
	declared, err := schemaext.NewObjects(objects...)
	c.Assert(err, qt.IsNil)
	return &schemamodel.Database{FeatureObjects: declared, Grants: []schemamodel.Grant{
		{Role: "app", Privileges: []string{"SELECT"}, OnObject: "main.jobs-fast", ObjectKind: "TOPIC"},
		{Role: "app", Privileges: []string{"SELECT"}, OnObject: "MAIN.Mail", ObjectKind: "topic"},
	}}
}

func grantTargets(grants []schemamodel.Grant) []string {
	var targets []string
	for _, grant := range grants {
		targets = append(targets, grant.OnObject)
	}
	return targets
}

// A grant whose object type is a selection's rides the object it names, read
// with the selection's naming and the engine's identifier rules: a scope that
// excludes the queue excludes the grant, and one that keeps it keeps it.
func TestProviderScopeSelectionsTakeGrantsWithTheirObject(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "scope-selections")
	declared := declaredSelections(c, runtime)

	excluded, err := atlasfilter.ExcludeGenerated(runtime.Selections(), declared, []string{"main.mail[type=queue]"})
	c.Assert(err, qt.IsNil)
	c.Assert(grantTargets(excluded.Grants), qt.DeepEquals, []string{"main.jobs-fast"})
	c.Assert(selected(excluded.FeatureObjects), qt.DeepEquals, []string{"pool fast", "pool slow", "queue main.jobs-fast"})

	kept, err := atlasfilter.ScopeGenerated(declared, atlasfilter.Scope{Selections: runtime.Selections(), Include: []string{"main.mail[type=queue]"}})
	c.Assert(err, qt.IsNil)
	c.Assert(grantTargets(kept.Grants), qt.DeepEquals, []string{"MAIN.Mail"})
	c.Assert(selected(kept.FeatureObjects), qt.DeepEquals, []string{"queue main.mail"})
}

// An object of a selected kind whose identity its naming cannot read refuses
// the read: a queue qualified by its database would answer to the selector of
// another database's queue.
func TestProviderScopeSelectionsNameItsObjects_FailurePath(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "scope-selections")
	db, err := observedConnection(c, host, "misfit").Reader().ReadSchema()
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*observed example.org/fixture/queue belongs to a database outside the exact session binding`)
	c.Assert(db, qt.IsNil)
}

// startSelections starts the scope-selections fixture with a second engine.
func startSelections(c *qt.C, second string) (*providerhost.Host, error) {
	c.Helper()
	executable := testutils.BuildFixtureProvider(c, "scope-selections")
	data, err := os.ReadFile(executable)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	host, err := providerhost.Start(c.Context(), providerhost.Spec{Executable: executable, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture",
		Environment: []string{"FIXTURE_SECOND_ENGINE=" + second}})
	if host != nil {
		c.Cleanup(host.Close)
	}
	return host, err
}

// Two engines that select a kind alike serve one selection of it.
func TestProviderScopeSelectionsOfTwoEngines(t *testing.T) {
	c := qt.New(t)
	host, err := startSelections(c, "same")
	c.Assert(err, qt.IsNil)
	runtime, err := engine.New(host.Provider())
	c.Assert(err, qt.IsNil)
	c.Assert(runtime.Selections().ForType("queue"), qt.HasLen, 1)
	c.Assert(runtime.Selections().ForType("pool"), qt.HasLen, 1)
}

// A scope reads a kind one way whichever engine holds it, so engines that
// select it differently, or read its names by other rules, are refused, and
// so is a grant object type two kinds claim.
func TestProviderScopeSelectionsOfTwoEngines_FailurePath(t *testing.T) {
	refusals := []struct {
		second string
		want   string
	}{
		{second: "selector", want: `.*invalid scope selections: engines declare the scope selection of example.org/fixture/queue differently`},
		{second: "rules", want: `.*invalid scope selections: engines declare the scope selection of example.org/fixture/queue differently`},
		{second: "grant", want: `.*invalid scope selections: grant object type TOPIC names example.org/fixture/queue and example.org/fixture/stream`},
	}
	for _, test := range refusals {
		t.Run(test.second, func(t *testing.T) {
			c := qt.New(t)
			host, err := startSelections(c, test.second)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(host, qt.IsNil)
		})
	}
}

// Live session qualification remains in the captured values but does not
// change how a selector names an object or keeps its prerequisite.
func TestProviderScopeSelectionsBindTheSessionDatabase(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "scope-selections")
	db, err := observedConnection(c, host, "bound").Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	kept, err := atlasfilter.ScopeDatabase(db, atlasfilter.Scope{Selections: runtime.Selections(), Include: []string{"main.jobs-fast[type=queue]"}})
	c.Assert(err, qt.IsNil)
	c.Assert(selected(kept.FeatureObjects), qt.DeepEquals, []string{"pool fast", "queue main.jobs-fast"})
	for _, ref := range db.FeatureObjects.Refs() {
		c.Assert(ref.Database.Empty(), qt.IsTrue)
	}
}
