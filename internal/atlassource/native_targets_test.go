package atlassource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/atlassource"
)

func TestExplicitDatabaseTargetsReachEverySourceExpansion(t *testing.T) {
	for _, test := range []struct {
		name, url string
		env       atlassource.ProjectEnv
	}{
		{name: "direct", url: "example://database"},
		{name: "alias", url: "example-alias://database"},
		{name: "environment URL", url: "env://url", env: atlassource.ProjectEnv{Loaded: true, Config: projectconfig.Config{DatabaseURL: "example://database"}}},
		{name: "environment dev", url: "env://dev", env: atlassource.ProjectEnv{Loaded: true, Config: projectconfig.Config{DevURL: "example://database"}}},
		{name: "environment source", url: "env://src", env: atlassource.ProjectEnv{Loaded: true, Config: projectconfig.Config{SchemaSources: []string{"example://database"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selected := map[string]string{"example": "example", "example-alias": "example"}
			set, err := atlassource.ClassifySet("--to", []string{test.url}, test.env, selected)
			c.Assert(err, qt.IsNil)
			c.Assert(set.Kind, qt.Equals, atlassource.KindDatabase)
			c.Assert(set.Sources, qt.HasLen, 1)
			c.Assert(set.Sources[0].Dialect, qt.Equals, "example")
			_, err = atlassource.ClassifySet("--to", []string{test.url}, test.env, nil)
			c.Assert(err, qt.IsNotNil)
		})
	}
}

func TestProviderSelectionCannotRedefineSourceProtocols(t *testing.T) {
	for _, scheme := range []string{"file", "env", "atlas", "docker", "docker+postgres", projectconfig.RemoteSchemaMarkerScheme, projectconfig.CompositeSchemaMarkerScheme, "ptah-external-schema"} {
		t.Run(scheme, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlassource.IsReservedScheme(scheme), qt.IsTrue)
			source, _ := atlassource.Classify(scheme+"://value", map[string]string{scheme: "example"})
			c.Assert(source.Kind, qt.Not(qt.Equals), atlassource.KindDatabase)
		})
	}
	qt.New(t).Assert(atlassource.IsReservedScheme("example"), qt.IsFalse)
}
