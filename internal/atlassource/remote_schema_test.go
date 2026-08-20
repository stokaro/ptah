package atlassource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlasregistry"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/schemaartifacttest"
)

// remoteUsersSchema is the desired state the test registry serves.
func remoteUsersSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT"},
		},
	}
}

// envWithSchemaSource builds the project env an `env.src` expansion reads.
func envWithSchemaSource(value string) atlassource.ProjectEnv {
	return atlassource.ProjectEnv{
		Loaded: true,
		Config: projectconfig.Config{SchemaSources: []string{value}},
	}
}

// TestClassify_RemoteSchemaMarkerIsReserved is the compatibility boundary, and
// the reason this feature is reached through a marker at all.
//
// `oci://` is deliberately not a desired-state scheme on this surface: the
// pinned community binary answers it with `unknown driver "oci"` at exit 1, and
// AGENTS.md rule (a) forbids ptah-compat exiting 0 where that binary exits 1.
// The marker keeps the capability reachable through a project file without
// making the registry spelling acceptable on a flag — so a hand-written marker
// has to be refused, or the boundary is decorative (stokaro/ptah#1210).
func TestClassify_RemoteSchemaMarkerIsReserved(t *testing.T) {
	c := qt.New(t)

	_, err := atlassource.Classify(
		projectconfig.RemoteSchemaMarkerScheme + "://ghcr.io/acme/app:prod")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "reserved internal marker scheme")
	c.Assert(err.Error(), qt.Contains, "data.remote_schema.<name>.url")
}

// TestClassify_OCIStaysUnsupportedOnAFlag is the other half of that boundary,
// stated directly rather than inferred from the marker being reserved.
//
// A change that taught the shared classifier `oci://` would make compat accept
// a URL the community binary rejects. This is the assertion that reddens if
// anyone does.
func TestClassify_OCIStaysUnsupportedOnAFlag(t *testing.T) {
	c := qt.New(t)

	_, err := atlassource.Classify("oci://ghcr.io/acme/app:prod")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `unsupported desired-state URL scheme "oci"`)
}

// TestClassifySet_RemoteSchemaMarkerFromEnvResolvesToTheArtifact is the
// acceptance case: the marker a project file minted is recognized on the env
// path and carries the reference to the resolver.
func TestClassifySet_RemoteSchemaMarkerFromEnvResolvesToTheArtifact(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasregistry.PlainHTTP.Name(), "1")
	host := schemaartifacttest.StartSchemaArtifactRegistry(c, "acme/app", "prod", remoteUsersSchema())
	marker := projectconfig.RemoteSchemaMarkerScheme + "://oci://" + host + "/acme/app:prod"

	set, err := atlassource.ClassifySet("--to", []string{"env://src"}, envWithSchemaSource(marker))
	c.Assert(err, qt.IsNil)
	c.Assert(set.Kind, qt.Equals, atlassource.KindRemoteSchema)

	state, err := set.Resolve(c.TB.Context(), atlassource.ResolveOptions{Dialect: "sqlite"})

	c.Assert(err, qt.IsNil)
	c.Assert(state.Kind, qt.Equals, atlassource.KindRemoteSchema)
	c.Assert(state.Schema, qt.IsNotNil)
	// The schema is the artifact's own IR, not a re-parse of rendered text.
	c.Assert(state.Schema.Tables, qt.HasLen, 1)
	c.Assert(state.Schema.Tables[0].Name, qt.Equals, "users")
	names := make([]string, 0, len(state.Schema.Fields))
	for _, field := range state.Schema.Fields {
		names = append(names, field.Name)
	}
	c.Assert(names, qt.Contains, "email")
}

// TestClassifySet_RemoteSchemaMissingArtifactNamesTheReference keeps a registry
// miss reporting what was asked for rather than a bare transport failure.
func TestClassifySet_RemoteSchemaMissingArtifactNamesTheReference(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasregistry.PlainHTTP.Name(), "1")
	host := schemaartifacttest.StartSchemaArtifactRegistry(c, "acme/app", "prod", remoteUsersSchema())
	marker := projectconfig.RemoteSchemaMarkerScheme + "://oci://" + host + "/acme/app:absent"

	set, err := atlassource.ClassifySet("--to", []string{"env://src"}, envWithSchemaSource(marker))
	c.Assert(err, qt.IsNil)
	_, err = set.Resolve(c.TB.Context(), atlassource.ResolveOptions{Dialect: "sqlite"})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "acme/app:absent")
}

// TestClassifyEnv_RemoteSchemaMarkerWithoutAReferenceIsRefused keeps an empty
// marker from reaching a registry client as a blank reference.
func TestClassifyEnv_RemoteSchemaMarkerWithoutAReferenceIsRefused(t *testing.T) {
	c := qt.New(t)
	marker := projectconfig.RemoteSchemaMarkerScheme + "://"

	_, err := atlassource.ClassifySet("--to", []string{"env://src"}, envWithSchemaSource(marker))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "carries no artifact reference")
}
