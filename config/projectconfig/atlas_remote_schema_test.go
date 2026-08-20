package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlasregistry"
)

// parseRemoteSchemaSource parses a project file whose env src is a
// remote_schema reference and returns what that reference resolved to.
func parseRemoteSchemaSource(c *qt.C, body string) (string, error) {
	c.Helper()
	cfg, err := projectconfig.ParseAtlas([]byte(`data "remote_schema" "app" {
`+body+`
}

env "local" {
  url = "sqlite://app.db"
  src = data.remote_schema.app.url
}
`), "atlas.hcl", "local")
	if err != nil {
		return "", err
	}
	c.Assert(cfg.SchemaSources, qt.HasLen, 1)
	return cfg.SchemaSources[0], nil
}

// TestParseAtlas_RemoteSchemaMintsAMarker is the project-file half of
// stokaro/ptah#1210.
//
// The block mints a MARKER rather than pulling: a project file is read by every
// verb, so fetching an artifact the run never uses would make an unrelated
// command fail whenever the registry is unreachable.
func TestParseAtlas_RemoteSchemaMintsAMarker(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "name only takes the movable latest tag",
			body: `  name = "app"`,
			want: "ptah-remote-schema://oci://ghcr.io/acme/app:latest",
		},
		{
			name: "tag names a movable tag",
			body: "  name = \"app\"\n  tag  = \"prod\"",
			want: "ptah-remote-schema://oci://ghcr.io/acme/app:prod",
		},
		{
			name: "version names a write-once tag",
			body: "  name = \"app\"\n  version = \"20260806123000\"",
			want: "ptah-remote-schema://oci://ghcr.io/acme/app:20260806123000",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")

			source, err := parseRemoteSchemaSource(c, test.body)

			c.Assert(err, qt.IsNil)
			c.Assert(source, qt.Equals, test.want)
		})
	}
}

// TestParseAtlas_RemoteSchemaMarkerCarriesTheOCISchemeExactlyOnce is the
// regression this change earned in both directions.
//
// The resolved reference already carries oci://, which every Ptah artifact API
// expects. Prepending it produced oci://oci://ghcr.io/... and stripping it
// produced a bare reference the reference parser refuses — and neither failure
// is visible until a pull is attempted.
func TestParseAtlas_RemoteSchemaMarkerCarriesTheOCISchemeExactlyOnce(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")

	source, err := parseRemoteSchemaSource(c, `  name = "app"`)

	c.Assert(err, qt.IsNil)
	c.Assert(source, qt.Not(qt.Contains), "oci://oci://")
	c.Assert(source, qt.Contains, projectconfig.RemoteSchemaMarkerScheme+"://oci://ghcr.io/")
}

// TestParseAtlas_RemoteSchemaNeedsAName keeps the required attribute required.
func TestParseAtlas_RemoteSchemaNeedsAName(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")

	_, err := parseRemoteSchemaSource(c, `  tag = "prod"`)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "name")
}

// TestParseAtlas_UnreferencedRemoteSchemaShapeIsChecked pins the change from
// lazy to checked.
//
// The declaration used to be accepted whatever it said, because resolving it
// was refused anyway — so a project could carry a `remote_schema` block with a
// misspelled attribute and learn nothing until the day it started using it.
func TestParseAtlas_UnreferencedRemoteSchemaShapeIsChecked(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")

	_, err := projectconfig.ParseAtlas([]byte(`data "remote_schema" "unused" {
  name       = "app"
  frobnicate = "x"
}

env "local" {
  url = "sqlite://app.db"
}
`), "atlas.hcl", "local")

	c.Assert(err, qt.IsNotNil)
}

// TestParseAtlas_UnreferencedRemoteSchemaIsNotFetched is the paired case, and
// the reason the check above is a SHAPE check rather than a resolution.
//
// With no namespace configured, resolving would refuse — so reaching a registry
// at all is observable here as an error that must not appear.
func TestParseAtlas_UnreferencedRemoteSchemaIsNotFetched(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasregistry.NamespaceEnvVar, "")

	_, err := projectconfig.ParseAtlas([]byte(`data "remote_schema" "unused" {
  name = "app"
  tag  = "prod"
}

env "local" {
  url = "sqlite://app.db"
}
`), "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
}

// TestParseAtlas_RemoteSchemaAndRemoteDirAgreeOnTheNamespace states the
// property that keeps the two sources usable together: a project addressing a
// migration directory and a schema in the same namespace must not have the two
// disagree about what name, tag and version mean.
func TestParseAtlas_RemoteSchemaAndRemoteDirAgreeOnTheNamespace(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")

	source, err := parseRemoteSchemaSource(c, "  name = \"app\"\n  tag  = \"prod\"")
	c.Assert(err, qt.IsNil)

	// remote_dir pulls at parse time and has no reachable registry here, so the
	// assertion is on the reference it built, which its failure names.
	_, dirErr := projectconfig.ParseAtlas([]byte(`data "remote_dir" "app" {
  name = "app"
  tag  = "prod"
}

env "local" {
  url = "sqlite://app.db"

  migration {
    dir = data.remote_dir.app.url
  }
}
`), "atlas.hcl", "local")

	c.Assert(dirErr, qt.IsNotNil)
	c.Assert(dirErr.Error(), qt.Contains, "ghcr.io/acme/app:prod")
	c.Assert(source, qt.Contains, "ghcr.io/acme/app:prod")
}
