package projectconfig_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

// A referenced `data "aws_rds_token"` or `data "gcp_cloudsql_token"` failed
// explicitly because neither provider was implemented. They are ordinary cloud
// IAM authentication with no registry and no Atlas dependency, which is why
// this half of stokaro/ptah#1617 had no scope argument against it.

// staticAWSCredentials pins the credential chain to the environment, so a
// shared config file or instance metadata on the host cannot reach the result.
func staticAWSCredentials(t *testing.T) {
	t.Helper()
	missing := filepath.Join(t.TempDir(), "absent")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", missing)
	t.Setenv("AWS_CONFIG_FILE", missing)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_PROFILE", "")
}

func TestParseAtlas_ResolvesAnAWSRDSTokenIntoTheURL(t *testing.T) {
	c := qt.New(t)
	staticAWSCredentials(t)
	raw := []byte(`
data "aws_rds_token" "token" {
  region   = "us-east-1"
  endpoint = "db.example.us-east-1.rds.amazonaws.com:5432"
  username = "app"
}
env "local" {
  url = "postgres://app:${data.aws_rds_token.token}@db.example.us-east-1.rds.amazonaws.com:5432/app"
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Contains, "Action=connect")
	c.Assert(cfg.DatabaseURL, qt.Contains, "DBUser=app")
	c.Assert(cfg.DatabaseURL, qt.Contains, "X-Amz-Signature=")
	// The token is a password, not a URL: a scheme carried into the userinfo
	// makes the whole DSN unparsable.
	c.Assert(cfg.DatabaseURL, qt.Not(qt.Contains), "app:https://")
}

func TestParseAtlas_AWSRDSTokenRequiresEndpointAndUsername(t *testing.T) {
	c := qt.New(t)
	staticAWSCredentials(t)
	withoutEndpoint := []byte(`
data "aws_rds_token" "token" {
  username = "app"
}
env "local" {
  url = "postgres://app:${data.aws_rds_token.token}@h:5432/app"
}
`)
	withoutUsername := []byte(`
data "aws_rds_token" "token" {
  endpoint = "h:5432"
}
env "local" {
  url = "postgres://app:${data.aws_rds_token.token}@h:5432/app"
}
`)

	_, endpointErr := projectconfig.ParseAtlas(withoutEndpoint, "atlas.hcl", "local")
	_, usernameErr := projectconfig.ParseAtlas(withoutUsername, "atlas.hcl", "local")

	c.Assert(endpointErr, qt.ErrorMatches, `.*requires endpoint.*`)
	c.Assert(usernameErr, qt.ErrorMatches, `.*requires username.*`)
}

// TestParseAtlas_AWSRDSTokenRefusesAnUnknownAttribute matches the pinned
// community binary v1.3.0, which answers a `bogus` argument on this block with
// "Unsupported argument" rather than minting a token that ignores it.
func TestParseAtlas_AWSRDSTokenRefusesAnUnknownAttribute(t *testing.T) {
	c := qt.New(t)
	staticAWSCredentials(t)
	raw := []byte(`
data "aws_rds_token" "token" {
  endpoint = "h:5432"
  username = "app"
  bogus    = "x"
}
env "local" {
  url = "postgres://app:${data.aws_rds_token.token}@h:5432/app"
}
`)

	_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.ErrorMatches, `.*bogus.*`)
}

// TestParseAtlas_AWSRDSTokenAcceptsRegionAndProfile holds the optional half of
// the block's schema, so a project naming either is not refused as unknown.
func TestParseAtlas_AWSRDSTokenAcceptsRegionAndProfile(t *testing.T) {
	c := qt.New(t)
	staticAWSCredentials(t)
	raw := []byte(`
data "aws_rds_token" "token" {
  endpoint = "h:5432"
  username = "app"
  region   = "eu-west-1"
}
env "local" {
  url = "postgres://app:${data.aws_rds_token.token}@h:5432/app"
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Contains, "%2Feu-west-1%2Frds-db%2F")
}

// TestParseAtlas_GCPCloudSQLTokenAcceptsAnUnrecognizedAttribute is a parity
// case, not a preference. The pinned community binary v1.3.0 decodes this
// block loosely: `bogus = "x"` reaches the token exchange rather than being
// refused, so refusing it here would refuse a project that binary accepts.
func TestParseAtlas_GCPCloudSQLTokenAcceptsAnUnrecognizedAttribute(t *testing.T) {
	c := qt.New(t)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "absent.json"))
	t.Setenv("CLOUDSDK_CONFIG", t.TempDir())
	t.Setenv("GCE_METADATA_HOST", "127.0.0.1:1")
	raw := []byte(`
data "gcp_cloudsql_token" "token" {
  bogus = "x"
}
env "local" {
  url = "postgres://app:${data.gcp_cloudsql_token.token}@h:5432/app"
}
`)

	_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	// Without credentials the exchange fails, and that is the point: the
	// failure names the token step, so decoding accepted the attribute.
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "Unsupported argument")
	c.Assert(err.Error(), qt.Contains, "data.gcp_cloudsql_token.token")
}

// TestParseAtlas_RemoteSourcesStillRefuse is the control for the pair this
// change does NOT implement: remote_dir and remote_schema fetch from the Atlas
// registry, and they keep their explicit refusal.
func TestParseAtlas_RemoteSourcesStillRefuse(t *testing.T) {
	c := qt.New(t)
	remoteDir := []byte(`
data "remote_dir" "d" {
  name = "x"
}
env "local" {
  url = "sqlite://x.db?${data.remote_dir.d.url}"
}
`)
	remoteSchema := []byte(`
data "remote_schema" "s" {
  name = "x"
}
env "local" {
  url = "sqlite://x.db?${data.remote_schema.s.url}"
}
`)

	_, dirErr := projectconfig.ParseAtlas(remoteDir, "atlas.hcl", "local")
	_, schemaErr := projectconfig.ParseAtlas(remoteSchema, "atlas.hcl", "local")

	c.Assert(dirErr, qt.IsNotNil)
	c.Assert(schemaErr, qt.IsNotNil)
}
