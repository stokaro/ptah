package projectconfig_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
)

// azureIdentityEndpoint serves the App Service managed identity protocol, the
// one source in the Azure default credential chain an offline test can answer
// over plain HTTP, and hides every source the chain tries before or after it.
// status is the HTTP status the endpoint answers with; a token is sent only
// with 200. It returns the resources the endpoint was asked for.
//
// The managed identity token cache is shared by the whole process, so each
// token expires within the renewal window and the next request still reaches
// an endpoint.
func azureIdentityEndpoint(c *qt.C, status int, token string) *[]string {
	resources := new([]string)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*resources = append(*resources, r.URL.Query().Get("resource"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = fmt.Fprintf(w, `{"access_token":%q,"expires_on":"%d","token_type":"Bearer"}`,
			token, time.Now().Add(time.Minute).Unix())
	}))
	c.Cleanup(server.Close)
	c.Setenv("PATH", c.TempDir())
	for _, name := range []string{
		"AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_CLIENT_SECRET",
		"AZURE_CLIENT_CERTIFICATE_PATH", "AZURE_USERNAME", "AZURE_PASSWORD",
		"AZURE_FEDERATED_TOKEN_FILE", "AZURE_TOKEN_CREDENTIALS",
		"IDENTITY_SERVER_THUMBPRINT", "MSI_ENDPOINT", "MSI_SECRET",
	} {
		// Setenv first so the cleanup restores the variable; the chain reads
		// some of these with LookupEnv, where an empty value is not absence.
		c.Setenv(name, "")
		c.Assert(os.Unsetenv(name), qt.IsNil)
	}
	c.Setenv("IDENTITY_ENDPOINT", server.URL)
	c.Setenv("IDENTITY_HEADER", "header")
	return resources
}

func TestParseAtlas_ResolvesAnAzureDBTokenIntoTheURL(t *testing.T) {
	cases := []struct {
		name     string
		block    string
		resource string
	}{
		{
			name:     "the default scope",
			block:    `data "azure_db_token" "db" {}`,
			resource: "https://ossrdbms-aad.database.windows.net",
		},
		{
			name: "a tenant and a scope",
			block: `data "azure_db_token" "db" {
  tenant_id = "00000000-0000-0000-0000-000000000000"
  scope     = "https://token.postgres.cosmos.azure.com/.default"
}`,
			resource: "https://token.postgres.cosmos.azure.com",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			resources := azureIdentityEndpoint(c, http.StatusOK, "entra-token")
			raw := []byte(tc.block + `
env "azure" {
  url = urluserinfo("postgres://srv.postgres.database.azure.com:5432/app?sslmode=require", "app@contoso.onmicrosoft.com", data.azure_db_token.db)
}
`)

			cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "azure")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.DatabaseURL, qt.Equals,
				"postgres://app%40contoso.onmicrosoft.com:entra-token@srv.postgres.database.azure.com:5432/app?sslmode=require")
			c.Assert(*resources, qt.DeepEquals, []string{tc.resource})
		})
	}
}

// TestParseAtlas_AzureDBTokenStaysLazy holds that a block no selected value
// references is never minted, so a project that declares one for another env
// loads on a host with no Azure identity.
func TestParseAtlas_AzureDBTokenStaysLazy(t *testing.T) {
	c := qt.New(t)
	resources := azureIdentityEndpoint(c, http.StatusOK, "entra-token")
	raw := []byte(`
data "azure_db_token" "db" {}
env "azure" {
  url = urluserinfo("postgres://srv.postgres.database.azure.com/app", "app@contoso.onmicrosoft.com", data.azure_db_token.db)
}
env "local" {
  url = "sqlite://local.db"
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://local.db")
	c.Assert(*resources, qt.HasLen, 0)
}

func TestParseAtlas_AzureDBTokenFailurePath(t *testing.T) {
	t.Run("an endpoint that refuses names the block", func(t *testing.T) {
		c := qt.New(t)
		azureIdentityEndpoint(c, http.StatusBadRequest, "")
		raw := []byte(`
data "azure_db_token" "db" {}
env "azure" {
  url = urluserinfo("postgres://srv.postgres.database.azure.com/app", "app@contoso.onmicrosoft.com", data.azure_db_token.db)
}
`)

		cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "azure")

		c.Assert(err, qt.ErrorMatches, `(?s).*data\.azure_db_token\.db: minting token: azure db token: getting token for https://ossrdbms-aad\.database\.windows\.net/\.default: .*`)
		c.Assert(cfg.DatabaseURL, qt.Equals, "")
	})

	t.Run("an unknown attribute is refused before anything is minted", func(t *testing.T) {
		c := qt.New(t)
		resources := azureIdentityEndpoint(c, http.StatusOK, "entra-token")
		raw := []byte(`
data "azure_db_token" "db" {
  tenant = "00000000-0000-0000-0000-000000000000"
}
env "local" {
  url = "sqlite://local.db"
}
`)

		cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

		c.Assert(err, qt.ErrorMatches, `(?s).*tenant.*`)
		c.Assert(cfg.DatabaseURL, qt.Equals, "")
		c.Assert(*resources, qt.HasLen, 0)
	})

	t.Run("the strict policy refuses a declared block", func(t *testing.T) {
		c := qt.New(t)
		resources := azureIdentityEndpoint(c, http.StatusOK, "entra-token")
		raw := []byte(`
data "azure_db_token" "db" {}
env "local" {
  url = "sqlite://local.db"
}
`)

		cfg, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
			EnvName:            "local",
			RejectAzureDBToken: true,
		})

		c.Assert(err, qt.ErrorMatches, `missing data source handler for "azure_db_token"`)
		c.Assert(cfg.DatabaseURL, qt.Equals, "")
		c.Assert(*resources, qt.HasLen, 0)
	})
}

// TestParseAtlas_AzureDBTokenStaysOutOfADiagnosticInItsEscapedForm feeds a
// token holding characters a URL reserves through each URL function into a
// value that fails. Each failure quotes the token as that function escaped
// it, which literal scrubbing of the raw token would not recognize.
func TestParseAtlas_AzureDBTokenStaysOutOfADiagnosticInItsEscapedForm(t *testing.T) {
	cases := []struct {
		name       string
		expression string
		escaped    string
	}{
		{name: "urlescape", expression: `urlescape(data.azure_db_token.db)`, escaped: "a%2Fb+c%25d%2Be%3Df"},
		{name: "urlsetpath", expression: `urlsetpath("x://h", data.azure_db_token.db)`, escaped: "a/b%20c%25d+e=f"},
		{name: "urluserinfo", expression: `urluserinfo("x://h", "u", data.azure_db_token.db)`, escaped: "a%2Fb%20c%25d+e=f"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			azureIdentityEndpoint(c, http.StatusOK, "a/b c%d+e=f")
			raw := []byte(`
data "azure_db_token" "db" {}
env "azure" {
  url = "sqlite://local.db"
  dev = file(` + tc.expression + `)
}
`)

			cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "azure")

			c.Assert(err, qt.ErrorMatches, `(?s).*\(sensitive value\).*`)
			c.Assert(err.Error(), qt.Not(qt.Contains), tc.escaped)
			c.Assert(err.Error(), qt.Not(qt.Contains), "a/b c%d+e=f")
			c.Assert(cfg.DatabaseURL, qt.Equals, "")
		})
	}
}
