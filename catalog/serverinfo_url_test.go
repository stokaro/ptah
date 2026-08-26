package catalog_test

import (
	"encoding/json"
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
)

// credentialURL assembles a database URL carrying a password, without writing
// one out as a literal.
func credentialURL(user, password string) string {
	return (&url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   "db.internal:5432",
		Path:   "/shop",
	}).String()
}

// TestServerInfo_MarshalCarriesNoCredential holds the one rule the struct's tags
// invite someone to break.
//
// Every other field on ServerInfo is tagged for JSON, so marshalling the struct is
// the obvious thing to do with it -- and while URL carried `json:"url"` that
// marshal wrote the database password into whatever the caller did with the
// bytes: a report, a log line, a cached artifact.
func TestServerInfo_MarshalCarriesNoCredential(t *testing.T) {
	c := qt.New(t)

	const password = "hunter2"
	redacted := credentialURL("app", "***")
	info := catalog.ServerInfo{
		Dialect:     "postgres",
		Version:     "17.2",
		Schema:      "public",
		URL:         credentialURL("app", password),
		RedactedURL: redacted,
	}

	encoded, err := json.Marshal(info)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Not(qt.Contains), password)

	var decoded map[string]any
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
	c.Assert(decoded["url"], qt.Equals, redacted)
}

// TestServerInfo_MarshalCarriesNoCredentialWithoutARedactedSibling is the same rule
// for a ServerInfo nobody filled in completely. The exclusion is the tag, not the
// value in RedactedURL, so a hand-assembled ServerInfo cannot leak either.
func TestServerInfo_MarshalCarriesNoCredentialWithoutARedactedSibling(t *testing.T) {
	c := qt.New(t)

	const token = "s3cr3t-token"
	info := catalog.ServerInfo{
		Dialect: "libsql",
		URL:     "libsql://db.turso.io?authToken=" + token,
	}

	encoded, err := json.Marshal(info)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Not(qt.Contains), token)
}
