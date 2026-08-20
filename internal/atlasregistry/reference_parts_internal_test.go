package atlasregistry

// White-box testing required: the refusal is a package-internal guard between
// url.Parse and the OCI reference, and no exported call reports which URL
// component made a reference unusable.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// unusedPartRow is one authored reference and the component that makes it
// ambiguous.
type unusedPartRow struct {
	name string
	raw  string
	want string
}

func TestResolveRefusesComponentsItWouldOtherwiseDiscard(t *testing.T) {
	rows := []unusedPartRow{{
		// Without this the run pulls app:latest and executes migrations from
		// an artifact the author did not name.
		name: "fragment",
		raw:  "atlas://app#staging",
		want: "carries a fragment",
	}, {
		name: "user information",
		raw:  "atlas://someone@app",
		want: "carries user information",
	}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(NamespaceEnvVar, "registry.example/acme")

			_, err := Resolve(row.raw)

			c.Assert(err, qt.ErrorMatches, ".*"+row.want+".*")
		})
	}
}

func TestResolveDoesNotEchoCredentialsItRefuses(t *testing.T) {
	c := qt.New(t)
	t.Setenv(NamespaceEnvVar, "registry.example/acme")

	_, err := Resolve("atlas://someone:hunter2@app")

	c.Assert(err, qt.IsNotNil)
	// The refusal reaches a log, so the password must not travel with it.
	c.Assert(err.Error(), qt.Not(qt.Contains), "hunter2")
	c.Assert(err.Error(), qt.Contains, "redacted")
}

func TestResolveAcceptsTheDocumentedForms(t *testing.T) {
	c := qt.New(t)
	t.Setenv(NamespaceEnvVar, "registry.example/acme")

	bare, err := Resolve("atlas://app")
	c.Assert(err, qt.IsNil)
	c.Assert(bare.OCI, qt.Equals, "oci://registry.example/acme/app:latest")

	tagged, err := Resolve("atlas://app?tag=v1")
	c.Assert(err, qt.IsNil)
	c.Assert(tagged.OCI, qt.Equals, "oci://registry.example/acme/app:v1")
}
