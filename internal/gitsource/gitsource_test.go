package gitsource_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/gitsource"
)

func TestParseReference_HappyPath(t *testing.T) {
	tests := []struct {
		name, raw, dial, origin string
	}{
		{name: "https", raw: "git+https://git.example.com/acme/app.git", dial: "https://git.example.com/acme/app.git", origin: "https://git.example.com"},
		{name: "https with a port", raw: "git+https://git.example.com:8443/app.git", dial: "https://git.example.com:8443/app.git", origin: "https://git.example.com:8443"},
		{name: "ssh with a user", raw: "git+ssh://git@git.example.com/acme/app.git", dial: "ssh://git@git.example.com/acme/app.git", origin: "ssh://git.example.com"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reference, err := gitsource.ParseReference(test.raw)
			c.Assert(err, qt.IsNil)
			c.Assert(reference.Raw, qt.Equals, test.raw)
			c.Assert(reference.URL.String(), qt.Equals, test.dial)
			c.Assert(reference.Origin(), qt.Equals, test.origin)
			c.Assert(gitsource.IsReference(test.raw), qt.IsTrue)
		})
	}
}

func TestParseReference_FailurePath(t *testing.T) {
	tests := []struct {
		name, raw, says string
	}{
		{name: "another scheme", raw: "git+http://git.example.com/app.git", says: `.*neither git\+https:// nor git\+ssh://`},
		{name: "a credential in an https URL", raw: "git+https://deploy:secret@git.example.com/app.git", // #nosec G101 -- the URL the test requires refused
			says: `.*put the credential in PTAH_GIT_USERNAME and PTAH_GIT_PASSWORD.*`},
		{name: "a password in an ssh URL", raw: "git+ssh://git:secret@git.example.com/app.git", // #nosec G101 -- the URL the test requires refused
			says: `.*SSH authenticates with a key.*`},
		{name: "no host", raw: "git+https:///app.git", says: `.*no host`},
		{name: "no repository", raw: "git+https://git.example.com/", says: `.*no repository path`},
		{name: "a query", raw: "git+https://git.example.com/app.git?ref=main", says: `.*no query or fragment.*`},
		{name: "a fragment", raw: "git+https://git.example.com/app.git#db/schema.hcl", says: `.*no query or fragment.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reference, err := gitsource.ParseReference(test.raw)
			c.Assert(err, qt.ErrorIs, gitsource.ErrReference)
			c.Assert(err, qt.ErrorMatches, test.says)
			c.Assert(reference, qt.DeepEquals, gitsource.Reference{})
		})
	}
}

func TestIsReference_FailurePath(t *testing.T) {
	for _, raw := range []string{"oci://registry.example.com/schema:1", "https://git.example.com/app.git", "git@git.example.com:app.git"} {
		t.Run(raw, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(gitsource.IsReference(raw), qt.IsFalse)
		})
	}
}

// What Fetch refuses before it dials anything: the transport is never used,
// so none is given.
func TestFetch_FailurePath(t *testing.T) {
	reference, err := gitsource.ParseReference("git+https://git.example.com/app.git")
	qt.New(t).Assert(err, qt.IsNil)
	commit := "0123456789abcdef0123456789abcdef01234567"
	tests := []struct {
		name    string
		ref     gitsource.Ref
		path    string
		kind    gitsource.Kind
		is      error
		matches string
	}{
		{name: "no ref", path: "db/schema.hcl", is: gitsource.ErrRef, matches: `.*name a branch, a tag or a commit`},
		{name: "a branch and a tag", ref: gitsource.Ref{Branch: "main", Tag: "v1"}, path: "db/schema.hcl", is: gitsource.ErrRef, matches: `.*not both`},
		{name: "a short commit", ref: gitsource.Ref{Commit: "0123abcd"}, path: "db/schema.hcl", is: gitsource.ErrRef, matches: `.*full 40-character lowercase id`},
		{name: "an uppercase commit", ref: gitsource.Ref{Commit: "0123456789ABCDEF0123456789ABCDEF01234567"}, path: "db/schema.hcl", is: gitsource.ErrRef, matches: `.*full 40-character lowercase id`},
		{name: "a branch that is no ref name", ref: gitsource.Ref{Branch: "main..x"}, path: "db/schema.hcl", is: gitsource.ErrRef, matches: `.*is not a ref name`},
		{name: "no path", ref: gitsource.Ref{Branch: "main"}, is: gitsource.ErrSelection, matches: `.*no path given`},
		{name: "a parent element", ref: gitsource.Ref{Commit: commit}, path: "../schema.hcl", is: gitsource.ErrSelection, matches: `.*element "\.\."`},
		{name: "an absolute path", ref: gitsource.Ref{Branch: "main"}, path: "/db/schema.hcl", is: gitsource.ErrSelection, matches: `.*empty element`},
		{name: "the root as a file", ref: gitsource.Ref{Branch: "main"}, path: ".", kind: gitsource.File, is: gitsource.ErrSelection, matches: `.*element "\."`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := gitsource.Fetch(context.Background(), gitsource.Options{Reference: reference, Ref: test.ref, Path: test.path, Kind: test.kind})
			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(err, qt.ErrorMatches, test.matches)
			c.Assert(result, qt.DeepEquals, gitsource.Result{})
		})
	}
}

func TestDial_FailurePath(t *testing.T) {
	https, err := gitsource.ParseReference("git+https://git.example.com/app.git")
	qt.New(t).Assert(err, qt.IsNil)
	ssh, err := gitsource.ParseReference("git+ssh://git@git.example.com/app.git")
	qt.New(t).Assert(err, qt.IsNil)
	tests := []struct {
		name        string
		reference   gitsource.Reference
		credentials gitsource.Credentials
		trust       gitsource.Trust
		is          error
		matches     string
	}{
		{name: "an SSH key for HTTPS", reference: https, credentials: gitsource.Credentials{SSHKey: []byte("key")},
			is: gitsource.ErrReference, matches: `.*is HTTPS, and PTAH_GIT_SSH_KEY_FILE is set`},
		{name: "known hosts for HTTPS", reference: https, trust: gitsource.Trust{KnownHostsFile: "known_hosts"},
			is: gitsource.ErrReference, matches: `.*known hosts are for SSH`},
		{name: "a password for SSH", reference: ssh, credentials: gitsource.Credentials{Password: "secret"},
			is: gitsource.ErrReference, matches: `.*is SSH, and PTAH_GIT_USERNAME or PTAH_GIT_PASSWORD is set`},
		{name: "a CA file for SSH", reference: ssh, trust: gitsource.Trust{CAFile: "ca.pem"},
			is: gitsource.ErrReference, matches: `.*a CA file is for HTTPS`},
		{name: "SSH without known hosts", reference: ssh, credentials: gitsource.Credentials{SSHKey: []byte("key")},
			is: gitsource.ErrNoKnownHosts, matches: `.*give the server's host keys with --known-hosts-file`},
		{name: "SSH without a key", reference: ssh, trust: gitsource.Trust{KnownHostsFile: "known_hosts"},
			is: gitsource.ErrReference, matches: `.*needs a key; name its file in PTAH_GIT_SSH_KEY_FILE`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			transport, auth, err := gitsource.Dial(test.reference, test.credentials, test.trust)
			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(err, qt.ErrorMatches, test.matches)
			c.Assert(transport, qt.IsNil)
			c.Assert(auth, qt.IsNil)
		})
	}
}

func TestCredentialsFromEnvironment_HappyPath(t *testing.T) {
	c := qt.New(t)
	environment := map[string]string{
		gitsource.UsernameEnv: "deploy", gitsource.PasswordEnv: "token", gitsource.SSHKeyFileEnv: "/keys/deploy",
	}
	credentials, err := gitsource.CredentialsFromEnvironment(
		func(name string) string { return environment[name] },
		func(name string) ([]byte, error) { return []byte("key of " + name), nil },
	)
	c.Assert(err, qt.IsNil)
	c.Assert(credentials, qt.DeepEquals, gitsource.Credentials{Username: "deploy", Password: "token", SSHKey: []byte("key of /keys/deploy")})
}

func TestCredentialsFromEnvironment_FailurePath(t *testing.T) {
	c := qt.New(t)
	missing := errors.New("no such file")
	credentials, err := gitsource.CredentialsFromEnvironment(
		func(name string) string { return map[string]string{gitsource.SSHKeyFileEnv: "/keys/gone"}[name] },
		func(string) ([]byte, error) { return nil, missing },
	)
	c.Assert(err, qt.ErrorIs, missing)
	c.Assert(err, qt.ErrorMatches, `read PTAH_GIT_SSH_KEY_FILE: .*`)
	c.Assert(credentials, qt.DeepEquals, gitsource.Credentials{})
}
