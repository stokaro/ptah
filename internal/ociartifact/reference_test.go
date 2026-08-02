package ociartifact_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/ociartifact"
)

func TestParseRef_HappyPath(t *testing.T) {
	c := qt.New(t)
	digest := "sha256:" + strings.Repeat("a", 64)
	tests := []struct {
		name       string
		raw        string
		registry   string
		repository string
		selector   string
		digest     bool
		canonical  string
	}{
		{
			name:       "default latest",
			raw:        "oci://ghcr.io/acme/app",
			registry:   "ghcr.io",
			repository: "acme/app",
			selector:   "latest",
			canonical:  "oci://ghcr.io/acme/app:latest",
		},
		{
			name:       "tag",
			raw:        "oci://registry.example:5000/team/app:v1.2.3",
			registry:   "registry.example:5000",
			repository: "team/app",
			selector:   "v1.2.3",
			canonical:  "oci://registry.example:5000/team/app:v1.2.3",
		},
		{
			name:       "digest",
			raw:        "oci://ghcr.io/acme/app@" + digest,
			registry:   "ghcr.io",
			repository: "acme/app",
			selector:   digest,
			digest:     true,
			canonical:  "oci://ghcr.io/acme/app@" + digest,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got, err := ociartifact.ParseRef(tt.raw)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Registry(), qt.Equals, tt.registry)
			c.Assert(got.Repository(), qt.Equals, tt.repository)
			c.Assert(got.Selector(), qt.Equals, tt.selector)
			c.Assert(got.IsDigest(), qt.Equals, tt.digest)
			c.Assert(got.String(), qt.Equals, tt.canonical)
		})
	}
}

func TestParseRef_FailurePath(t *testing.T) {
	c := qt.New(t)
	digest := "sha256:" + strings.Repeat("a", 64)
	tests := []struct {
		name string
		raw  string
	}{
		{name: "empty", raw: ""},
		{name: "wrong scheme", raw: "https://ghcr.io/acme/app"},
		{name: "missing repository", raw: "oci://ghcr.io"},
		{name: "uppercase repository", raw: "oci://ghcr.io/Acme/app"},
		{name: "query", raw: "oci://ghcr.io/acme/app?tag=stable"},
		{name: "fragment", raw: "oci://ghcr.io/acme/app#stable"},
		{name: "surrounding whitespace", raw: " oci://ghcr.io/acme/app"},
		{name: "embedded credentials", raw: "oci://user:secret@ghcr.io/acme/app"}, //nolint:gosec // Deliberately verifies that inline credentials are rejected.
		{name: "encoded slash", raw: "oci://ghcr.io/acme%2fapp"},
		{name: "encoded backslash", raw: "oci://ghcr.io/acme%5Capp"},
		{name: "backslash", raw: `oci://ghcr.io/acme\app`},
		{name: "NUL byte", raw: "oci://ghcr.io/acme/\x00app"},
		{name: "tag and digest", raw: "oci://ghcr.io/acme/app:stable@" + digest},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			_, err := ociartifact.ParseRef(tt.raw)
			c.Assert(err, qt.ErrorIs, ociartifact.ErrInvalidReference)
		})
	}
}
