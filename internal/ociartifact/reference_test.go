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
		tag        string
		digest     bool
		canonical  string
	}{
		{
			name:       "default latest",
			raw:        "oci://ghcr.io/acme/app",
			registry:   "ghcr.io",
			repository: "acme/app",
			selector:   "latest",
			tag:        "latest",
			canonical:  "oci://ghcr.io/acme/app:latest",
		},
		{
			name:       "tag",
			raw:        "oci://registry.example:5000/team/app:v1.2.3",
			registry:   "registry.example:5000",
			repository: "team/app",
			selector:   "v1.2.3",
			tag:        "v1.2.3",
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
		{
			// The readable pin a promotion pipeline emits. The selector must
			// stay the digest — if it ever became the tag, a display nicety
			// would have silently downgraded resolution to a movable pointer.
			name:       "tag and digest",
			raw:        "oci://ghcr.io/acme/app:stable@" + digest,
			registry:   "ghcr.io",
			repository: "acme/app",
			selector:   digest,
			tag:        "stable",
			digest:     true,
			canonical:  "oci://ghcr.io/acme/app:stable@" + digest,
		},
		{
			name:       "tag and digest with registry port",
			raw:        "oci://registry.example:5000/team/app:release@" + digest,
			registry:   "registry.example:5000",
			repository: "team/app",
			selector:   digest,
			tag:        "release",
			digest:     true,
			canonical:  "oci://registry.example:5000/team/app:release@" + digest,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got, err := ociartifact.ParseRef(tt.raw)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Registry(), qt.Equals, tt.registry)
			c.Assert(got.Repository(), qt.Equals, tt.repository)
			c.Assert(got.Selector(), qt.Equals, tt.selector)
			c.Assert(got.Tag(), qt.Equals, tt.tag)
			c.Assert(got.IsDigest(), qt.Equals, tt.digest)
			c.Assert(got.String(), qt.Equals, tt.canonical)

			// The canonical form must parse back to the same reference, so
			// nothing the author wrote is lost by a round trip through a
			// display string.
			round, err := ociartifact.ParseRef(got.String())
			c.Assert(err, qt.IsNil)
			c.Assert(round.Selector(), qt.Equals, tt.selector)
			c.Assert(round.Tag(), qt.Equals, tt.tag)
			c.Assert(round.String(), qt.Equals, tt.canonical)
		})
	}
}

func TestParseRef_FailurePath(t *testing.T) {
	c := qt.New(t)
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
		// A tag next to a digest is accepted, but the digest still has to be a
		// digest: accepting the shape must not weaken what it selects.
		{name: "tag and malformed digest", raw: "oci://ghcr.io/acme/app:stable@sha256:beef"},
		{name: "tag and non-digest suffix", raw: "oci://ghcr.io/acme/app:stable@notadigest"},
		{name: "tag and unknown digest algorithm", raw: "oci://ghcr.io/acme/app:stable@md5:" + strings.Repeat("a", 32)},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			_, err := ociartifact.ParseRef(tt.raw)
			c.Assert(err, qt.ErrorIs, ociartifact.ErrInvalidReference)
		})
	}
}
