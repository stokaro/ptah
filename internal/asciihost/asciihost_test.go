package asciihost_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/asciihost"
)

func parse(c *qt.C, raw string) *url.URL {
	parsed, err := url.Parse(raw)
	c.Assert(err, qt.IsNil)
	return parsed
}

func TestOrigin_HappyPath(t *testing.T) {
	tests := []struct {
		name, raw, want string
	}{
		{name: "https", raw: "https://github.com/acme/app", want: "https://github.com:443"},
		{name: "ASCII case", raw: "HTTPS://GitHub.COM/acme/app", want: "https://github.com:443"},
		{name: "the default port written out", raw: "https://github.com:443/acme/app", want: "https://github.com:443"},
		{name: "another port", raw: "https://github.com:8443/acme/app", want: "https://github.com:8443"},
		{name: "http", raw: "http://github.com/acme/app", want: "http://github.com:80"},
		{name: "an IPv6 address", raw: "https://[::1]:8443/x", want: "https://[::1]:8443"},
		{name: "an IDNA form", raw: "https://xn--github-qyd.com/x", want: "https://xn--github-qyd.com:443"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := asciihost.Origin(parse(c, test.raw))

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// Each row is a host strings.ToLower folds onto github.com or kube.example.com,
// while the transport dials another: xn--github-qyd.com for U+0130.
func TestOrigin_FailurePath(t *testing.T) {
	tests := []struct {
		name, raw, says string
	}{
		{name: "U+0130", raw: "https://gİthub.com/x", says: `host has a byte outside printable ASCII: "gİthub.com"`},
		{name: "U+0130 percent-escaped", raw: "https://g%C4%B0thub.com/x", says: `host has a byte outside printable ASCII: "gİthub.com"`},
		{name: "U+0130 in capitals", raw: "https://GİTHUB.COM/x", says: `host has a byte outside printable ASCII: "GİTHUB.COM"`},
		{name: "the Kelvin sign", raw: "https://Kube.example.com/x", says: `host has a byte outside printable ASCII: "\x{212A}ube.example.com"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := asciihost.Origin(parse(c, test.raw))

			c.Assert(err, qt.ErrorIs, asciihost.ErrHost)
			c.Assert(err, qt.ErrorMatches, test.says)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestEqual_HappyPath(t *testing.T) {
	tests := []struct {
		name, a, b string
	}{
		{name: "one spelling", a: "registry.example.com", b: "registry.example.com"},
		{name: "ASCII case", a: "Registry.Example.COM", b: "registry.example.com"},
		{name: "a port", a: "registry.example.com:5000", b: "REGISTRY.example.com:5000"},
		{name: "a server address", a: "https://index.docker.io/v1/", b: "https://INDEX.docker.io/v1/"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(asciihost.Equal(test.a, test.b), qt.IsTrue)
		})
	}
}

// A host with a byte outside printable ASCII equals nothing, so no fold of it
// can match the host a credential is scoped to.
func TestEqual_FailurePath(t *testing.T) {
	tests := []struct {
		name, a, b string
	}{
		{name: "another host", a: "registry.example.com", b: "other.example.com"},
		{name: "U+0130", a: "github.com", b: "gİthub.com"},
		{name: "U+0130 against itself", a: "gİthub.com", b: "gİthub.com"},
		{name: "the Kelvin sign", a: "kube.example.com", b: "Kube.example.com"},
		{name: "a long s", a: "registry.example.com", b: "regiſtry.example.com"},
		{name: "a space", a: "registry.example.com", b: "registry.example.com "},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(asciihost.Equal(test.a, test.b), qt.IsFalse)
		})
	}
}

func TestCheck_HappyPath(t *testing.T) {
	c := qt.New(t)

	c.Assert(asciihost.Check("xn--github-qyd.com"), qt.IsNil)
}

func TestCheck_FailurePath(t *testing.T) {
	tests := []struct {
		name, host string
	}{
		{name: "U+0130", host: "gİthub.com"},
		{name: "a control byte", host: "github.com\x00"},
		{name: "DEL", host: "github.com\x7f"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(asciihost.Check(test.host), qt.ErrorIs, asciihost.ErrHost)
		})
	}
}
