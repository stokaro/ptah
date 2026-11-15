// Package asciihost compares the hosts a credential may be sent to.
//
// A host is compared only when every byte is printable ASCII, and then without
// ASCII case. Unicode case folding is the wrong comparison here:
// strings.ToLower turns U+0130 into "i", so gİthub.com reads as github.com,
// while the transport dials the IDNA form xn--github-qyd.com, another host.
// strings.EqualFold folds the Kelvin sign into "k" the same way. The HTTPS and
// Git source pulls check their references and redirects with it, and the OCI
// credential scope compares registries with it, so one rule decides where a
// credential may go.
package asciihost

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// ErrHost is the refusal of a host with a byte outside printable ASCII.
var ErrHost = errors.New("host has a byte outside printable ASCII")

// Check refuses a host with a byte outside printable ASCII, 0x21 to 0x7E. It
// reads bytes, so a host url.Parse decoded from %C4%B0 is refused as the U+0130
// it stands for.
func Check(host string) error {
	for i := range len(host) {
		if b := host[i]; b < 0x21 || b > 0x7e {
			return fmt.Errorf("%w: %q", ErrHost, host)
		}
	}
	return nil
}

// Equal reports whether a and b spell one host, ignoring ASCII case only. A
// host Check refuses equals nothing, itself included.
func Equal(a, b string) bool {
	// Both are ASCII here, and over ASCII strings.EqualFold folds nothing but
	// the letters A to Z.
	return Check(a) == nil && Check(b) == nil && strings.EqualFold(a, b)
}

// Origin is the scheme, host and port u requests, in lowercase, with the port
// of https and http written out: https://Example.com and
// https://example.com:443 have one origin. It refuses a host Check refuses,
// so two origins it returns are equal only when they name one server.
func Origin(u *url.URL) (string, error) {
	host := u.Hostname()
	if err := Check(host); err != nil {
		return "", err
	}
	scheme := strings.ToLower(u.Scheme)
	port := u.Port()
	if port == "" {
		switch scheme {
		case "https":
			port = "443"
		case "http":
			port = "80"
		}
	}
	return scheme + "://" + net.JoinHostPort(strings.ToLower(host), port), nil
}
