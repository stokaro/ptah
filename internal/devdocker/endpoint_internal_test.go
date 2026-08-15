package devdocker

// White-box testing required: endpoint classification is the one decision in
// this package with no observable output. A wrong answer changes no exported
// value -- it changes which machine a container's port is published on, and
// seeing that from outside needs a second machine. The exported surface offers
// nothing to assert against, so the alternative to this file is a rule that can
// only be checked by hand against a remote daemon.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// The rows below were established against a real remote daemon on 2026-08-13:
// `DOCKER_HOST=ssh://remote-dev`, a host reachable over a tailnet, with exit
// statuses read from unpiped invocations.
//
//	the container is on the remote host   ptah-dev-… postgres:18 127.0.0.1:32768->5432/tcp
//	`nc -z 127.0.0.1 32768` on that host  exit 0
//	`nc -z 127.0.0.1 32768` here          exit 1
//	ptah-compat before this change        exit 1 after 2m0s, `connection refused`
//	ptah-compat after                     exit 0
//	the pinned community binary v1.3.0    exit 0, container published 0.0.0.0:59319->5432/tcp
//
// The last row is why a remote daemon is supported rather than refused:
// refusing would exit 1 where that binary exits 0, which is the capability gap
// stokaro/ptah#844 exists to close. The row above it is why the loopback
// binding is kept for a LOCAL daemon: that binary exposes the dev database on
// every interface unconditionally, and matching it there would be copying a
// weaker default for nothing (AGENTS.md compatibility rule (b)).
func TestParseDockerEndpointClassifiesTheDaemonLocation(t *testing.T) {
	tests := []struct {
		name            string
		raw             string
		wantRemote      bool
		wantBindAddress string
		wantConnectHost string
	}{
		{
			// An unset DOCKER_HOST and an unreadable context both arrive here
			// as the empty string, and both mean the local daemon.
			name:            "empty is the local daemon",
			raw:             "",
			wantBindAddress: "127.0.0.1",
			wantConnectHost: "127.0.0.1",
		},
		{
			name:            "unix socket",
			raw:             "unix:///var/run/docker.sock",
			wantBindAddress: "127.0.0.1",
			wantConnectHost: "127.0.0.1",
		},
		{
			name:            "windows named pipe",
			raw:             "npipe:////./pipe/docker_engine",
			wantBindAddress: "127.0.0.1",
			wantConnectHost: "127.0.0.1",
		},
		{
			// A daemon on this machine reached over TCP. Its loopback ports are
			// this process's loopback ports, so nothing needs to change and the
			// tighter binding is kept.
			name:            "tcp on loopback is still local",
			raw:             "tcp://127.0.0.1:2375",
			wantBindAddress: "127.0.0.1",
			wantConnectHost: "127.0.0.1",
		},
		{
			name:            "tcp on localhost is still local",
			raw:             "tcp://localhost:2375",
			wantBindAddress: "127.0.0.1",
			wantConnectHost: "127.0.0.1",
		},
		{
			// The measured case. `ssh://remote-dev` is what an agent pointed at
			// the shared build box actually has set.
			name:            "ssh to another machine",
			raw:             "ssh://remote-dev",
			wantRemote:      true,
			wantBindAddress: "0.0.0.0",
			wantConnectHost: "remote-dev",
		},
		{
			name:            "ssh with a user and port",
			raw:             "ssh://builder@buildbox.example:2222",
			wantRemote:      true,
			wantBindAddress: "0.0.0.0",
			wantConnectHost: "buildbox.example",
		},
		{
			name:            "tcp to another machine",
			raw:             "tcp://100.101.64.121:2375",
			wantRemote:      true,
			wantBindAddress: "0.0.0.0",
			wantConnectHost: "100.101.64.121",
		},
		{
			// The bind family has to match the family the probe will dial.
			// Publishing `0.0.0.0` for an IPv6-only daemon binds IPv4 only, so
			// the container starts and the readiness probe dials an IPv6
			// address with nothing listening on it.
			name:            "ipv6 literal binds the ipv6 wildcard",
			raw:             "ssh://[2001:db8::1]",
			wantRemote:      true,
			wantBindAddress: "::",
			wantConnectHost: "2001:db8::1",
		},
		{
			// A NAME is left on IPv4 even if it happens to have an AAAA record:
			// the daemon resolves it, and a dual-stack host reached over IPv4
			// would be mis-bound by `::`.
			name:            "a name is not assumed to be ipv6",
			raw:             "tcp://buildbox.example:2375",
			wantRemote:      true,
			wantBindAddress: "0.0.0.0",
			wantConnectHost: "buildbox.example",
		},
		{
			// An IPv4-mapped IPv6 literal is an IPv4 address wearing a costume.
			name:            "ipv4-mapped ipv6 is still ipv4",
			raw:             "tcp://[::ffff:192.0.2.7]:2375",
			wantRemote:      true,
			wantBindAddress: "0.0.0.0",
			wantConnectHost: "::ffff:192.0.2.7",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			t.Parallel()
			endpoint, err := parseDockerEndpoint(tc.raw)
			c.Assert(err, qt.IsNil)
			c.Check(endpoint.remote, qt.Equals, tc.wantRemote)
			// Both halves are asserted on every row. A build that got the bind
			// address right and the connect host wrong would publish where the
			// port is reachable and then dial somewhere else, which is the
			// original defect wearing a different hat.
			c.Check(endpoint.bindAddress(), qt.Equals, tc.wantBindAddress)
			c.Check(endpoint.connectHost(), qt.Equals, tc.wantConnectHost)
		})
	}
}

// The fixtures below are real `ssh -G` output shapes, captured on 2026-08-13.
// The command prints one lowercase `key value` pair per line and always answers
// `hostname`, even for a destination no config mentions -- `ssh -G
// ptah-no-such-alias-xyz` answers `hostname ptah-no-such-alias-xyz`.
//
// This matters because the host in a docker `ssh://` endpoint is an SSH
// destination, not necessarily a DNS name, while the published port is dialed
// by a SQL driver over ordinary TCP that knows nothing about ~/.ssh/config. The
// case this was first measured on, `ssh://remote-dev`, worked only because that
// name happens to resolve through the tailnet's DNS to 100.101.64.121 -- `ssh
// -G remote-dev` answers `hostname remote-dev`, so nothing was being resolved
// at all.
func TestParseSSHConfigReadsTheEffectiveDestination(t *testing.T) {
	tests := []struct {
		name         string
		out          string
		wantHostname string
		// wantProxy is the description of the indirection, empty when the
		// destination is reached directly.
		wantProxy string
	}{
		{
			// The measured shape for a destination with no rewriting. Note the
			// two `proxy`-ish keys a plain host really does carry: an
			// implementation matching by substring rather than by exact key
			// would read the first as a proxy command and refuse every ordinary
			// host.
			name: "no rewriting, and the proxy-shaped keys a plain host carries",
			out: "host remote-dev\nuser buster\nhostname remote-dev\nport 22\n" +
				"nohostauthenticationforproxycommand no\nproxyusefdpass no\n",
			wantHostname: "remote-dev",
		},
		{
			// The case the endpoint host cannot be used verbatim for: an alias
			// whose real address is somewhere else.
			name:         "alias with a different hostname",
			out:          "host devbox\nuser deploy\nhostname 10.0.0.5\nport 2222\n",
			wantHostname: "10.0.0.5",
		},
		{
			// No direct TCP route to the published port exists here.
			name:         "reachable only through a jump host",
			out:          "host devbox\nhostname 10.0.0.5\nproxyjump bastion.example\n",
			wantHostname: "10.0.0.5",
			wantProxy:    `the jump host "bastion.example"`,
		},
		{
			// The other spelling of the same fact. Measured: `ssh -G -o
			// 'ProxyCommand=ssh bastion -W %h:%p' <host>` emits a `proxycommand`
			// line and NO `proxyjump` line at all, so a parser that knew only
			// about jump hosts would call this destination directly dialable.
			name:         "reachable only through a proxy command",
			out:          "host devbox\nhostname 10.0.0.5\nproxycommand ssh bastion -W %h:%p\n",
			wantHostname: "10.0.0.5",
			wantProxy:    `the proxy command "ssh bastion -W %h:%p"`,
		},
		{
			// ssh spells "no jump host" as the literal `none`, which must not
			// be mistaken for one.
			name:         "proxyjump none is not a jump host",
			out:          "host remote-dev\nhostname remote-dev\nproxyjump none\n",
			wantHostname: "remote-dev",
		},
		{
			name:         "proxycommand none is not a proxy command",
			out:          "host remote-dev\nhostname remote-dev\nproxycommand none\n",
			wantHostname: "remote-dev",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			t.Parallel()
			hostname, proxy := parseSSHConfig(tc.out)
			c.Check(hostname, qt.Equals, tc.wantHostname)
			c.Check(proxy, qt.Equals, tc.wantProxy)
		})
	}
}

func TestParseDockerEndpointRefusesAnEndpointNamingNoHost(t *testing.T) {
	c := qt.New(t)
	_, err := parseDockerEndpoint("tcp://")
	c.Assert(err, qt.IsNotNil)
	c.Check(err.Error(), qt.Contains, "names no host")
}

// TestParseDockerEndpointRefusesAHostThatReadsAsAnSSHOption closes the one way
// operator input reaches an argument list.
//
// The host is handed to `ssh -G` as an argument, and net/url is happy to parse
// a leading dash as a hostname, so `ssh://-oProxyCommand=…` would arrive as an
// option rather than a destination.
func TestParseDockerEndpointRefusesAHostThatReadsAsAnSSHOption(t *testing.T) {
	c := qt.New(t)
	_, err := parseDockerEndpoint("ssh://-lroot")
	c.Assert(err, qt.IsNotNil)
	c.Check(err.Error(), qt.Contains, "beginning with a dash")
}
