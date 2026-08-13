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
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			endpoint, err := parseDockerEndpoint(tc.raw)
			qt.Assert(t, err, qt.IsNil)
			qt.Check(t, endpoint.remote, qt.Equals, tc.wantRemote)
			// Both halves are asserted on every row. A build that got the bind
			// address right and the connect host wrong would publish where the
			// port is reachable and then dial somewhere else, which is the
			// original defect wearing a different hat.
			qt.Check(t, endpoint.bindAddress(), qt.Equals, tc.wantBindAddress)
			qt.Check(t, endpoint.connectHost(), qt.Equals, tc.wantConnectHost)
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
		name          string
		out           string
		wantHostname  string
		wantProxyJump string
	}{
		{
			// The measured shape for a destination with no rewriting.
			name:         "no rewriting",
			out:          "host remote-dev\nuser buster\nhostname remote-dev\nport 22\naddressfamily any\n",
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
			name:          "reachable only through a jump host",
			out:           "host devbox\nhostname 10.0.0.5\nproxyjump bastion.example\n",
			wantHostname:  "10.0.0.5",
			wantProxyJump: "bastion.example",
		},
		{
			// ssh spells "no jump host" as the literal `none`, which must not
			// be mistaken for one.
			name:         "proxyjump none is not a jump host",
			out:          "host remote-dev\nhostname remote-dev\nproxyjump none\n",
			wantHostname: "remote-dev",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hostname, proxyJump := parseSSHConfig(tc.out)
			qt.Check(t, hostname, qt.Equals, tc.wantHostname)
			qt.Check(t, proxyJump, qt.Equals, tc.wantProxyJump)
		})
	}
}

func TestParseDockerEndpointRefusesAnEndpointNamingNoHost(t *testing.T) {
	_, err := parseDockerEndpoint("tcp://")
	qt.Assert(t, err, qt.IsNotNil)
	qt.Check(t, err.Error(), qt.Contains, "names no host")
}

// TestParseDockerEndpointRefusesAHostThatReadsAsAnSSHOption closes the one way
// operator input reaches an argument list.
//
// The host is handed to `ssh -G` as an argument, and net/url is happy to parse
// a leading dash as a hostname, so `ssh://-oProxyCommand=…` would arrive as an
// option rather than a destination.
func TestParseDockerEndpointRefusesAHostThatReadsAsAnSSHOption(t *testing.T) {
	_, err := parseDockerEndpoint("ssh://-lroot")
	qt.Assert(t, err, qt.IsNotNil)
	qt.Check(t, err.Error(), qt.Contains, "beginning with a dash")
}
