package devdocker

import (
	"context"
	"fmt"
	"log/slog"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// This file answers one question: where does the container runtime actually
// run, and therefore what address can this process reach a published port on.
//
// Getting it wrong is silent, which is why it is a file rather than a line. A
// container's ports are published on the DAEMON's interfaces. When the daemon
// is on another machine, `--publish 127.0.0.1::5432` binds that machine's
// loopback and `docker port` answers `127.0.0.1:<port>` -- a string this
// process would resolve against its own loopback, where nothing is listening.
//
// Measured on 2026-08-13 with `DOCKER_HOST=ssh://remote-dev`, a host reachable
// over a tailnet, exit statuses read from unpiped invocations:
//
//	the container really is remote     ptah-dev-… postgres:18 127.0.0.1:32768->5432/tcp, on remote-dev
//	`nc -z 127.0.0.1 32768` there      exit 0
//	`nc -z 127.0.0.1 32768` here       exit 1
//	`ptah-compat schema inspect …`     exit 1 after 2m0s, `dial tcp 127.0.0.1:32768: connect: connection refused`
//
// The pinned community binary v1.3.0 succeeds on the same setup -- exit 0 --
// and the reason was measured rather than assumed: polling the remote daemon
// while it ran caught its container published as `0.0.0.0:59319->5432/tcp`. It
// binds every interface and connects to the daemon's host. It also genuinely
// honors DOCKER_HOST: pointed at a socket that does not exist it exits 1 with
// `failed to connect to the docker API`, so its success was not a matter of
// ignoring the variable and using the local daemon.
//
// Ptah therefore does what that binary does where it is the only thing that
// works, and keeps the tighter binding everywhere else:
//
//   - local daemon: publish on 127.0.0.1 and connect to 127.0.0.1. Strictly
//     safer than the pinned binary, which exposes the dev database on every
//     interface even for a local daemon, and it costs nothing.
//   - remote daemon: publish on every interface of the daemon host and connect
//     to that host. This is the pinned binary's own exposure decision, taken
//     only when the operator has pointed the runtime at another machine, and
//     it is logged rather than done quietly.
//
// Refusing a remote daemon was the other candidate. It was rejected on the
// measurement: the pinned binary exits 0 there, so refusing would be
// `ptah-compat` exiting 1 where it exits 0 -- the capability gap
// stokaro/ptah#844 exists to close, reintroduced in a new place.

// dockerEndpoint is where the container runtime listens, and what that implies
// for reaching a port it publishes.
type dockerEndpoint struct {
	// host is the daemon's hostname or address, empty for a local socket.
	host string
	// remote reports whether the daemon runs on another machine.
	remote bool
	// ssh reports whether the endpoint is an SSH destination, whose host is
	// resolved through ssh's own configuration rather than through DNS alone.
	ssh bool
}

// bindAddress is the address to publish the container port on.
func (e dockerEndpoint) bindAddress() string {
	if e.remote {
		// Every interface of the daemon host: its loopback is not reachable
		// from here, and the daemon cannot bind an address it does not own.
		return "0.0.0.0"
	}
	return "127.0.0.1"
}

// connectHost is the address this process reaches the published port on.
func (e dockerEndpoint) connectHost() string {
	if e.remote {
		return e.host
	}
	return "127.0.0.1"
}

// endpoint reports where the daemon this CLI will talk to is running.
//
// The precedence is the CLI's own, measured rather than read off a document.
// On the docker client here (29.4.0), with a context `ptah-844-remote` pointing
// at `ssh://remote-dev`, `docker info --format {{.Name}}` answers:
//
//	DOCKER_HOST          DOCKER_CONTEXT      daemon
//	unset                unset               orbstack     (the active context)
//	unset                ptah-844-remote     remote-dev
//	unix://…orbstack…    ptah-844-remote     orbstack     (DOCKER_HOST wins)
//	ssh://remote-dev     orbstack            remote-dev   (DOCKER_HOST wins)
//
// So DOCKER_HOST is read first, and it is read first because it WINS -- not
// merely because it is convenient. DOCKER_CONTEXT needs no separate branch:
// `docker context inspect` honors it, so the fallback below answers for exactly
// the daemon the next `docker run` will use in the two rows where DOCKER_HOST
// is unset. Measured: with `DOCKER_CONTEXT=ptah-844-remote` and no DOCKER_HOST,
// `docker context inspect --format {{.Endpoints.docker.Host}}` answers
// `ssh://remote-dev`, and a full `schema inspect` run against it exits 0 with
// the remote-exposure warning and leaves no container behind.
func (d DockerCLI) endpoint(ctx context.Context) (dockerEndpoint, error) {
	raw := strings.TrimSpace(os.Getenv("DOCKER_HOST"))
	if raw == "" {
		var err error
		raw, err = d.contextEndpoint(ctx)
		if err != nil {
			return dockerEndpoint{}, err
		}
	}
	endpoint, err := parseDockerEndpoint(raw)
	if err != nil {
		return dockerEndpoint{}, err
	}
	endpoint, err = resolveSSHDestination(ctx, endpoint)
	if err != nil {
		return dockerEndpoint{}, err
	}
	if endpoint.remote {
		slog.Warn("the container runtime is on another machine, so the dev database is published on every interface of that host for the life of the command",
			"daemon", raw, "host", endpoint.host)
	}
	return endpoint, nil
}

// contextEndpoint asks the CLI which endpoint the active context selects.
//
// A failure here is not fatal: an older client, or one whose context store is
// unreadable, still runs containers on its default local socket, and refusing
// to provision because a metadata query failed would be worse than assuming the
// common case. The assumption is the SAFE one -- loopback binding, which is
// wrong only by being unreachable rather than by exposing anything.
func (DockerCLI) contextEndpoint(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(
		ctx, "docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}",
	).Output()
	if err != nil {
		slog.Debug("could not read the docker context endpoint; assuming a local daemon",
			"error", err)
		return "", nil
	}
	return strings.TrimSpace(string(out)), nil
}

// parseDockerEndpoint classifies a docker endpoint string.
//
// An empty value is a local daemon: that is what an unset DOCKER_HOST and an
// unreadable context both mean.
func parseDockerEndpoint(raw string) (dockerEndpoint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return dockerEndpoint{}, nil
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return dockerEndpoint{}, fmt.Errorf("parse the docker endpoint %q: %w", raw, err)
	}
	switch parsed.Scheme {
	case "unix", "npipe", "fd", "":
		// A socket or pipe is on this machine by construction.
		return dockerEndpoint{}, nil
	}
	host := parsed.Hostname()
	if host == "" {
		return dockerEndpoint{}, fmt.Errorf("the docker endpoint %q names no host", raw)
	}
	// A host is passed to `ssh -G` as an argument, and `url.Parse` is happy to
	// produce one beginning with a dash -- `ssh://-oProxyCommand=…` parses with
	// that as the hostname. ssh would read it as an option rather than a
	// destination, so it is refused here instead of being quoted somewhere
	// downstream and forgotten.
	if strings.HasPrefix(host, "-") {
		return dockerEndpoint{}, fmt.Errorf("the docker endpoint %q names a host beginning with a dash", raw)
	}
	if isLoopbackHost(host) {
		// `tcp://127.0.0.1:2375` is a daemon on this machine reached over TCP.
		// Its published loopback ports are this process's loopback ports, so
		// nothing about the address needs to change.
		return dockerEndpoint{}, nil
	}
	return dockerEndpoint{host: host, remote: true, ssh: parsed.Scheme == "ssh"}, nil
}

// resolveSSHDestination replaces an `ssh://` endpoint's host with the address
// ssh itself would dial, and refuses one that has no direct route.
//
// The host in a docker `ssh://` endpoint is an SSH destination, not necessarily
// a DNS name. `ssh://devbox` can be an alias whose real HostName is somewhere
// else entirely, and the published port is dialed by a SQL driver over ordinary
// TCP -- which knows nothing about ~/.ssh/config. Returning the alias verbatim
// works only when it happens to resolve. It did in the case this was first
// measured on, `ssh://remote-dev`, because that name resolves through the
// tailnet's DNS to 100.101.64.121; that was luck, not design.
//
// `ssh -G` is the authority, because it is the same resolution ssh performs.
// A destination reachable only through a ProxyJump is refused rather than
// dialed: no direct TCP route to the published port exists, so returning
// anything would be returning an address that cannot work, and a refusal that
// names DOCKER_HOST costs the operator seconds where a dial costs two minutes.
func resolveSSHDestination(ctx context.Context, endpoint dockerEndpoint) (dockerEndpoint, error) {
	if !endpoint.ssh {
		return endpoint, nil
	}
	//nolint:gosec // G204: the host is the operator's own DOCKER_HOST, is refused above if it could read as an option, and -G resolves configuration without connecting
	out, err := exec.CommandContext(ctx, "ssh", "-G", endpoint.host).Output()
	if err != nil {
		// An ssh client that cannot be asked is not a reason to refuse: the
		// literal host is what this code used before, and it works whenever the
		// name resolves. Failing here would turn a working setup into an error.
		slog.Debug("could not resolve the ssh destination; using the endpoint host as written",
			"host", endpoint.host, "error", err)
		return endpoint, nil
	}
	resolved, proxy := parseSSHConfig(string(out))
	if proxy != "" {
		return dockerEndpoint{}, fmt.Errorf(
			"the docker endpoint ssh://%s is reachable only through %s,"+
				" so the dev database port published on it cannot be dialed directly;"+
				" point DOCKER_HOST at a daemon whose host this machine can reach,"+
				" or pass a directly connectable dev database URL",
			endpoint.host, proxy,
		)
	}
	if resolved != "" {
		endpoint.host = resolved
	}
	return endpoint, nil
}

// parseSSHConfig reads the effective hostname and the indirection settings out
// of `ssh -G` output, which is one lowercase `key value` pair per line.
//
// proxy describes the route when the destination is reached through something
// other than a direct connection, and is empty when it is direct. BOTH forms
// count: `ProxyJump` and `ProxyCommand` are different spellings of the same
// fact, and measured on OpenSSH here, `ssh -G -o 'ProxyCommand=ssh bastion -W
// %h:%p' <host>` emits a `proxycommand` line while emitting no `proxyjump` at
// all. A parser that knew only about jump hosts would report that destination
// as directly dialable.
//
// The keys are matched exactly, not by substring. A plain destination's output
// contains `nohostauthenticationforproxycommand no` and `proxyusefdpass no` --
// measured -- and a `strings.Contains(line, "proxycommand")` test would read the
// first of those as a proxy command and refuse every ordinary host.
func parseSSHConfig(out string) (hostname, proxy string) {
	for line := range strings.SplitSeq(out, "\n") {
		key, value, found := strings.Cut(strings.TrimSpace(line), " ")
		if !found {
			continue
		}
		value = strings.TrimSpace(value)
		switch strings.ToLower(key) {
		case "hostname":
			hostname = value
		case "proxyjump":
			// ssh spells "no jump host" as the literal `none`.
			if !strings.EqualFold(value, "none") {
				proxy = "the jump host " + strconv.Quote(value)
			}
		case "proxycommand":
			if !strings.EqualFold(value, "none") {
				proxy = "the proxy command " + strconv.Quote(value)
			}
		}
	}
	return hostname, proxy
}

// isLoopbackHost reports whether host names this machine's loopback.
func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return addr.IsLoopback()
}
