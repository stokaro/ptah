package devdocker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os/exec"
	"strings"
	"time"
)

// ContainerLabel marks every container this package starts. It exists so an
// operator can find and remove a container a killed process left behind:
// `docker rm -f $(docker ps -aq --filter label=ptah-dev)`.
const ContainerLabel = "ptah-dev"

// containerNamePrefix begins every container name. The remainder is random, so
// two invocations running at the same time cannot collide on a name -- which
// they would, deterministically, if the name were derived from the URL.
const containerNamePrefix = "ptah-dev-"

// defaultReadyTimeout bounds how long a freshly started server is waited for.
// A first-run MySQL image initializes its data directory before it listens, so
// this is generous; the wait ends as soon as a connection succeeds.
const defaultReadyTimeout = 2 * time.Minute

// removeTimeout bounds the container removal. It is short because removal runs
// on a context detached from the caller's -- including a canceled one -- and a
// hung docker daemon must not turn cleanup into a hang.
const removeTimeout = 30 * time.Second

// readyPollInterval is how often a starting server is probed.
const readyPollInterval = 250 * time.Millisecond

// Runner is the container runtime this package drives.
//
// It is an interface on the consumer side so the lifecycle -- naming, readiness,
// and above all removal on every exit path -- can be tested without a container
// runtime installed. [DockerCLI] is the only implementation shipped.
type Runner interface {
	// Available reports why containers cannot be started, or nil when they can.
	Available(ctx context.Context) error
	// Start runs image detached under name, publishing containerPort on a
	// loopback port the runtime chooses, and returns the published host:port.
	Start(ctx context.Context, name, image, containerPort string, env []string) (hostPort string, err error)
	// Remove deletes the container, whether or not it is still running. It must
	// succeed when the container is already gone.
	Remove(ctx context.Context, name string) error
}

// ReadyFunc reports whether a provisioned database is accepting connections.
type ReadyFunc func(ctx context.Context, rawURL string) error

// Options configures [Provision]. The zero value drives the `docker` CLI and
// probes readiness with a real connection.
type Options struct {
	// Runner starts and removes containers. Defaults to [DockerCLI].
	Runner Runner
	// Ready probes a provisioned database. Defaults to [Connectable].
	Ready ReadyFunc
	// ReadyTimeout bounds the readiness wait. Defaults to two minutes.
	ReadyTimeout time.Duration
	// ReleaseAttempts bounds how many times the release function returned by
	// [Resolve] retries a removal the runtime refused. Defaults to three.
	ReleaseAttempts int
	// ReleaseRetryDelay spaces those attempts. Defaults to one second.
	ReleaseRetryDelay time.Duration
}

func (o Options) releaseAttempts() int {
	if o.ReleaseAttempts > 0 {
		return o.ReleaseAttempts
	}
	return defaultReleaseAttempts
}

func (o Options) releaseRetryDelay() time.Duration {
	if o.ReleaseRetryDelay > 0 {
		return o.ReleaseRetryDelay
	}
	return defaultReleaseRetryDelay
}

func (o Options) runner() Runner {
	if o.Runner != nil {
		return o.Runner
	}
	return DockerCLI{}
}

func (o Options) ready() ReadyFunc {
	if o.Ready != nil {
		return o.Ready
	}
	return Connectable
}

func (o Options) readyTimeout() time.Duration {
	if o.ReadyTimeout > 0 {
		return o.ReadyTimeout
	}
	return defaultReadyTimeout
}

// Instance is a running dev database. Close removes its container and must be
// called on every path, including error paths; [Resolve] returns a release
// function that does so.
type Instance struct {
	url       string
	container string
	runner    Runner
	closed    bool
}

// URL is the directly connectable URL of the provisioned database.
func (i *Instance) URL() string {
	if i == nil {
		return ""
	}
	return i.url
}

// Container is the name of the container backing the instance. It is reported
// in diagnostics and is what an operator would remove by hand.
func (i *Instance) Container() string {
	if i == nil {
		return ""
	}
	return i.container
}

// Close removes the container. It is safe to call more than once, and a call
// that fails leaves the instance retryable.
//
// The `closed` flag is set only after the runtime confirms removal. Setting it
// first is the obvious spelling and it is wrong in the direction that leaks: a
// `docker rm` that fails transiently or times out would leave the container
// running while every later Close returned success, and the deferred release
// the consumers use is exactly such a later call. A dev database is reset
// destructively and holds a copy of the operator's schema; one left running is
// not a tidiness problem.
//
// The removal runs on a context detached from any the caller may have canceled:
// a canceled command must still clean up after itself, and a removal issued on
// an already-canceled context would be refused before it reached the daemon.
func (i *Instance) Close() error {
	if i == nil || i.closed {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), removeTimeout)
	defer cancel()
	if err := i.runner.Remove(ctx, i.container); err != nil {
		return fmt.Errorf("remove dev database container %s: %w", i.container, err)
	}
	i.closed = true
	return nil
}

// Provision starts a dev database for rawURL and waits until it accepts
// connections. The caller owns the returned instance and must Close it.
//
// Every failure after the container starts removes it before returning, so a
// refused image, a server that never becomes ready, and a canceled context all
// leave nothing behind. That is the whole reason the readiness wait is here and
// not in the caller: a caller that received a container and then failed its own
// wait would have to know to remove it, and one of them would eventually not.
func Provision(ctx context.Context, rawURL string, opts Options) (*Instance, error) {
	spec, err := Parse(rawURL)
	if err != nil {
		return nil, err
	}
	runner := opts.runner()
	if err := runner.Available(ctx); err != nil {
		return nil, err
	}
	name, err := containerName()
	if err != nil {
		return nil, err
	}
	hostPort, err := runner.Start(ctx, name, spec.Image, spec.engine.port, spec.engine.env(spec.Database))
	if err != nil {
		// Start may have created the container before failing to publish or
		// inspect it -- `docker run` succeeding and `docker port` failing leaves
		// a live container behind -- so removal is attempted regardless of where
		// it stopped, and with the same bounded retry every other exit path
		// uses. This is the third branch of the same question, and the answer
		// is spelled once so a fourth cannot get a weaker one.
		releaseInstance(&Instance{container: name, runner: runner}, opts)
		return nil, err
	}
	instance := &Instance{
		url:       spec.URL(hostPort),
		container: name,
		runner:    runner,
	}
	// The wait probes the server, not the operator's parameters; see
	// [Spec.ReadyURL] for the two minutes that distinction is worth.
	if err := waitReady(ctx, spec.ReadyURL(hostPort), opts); err != nil {
		// The same bounded retry the release uses, not a single discarded
		// Close. On this path the caller receives no instance, so nothing else
		// is left holding a handle to the container: a removal refused here
		// leaks it permanently, with no later call able to retry. That is the
		// defect Close was just fixed for, on the branch Close is not reached
		// from.
		releaseInstance(instance, opts)
		return nil, fmt.Errorf("dev database %s did not become ready: %w", spec.Image, err)
	}
	return instance, nil
}

// Resolve returns a directly connectable dev database URL for rawURL, together
// with a release function the caller must always call.
//
// A URL that is not a `docker://` one is returned untouched with a release that
// does nothing, so a consumer can call this unconditionally on whatever value
// its `--dev-url` carries. That is deliberate: this is the one seam every dev
// database consumer shares, and a consumer that had to decide first would be a
// consumer that could forget to.
func Resolve(ctx context.Context, rawURL string, opts Options) (string, func(), error) {
	if !IsURL(rawURL) {
		return rawURL, func() {}, nil
	}
	instance, err := Provision(ctx, rawURL, opts)
	if err != nil {
		return "", func() {}, err
	}
	return instance.URL(), func() { releaseInstance(instance, opts) }, nil
}

// defaultReleaseAttempts bounds how many times a release retries a removal the
// runtime refused. Most consumers call the release exactly once, from a defer,
// so a removal that fails there has no later caller to retry it; the retry
// lives here rather than relying on one existing.
const defaultReleaseAttempts = 3

// defaultReleaseRetryDelay spaces those attempts. A daemon that is briefly busy
// answers the second attempt; one that is down is not worth waiting for while
// the operator's command is trying to exit.
const defaultReleaseRetryDelay = time.Second

// releaseInstance removes the container, retrying a refused removal, and
// reports a container it could not remove by the name an operator would need
// to remove it by hand.
func releaseInstance(instance *Instance, opts Options) {
	attempts := opts.releaseAttempts()
	var err error
	for attempt := range attempts {
		err = instance.Close()
		if err == nil {
			return
		}
		if attempt < attempts-1 {
			time.Sleep(opts.releaseRetryDelay())
		}
	}
	slog.Warn("failed to remove the provisioned dev database container; remove it by hand",
		"container", instance.Container(),
		"attempts", attempts,
		"sweep", "docker rm -f $(docker ps -aq --filter label="+ContainerLabel+")",
		"error", err)
}

// waitReady polls until the database accepts a connection, the deadline passes,
// or the caller's context ends.
//
// Every probe runs on a context carrying the readiness deadline, not on the
// caller's. The deadline check between probes is not enough on its own: the
// default probe opens a real connection, and a container whose port is
// published but whose server never answers leaves it stalled inside a TCP or
// database handshake. The caller's context is normally the command's, which is
// unbounded, so control would never return to the check and the documented
// timeout would not bound anything. Measured against a remote daemon before
// this change, a probe that could never succeed still returned on its own; a
// probe that HANGS is the case the check cannot see.
func waitReady(ctx context.Context, rawURL string, opts Options) error {
	timeout := opts.readyTimeout()
	deadline := time.Now().Add(timeout)
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	ready := opts.ready()
	ticker := time.NewTicker(readyPollInterval)
	defer ticker.Stop()
	var last error
	for {
		if err := ready(waitCtx, rawURL); err == nil {
			return nil
		} else {
			last = err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s: %w", timeout, last)
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

// containerName returns a name no concurrent invocation can also choose.
func containerName() (string, error) {
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return "", fmt.Errorf("generate dev database container name: %w", err)
	}
	return containerNamePrefix + hex.EncodeToString(suffix), nil
}

// DockerCLI drives the `docker` command-line client.
//
// The client is used rather than the Docker HTTP API so that this package adds
// no module dependency and inherits the operator's existing DOCKER_HOST,
// context and credential configuration without restating any of it.
type DockerCLI struct{}

// Available reports an actionable error when no usable container runtime is
// reachable, naming which of the two problems it is: no client installed, or a
// client that cannot reach a daemon.
func (DockerCLI) Available(ctx context.Context) error {
	if _, err := exec.LookPath("docker"); err != nil {
		return errors.New(
			"a docker:// dev database URL needs a container runtime, and no `docker`" +
				" client was found on PATH; install Docker or pass a directly" +
				" connectable dev database URL instead",
		)
	}
	cmd := exec.CommandContext(ctx, "docker", "info", "--format", "{{.ServerVersion}}")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf(
			"a docker:// dev database URL needs a running container runtime, and"+
				" `docker info` failed: %s: %w", strings.TrimSpace(string(out)), err,
		)
	}
	return nil
}

// Start runs the image detached with the container port published on a port the
// daemon picks, then reads back which port that was and pairs it with an
// address this process can actually reach.
//
// The port is not chosen by Ptah. Picking a free port and then binding it is a
// race two concurrent invocations lose sooner or later; letting the daemon
// allocate and asking it afterwards has no window at all.
//
// The bind address depends on where the daemon is, and getting that wrong is
// silent. A container is published on the DAEMON's interfaces, so a daemon on
// another machine that binds loopback is reachable from that machine and from
// nowhere else -- while `docker port` still answers `127.0.0.1:<port>`, which
// this process would resolve against its own loopback. Measured on 2026-08-13
// with `DOCKER_HOST=ssh://remote-dev`: the container came up on the remote host
// as `127.0.0.1:32768->5432/tcp`, `nc -z 127.0.0.1 32768` succeeded there and
// failed here, and the run spent its full two-minute readiness budget before
// reporting `connection refused`.
func (d DockerCLI) Start(ctx context.Context, name, image, containerPort string, env []string) (string, error) {
	endpoint, err := d.endpoint(ctx)
	if err != nil {
		return "", err
	}
	args := []string{
		"run", "--detach", "--rm",
		"--name", name,
		"--label", ContainerLabel + "=1",
		// `<bind>::<container>` — the empty middle field is what asks the
		// daemon to choose the host port. `<bind>:<container>` reads the bind
		// address as the host port and is refused: `invalid hostPort: 0.0.0.0`.
		"--publish", endpoint.bindAddress() + "::" + containerPort,
	}
	for _, entry := range env {
		args = append(args, "--env", entry)
	}
	args = append(args, image)
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("start dev database container from %s: %s: %w", image, trimDockerOutput(out), err)
	}
	port, err := d.publishedPort(ctx, name, containerPort)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(endpoint.connectHost(), port), nil
}

// publishedPort asks the daemon which port it bound, and returns the port
// alone: the address to pair it with is the daemon's, not the one in the
// mapping.
//
// `docker port` prints one mapping per line and the bindings differ by bind
// address -- `127.0.0.1:32768` for a loopback publish, `0.0.0.0:59319` plus an
// `[::]:59319` twin for an all-interfaces one. Reading the port off the last
// colon is what makes all three shapes answer the same thing.
func (DockerCLI) publishedPort(ctx context.Context, name, containerPort string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", "port", name, containerPort+"/tcp").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("read published port of container %s: %s: %w", name, trimDockerOutput(out), err)
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		mapping := strings.TrimSpace(line)
		if _, port, found := strings.Cut(mapping, ":"); found {
			// An IPv6 mapping is `[::]:59319`, so the port is after the LAST
			// colon rather than the first.
			if idx := strings.LastIndex(mapping, ":"); idx >= 0 {
				port = mapping[idx+1:]
			}
			if port != "" {
				return port, nil
			}
		}
	}
	return "", fmt.Errorf("container %s published no port for %s", name, containerPort)
}

// Remove deletes the container and treats an already-absent one as success, so
// a second Close or a container the daemon already reaped is not an error.
func (DockerCLI) Remove(ctx context.Context, name string) error {
	out, err := exec.CommandContext(ctx, "docker", "rm", "--force", "--volumes", name).CombinedOutput()
	if err == nil {
		return nil
	}
	if strings.Contains(string(out), "No such container") {
		return nil
	}
	return fmt.Errorf("%s: %w", trimDockerOutput(out), err)
}

// trimDockerOutput reduces a docker diagnostic to a single line so it can be
// wrapped into an error without reformatting the caller's output.
func trimDockerOutput(out []byte) string {
	fields := strings.Fields(string(out))
	return strings.Join(fields, " ")
}
