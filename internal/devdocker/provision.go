package devdocker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
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

// Close removes the container. It is safe to call more than once.
//
// The removal runs on a context detached from any the caller may have canceled:
// a canceled command must still clean up after itself, and a removal issued on
// an already-canceled context would be refused before it reached the daemon.
func (i *Instance) Close() error {
	if i == nil || i.closed {
		return nil
	}
	i.closed = true
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), removeTimeout)
	defer cancel()
	if err := i.runner.Remove(ctx, i.container); err != nil {
		return fmt.Errorf("remove dev database container %s: %w", i.container, err)
	}
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
		// inspect it, so removal is attempted regardless of where it stopped.
		removeQuietly(runner, name)
		return nil, err
	}
	instance := &Instance{
		url:       spec.engine.url(hostPort, spec.Database),
		container: name,
		runner:    runner,
	}
	if err := waitReady(ctx, instance.url, opts); err != nil {
		_ = instance.Close()
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
	return instance.URL(), func() {
		if err := instance.Close(); err != nil {
			slog.Warn("failed to remove the provisioned dev database container",
				"container", instance.Container(), "error", err)
		}
	}, nil
}

// waitReady polls until the database accepts a connection, the deadline passes,
// or the caller's context ends.
func waitReady(ctx context.Context, rawURL string, opts Options) error {
	deadline := time.Now().Add(opts.readyTimeout())
	ready := opts.ready()
	ticker := time.NewTicker(readyPollInterval)
	defer ticker.Stop()
	var last error
	for {
		if err := ready(ctx, rawURL); err == nil {
			return nil
		} else {
			last = err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s: %w", opts.readyTimeout(), last)
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

// removeQuietly drops a container whose creation state is unknown. Its error is
// discarded on purpose: it is reported on a path that already carries a more
// specific failure, and "no such container" is the expected answer.
func removeQuietly(runner Runner, name string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), removeTimeout)
	defer cancel()
	_ = runner.Remove(ctx, name)
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

// Start runs the image detached with the container port published on a
// loopback port the daemon picks, then reads back which port that was.
//
// The port is not chosen by Ptah. Picking a free port and then binding it is a
// race two concurrent invocations lose sooner or later; letting the daemon
// allocate and asking it afterwards has no window at all.
func (d DockerCLI) Start(ctx context.Context, name, image, containerPort string, env []string) (string, error) {
	args := []string{
		"run", "--detach", "--rm",
		"--name", name,
		"--label", ContainerLabel + "=1",
		"--publish", "127.0.0.1::" + containerPort,
	}
	for _, entry := range env {
		args = append(args, "--env", entry)
	}
	args = append(args, image)
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("start dev database container from %s: %s: %w", image, trimDockerOutput(out), err)
	}
	return d.publishedPort(ctx, name, containerPort)
}

// publishedPort asks the daemon which loopback port it bound.
func (DockerCLI) publishedPort(ctx context.Context, name, containerPort string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", "port", name, containerPort+"/tcp").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("read published port of container %s: %s: %w", name, trimDockerOutput(out), err)
	}
	// `docker port` prints one mapping per line, and an IPv6 binding can be
	// among them; the first IPv4 loopback line is the one that was asked for.
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		mapping := strings.TrimSpace(line)
		if host, ok := strings.CutPrefix(mapping, "127.0.0.1:"); ok {
			return "127.0.0.1:" + host, nil
		}
	}
	return "", fmt.Errorf("container %s published no loopback port for %s", name, containerPort)
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
