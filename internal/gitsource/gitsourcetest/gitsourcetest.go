// Package gitsourcetest stands up Git repositories and servers for the tests
// of Ptah's Git pull: a bare repository the git CLI commits to, served over
// smart HTTP behind TLS by git http-backend, and over SSH by git-upload-pack.
// Both servers run the system's git, so what they answer is what Git answers.
package gitsourcetest

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/cgi" // #nosec G504 -- git http-backend is a CGI program; the test server runs it, and the toolchain is far past 1.6.3
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	qt "github.com/frankban/quicktest"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// Repository is a bare repository and a working clone that commits to it.
type Repository struct {
	// Root is the directory the servers export; the repository is Root/Name.
	Root string
	Name string
	work string
}

// NewRepository creates an empty bare repository with a main branch.
func NewRepository(c *qt.C) *Repository {
	root := c.TempDir()
	r := &Repository{Root: root, Name: "app.git", work: filepath.Join(c.TempDir(), "work")}
	git(c, root, "init", "--quiet", "--bare", "--initial-branch=main", r.Name)
	git(c, "", "init", "--quiet", "--initial-branch=main", r.work)
	git(c, r.work, "remote", "add", "origin", r.bare())
	return r
}

func (r *Repository) bare() string { return filepath.Join(r.Root, r.Name) }

// Commit writes files, relative to the repository's root, commits them and
// pushes main, and returns the commit's id.
func (r *Repository) Commit(c *qt.C, files map[string]string, message string) string {
	for name, content := range files {
		full := filepath.Join(r.work, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(full), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(full, []byte(content), 0o600), qt.IsNil)
	}
	git(c, r.work, "add", "--all")
	return r.commitStaged(c, message)
}

// commitStaged commits what is staged, pushes main and returns the commit.
func (r *Repository) commitStaged(c *qt.C, message string) string {
	git(c, r.work, "-c", "user.name=Ptah", "-c", "user.email=ptah@example.com", "commit", "--quiet", "--allow-empty", "--message", message)
	git(c, r.work, "push", "--quiet", "--force", "origin", "HEAD:refs/heads/main")
	return strings.TrimSpace(git(c, r.work, "rev-parse", "HEAD"))
}

// Symlink commits a symbolic link at name pointing at target.
func (r *Repository) Symlink(c *qt.C, name, target string) string {
	full := filepath.Join(r.work, filepath.FromSlash(name))
	c.Assert(os.MkdirAll(filepath.Dir(full), 0o755), qt.IsNil)
	c.Assert(os.Symlink(target, full), qt.IsNil)
	return r.Commit(c, nil, "link "+name)
}

// Submodule records a submodule at name, pointing at commit of another
// repository, without fetching it.
func (r *Repository) Submodule(c *qt.C, name, url, commit string) string {
	modules := fmt.Sprintf("[submodule %q]\n\tpath = %s\n\turl = %s\n", name, name, url)
	c.Assert(os.WriteFile(filepath.Join(r.work, ".gitmodules"), []byte(modules), 0o600), qt.IsNil)
	git(c, r.work, "add", ".gitmodules")
	// The gitlink is staged by hand: there is no checkout at name for git add
	// to find, and adding everything would stage its absence.
	git(c, r.work, "update-index", "--add", "--cacheinfo", "160000,"+commit+","+name)
	return r.commitStaged(c, "submodule "+name)
}

// Tag tags HEAD with a lightweight tag and pushes it.
func (r *Repository) Tag(c *qt.C, name string) {
	r.tag(c, name)
}

// AnnotatedTag tags HEAD with an annotated tag, a tag object of its own, and
// pushes it.
func (r *Repository) AnnotatedTag(c *qt.C, name string) {
	r.tag(c, name, "--annotate", "--message", name)
}

func (r *Repository) tag(c *qt.C, name string, extra ...string) {
	args := append([]string{"-c", "user.name=Ptah", "-c", "user.email=ptah@example.com", "tag", name}, extra...)
	git(c, r.work, args...)
	git(c, r.work, "push", "--quiet", "origin", "refs/tags/"+name)
}

// Reset moves main back to commit and force-pushes it, so the commits after
// it are reachable from nothing, and prunes them from the bare repository.
func (r *Repository) Reset(c *qt.C, commit string) {
	git(c, r.work, "reset", "--quiet", "--hard", commit)
	git(c, r.work, "push", "--quiet", "--force", "origin", "HEAD:refs/heads/main")
	git(c, r.bare(), "reflog", "expire", "--expire=now", "--all")
	git(c, r.bare(), "gc", "--quiet", "--prune=now")
}

// Config sets one configuration key of the bare repository, which is what
// its server reads, such as uploadpack.allowReachableSHA1InWant.
func (r *Repository) Config(c *qt.C, key, value string) {
	git(c, r.bare(), "config", key, value)
}

func git(c *qt.C, dir string, args ...string) string {
	command := exec.Command("git", args...) // #nosec G204 -- fixed git subcommands of a test
	command.Dir = dir
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0")
	out, err := command.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("git %s: %s", strings.Join(args, " "), out))
	return string(out)
}

// HTTPSOptions shape an HTTPS server.
type HTTPSOptions struct {
	// Username and Password, when set, are the only credential it accepts.
	Username, Password string
	// RedirectTo, when set, is an origin every request is redirected to.
	RedirectTo string
}

// HTTPS is a smart HTTP server behind TLS.
type HTTPS struct {
	// Origin is https://host:port.
	Origin string
	// CAFile holds the server's certificate, the one root it chains to.
	CAFile string
}

// Reference is the git+https reference of a repository the server exports.
func (s *HTTPS) Reference(name string) string {
	return "git+" + s.Origin + "/" + name
}

// ServeHTTPS serves root over smart HTTP until the test ends.
func ServeHTTPS(c *qt.C, root string, options HTTPSOptions) *HTTPS {
	gitPath, err := exec.LookPath("git")
	c.Assert(err, qt.IsNil)
	backend := &cgi.Handler{
		Path: gitPath,
		Args: []string{"http-backend"},
		Env:  []string{"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null"},
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if options.RedirectTo != "" {
			// #nosec G710 -- the test redirects to the origin it was given, which is the point
			http.Redirect(w, request, options.RedirectTo+request.URL.RequestURI(), http.StatusFound)
			return
		}
		if options.Username != "" || options.Password != "" {
			username, password, ok := request.BasicAuth()
			if !ok || username != options.Username || password != options.Password {
				w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		backend.ServeHTTP(w, request)
	})
	server := httptest.NewTLSServer(handler)
	c.Cleanup(server.Close)
	caFile := filepath.Join(c.TempDir(), "ca.pem")
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	c.Assert(os.WriteFile(caFile, certificate, 0o600), qt.IsNil)
	return &HTTPS{Origin: server.URL, CAFile: caFile}
}

// SSH is an SSH server that runs git-upload-pack for one client key.
type SSH struct {
	// Address is host:port.
	Address string
	// KnownHostsFile names the server's host key; OtherKnownHostsFile names
	// another key for the same address, which the server does not hold.
	KnownHostsFile      string
	OtherKnownHostsFile string
	// KeyFile is the client's unencrypted private key, in PEM.
	KeyFile string
}

// Reference is the git+ssh reference of a repository the server exports.
func (s *SSH) Reference(name string) string {
	return "git+ssh://git@" + s.Address + "/" + name
}

// ServeSSH serves root over SSH until the test ends.
func ServeSSH(c *qt.C, root string) *SSH {
	hostSigner := newSigner(c)
	clientKey, clientPEM := newKey(c)
	clientPublic, err := ssh.NewPublicKey(clientKey.Public())
	c.Assert(err, qt.IsNil)
	config := &ssh.ServerConfig{
		PublicKeyCallback: func(_ ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if string(key.Marshal()) != string(clientPublic.Marshal()) {
				return nil, fmt.Errorf("unknown key")
			}
			return &ssh.Permissions{}, nil
		},
	}
	config.AddHostKey(hostSigner)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	var connections sync.WaitGroup
	c.Cleanup(func() {
		_ = listener.Close()
		connections.Wait()
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Go(func() { serveSSH(conn, config, root) })
		}
	}()
	address := listener.Addr().String()
	dir := c.TempDir()
	server := &SSH{
		Address:             address,
		KnownHostsFile:      filepath.Join(dir, "known_hosts"),
		OtherKnownHostsFile: filepath.Join(dir, "other_known_hosts"),
		KeyFile:             filepath.Join(dir, "id_ed25519"),
	}
	line := func(key ssh.PublicKey) []byte {
		return []byte(knownhosts.Line([]string{knownhosts.Normalize(address)}, key) + "\n")
	}
	c.Assert(os.WriteFile(server.KnownHostsFile, line(hostSigner.PublicKey()), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(server.OtherKnownHostsFile, line(newSigner(c).PublicKey()), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(server.KeyFile, clientPEM, 0o600), qt.IsNil)
	return server
}

func newKey(c *qt.C) (ed25519.PrivateKey, []byte) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	c.Assert(err, qt.IsNil)
	block, err := ssh.MarshalPrivateKey(key, "")
	c.Assert(err, qt.IsNil)
	return key, pem.EncodeToMemory(block)
}

func newSigner(c *qt.C) ssh.Signer {
	key, _ := newKey(c)
	signer, err := ssh.NewSignerFromKey(key)
	c.Assert(err, qt.IsNil)
	return signer
}

// serveSSH answers one connection: every session may exec git-upload-pack on
// a repository under root, and nothing else.
func serveSSH(conn net.Conn, config *ssh.ServerConfig, root string) {
	defer func() { _ = conn.Close() }()
	server, channels, requests, err := ssh.NewServerConn(conn, config)
	if err != nil {
		return
	}
	defer func() { _ = server.Close() }()
	go ssh.DiscardRequests(requests)
	for newChannel := range channels {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(ssh.UnknownChannelType, "sessions only")
			continue
		}
		channel, channelRequests, err := newChannel.Accept()
		if err != nil {
			return
		}
		go serveSession(channel, channelRequests, root)
	}
}

func serveSession(channel ssh.Channel, requests <-chan *ssh.Request, root string) {
	defer func() { _ = channel.Close() }()
	for request := range requests {
		if request.Type != "exec" {
			_ = request.Reply(false, nil)
			continue
		}
		var execRequest struct{ Command string }
		if ssh.Unmarshal(request.Payload, &execRequest) != nil {
			_ = request.Reply(false, nil)
			return
		}
		repository, ok := strings.CutPrefix(execRequest.Command, "git-upload-pack ")
		if !ok {
			_ = request.Reply(false, nil)
			return
		}
		_ = request.Reply(true, nil)
		status := uploadPack(channel, filepath.Join(root, strings.Trim(repository, "'/")))
		_, _ = channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{status}))
		return
	}
}

func uploadPack(channel ssh.Channel, repository string) uint32 {
	command := exec.Command("git", "upload-pack", repository) // #nosec G204 -- a repository under the test's root
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	command.Stdin, command.Stdout, command.Stderr = channel, channel, channel.Stderr()
	if err := command.Run(); err != nil {
		return 1
	}
	return 0
}
