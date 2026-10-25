package providerinstall_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/internal/providerinstall"
	"ptah.run/internal/providerrelease"
)

// installChild, as the first argument, makes the test binary run one install
// instead of the tests: <test binary> install-child <root> <API URL>. A test
// that needs installs in separate processes starts the binary it runs in.
const installChild = "install-child"

func TestMain(m *testing.M) {
	if len(os.Args) == 4 && os.Args[1] == installChild {
		os.Exit(runInstallChild(os.Args[2], os.Args[3]))
	}
	os.Exit(m.Run())
}

// childReport is what a child install prints on standard output when it
// succeeds.
type childReport struct {
	Installed  bool   `json:"installed"`
	Executable string `json:"executable"`
	SHA256     string `json:"sha256"`
}

// runInstallChild installs the ydb provider into root from the releases API at
// api, the way the tests in this file do in their own process, and returns the
// exit code of the child.
func runInstallChild(root, api string) int {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	result, err := providerinstall.Installer{Root: root, APIURL: api, Platform: linux, Host: host}.Install(ctx, "ydb", "")
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 1
	}
	report := childReport{Installed: result.Installed, Executable: result.Executable, SHA256: result.SHA256}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

// together answers the requests it serves with body once n of them have
// arrived, or after a minute, so n installs pass that request at the same
// moment instead of in the order they happened to start.
func together(n int, body []byte) http.HandlerFunc {
	var mu sync.Mutex
	arrived := 0
	all := make(chan struct{})
	return func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		arrived++
		if arrived == n {
			close(all)
		}
		mu.Unlock()
		select {
		case <-all:
		case <-time.After(time.Minute):
		case <-r.Context().Done():
		}
		_, _ = w.Write(body)
	}
}

// holdFirst closes started when the first request it serves arrives, and
// answers that request with body only once proceed is closed. It answers every
// later request at once.
func holdFirst(started, proceed chan struct{}, body []byte) http.HandlerFunc {
	var once sync.Once
	return func(w http.ResponseWriter, r *http.Request) {
		first := false
		once.Do(func() {
			first = true
			close(started)
		})
		if first {
			select {
			case <-proceed:
			case <-r.Context().Done():
			}
		}
		_, _ = w.Write(body)
	}
}

// awaitStart waits until started is closed. It fails the test when the install
// that should close it ends first, or does neither within a minute.
func awaitStart(c *qt.C, started chan struct{}, ended chan error) {
	c.Helper()
	select {
	case <-started:
	case err := <-ended:
		c.Fatalf("the install ended before it began its download: %v", err)
	case <-time.After(time.Minute):
		c.Fatal("the install did not begin its download within a minute")
	}
}

// TestInstall_ParallelProcessesAgree is TestInstall_ParallelInstallsAgree with
// each install in a process of its own. The install lock is a file lock, so it
// excludes another process the way it excludes another goroutine: one process
// downloads and publishes the release, and the others find it installed.
func TestInstall_ParallelProcessesAgree(t *testing.T) {
	c := qt.New(t)
	release := provider("0.2.0", 25, 0)
	srv := newServer(c, release)
	const processes = 3
	srv.Handle(providerrelease.ChecksumsName, together(processes, release.Assets[providerrelease.ChecksumsName]))
	root := c.TempDir()
	binary := must.Must(os.Executable())
	children := make([]*exec.Cmd, processes)
	stdout := make([]bytes.Buffer, processes)
	stderr := make([]bytes.Buffer, processes)
	for n := range processes {
		children[n] = exec.CommandContext(c.Context(), binary, installChild, root, srv.APIURL())
		children[n].Stdout, children[n].Stderr = &stdout[n], &stderr[n]
		c.Assert(children[n].Start(), qt.IsNil)
	}
	installed := 0
	for n := range processes {
		c.Assert(children[n].Wait(), qt.IsNil, qt.Commentf("process %d: %s", n, stderr[n].String()))
		var report childReport
		c.Assert(json.Unmarshal(stdout[n].Bytes(), &report), qt.IsNil, qt.Commentf("process %d: %q", n, stdout[n].String()))
		c.Assert(string(must.Must(os.ReadFile(report.Executable))), qt.Equals, "executable 0.2.0")
		installed += map[bool]int{true: 1, false: 0}[report.Installed]
	}
	c.Assert(installed, qt.Equals, 1)
	c.Assert(count(srv, ".tar.gz"), qt.Equals, 1)
	c.Assert(names(providerinstall.ProviderDir(root, providerID)), qt.DeepEquals, []string{".install.lock", "0.2.0", "installed-linux_amd64.json"})
	record, executable, err := providerinstall.ReadRecord(root, providerID, linux)
	c.Assert(err, qt.IsNil)
	c.Assert(executable, qt.Equals, filepath.Join(providerinstall.Dir(root, providerID, "0.2.0", linux), "ptah-plugin-ydb"))
	c.Assert(record.Version, qt.Equals, "0.2.0")
}

// TestInstall_WaitsForTheInstallThatHoldsTheStore holds one install inside its
// archive download, then starts a second install of the release with a
// deadline. The second waits for the lock the first holds, gives up when the
// deadline passes and downloads nothing; the first then completes.
func TestInstall_WaitsForTheInstallThatHoldsTheStore(t *testing.T) {
	c := qt.New(t)
	release := provider("0.2.0", 25, 0)
	srv := newServer(c, release)
	started, proceed := make(chan struct{}), make(chan struct{})
	srv.Handle(archive020, holdFirst(started, proceed, release.Assets[archive020]))
	i := discovering(c, srv)
	first := make(chan error, 1)
	go func() {
		_, err := i.Install(c.Context(), "ydb", "")
		first <- err
	}()
	awaitStart(c, started, first)

	ctx, cancel := context.WithTimeout(c.Context(), time.Second)
	defer cancel()
	result, err := i.Install(ctx, "ydb", "")

	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
	c.Assert(code(c, err), qt.Equals, providerinstall.CodeStoreFailed)
	c.Assert(err, qt.ErrorMatches, `(?s)cannot lock the provider store .*: another install holds .*\.install\.lock: context deadline exceeded`)
	c.Assert(result, qt.DeepEquals, providerinstall.Result{})
	close(proceed)
	c.Assert(<-first, qt.IsNil)
	c.Assert(count(srv, ".tar.gz"), qt.Equals, 1)
	record, _, err := providerinstall.ReadRecord(i.Root, providerID, linux)
	c.Assert(err, qt.IsNil)
	c.Assert(record.Version, qt.Equals, "0.2.0")
}
