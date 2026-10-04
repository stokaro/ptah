//go:build integration

package ydb_test

import (
	"context"
	"io"
	"net"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// cuttingProxy forwards TCP connections to a server, and cut closes every
// connection it carries at that moment, as a network failure does. A
// connection opened after a cut is forwarded again.
type cuttingProxy struct {
	listener net.Listener
	target   string

	mu    sync.Mutex
	conns []net.Conn
}

// startCuttingProxy listens on a local port and forwards to target until the
// test ends.
func startCuttingProxy(c *qt.C, target string) *cuttingProxy {
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	proxy := &cuttingProxy{listener: listener, target: target}
	go proxy.serve()
	c.Cleanup(func() {
		_ = listener.Close()
		proxy.cut()
	})
	return proxy
}

func (p *cuttingProxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, client, server)
		p.mu.Unlock()
		go p.pipe(client, server)
		go p.pipe(server, client)
	}
}

func (p *cuttingProxy) pipe(to, from net.Conn) {
	_, _ = io.Copy(to, from)
	_ = to.Close()
	_ = from.Close()
}

// cut closes every connection the proxy carries.
func (p *cuttingProxy) cut() {
	p.mu.Lock()
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

// throughProxy is the line's URL with the proxy as its endpoint. The balancer
// is off, so the driver keeps to that endpoint rather than to the node
// discovery names.
func throughProxy(c *qt.C, line ydbLine, proxy *cuttingProxy) string {
	c.Helper()
	parsed, err := url.Parse(dbtarget.URL(c, line.engine))
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("go_balancer", "disable")
	parsed.RawQuery = query.Encode()
	parsed.Host = proxy.listener.Addr().String()
	return parsed.String()
}

// The connection breaks while ALTER TABLE ... ADD INDEX runs: the client gets
// a transport error, and the server builds the index all the same. The
// statement ran outside a transaction after the migrator marked it in flight,
// so the mark stays: the revision records the outcome as unknown, and a resume
// refuses to run the statement again rather than guess, which here would
// answer that the index exists. A failure the server answered is recorded as
// one; TestYDBMigrator_ResumesWhereAFailedMigrationStopped is that control.
func TestYDBMigrator_TransportFailureLeavesTheOutcomeUnknown(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			direct := openYDB(c, line)
			const dir = "ptah_ydb_mig_transport"
			dropDirectory(c, direct, dir, "big")
			c.Cleanup(func() {
				settleIndexBuild(c, direct, dir, "big_v")
				dropDirectory(c, direct, dir, "big")
			})
			fillRows(c, direct, dir+"/big", 1000000)
			proxy := startCuttingProxy(c, parsedHost(c, dbtarget.URL(c, line.engine)))
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			proxied, err := dbschema.ConnectToDatabase(ctx, throughProxy(c, line, proxy))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(proxied) })
			files := map[string]string{
				"0000000001_index.up.sql":   "ALTER TABLE `" + dir + "/big` ADD INDEX big_v GLOBAL SYNC ON (v);\n",
				"0000000001_index.down.sql": "ALTER TABLE `" + dir + "/big` DROP INDEX big_v;\n",
			}
			m := newMigrator(c, proxied, files, migrator.RevisionTableFormatPtah, dir)

			done := make(chan error, 1)
			go func() { done <- m.MigrateUp(ctx) }()
			waitForInFlightMark(c, direct, files, dir)
			time.Sleep(300 * time.Millisecond)
			proxy.cut()
			runErr := <-done

			c.Assert(runErr, qt.IsNotNil)
			c.Assert(settleIndexBuild(c, direct, dir, "big_v"), qt.IsTrue)
			revision := revisionOf(c, newMigrator(c, direct, files, migrator.RevisionTableFormatPtah, dir))
			c.Assert(revision.StatementOutcomeUnknown(), qt.IsTrue, qt.Commentf("run: %v\nrevision: %+v", runErr, revision))
			resumed := newMigrator(c, direct, files, migrator.RevisionTableFormatPtah, dir)
			c.Assert(resumed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}),
				qt.ErrorMatches, `(?s).*migration 1 cannot resume automatically: the outcome of statement 1 is unknown.*`)
		})
	}
}

// parsedHost is the host and port a URL names.
func parsedHost(c *qt.C, raw string) string {
	c.Helper()
	parsed, err := url.Parse(raw)
	c.Assert(err, qt.IsNil)
	return parsed.Host
}

// waitForInFlightMark waits until the migration's revision records its first
// statement in flight, which the migrator writes right before it sends it.
func waitForInFlightMark(c *qt.C, conn *dbschema.DatabaseConnection, files map[string]string, dir string) {
	c.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		revisions, err := newMigrator(c, conn, files, migrator.RevisionTableFormatPtah, dir).GetRevisions(c.Context())
		if err == nil && len(revisions) == 1 && revisions[0].StatementOutcomeUnknown() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.Fatalf("the migration's statement was not marked in flight within thirty seconds")
}

// settleIndexBuild waits until index exists on the table, and reports
// whether it did within a minute.
func settleIndexBuild(c *qt.C, conn *dbschema.DatabaseConnection, dir, index string) bool {
	c.Helper()
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		live, err := dbschema.ReadSchemaWithSchemasContext(context.Background(), conn, []string{dir})
		if err == nil && slices.Contains(indexNamesOf(live), index) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}
