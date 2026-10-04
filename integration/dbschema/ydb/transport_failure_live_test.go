//go:build integration

package ydb_test

import (
	"context"
	"io"
	"net"
	"net/url"
	"path"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"ptah.run/dbschema"
	ydbschema "ptah.run/internal/dbschema/ydb"
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
//
// The cut waits for the server's own sign that it took the statement: the
// index the build creates, which DescribeTable lists while it builds. A cut
// timed by the client's clock measures the machine's speed instead, and a cut
// that lands before the server has the statement is a failure before sending.
// The cleanup cancels a build still running: until it ends, the table is
// locked, and DROP TABLE answers `path ... has been locked by tx`.
func TestYDBMigrator_TransportFailureLeavesTheOutcomeUnknown(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			direct := openYDB(c, line)
			const dir = "ptah_ydb_mig_transport"
			dropDirectory(c, direct, dir, "big")
			c.Cleanup(func() {
				cancelIndexBuild(c, direct, dir+"/big")
				dropDirectory(c, direct, dir, "big")
			})
			fillRows(c, direct, dir+"/big", 1000000)
			indexes := watchIndexes(c, line)
			proxy := startCuttingProxy(c, parsedHost(c, dbtarget.URL(c, line.engine)))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
			indexes.waitForBuild(c, dir+"/big", "big_v", done)
			proxy.cut()
			runErr := <-done

			c.Assert(runErr, qt.IsNotNil)
			c.Assert(indexes.waitForReady(c, dir+"/big", "big_v"), qt.IsTrue)
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

// indexWatch reads a table's indexes and their state through a driver of its
// own, so a test sees what the server holds whatever its other connections
// are doing.
type indexWatch struct {
	driver *ydbsdk.Driver
}

// watchIndexes opens the driver an indexWatch reads through.
func watchIndexes(c *qt.C, line ydbLine) *indexWatch {
	c.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	driver, err := ydbsdk.Open(ctx, dbtarget.DriverDSN(c, line.engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = driver.Close(context.Background()) })
	return &indexWatch{driver: driver}
}

// status reads the state of index on tablePath, a path relative to the database
// root, and reports whether the table has the index.
func (w *indexWatch) status(tablePath, index string) (Ydb_Table.TableIndexDescription_Status, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var status Ydb_Table.TableIndexDescription_Status
	found := false
	_ = w.driver.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
		description, err := session.DescribeTable(ctx, path.Join(w.driver.Name(), tablePath))
		if err != nil {
			return err
		}
		for _, described := range description.Indexes {
			if described.Name == index {
				status, found = described.Status, true
			}
		}
		return nil
	})
	return status, found
}

// waitForBuild waits until the server lists index on tablePath, which it does
// from the moment the build starts, and fails the test when the run ends
// before that or the build does not start within two minutes.
func (w *indexWatch) waitForBuild(c *qt.C, tablePath, index string, run <-chan error) {
	c.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		select {
		case err := <-run:
			c.Fatalf("the migration ended before the server started building %s: %v", index, err)
		default:
		}
		if _, found := w.status(tablePath, index); found {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.Fatalf("the server did not start building %s within two minutes", index)
}

// waitForReady reports whether index on tablePath becomes ready to use within
// five minutes: the build ran to its end.
func (w *indexWatch) waitForReady(c *qt.C, tablePath, index string) bool {
	c.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if status, found := w.status(tablePath, index); found && status == Ydb_Table.TableIndexDescription_STATUS_READY {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// cancelIndexBuild cancels a build still running on tablePath and waits for it to
// end, so the table can be dropped. A build that ended already is left alone.
func cancelIndexBuild(c *qt.C, conn *dbschema.DatabaseConnection, tablePath string) {
	c.Helper()
	canceller, ok := conn.SchemaWriter().(interface {
		CancelRunningBuild(ctx context.Context, tablePath string, wait ydbschema.BuildWait) (ydbschema.BuildOutcome, error)
	})
	c.Assert(ok, qt.IsTrue)
	outcome, err := canceller.CancelRunningBuild(context.Background(), tablePath,
		ydbschema.BuildWait{Settle: 2 * time.Minute, Poll: 100 * time.Millisecond})
	c.Assert(err, qt.IsNil)
	c.Assert(outcome, qt.Not(qt.Equals), ydbschema.BuildUnsettled)
}
