// Package devdb serves the fixture's dev databases on SQLite with the
// services contract 25.1 adds: their cleanup, the replay guard, realms in a
// database the operator names, and a statement timeout on Execute. Its
// provider also declares the operator statements of contract 25.4, which
// package statements serves. It lives apart from package backend, which keeps
// to contract 25.0.
package devdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// Provider provisions, cleans and guards fixture dev databases. A provisioned
// database is a SQLite file in Dir, named for the process and the lease, and a
// realm is a file beside the operator's database; the release removes either.
type Provider struct {
	backend.TransactionalProvider
	// Dir holds the databases the provider provisions.
	Dir    string
	mu     sync.Mutex
	next   int
	leases map[string]string
}

// Features are the services and behaviors this package adds: the ones the
// provider declares, and the ones its engine declares. A statement timeout is
// the provider's, and dev realms are the engine's alone.
func Features() (providerFeatures, engineFeatures []string) {
	services := []string{provider.TransactionsFeature, provider.HistoryFeature, provider.LocksFeature, provider.DevDatabaseFeature,
		provider.DevCleanupFeature, provider.ReplayGuardFeature, provider.StatementsFeature}
	return append(slices.Clone(services), provider.StatementTimeoutFeature), append(services, provider.DevRealmsFeature)
}

// Description is the fixture's description with the dev database services
// and two declared release lines.
func Description() *protocol.Description {
	description := backend.Description()
	description.SemanticIdentity = "fixture-devdb-2"
	providerFeatures, engineFeatures := Features()
	description.Features = append(description.Features, providerFeatures...)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, engineFeatures...)
	engine.ReleaseLines = releaseLines()
	return description
}

// BareDescription is Description with an engine that declares none of the dev
// database services and no operator statements, which the provider declares: a
// provider of several engines may clean, guard and run statements on one and
// not on another.
func BareDescription() *protocol.Description {
	description := Description()
	description.SemanticIdentity += "-bare"
	engine := description.Engines[0]
	engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool {
		return feature == provider.DevCleanupFeature || feature == provider.ReplayGuardFeature || feature == provider.DevRealmsFeature ||
			feature == provider.StatementsFeature
	})
	return description
}

func releaseLines() []*protocol.ReleaseLine {
	return []*protocol.ReleaseLine{
		{Line: "2.0", Label: "Fixture 2", Support: protocol.ReleaseLine_CERTIFIED, Image: "example.org/fixture:2.0"},
		{Line: "1.0", Support: protocol.ReleaseLine_BEST_EFFORT, Unprobed: "the vendor ended support", Image: "example.org/fixture:1.0"},
	}
}

// Inspect reports the revision storage the provider keeps as reserved, as the
// contract asks of a provider's own history, so a run that reads a replayed
// database back sees the schema the migrations built and nothing else.
func (p *Provider) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	snapshot, err := p.TransactionalProvider.Inspect(ctx, request)
	if err != nil {
		return nil, err
	}
	snapshot.Tables = slices.DeleteFunc(snapshot.Tables, func(table *protocol.Table) bool {
		if !backend.HistoryStorage(table) {
			return false
		}
		snapshot.ReservedObjects = append(snapshot.ReservedObjects, &protocol.ReservedObject{Object: proto.CloneOf(table.GetIdentity()),
			NormalizedObject: proto.CloneOf(table.GetNormalizedIdentity()), Purpose: protocol.ReservedObject_HISTORY, ConflictKinds: []string{"table"}})
		return true
	})
	return snapshot, nil
}

// Open accepts a fixture address with the host localhost, which a realm
// address keeps from the operator's, as well as one with no host.
func (p *Provider) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	if address, err := url.Parse(request.GetAddress()); err == nil && address.Host == "localhost" {
		address.Host = ""
		request = &protocol.OpenRequest{Engine: request.GetEngine(), Address: address.String()}
	}
	return p.TransactionalProvider.Open(ctx, request)
}

// ProvisionDevDatabase creates a dev database file for a docker URL in either
// form, or a realm file beside the database an operator's address names. The
// spec "docker://fixture/none" is refused as unsupported and
// "docker://fixture/offline" as unavailable, and so is a spec in neither docker
// form, such as an address sent where a realm was meant.
func (p *Provider) ProvisionDevDatabase(_ context.Context, request *protocol.ProvisionDevRequest) (*protocol.DevDatabase, error) {
	spec := request.GetSpec()
	switch {
	case spec != "" && spec != "docker://fixture" && !strings.HasPrefix(spec, "docker://fixture/") && !strings.HasPrefix(spec, "docker+fixture://"):
		return &protocol.DevDatabase{Refusal: &protocol.DevDatabaseRefusal{
			Kind: protocol.DevDatabaseRefusal_UNSUPPORTED, Reason: "the fixture provisions a database only from a docker spec"}}, nil
	case strings.HasSuffix(spec, "none"):
		return &protocol.DevDatabase{Refusal: &protocol.DevDatabaseRefusal{
			Kind: protocol.DevDatabaseRefusal_UNSUPPORTED, Reason: "the fixture provisions no database for " + request.GetSpec()}}, nil
	case strings.HasSuffix(request.GetSpec(), "offline"):
		return &protocol.DevDatabase{Refusal: &protocol.DevDatabaseRefusal{
			Kind: protocol.DevDatabaseRefusal_UNAVAILABLE, Reason: "the fixture runtime is not running"}}, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.leases == nil {
		p.leases = make(map[string]string)
	}
	p.next++
	lease := fmt.Sprintf("lease-%d", p.next)
	if address := request.GetAddress(); address != "" {
		operator, err := url.Parse(address)
		if err != nil || operator.Scheme != "fixture" {
			return nil, fmt.Errorf("the fixture creates a realm only in a fixture database")
		}
		realm := *operator
		realm.Path += "." + lease
		realm.RawQuery = ""
		p.leases[lease] = filePath(realm.Path)
		return &protocol.DevDatabase{Lease: lease, Url: realm.String()}, nil
	}
	path := filepath.Join(p.Dir, fmt.Sprintf("fixture-%d-%s.db", os.Getpid(), lease))
	p.leases[lease] = path
	return &protocol.DevDatabase{Lease: lease, Url: (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/")}).String()}, nil
}

// ReleaseDevDatabase removes the file a lease names. A lease already gone
// succeeds.
func (p *Provider) ReleaseDevDatabase(_ context.Context, request *protocol.ReleaseDevRequest) (*protocol.Empty, error) {
	p.mu.Lock()
	path, found := p.leases[request.GetLease()]
	delete(p.leases, request.GetLease())
	p.mu.Unlock()
	if found {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("remove the fixture dev database")
		}
	}
	return &protocol.Empty{}, nil
}

// storage reports a table the fixture keeps for itself, which a cleanup
// leaves alone.
func storage(name string) bool {
	return strings.HasPrefix(name, "sqlite_") || strings.HasPrefix(name, "ptah_fixture_")
}

// ListDevObjects lists every table, view and trigger of the database. The
// fixture removes tables and refuses a view, for which it has no statement.
func (p *Provider) ListDevObjects(ctx context.Context, request *protocol.ListDevObjectsRequest) (*protocol.DevObjects, error) {
	if err := mainOnly(request.GetSchemas()); err != nil {
		return nil, err
	}
	result := &protocol.DevObjects{Complete: true}
	err := p.WithSession(ctx, request.GetSessionId(), "", func(access backend.Access) error {
		var err error
		result.Objects, err = objects(ctx, access.Querier)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func mainOnly(schemas []string) error {
	for _, schema := range schemas {
		if !strings.EqualFold(schema, "main") {
			return fmt.Errorf("the fixture has no schema other than main")
		}
	}
	return nil
}

func objects(ctx context.Context, queries backend.Querier) ([]*protocol.DevObject, error) {
	rows, err := queries.QueryContext(ctx, "SELECT type, name FROM sqlite_schema WHERE type IN ('table', 'view', 'trigger') ORDER BY type, name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var listed []*protocol.DevObject
	for rows.Next() {
		var kind, name string
		if err := rows.Scan(&kind, &name); err != nil {
			return nil, err
		}
		if storage(name) {
			continue
		}
		object := &protocol.DevObject{Object: &protocol.Reference{Kind: kind, Name: name}}
		if kind != "table" {
			object.Refusal = "the fixture has no statement that drops a " + kind
		}
		listed = append(listed, object)
	}
	return listed, rows.Err()
}

// CleanDevDatabase drops every table in one transaction, or removes nothing
// and names what stopped it. The fault lost-cleanup drops the tables and then
// loses the reply.
func (p *Provider) CleanDevDatabase(ctx context.Context, request *protocol.CleanDevRequest) (*protocol.DevCleanup, error) {
	if err := mainOnly(request.GetSchemas()); err != nil {
		return nil, err
	}
	result := &protocol.DevCleanup{RequestId: request.GetRequestId(), Outcome: protocol.DevCleanup_CLEANED}
	lost := false
	err := p.WithSession(ctx, request.GetSessionId(), "", func(access backend.Access) error {
		lost = access.Fault == "lost-cleanup"
		found, err := objects(ctx, access.Querier)
		if err != nil {
			return err
		}
		for _, object := range found {
			if object.GetRefusal() != "" {
				result.Blocking = append(result.Blocking, object)
			}
		}
		if len(result.Blocking) > 0 {
			result.Outcome = protocol.DevCleanup_REFUSED
			return nil
		}
		return dropTables(ctx, access.DB, found)
	})
	if err != nil {
		return nil, err
	}
	if lost {
		return nil, fmt.Errorf("fixture lost the cleanup reply after it dropped the tables")
	}
	return result, nil
}

func dropTables(ctx context.Context, db *sql.DB, found []*protocol.DevObject) error {
	if db == nil {
		return fmt.Errorf("a cleanup runs outside the session's transaction")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, object := range found {
		if _, err := tx.ExecContext(ctx, `DROP TABLE "`+strings.ReplaceAll(object.GetObject().GetName(), `"`, `""`)+`"`); err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}
	return tx.Commit()
}

// GuardReplay refuses a statement that reaches past the database file, or
// that changes the replay session's own settings, and accepts the rest.
func (*Provider) GuardReplay(_ context.Context, request *protocol.GuardReplayRequest) (*protocol.GuardedReplay, error) {
	result := &protocol.GuardedReplay{}
	for _, statement := range request.GetStatements() {
		verdict := &protocol.ReplayVerdict{}
		fields := strings.Fields(strings.ToUpper(statement))
		switch {
		case len(fields) > 0 && (fields[0] == "ATTACH" || fields[0] == "DETACH"):
			verdict.Refused, verdict.Operation = true, fields[0]+" reaches another database file"
		case len(fields) > 1 && fields[0] == "VACUUM" && fields[1] == "INTO":
			verdict.Refused, verdict.Operation = true, "VACUUM INTO writes another database file"
		case len(fields) > 0 && fields[0] == "PRAGMA":
			verdict.Refused, verdict.Operation = true, "PRAGMA changes the replay session's settings"
		}
		result.Verdicts = append(result.Verdicts, verdict)
	}
	return result, nil
}

// slowQuery runs until its context ends: the fault slow-unit runs it before
// each unit, as a statement that outlasts any timeout would.
const slowQuery = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c"

// Execute bounds each unit by the request's statement timeout, when it has
// one, and reports a unit the timeout stopped. SQLite interrupts the
// statement and rolls back what it did, so the prefix before it is what
// applied.
func (p *Provider) Execute(ctx context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	limit := time.Duration(request.GetStatementTimeoutNanoseconds())
	if limit <= 0 {
		return p.TransactionalProvider.Execute(ctx, request)
	}
	result := &protocol.Execution{Outcome: protocol.Execution_APPLIED}
	err := p.WithSession(ctx, request.GetSessionId(), request.GetTransactionId(), func(access backend.Access) error {
		for _, unit := range request.GetUnits() {
			text, ok := unit.GetBody().(*protocol.Unit_Sql)
			if !ok {
				return fmt.Errorf("fixture accepts SQL units only")
			}
			bounded, cancel := context.WithTimeout(ctx, limit)
			var err error
			if access.Fault == "slow-unit" {
				var count int64
				err = access.Querier.QueryRowContext(bounded, slowQuery).Scan(&count)
			}
			if err == nil {
				_, err = access.Querier.ExecContext(bounded, text.Sql)
			}
			timedOut := errors.Is(bounded.Err(), context.DeadlineExceeded) && ctx.Err() == nil
			cancel()
			switch {
			case err == nil:
				result.AppliedCount++
			case timedOut:
				result.Outcome, result.StatementTimedOut = protocol.Execution_PARTIAL, true
				if result.AppliedCount == 0 {
					result.Outcome = protocol.Execution_NOT_APPLIED
				}
				return nil
			default:
				result.Outcome = protocol.Execution_UNKNOWN
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// filePath is the file a fixture address path names, with the drive letter
// a Windows path carries after its leading slash.
func filePath(urlPath string) string {
	path := filepath.FromSlash(urlPath)
	if runtime.GOOS == "windows" && len(path) > 3 && path[0] == '\\' && path[2] == ':' {
		path = path[1:]
	}
	return path
}
