package backend

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/grpc/codes"
	// The provider owns its database driver; Ptah and the SDK load none.
	_ "modernc.org/sqlite"
)

// Provider serves the tutorial engine. The zero value is ready to use, and
// one value serves every session of the process.
type Provider struct {
	mu       sync.Mutex
	sessions map[string]*session
}

// session is one open database. Its mutex serializes the calls that use it.
type session struct {
	mu    sync.Mutex
	db    *sql.DB
	path  string
	locks map[string]*lock
	// lost is set once a lock the session held is gone. It never clears:
	// the session runs nothing more.
	lost bool
}

// Open connects to the database file the address names and returns a session
// bound to it. SQLite creates a missing file on the first connection.
func (p *Provider) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	if request.GetEngine() != Engine {
		return nil, runtime.ErrorWithReason(codes.InvalidArgument, "the provider serves the tutorial engine only")
	}
	path, err := databasePath(request.GetAddress())
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, runtime.ErrorWithReason(codes.InvalidArgument, "the database file cannot be opened")
	}
	db.SetMaxOpenConns(1)
	var serverVersion string
	if err := db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&serverVersion); err != nil {
		_ = db.Close()
		return nil, runtime.ErrorWithReason(codes.Unavailable, "the database file cannot be read")
	}
	id := rand.Text()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sessions == nil {
		p.sessions = make(map[string]*session)
	}
	p.sessions[id] = &session{db: db, path: path}
	return &protocol.Session{
		Id:            id,
		Engine:        Engine,
		Database:      path,
		DefaultSchema: defaultSchema,
		ServerVersion: serverVersion,
		Identifiers:   identifierRules(),
	}, nil
}

// Close releases one session and the locks it holds. Closing a session twice
// succeeds.
func (p *Provider) Close(_ context.Context, request *protocol.CloseRequest) error {
	p.mu.Lock()
	found := p.sessions[request.GetSessionId()]
	delete(p.sessions, request.GetSessionId())
	p.mu.Unlock()
	if found == nil {
		return nil
	}
	found.mu.Lock()
	defer found.mu.Unlock()
	failures := make([]error, 0, len(found.locks)+1)
	for _, record := range found.locks {
		failures = append(failures, record.release())
	}
	failures = append(failures, found.db.Close())
	if errors.Join(failures...) != nil {
		return runtime.ErrorWithReason(codes.Internal, "the session did not close cleanly")
	}
	return nil
}

// lookup returns the open session id names.
func (p *Provider) lookup(id string) (*session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	found := p.sessions[id]
	if found == nil {
		return nil, runtime.ErrorWithReason(codes.NotFound, "the session is closed or was never opened")
	}
	return found, nil
}

// databasePath returns the file a tutorial address names. The address is
// tutorial:///absolute/path, or tutorial:///C:/path on Windows. A reason
// never quotes the address, because an address can carry a credential.
func databasePath(address string) (string, error) {
	refused := runtime.ErrorWithReason(codes.InvalidArgument, "a tutorial address names an absolute file path: tutorial:///path/to/file.db")
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme != Engine || parsed.Host != "" || parsed.Opaque != "" || parsed.RawQuery != "" {
		return "", refused
	}
	path := parsed.Path
	if len(path) >= 3 && path[0] == '/' && path[2] == ':' {
		path = path[1:]
	}
	path = filepath.FromSlash(path)
	if !filepath.IsAbs(path) {
		return "", refused
	}
	return path, nil
}
