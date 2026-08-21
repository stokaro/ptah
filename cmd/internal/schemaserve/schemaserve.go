// Package schemaserve serves a live view of a schema and the database it
// describes.
//
// It is the second surface of stokaro/ptah#1857 and deliberately the second:
// it renders the schema through internal/schemadoc rather than through markup
// of its own, so the served page and the exported document cannot disagree
// about what a schema looks like. A copy would agree on the day it was made
// and not the day after (stokaro/ptah#1863).
//
// What it adds is what only means something over time: drift between the
// declared schema and the live database, refreshed while a person is watching.
//
// It is read-only by construction. Every route answers GET and HEAD and nothing
// else, and no code path here writes to the database. A dashboard that can
// apply a migration is a different security question, and running one on a
// machine that holds production credentials should not also mean it can change
// production.
package schemaserve

import (
	"context"
	"fmt"
	"html"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.5x5.cz/ptah/cmd/internal/schemaops"
	"go.5x5.cz/ptah/internal/schemadoc"
	"go.5x5.cz/ptah/migration/safety"
)

// Options configures the served view.
type Options struct {
	// DatabaseURL is the database to compare against. Required.
	DatabaseURL string
	// RootDirs names the declared schema.
	//
	// There is no schema-file counterpart on purpose: such a file may name an
	// oci:// artifact, and this process re-reads on a timer, so pulling one
	// would put a registry request on a schedule nobody asked for. What that
	// should mean is a design question rather than an oversight
	// (stokaro/ptah#1863).
	RootDirs []string
	// Schemas limits the read to named schemas.
	Schemas []string
	// Title heads the page.
	Title string
	// Refresh is how often the page reloads itself. Zero means never, which is
	// what a reader who wants a stable page asks for.
	Refresh time.Duration
	// ConnectTimeout bounds each comparison, so a database that stops answering
	// makes the page say so rather than hang.
	ConnectTimeout time.Duration
	// Now supplies the time a snapshot is stamped with. Nil uses the clock; a
	// test supplies its own so the page it asserts on is the page it rendered.
	Now func() time.Time
}

// observation is one comparison, kept so the page can say when it last managed
// to reach the database rather than only what it found.
type observation struct {
	At       time.Time
	Findings []safety.Finding
	Highest  safety.Severity
	Err      error
	Schema   *schemaSnapshot
}

// schemaSnapshot is the declared schema as the page renders it, resolved with
// the comparison so a reader is never shown a schema from one moment and drift
// from another.
type schemaSnapshot struct {
	Sidebar string
	Content string
}

// Handler serves the dashboard.
func Handler(opts Options) (http.Handler, error) {
	if strings.TrimSpace(opts.DatabaseURL) == "" {
		return nil, fmt.Errorf("database URL is required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	server := &server{opts: opts}
	mux := http.NewServeMux()
	mux.Handle("/", readOnly(http.HandlerFunc(server.page)))
	return mux, nil
}

// readOnly refuses every method that could mean a change.
//
// The refusal is here rather than in each route so a route added later inherits
// it, which is the property that matters: the guarantee should not depend on
// the next person remembering.
func readOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "this dashboard is read-only", http.StatusMethodNotAllowed)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type server struct {
	opts Options
	mu   sync.Mutex
	last *observation
}

func (s *server) page(w http.ResponseWriter, r *http.Request) {
	current := s.observe(r.Context())
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// The page is regenerated on every request, so a cached copy would show a
	// reader drift that has since been fixed.
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(s.render(current)))
}

// observe compares the declared schema against the database, keeping the last
// successful schema render so a database that stops answering leaves the page
// showing the schema and an explicit failure rather than an empty screen.
func (s *server) observe(ctx context.Context) observation {
	result, err := schemaops.Compare(ctx, schemaops.CompareOptions{
		RootDirs:       s.opts.RootDirs,
		DatabaseURL:    s.opts.DatabaseURL,
		Schemas:        s.opts.Schemas,
		ConnectTimeout: s.opts.ConnectTimeout,
	})
	current := observation{At: s.opts.Now()}
	if err != nil {
		current.Err = err
		current.Schema = s.previousSchema()
		return current
	}
	current.Findings = safety.ClassifySchemaDiff(result.Diff)
	current.Highest = safety.Highest(current.Findings)
	sidebar, content, renderErr := schemadoc.Page(result.Generated, schemadoc.Options{Title: s.opts.Title})
	if renderErr == nil {
		current.Schema = &schemaSnapshot{Sidebar: sidebar, Content: content}
	}
	s.remember(current)
	return current
}

func (s *server) remember(current observation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = &current
}

func (s *server) previousSchema() *schemaSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last == nil {
		return nil
	}
	return s.last.Schema
}

func escape(value string) string { return html.EscapeString(value) }
