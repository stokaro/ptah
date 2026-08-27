package inference

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib" // the driver the PostgreSQL vertical uses

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedspec"
)

// commonOptions are what every verb needs.
type commonOptions struct {
	// specPath is the specification file.
	specPath string
	// dbURL is the database the generation lives in.
	dbURL string
}

// session is a resolved specification and an open connection.
type session struct {
	loaded embedspec.Loaded
	db     *sql.DB
	store  *embedpg.Store
}

// close releases the connection.
func (s *session) close() {
	if s.db != nil {
		_ = s.db.Close()
	}
}

// open reads the specification and connects.
//
// The two are separated in the error messages because they fail for different
// reasons and send an operator to different places: a specification that will
// not parse is a file to edit, and a database that will not open is a URL or a
// server.
func open(ctx context.Context, options commonOptions) (*session, error) {
	if strings.TrimSpace(options.specPath) == "" {
		return nil, fmt.Errorf("--spec is required")
	}
	loaded, err := embedspec.Load(options.specPath)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(options.dbURL) == "" {
		return nil, fmt.Errorf("--db-url is required")
	}
	if err := refuseAnotherEngine(options.dbURL); err != nil {
		return nil, err
	}
	db, err := sql.Open("pgx", options.dbURL)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", redact(options.dbURL), err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to %s: %w", redact(options.dbURL), err)
	}
	return &session{loaded: loaded, db: db, store: embedpg.NewStore(db)}, nil
}

// refuseAnotherEngine says what this namespace speaks to, before pgx says it
// cannot parse a URL.
//
// Every verb here opens its database on the PostgreSQL driver directly, because
// the run-state tables and the vector catalogs it reads have no dialect-agnostic
// form. So another engine's URL was already refused -- by pgx failing to parse
// it, with `cannot parse ...: invalid keyword/value`, which reads as a malformed
// connection string and sends an operator to check one that is correct
// (stokaro/ptah#2386).
//
// It refuses only a scheme Ptah RECOGNIZES as another dialect. An unrecognized
// scheme, and a keyword/value DSN with no scheme at all, still reach the driver:
// `host=localhost user=ptah` is a form pgx accepts and this must not start
// rejecting it for having no scheme to read.
func refuseAnotherEngine(dbURL string) error {
	scheme, _, found := strings.Cut(dbURL, "://")
	if !found {
		return nil
	}
	dialect := platform.NormalizeDialect(strings.ToLower(scheme))
	if dialect == "" || dialect == platform.Postgres {
		return nil
	}
	// The dialect is named once. Saying it mid-sentence as well would put the
	// same answer in two places, and neither could then be measured: remove
	// either and the other still carries it.
	return fmt.Errorf(
		"ptah inference works against PostgreSQL with pgvector, and %q names another engine: "+
			"a generation's run state and its vectors are a PostgreSQL vertical, "+
			"so there is nothing here to run against %s",
		scheme+"://", dialect)
}

// redact removes a password from a URL before it reaches a terminal or a log.
//
// A connection error is the most likely thing to be pasted into an issue, and
// the URL is where the credential is.
func redact(dbURL string) string {
	scheme, rest, found := strings.Cut(dbURL, "://")
	if !found {
		return dbURL
	}
	credentials, host, found := strings.Cut(rest, "@")
	if !found {
		return dbURL
	}
	user, _, hasPassword := strings.Cut(credentials, ":")
	if !hasPassword {
		return dbURL
	}
	return scheme + "://" + user + ":***@" + host
}

// writeLines prints a block of text.
func writeLines(out io.Writer, lines ...string) error {
	for _, line := range lines {
		if _, err := fmt.Fprintln(out, line); err != nil {
			return err
		}
	}
	return nil
}

// bullet renders one item of a list a person reads.
func bullet(text string) string {
	return "  - " + text
}

// section renders a heading.
func section(title string) string {
	return "\n" + title
}
