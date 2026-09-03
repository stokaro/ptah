package inference

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib" // the driver the PostgreSQL vertical uses
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdflags"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/embeddigest"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedrelease"
	"go.5x5.cz/ptah/internal/embedspec"
)

// commonOptions are what the specification-led database verbs share.
type commonOptions struct {
	// spec is where the specification comes from. It is a pointer so that every
	// copy of these options shares one resolution; see [specSource].
	spec *specSource
	// dbURL is the database the generation lives in.
	dbURL string
}

// specSource is where a verb's specification comes from, and the one copy of it
// this invocation runs against.
//
// Resolved once rather than at each use. A verb opens the database several
// times -- a cutover verifies, reads the pointer, then advances the phase --
// and reading a local file three times is three reads of the same bytes.
// Reading a MUTABLE OCI reference three times is three chances to be handed a
// different specification, and a cutover carried out against two of them is one
// that no record describes.
//
// Resolved lazily rather than in a PreRunE, because cobra runs PreRunE before
// its own flag-group validation and before the verb has checked its arguments.
// A run naming two destinations for one record, or naming no run at all, would
// have reached a registry before being refused for a reason that has nothing to
// do with one.
type specSource struct {
	// path is the specification file, and reference an OCI reference to a
	// published release carrying one instead. Exactly one is given.
	path      string
	reference string
	// plainHTTP permits an unencrypted connection to that registry.
	plainHTTP bool
	// notices is where the resolution of a mutable reference is reported. It is
	// taken at resolution time rather than at flag registration, because a test
	// installs its own streams on the root command afterwards.
	notices func() io.Writer
	// loaded is what the first resolution produced, nil until then.
	loaded *embedspec.Loaded
}

// session is a resolved specification and an open connection.
type session struct {
	loaded embedspec.Loaded
	db     *sql.DB
	store  *embedpg.Store
	// source and target are the physical relations the specification's
	// spellings resolve to on this server, which is what every durable
	// identity is derived from: the source digest the outbox is named after,
	// the source-scoped advisory lock, and the target pointer.
	//
	// They are resolved once, here, because every verb comes through open()
	// and because an unqualified spelling means whatever the session's
	// search_path says it means. A spelling that names no relation keeps the
	// authored value, so the verb that needs the relation reports its absence
	// rather than this resolution doing it (stokaro/ptah#2806).
	source embedpg.Relation
	target embedpg.Relation
}

// close releases the connection.
func (s *session) close() {
	if s.db != nil {
		_ = s.db.Close()
	}
}

// open resolves the specification and connects.
//
// The two are separated in the error messages because they fail for different
// reasons and send an operator to different places: a specification that will
// not parse is a file to edit, and a database that will not open is a URL or a
// server.
//
// The argument checks come first, before either. Resolving a --release reaches
// a registry, and a run that was going to be refused for naming no database
// should not spend a network round trip finding that out.
func open(ctx context.Context, options commonOptions) (*session, error) {
	if err := validateDatabaseURL(options.dbURL); err != nil {
		return nil, err
	}
	loaded, err := options.spec.resolve(ctx)
	if err != nil {
		return nil, err
	}
	db, err := connectDatabase(ctx, options.dbURL)
	if err != nil {
		return nil, err
	}
	source, _, err := embedpg.ResolveRelation(
		ctx, db, loaded.Spec.Source.Schema, loaded.Spec.Source.Table)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	target, _, err := embedpg.ResolveRelation(
		ctx, db, loaded.Spec.Target.Schema, loaded.Spec.Target.Table)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &session{
		loaded: loaded, db: db, store: embedpg.NewStore(db),
		source: source, target: target,
	}, nil
}

// validateDatabaseURL checks the part of a database connection that does not
// require opening it. Kept separate from connectDatabase so specification-led
// verbs preserve their error order: arguments and engine first, specification
// second, network third.
func validateDatabaseURL(dbURL string) error {
	if strings.TrimSpace(dbURL) == "" {
		return fmt.Errorf("--db-url is required")
	}
	return refuseAnotherEngine(dbURL)
}

// connectDatabase opens the PostgreSQL run-state database after its arguments
// have been checked.
func connectDatabase(ctx context.Context, dbURL string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", redact(dbURL), err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to %s: %w", redact(dbURL), err)
	}
	return db, nil
}

// resolve reads the specification from wherever the operator put it: a file
// they are editing, or a release somebody else published.
//
// The two are one function because everything downstream takes a resolved
// specification and must not learn which. A verb that behaved differently on a
// promoted release would make the promotion untestable by the environment that
// published it.
func (s *specSource) resolve(ctx context.Context) (embedspec.Loaded, error) {
	if s == nil {
		return embedspec.Loaded{}, fmt.Errorf("--spec or --release is required")
	}
	if s.loaded != nil {
		return *s.loaded, nil
	}
	loaded, err := s.read(ctx)
	if err != nil {
		return embedspec.Loaded{}, err
	}
	s.loaded = &loaded
	return loaded, nil
}

// read is the resolution itself, without the memory of having done it.
func (s *specSource) read(ctx context.Context) (embedspec.Loaded, error) {
	path := strings.TrimSpace(s.path)
	reference := strings.TrimSpace(s.reference)
	switch {
	case path == "" && reference == "":
		return embedspec.Loaded{}, fmt.Errorf("--spec or --release is required")
	case path != "":
		return embedspec.Load(path)
	}

	fetched, err := embedrelease.Fetch(ctx, reference,
		embedrelease.FetchOptions{PlainHTTP: s.plainHTTP})
	if err != nil {
		return embedspec.Loaded{}, err
	}
	loaded, err := embedspec.ParsePublished(
		fetched.Specification, reference, fetched.Release.SpecDigest)
	if err != nil {
		return embedspec.Loaded{}, err
	}
	if err := agreesWithItsRecord(reference, fetched, loaded); err != nil {
		return embedspec.Loaded{}, err
	}
	return loaded, s.reportResolution(fetched)
}

// agreesWithItsRecord checks the release against the specification it carries.
//
// Two checks, and the digest one is not enough on its own. It establishes that
// the bytes are the document the record named; this establishes that the
// document produces the generation the record CLAIMS. A release whose record
// says generation A while its correctly digested specification resolves to B
// would otherwise run B while the resolution notice, the OCI subject and every
// verification attached to it went on identifying A.
func agreesWithItsRecord(
	reference string, fetched embedrelease.Fetched, loaded embedspec.Loaded,
) error {
	identity := loaded.Spec.Identity().Digest
	if fetched.Release.Generation == identity {
		return nil
	}
	return fmt.Errorf(
		"%s records generation %s and the specification it carries produces %s, "+
			"so the release does not describe what it holds",
		reference, embeddigest.Short(fetched.Release.Generation), embeddigest.Short(identity))
}

// reportResolution says which artifact a mutable reference turned out to name.
//
// It goes to the notice stream rather than into the answer because the answer
// is machine-read: `describe --format json` is diffed between two commits to
// decide whether a corpus has to be recomputed, and a document that also
// carried where it was fetched from would differ on every promotion for a
// reason that changes no vector.
//
// Reported at all because a promotion whose record kept only the tag says two
// environments agreed without establishing that they did. A tag moves.
func (s *specSource) reportResolution(fetched embedrelease.Fetched) error {
	if s.notices == nil {
		return nil
	}
	_, err := fmt.Fprintf(s.notices(),
		"release %s resolved to %s, generation %s\n",
		fetched.Reference, fetched.Digest, fetched.Release.Generation)
	return err
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

// requireExclusiveOnCommandLine declares a mutually exclusive flag group whose
// members carry PTAH_* values, composing onto whatever PreRunE cmd already has.
//
// It is not cmd.MarkFlagsMutuallyExclusive, for the reason
// [cmdflags.ExclusiveOnCommandLine] states: cobra's group validation reads
// pflag's Changed bit, which an applied environment value sets exactly as an
// argv occurrence does. Nor is it an assignment to cmd.PreRunE, because
// "cutover" declares three of these groups and the last assignment would be
// the only one that ran.
func requireExclusiveOnCommandLine(cmd *cobra.Command, names ...string) {
	previous := cmd.PreRunE
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if previous != nil {
			if err := previous(cmd, args); err != nil {
				return err
			}
		}
		return cmdflags.ExclusiveOnCommandLine(cmd.Flags(), names...)
	}
}
