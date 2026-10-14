package pgconnect

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"ptah.run/internal/schemaselection"
)

// reconnectCarryingSearchPath opens the connection a second time without the
// schema selection, for the one failure that is about SENDING it rather than
// about the database.
//
// A `?search_path=` on a PostgreSQL URL is a STARTUP parameter, and a
// transaction-pooling proxy refuses the whole connection for a parameter
// outside its allow-list rather than ignoring it:
//
//	FATAL: unsupported startup parameter: search_path (SQLSTATE 08P01)
//
// Nothing about the schema itself is wrong there. Measured on PgBouncer 1.25.2
// in transaction mode against PostgreSQL 17, one server reached two ways: the
// same URL without the parameter connects, and `schema inspect --schemas app`
// through the proxy produces a document byte-identical to
// `?search_path=app` direct. An apply through the proxy emits
// `ALTER TABLE "app"."widget"` and the column lands in `app`. So neither the
// read nor the write depends on a session `search_path`; only the URL spelling
// did (stokaro/ptah#1029).
//
// The retry is triggered by the refusal and by nothing else. A direct server
// never answers it, so a direct connection is opened once and behaves exactly
// as it did -- which is what keeps this from being a change to every
// PostgreSQL run.
//
// When there is nothing to retry the original failure is returned, explained,
// because the operator's next step is then about the URL rather than about the
// proxy.
func reconnectCarryingSearchPath(
	ctx context.Context,
	dbURL, driverName, dialect string,
	pingErr error,
) (*sql.DB, error) {
	retryURL, retryable := urlWithoutSearchPath(dbURL, dialect, pingErr)
	if !retryable {
		return nil, fmt.Errorf("failed to ping database: %w",
			explainRefusedStartupParameter(dbURL, dialect, pingErr))
	}

	slog.Debug(
		"reconnecting without the schema selection: the server refused it as a startup parameter",
		"parameter", "search_path",
		"selection", schemaselection.FromURL(dbURL).Raw,
	)

	db, err := sql.Open(driverName, convertPostgresWireURL(retryURL))
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		// The second failure is the one to report: the first was about a
		// parameter this connection does not send.
		return nil, fmt.Errorf("failed to ping database: %w",
			explainRefusedStartupParameter(retryURL, dialect, err))
	}
	return db, nil
}

// removePostgresPoolParams removes the pgx pool parameters from a database URL.
// pool_max_conns and pool_min_conns configure pgx's own pool and are not
// connection parameters, so a standard connection is opened without them. Every
// other parameter is kept, and kept BYTE FOR BYTE.
//
// That last part is why this rewrites the raw query rather than going through
// url.Values. Encode spells a space as "+", which is correct for a form and
// wrong for a value libpq reads as content: `options=-c%20search_path%3Dextra`
// came back as `options=-c+search_path%3Dextra`, and the server then refused
// the connection for a startup parameter literally named "+search_path"
// (SQLSTATE 42704). The function removed nothing from that URL and corrupted it
// anyway, because it re-encoded unconditionally.
//
// A URL carrying neither parameter is returned unchanged, so the common path
// does not re-serialize at all.
func removePostgresPoolParams(dbURL string) string {
	parsedURL, err := url.Parse(dbURL)
	if err != nil || parsedURL.RawQuery == "" {
		return dbURL
	}
	kept := make([]string, 0, strings.Count(parsedURL.RawQuery, "&")+1)
	removed := false
	for pair := range strings.SplitSeq(parsedURL.RawQuery, "&") {
		rawKey, _, _ := strings.Cut(pair, "=")
		key, decodeErr := url.QueryUnescape(rawKey)
		if decodeErr == nil && (key == "pool_max_conns" || key == "pool_min_conns") {
			removed = true
			continue
		}
		kept = append(kept, pair)
	}
	if !removed {
		return dbURL
	}
	parsedURL.RawQuery = strings.Join(kept, "&")
	return parsedURL.String()
}
