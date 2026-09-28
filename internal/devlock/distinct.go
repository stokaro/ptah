package devlock

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/dbschema"
)

// Protected is a database a dev database must not turn out to be: the target
// of a run, or a database read as a desired or current state. A dev database is
// reset, so a dev URL that names one of these destroys it.
type Protected struct {
	// Conn is a connection already open to the protected database. When it is
	// nil, URL is connected for the comparison and closed after it.
	Conn *dbschema.DatabaseConnection
	// URL is the protected database's URL, used when Conn is nil. An empty URL
	// names nothing and is skipped.
	URL string
	// Refusal is what the caller answers when the dev database is this one. It
	// is the caller's, so an operator reads the same sentence whichever check
	// found the alias.
	Refusal error
}

// EnsureDistinct refuses a dev database that is the same realm as any of the
// protected databases, comparing live connections by [SameRealm].
//
// It is the check a destructive dev cleanup is armed behind. A comparison of
// URLs cannot see every alias: a connection pooler can serve one database
// under two names, so two URLs naming different databases reach the same one.
// The live comparison asks each server which database the session selected.
// Run it after connecting to the dev database and before anything resets it or
// registers a reset for later: a reset registered first still runs when this
// refuses, and empties the database the refusal was protecting
// (stokaro/ptah#3769).
//
// A protected database that cannot be connected fails closed: the comparison
// did not happen, so the dev database is not proven distinct.
func EnsureDistinct(ctx context.Context, dev *dbschema.DatabaseConnection, protected ...Protected) error {
	for _, candidate := range protected {
		same, err := sameRealmAs(ctx, dev, candidate)
		if err != nil {
			return fmt.Errorf("compare the dev database with a database it must not be: %w", err)
		}
		if same {
			return candidate.Refusal
		}
	}
	return nil
}

// sameRealmAs compares dev with one protected database, connecting to it when
// the caller holds no connection.
func sameRealmAs(ctx context.Context, dev *dbschema.DatabaseConnection, candidate Protected) (bool, error) {
	if candidate.Conn != nil {
		return SameRealm(ctx, dev, candidate.Conn)
	}
	protectedURL := strings.TrimSpace(candidate.URL)
	if protectedURL == "" {
		return false, nil
	}
	conn, err := dbschema.ConnectToServer(ctx, protectedURL)
	if err != nil {
		return false, err
	}
	defer dbschema.CloseAndWarn(conn)
	return SameRealm(ctx, dev, conn)
}
