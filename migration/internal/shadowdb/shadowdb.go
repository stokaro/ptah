// Package shadowdb owns the connection lifecycle for disposable databases used
// by migration workflows.
package shadowdb

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"go.5x5.cz/ptah/dbschema"
)

const defaultTemporaryPattern = "ptah-shadow-*"

// Database is a disposable database connection. When Open provisions an
// ephemeral SQLite database, Close removes its temporary directory.
type Database struct {
	connection   *dbschema.DatabaseConnection
	temporaryDir string
}

// Open connects to databaseURL. When databaseURL is empty, it provisions an
// ephemeral SQLite database in a temporary directory.
func Open(ctx context.Context, databaseURL, temporaryPattern string) (*Database, error) {
	temporaryDir := ""
	if databaseURL == "" {
		if temporaryPattern == "" {
			temporaryPattern = defaultTemporaryPattern
		}
		var err error
		temporaryDir, err = os.MkdirTemp("", temporaryPattern)
		if err != nil {
			return nil, err
		}
		databaseURL = sqliteDatabaseURL(filepath.Join(temporaryDir, "shadow.db"))
	}

	connection, err := dbschema.ConnectToDatabase(ctx, databaseURL)
	if err != nil {
		return nil, errors.Join(err, removeTemporaryDir(temporaryDir))
	}
	return &Database{connection: connection, temporaryDir: temporaryDir}, nil
}

// Connection returns the disposable database connection.
func (d *Database) Connection() *dbschema.DatabaseConnection {
	return d.connection
}

// Close closes the database connection and removes any temporary directory
// provisioned by Open.
func (d *Database) Close() error {
	if d == nil {
		return nil
	}
	var connectionErr error
	if d.connection != nil {
		connectionErr = d.connection.Close()
	}
	return errors.Join(connectionErr, removeTemporaryDir(d.temporaryDir))
}

// CloseAndWarn closes the database and logs cleanup failures.
func (d *Database) CloseAndWarn() {
	if err := d.Close(); err != nil {
		slog.Warn("failed to close disposable database", "error", err)
	}
}

func sqliteDatabaseURL(databasePath string) string {
	normalizedPath := strings.ReplaceAll(filepath.ToSlash(databasePath), `\`, "/")
	return (&url.URL{Scheme: "sqlite", Path: normalizedPath}).String()
}

func removeTemporaryDir(dir string) error {
	if dir == "" {
		return nil
	}
	return os.RemoveAll(dir)
}
