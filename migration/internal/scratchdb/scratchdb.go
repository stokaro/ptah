// Package scratchdb provisions one disposable database per test case on a
// server the caller already named. It sits under migration/dbtest, which needs
// each case to run against state no other case can see, and it exists because
// sharing one connection between cases makes a suite's result depend on the
// order its cases happened to run in.
package scratchdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasurl"
)

// ErrNoIsolation reports that the dialect a URL names has no way to give one
// case a database of its own here.
//
// It is a refusal rather than a downgrade. Running cases against one shared
// database when they asked not to would make a suite pass or fail on the order
// two of them happened to reach a table.
var ErrNoIsolation = errors.New("this dialect has no per-case database isolation")

// Scratch is one disposable database and the means to remove it.
type Scratch struct {
	url         string
	temporary   string
	dropFrom    string
	dropCommand string
}

// URL is the address of the disposable database.
func (s *Scratch) URL() string {
	if s == nil {
		return ""
	}
	return s.url
}

// Close removes the database this provisioned.
//
// It is safe on a nil receiver and safe to call twice, so a caller may defer it
// beside the constructor without knowing which path ran.
func (s *Scratch) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if s.temporary != "" {
		dir := s.temporary
		s.temporary = ""
		return os.RemoveAll(dir)
	}
	if s.dropCommand == "" {
		return nil
	}
	command, from := s.dropCommand, s.dropFrom
	s.dropCommand, s.dropFrom = "", ""

	// Dropping runs against the server rather than the database being removed:
	// an engine refuses to drop the database a session is currently using.
	connection, err := dbschema.ConnectToDatabase(ctx, from)
	if err != nil {
		return fmt.Errorf("connect to drop scratch database: %w", err)
	}
	defer connection.Close()

	if _, err := connection.ExecContext(ctx, command); err != nil {
		return fmt.Errorf("drop scratch database: %w", err)
	}
	return nil
}

// Provision creates a disposable database on the server baseURL names.
//
// SQLite gets a file of its own in a temporary directory rather than a database
// on the named file, because a SQLite URL names one database rather than a
// server. The server engines create a database beside the one the URL names and
// return an address for it; Close removes what was created.
//
// A dialect this cannot isolate returns [ErrNoIsolation], so a caller refuses
// rather than silently sharing.
func Provision(ctx context.Context, baseURL string) (*Scratch, error) {
	dialect, err := atlasurl.DialectFromURL(baseURL)
	if err != nil {
		return nil, fmt.Errorf("read the dialect of the scratch server URL: %w", err)
	}

	if dialect == platform.SQLite {
		return provisionSQLite()
	}
	if platform.IsPostgresFamily(dialect) || dialect == platform.MySQL || dialect == platform.MariaDB {
		return provisionServerDatabase(ctx, baseURL)
	}
	return nil, fmt.Errorf("%w: %s", ErrNoIsolation, dialect)
}

// provisionSQLite gives the case a database file nothing else opens.
func provisionSQLite() (*Scratch, error) {
	dir, err := os.MkdirTemp("", "ptah-scratch-*")
	if err != nil {
		return nil, fmt.Errorf("create scratch directory: %w", err)
	}
	return &Scratch{
		url:       atlasurl.SQLiteURLFromPath(filepath.Join(dir, "scratch.db")),
		temporary: dir,
	}, nil
}

// provisionServerDatabase creates a database on the server and returns an
// address for it.
//
// The name carries random bytes rather than a counter, because two runs of a
// suite against one server overlap in continuous integration and a counter
// would collide between them rather than within one of them.
func provisionServerDatabase(ctx context.Context, baseURL string) (*Scratch, error) {
	name, err := scratchName()
	if err != nil {
		return nil, err
	}

	connection, err := dbschema.ConnectToDatabase(ctx, baseURL)
	if err != nil {
		return nil, fmt.Errorf("connect to the scratch server: %w", err)
	}
	defer connection.Close()

	// Unquoted, and the name is what makes that safe: it is generated here from
	// hex digits and an underscore, never taken from a caller, so it can carry
	// neither an identifier's quoting nor a statement separator. Quoting it
	// would need the engine's own spelling -- double quotes on PostgreSQL,
	// backticks on MySQL -- and a name that needs none avoids choosing.
	if _, err := connection.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		return nil, fmt.Errorf("create scratch database %q: %w", name, err)
	}

	scratchURL, err := atlasurl.WithDatabaseName(baseURL, name)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("address the scratch database %q: %w", name, err),
			dropDatabase(ctx, baseURL, name),
		)
	}
	return &Scratch{
		url:         scratchURL,
		dropFrom:    baseURL,
		dropCommand: "DROP DATABASE " + name,
	}, nil
}

// dropDatabase removes a database created moments earlier, for the path where
// addressing it failed and Close will never run.
func dropDatabase(ctx context.Context, baseURL, name string) error {
	connection, err := dbschema.ConnectToDatabase(ctx, baseURL)
	if err != nil {
		return fmt.Errorf("connect to drop scratch database: %w", err)
	}
	defer connection.Close()

	if _, err := connection.ExecContext(ctx, "DROP DATABASE "+name); err != nil {
		return fmt.Errorf("drop scratch database: %w", err)
	}
	return nil
}

// scratchName is the identifier a scratch database carries.
//
// Lowercase hex and one underscore. PostgreSQL folds an unquoted identifier to
// lowercase and a MySQL database name is a directory name on some platforms, so
// a name needing neither quoting nor case preservation survives both -- and an
// unquoted statement then needs no per-engine quoting spelling at all.
func scratchName() (string, error) {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate a scratch database name: %w", err)
	}
	return "ptah_scratch_" + hex.EncodeToString(raw), nil
}

// CanIsolate reports whether Provision can give a case its own database on the
// server baseURL names.
//
// It asks the question without creating anything, so a caller refuses a whole
// run before provisioning rather than failing partway through it: a suite whose
// third case cannot be isolated must not first create two databases and apply a
// schema to each.
func CanIsolate(baseURL string) error {
	if baseURL == "" {
		return nil
	}
	dialect, err := atlasurl.DialectFromURL(baseURL)
	if err != nil {
		return fmt.Errorf("read the dialect of the scratch server URL: %w", err)
	}
	if dialect == platform.SQLite ||
		platform.IsPostgresFamily(dialect) ||
		dialect == platform.MySQL ||
		dialect == platform.MariaDB {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrNoIsolation, dialect)
}
