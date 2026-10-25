package shadowdb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// LoadMigrations reads the migration history that a replay applies to a
// disposable database.
//
// fsys is the authoritative source when it is non-nil; displayDir names the
// directory in messages and is opened when fsys is nil, which is the path
// embedders that pass only a directory take. A directory that does not exist
// yields no migrations rather than an error: the first generated migration has
// no history to replay in front of it.
func LoadMigrations(fsys fs.FS, displayDir string, opts ...migrator.FSProviderOption) ([]*migrator.Migration, error) {
	if fsys == nil {
		return loadMigrationsFromDir(displayDir, opts...)
	}
	provider, err := migrator.NewFSMigrationProvider(fsys, opts...)
	if err != nil {
		return nil, err
	}
	migrations := provider.Migrations()
	out := make([]*migrator.Migration, len(migrations))
	copy(out, migrations)
	return out, nil
}

func loadMigrationsFromDir(dir string, opts ...migrator.FSProviderOption) ([]*migrator.Migration, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, nil
	}
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	return LoadMigrations(os.DirFS(dir), dir, opts...)
}

var missingColumnErrorRe = regexp.MustCompile(`column "([^"]+)" of relation "([^"]+)" does not exist`)

// DescribeReplayError renders a replay failure as the schema fact behind it,
// and returns the empty string when it recognizes nothing.
//
// A replay fails against the database, so the driver reports the statement that
// broke rather than the difference that broke it. The caller wants the second
// one: "missing column users.email" names what the migration did not create.
func DescribeReplayError(err error) string {
	match := missingColumnErrorRe.FindStringSubmatch(err.Error())
	if match == nil {
		return ""
	}
	return fmt.Sprintf("missing column %s.%s", match[2], match[1])
}

// SameDialect answers whether two dialect spellings name the same dialect.
func SameDialect(left, right string) bool {
	return platform.NormalizeDialect(left) == platform.NormalizeDialect(right)
}

// ResetSchemas drops the named schemas from a disposable database so a replay
// starts from nothing.
//
// Dropping every table leaves the schemas that held them, and on PostgreSQL a
// migration that creates a schema then fails against the one already there.
// The statement is the descriptor's DevRealm.DropSchema. The default schema,
// public on PostgreSQL, is left alone: it is part of the database rather than
// something a migration created. A dialect whose descriptor states no such
// statement has no schema to reset here, so the call is a no-op for it.
func ResetSchemas(ctx context.Context, conn *dbschema.DatabaseConnection, schemas []string) error {
	described, _ := platform.Lookup(conn.Info().Dialect)
	if described.DevRealm.DropSchema == "" {
		return nil
	}
	for _, schema := range schemas {
		if schema == "" || schema == described.DefaultSchema {
			continue
		}
		_, err := conn.ExecContext(ctx, fmt.Sprintf(described.DevRealm.DropSchema, quoteIdentifier(schema)))
		if err != nil {
			return fmt.Errorf("drop schema %q: %w", schema, err)
		}
	}
	return nil
}

// DropMigrationMetadata removes the migration bookkeeping tables a replay
// wrote, so what remains is the schema the migrations describe and nothing
// else.
//
// Every one of them, which is why this takes a list: a replay that left one
// behind would hand the comparison a table no migration declares, and the
// mismatch reads as a schema difference rather than as Ptah's own bookkeeping.
//
// On an engine an executable provider serves, the bookkeeping is the
// provider's storage, and Ptah writes no SQL of its own there. The provider's
// inspection reports that storage as reserved, outside the schema a replay
// reads, and its dev database cleanup removes it with the rest of the
// database, so there is nothing for this to drop.
func DropMigrationMetadata(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	tableIdentifiers ...string,
) error {
	if !conn.Info().Provider.IsZero() {
		return nil
	}
	for _, identifier := range tableIdentifiers {
		if identifier == "" {
			continue
		}
		if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+identifier); err != nil {
			return fmt.Errorf("drop metadata table: %w", err)
		}
	}
	return nil
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
