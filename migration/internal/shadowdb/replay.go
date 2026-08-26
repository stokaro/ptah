package shadowdb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
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
// migration that creates a schema then fails against the one already there. The
// public schema is left alone: it is part of the database rather than something
// a migration created. Other dialects have no schema to reset here, so the call
// is a no-op for them.
func ResetSchemas(ctx context.Context, conn *dbschema.DatabaseConnection, schemas []string) error {
	if conn.Info().Dialect != platform.Postgres {
		return nil
	}
	for _, schema := range schemas {
		if schema == "" || schema == "public" {
			continue
		}
		_, err := conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quotePostgresIdentifier(schema)+" CASCADE")
		if err != nil {
			return fmt.Errorf("drop schema %q: %w", schema, err)
		}
	}
	return nil
}

// DropMigrationMetadata removes the migration bookkeeping table a replay wrote,
// so what remains is the schema the migrations describe and nothing else.
func DropMigrationMetadata(ctx context.Context, conn *dbschema.DatabaseConnection, tableIdentifier string) error {
	if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableIdentifier); err != nil {
		return fmt.Errorf("drop metadata table: %w", err)
	}
	return nil
}

func quotePostgresIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
