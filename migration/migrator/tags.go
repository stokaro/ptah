package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/sqlutil"
)

// defaultMigrationTagsTable is where the tag namespace lives.
//
// # Why a table of its own
//
// A tag names a migration-directory state, not a database state, so it does not
// belong to any one revision row. Two revisions can be applied under one tag and
// a tag can be recorded for a directory whose migrations were never applied at
// all, neither of which a column on a revision can express.
//
// Keeping it separate also leaves the Atlas-shaped revision table alone. That
// layout is a compatibility contract: a column Ptah added to it would be a
// column the shape does not define, written into a table another tool may read.
// A table Ptah owns carries the extension without touching the contract, and it
// works the same under both revision-table formats -- which a column could not,
// since only one of the two layouts could have taken it (stokaro/ptah#1621).
const defaultMigrationTagsTable = "ptah_migration_tags"

// MigrationTag records a directory tag and the schema version it selects.
type MigrationTag struct {
	// Tag is the name the operator gave the directory state.
	Tag string `json:"tag"`
	// Version is the highest migration version the tagged directory carries.
	Version int64 `json:"version"`
	// RecordedAt is when this tag last resolved to Version.
	RecordedAt time.Time `json:"recorded_at"`
}

// ErrMigrationTagNotFound reports a tag that names no recorded directory state.
var ErrMigrationTagNotFound = errors.New("migration tag not found")

func (m *Migrator) migrationTagsTableName() string {
	return defaultMigrationTagsTable
}

func (m *Migrator) qualifiedMigrationTagsTable() string {
	table := m.migrationTagsTableName()
	schema := m.metadataTableSchemaName()
	if schema == "" {
		return m.quoteIdentifier(table)
	}
	return m.quoteIdentifier(schema) + "." + m.quoteIdentifier(table)
}

// createMigrationTagsTableSQL renders the tag table for the connected dialect.
//
// The version column is signed BIGINT rather than the revision table's version
// type, because a tag selects a numeric schema version even where revisions are
// stored under the Atlas layout's opaque string tokens. Resolving one to the
// other is the reader's job, not the column's.
func (m *Migrator) createMigrationTagsTableSQL() string {
	qualifiedTable := m.qualifiedMigrationTagsTable()
	switch platform.NormalizeDialect(m.connectionDialect()) {
	case platform.SQLServer:
		return fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(%s) AND type = 'U')
BEGIN
    CREATE TABLE %s (
        tag NVARCHAR(255) NOT NULL PRIMARY KEY,
        version BIGINT NOT NULL,
        recorded_at DATETIME2 NOT NULL
    )
END`, sqlStringLiteral(m.sqlServerTagsObjectName()), qualifiedTable)
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    tag VARCHAR(255) NOT NULL PRIMARY KEY,
    version BIGINT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL
)`, qualifiedTable)
	}
	engineClause := ""
	if implicitCommitDialect(m.connectionDialect()) {
		engineClause = " ENGINE=InnoDB"
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    tag VARCHAR(255) NOT NULL PRIMARY KEY,
    version BIGINT NOT NULL,
    recorded_at TIMESTAMP NOT NULL
)%s`, qualifiedTable, engineClause)
}

func (m *Migrator) sqlServerTagsObjectName() string {
	schema := m.metadataTableSchemaName()
	if schema == "" {
		return m.migrationTagsTableName()
	}
	return schema + "." + m.migrationTagsTableName()
}

// EnsureMigrationTagsTable creates the tag table when it does not exist.
func (m *Migrator) EnsureMigrationTagsTable(ctx context.Context) error {
	if m.conn == nil {
		return errors.New("ensure migration tags table: no database connection")
	}
	if statement := m.migrationsSchemaStatement(); statement != "" {
		if _, err := m.conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("ensure migration tags schema: %w", err)
		}
	}
	if _, err := m.conn.ExecContext(ctx, m.createMigrationTagsTableSQL()); err != nil {
		return fmt.Errorf("ensure migration tags table: %w", err)
	}
	return nil
}

// migrationTagsTableExists reports whether the tag namespace has been created.
func (m *Migrator) migrationTagsTableExists(ctx context.Context) (bool, error) {
	if m.conn == nil {
		return false, errors.New("migration tags table: no database connection")
	}
	query, args, err := migrationTablePresenceQuery(
		m.connectionDialect(),
		m.metadataTableSchemaName(),
		m.connectionSchemaName(),
		m.migrationTagsTableName(),
		m.quoteIdentifier,
	)
	if err != nil {
		return false, err
	}
	var count int64
	query = sqlutil.Rebind(m.connectionDialect(), query)
	if err := m.conn.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// RecordMigrationTag points tag at version, creating the tag or moving it.
//
// Moving is the normal case, not an error: the registry tags this mirrors are
// movable pointers, and refusing to move one would leave an operator who
// re-tagged a directory unable to say so here.
func (m *Migrator) RecordMigrationTag(ctx context.Context, tag string, version int64) error {
	normalized, err := NormalizeMigrationTag(tag)
	if err != nil {
		return err
	}
	if err := m.EnsureMigrationTagsTable(ctx); err != nil {
		return err
	}
	now := time.Now().UTC()
	statement := sqlutil.Rebind(m.connectionDialect(), m.upsertMigrationTagSQL())
	if _, err := m.conn.ExecContext(ctx, statement, normalized, version, now); err != nil {
		return fmt.Errorf("record migration tag %q: %w", normalized, err)
	}
	return nil
}

func (m *Migrator) upsertMigrationTagSQL() string {
	table := m.qualifiedMigrationTagsTable()
	switch platform.NormalizeDialect(m.connectionDialect()) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.SQLite:
		return fmt.Sprintf(
			"INSERT INTO %s (tag, version, recorded_at) VALUES (?, ?, ?)"+
				" ON CONFLICT (tag) DO UPDATE SET version = EXCLUDED.version,"+
				" recorded_at = EXCLUDED.recorded_at",
			table)
	case platform.SQLServer:
		return fmt.Sprintf(
			"MERGE %s AS target USING (SELECT ? AS tag, ? AS version, ? AS recorded_at) AS source"+
				" ON target.tag = source.tag"+
				" WHEN MATCHED THEN UPDATE SET version = source.version, recorded_at = source.recorded_at"+
				" WHEN NOT MATCHED THEN INSERT (tag, version, recorded_at)"+
				" VALUES (source.tag, source.version, source.recorded_at);",
			table)
	}
	return fmt.Sprintf(
		"INSERT INTO %s (tag, version, recorded_at) VALUES (?, ?, ?)"+
			" ON DUPLICATE KEY UPDATE version = VALUES(version), recorded_at = VALUES(recorded_at)",
		table)
}

// ResolveMigrationTag returns the version a tag selects.
//
// A tag that was never recorded returns ErrMigrationTagNotFound rather than
// version zero. Zero is a real schema version -- an empty database -- so a
// caller that could not tell the two apart would revert everything when it
// meant to report a typo.
func (m *Migrator) ResolveMigrationTag(ctx context.Context, tag string) (int64, error) {
	normalized, err := NormalizeMigrationTag(tag)
	if err != nil {
		return 0, err
	}
	exists, err := m.migrationTagsTableExists(ctx)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, fmt.Errorf("%w: %q", ErrMigrationTagNotFound, normalized)
	}
	var version int64
	query := sqlutil.Rebind(m.connectionDialect(),
		fmt.Sprintf("SELECT version FROM %s WHERE tag = ?", m.qualifiedMigrationTagsTable()))
	switch err := m.conn.QueryRowContext(ctx, query, normalized).Scan(&version); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, fmt.Errorf("%w: %q", ErrMigrationTagNotFound, normalized)
	case err != nil:
		return 0, fmt.Errorf("resolve migration tag %q: %w", normalized, err)
	}
	return version, nil
}

// MigrationTags returns every recorded tag, ordered by name.
//
// An absent table is an empty namespace, not a failure: a database that has
// never been tagged has no tags, and reporting that as an error would make
// `migrations status` fail on every project that does not use them.
func (m *Migrator) MigrationTags(ctx context.Context) ([]MigrationTag, error) {
	exists, err := m.migrationTagsTableExists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	query := fmt.Sprintf("SELECT tag, version, recorded_at FROM %s ORDER BY tag",
		m.qualifiedMigrationTagsTable())
	rows, err := m.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read migration tags: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var tags []MigrationTag
	for rows.Next() {
		var tag MigrationTag
		if err := rows.Scan(&tag.Tag, &tag.Version, &tag.RecordedAt); err != nil {
			return nil, fmt.Errorf("read migration tags: %w", err)
		}
		tags = append(tags, tag)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read migration tags: %w", err)
	}
	return tags, nil
}

// DeleteMigrationTag removes a tag. Removing one that does not exist reports
// ErrMigrationTagNotFound, so a script deleting a typo hears about it.
func (m *Migrator) DeleteMigrationTag(ctx context.Context, tag string) error {
	normalized, err := NormalizeMigrationTag(tag)
	if err != nil {
		return err
	}
	exists, err := m.migrationTagsTableExists(ctx)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: %q", ErrMigrationTagNotFound, normalized)
	}
	query := sqlutil.Rebind(m.connectionDialect(),
		fmt.Sprintf("DELETE FROM %s WHERE tag = ?", m.qualifiedMigrationTagsTable()))
	result, err := m.conn.ExecContext(ctx, query, normalized)
	if err != nil {
		return fmt.Errorf("delete migration tag %q: %w", normalized, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		// A driver that cannot count rows cannot prove the tag was there, and
		// reporting a deletion that may not have happened is worse than saying
		// the count is unavailable.
		return fmt.Errorf("delete migration tag %q: %w", normalized, err)
	}
	if affected == 0 {
		return fmt.Errorf("%w: %q", ErrMigrationTagNotFound, normalized)
	}
	return nil
}

// NormalizeMigrationTag trims a tag and refuses the shapes that cannot name a
// directory state.
//
// The rules are the registry's, because these tags exist to line up with the
// ones a migration directory is pushed under: an empty tag names nothing, and
// whitespace inside one would make `--to-tag` ambiguous to any shell that
// splits arguments.
func NormalizeMigrationTag(tag string) (string, error) {
	trimmed := strings.TrimSpace(tag)
	if trimmed == "" {
		return "", errors.New("migration tag is empty")
	}
	if strings.ContainsFunc(trimmed, func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\n' || r == '\r'
	}) {
		return "", fmt.Errorf("migration tag %q contains whitespace", tag)
	}
	if len(trimmed) > migrationTagMaxLength {
		return "", fmt.Errorf("migration tag is %d characters, over the %d the column holds",
			len(trimmed), migrationTagMaxLength)
	}
	return trimmed, nil
}

// migrationTagMaxLength matches the column width, so an over-long tag is
// refused where it can be explained instead of truncated by the database into
// a tag that silently collides with another.
const migrationTagMaxLength = 255
