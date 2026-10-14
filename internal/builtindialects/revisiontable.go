package builtindialects

import "ptah.run/core/platform/descriptor"

// The migrator's statements that several engines share. An engine's own
// RevisionTable names the ones it takes; descriptor.RevisionTable says what
// each placeholder holds.

// createRevisions is the revision table in Ptah's layout, as an engine with
// BIGINT, TEXT and an inline PRIMARY KEY spells it.
const createRevisions = `CREATE TABLE IF NOT EXISTS {table} (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'applied',
    applied INTEGER NOT NULL DEFAULT 1,
    total INTEGER NOT NULL DEFAULT 1,
    error TEXT NULL,
    error_stmt TEXT NULL,
    execution_time_ms BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(64) NOT NULL DEFAULT ''
){engine}`

// createAtlasRevisions is the revision table in Atlas's layout, with a JSON
// partial_hashes column.
const createAtlasRevisions = `CREATE TABLE IF NOT EXISTS {table} (
    version VARCHAR(255) PRIMARY KEY,
    description TEXT NOT NULL,
    type BIGINT NOT NULL DEFAULT 2,
    applied BIGINT NOT NULL DEFAULT 0,
    total BIGINT NOT NULL DEFAULT 0,
    executed_at TIMESTAMP NOT NULL,
    execution_time BIGINT NOT NULL,
    error TEXT NULL,
    error_stmt TEXT NULL,
    hash VARCHAR(255) NOT NULL,
    partial_hashes JSON NULL,
    operator_version VARCHAR(255) NOT NULL
){engine}`

// createPostgresAtlasRevisions is Atlas's layout as the PostgreSQL family
// spells it: TIMESTAMPTZ and JSONB, the spellings Spanner's PostgreSQL
// interface has too.
const createPostgresAtlasRevisions = `CREATE TABLE IF NOT EXISTS {table} (
    version VARCHAR PRIMARY KEY,
    description VARCHAR NOT NULL,
    type BIGINT NOT NULL DEFAULT 2,
    applied BIGINT NOT NULL DEFAULT 0,
    total BIGINT NOT NULL DEFAULT 0,
    executed_at TIMESTAMPTZ NOT NULL,
    execution_time BIGINT NOT NULL,
    error TEXT NULL,
    error_stmt TEXT NULL,
    hash VARCHAR NOT NULL,
    partial_hashes JSONB NULL,
    operator_version VARCHAR NOT NULL
)`

// createTags is the tag table. Its version column is a signed BIGINT rather
// than the revision table's version type, because a tag selects a numeric
// schema version even where revisions are stored under Atlas's string
// tokens.
const createTags = `CREATE TABLE IF NOT EXISTS {table} (
    tag VARCHAR(255) NOT NULL PRIMARY KEY,
    version BIGINT NOT NULL,
    recorded_at TIMESTAMP NOT NULL
)`

// createLog is the operation log. The primary key is (run_id, seq) rather
// than a generated identity, because every engine spells auto-increment
// differently and the order the log needs is the order the migrator wrote
// its entries in.
const createLog = `CREATE TABLE IF NOT EXISTS {table} (
    run_id VARCHAR(64) NOT NULL,
    seq BIGINT NOT NULL,
    operation VARCHAR(32) NOT NULL,
    version BIGINT NOT NULL,
    state VARCHAR(32) NOT NULL,
    actor VARCHAR(256) NULL,
    actor_source VARCHAR(32) NOT NULL,
    checksum VARCHAR(64) NULL,
    logged_at TIMESTAMP NOT NULL,
    error TEXT NULL,
    PRIMARY KEY (run_id, seq)
){engine}`

// respellRevision gives a revision row its file's version spelling in place.
func respellRevision() []string {
	return []string{"UPDATE {table} SET version = '{file}' WHERE version = '{recorded}'"}
}

// createSchema creates a schema with the IF NOT EXISTS guard.
const createSchema = "CREATE SCHEMA IF NOT EXISTS {schema}"

// updateRevision rewrites one revision row in place.
const updateRevision = `UPDATE {table}
SET {assignments}
WHERE version = ?`

// upsertTagOnConflict moves a tag with ON CONFLICT, as the PostgreSQL family
// and SQLite spell it.
const upsertTagOnConflict = "INSERT INTO {table} (tag, version, recorded_at) VALUES (?, ?, ?)" +
	" ON CONFLICT (tag) DO UPDATE SET version = EXCLUDED.version," +
	" recorded_at = EXCLUDED.recorded_at"

// upsertTagOnDuplicateKey moves a tag with ON DUPLICATE KEY UPDATE.
const upsertTagOnDuplicateKey = "INSERT INTO {table} (tag, version, recorded_at) VALUES (?, ?, ?)" +
	" ON DUPLICATE KEY UPDATE version = VALUES(version), recorded_at = VALUES(recorded_at)"

// conflictOnConflict and conflictOnConflictAtlas make a forced revision
// replace the row its version has, as the PostgreSQL family spells it.
const (
	conflictOnConflict = `ON CONFLICT (version) DO UPDATE SET
description = EXCLUDED.description,
applied_at = EXCLUDED.applied_at,
state = EXCLUDED.state,
applied = EXCLUDED.applied,
total = EXCLUDED.total,
error = NULL,
error_stmt = NULL,
execution_time_ms = EXCLUDED.execution_time_ms,
checksum = EXCLUDED.checksum`
	conflictOnConflictAtlas = `ON CONFLICT (version) DO UPDATE SET
description = EXCLUDED.description,
type = EXCLUDED.type,
applied = EXCLUDED.applied,
total = EXCLUDED.total,
executed_at = EXCLUDED.executed_at,
execution_time = EXCLUDED.execution_time,
error = '',
error_stmt = '',
hash = EXCLUDED.hash,
partial_hashes = EXCLUDED.partial_hashes,
operator_version = EXCLUDED.operator_version`
)

// postgresFamilyRevisionTable is how PostgreSQL, CockroachDB and YugabyteDB
// keep the migrator's tables: they share the wire protocol, the catalog and
// every statement here.
func postgresFamilyRevisionTable() descriptor.RevisionTable {
	return descriptor.RevisionTable{
		Create:      createRevisions,
		CreateAtlas: createPostgresAtlasRevisions,
		CreateTags: `CREATE TABLE IF NOT EXISTS {table} (
    tag VARCHAR(255) NOT NULL PRIMARY KEY,
    version BIGINT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL
)`,
		CreateLog:      createLog,
		CreateSchema:   createSchema,
		FirstRow:       "limit",
		RowLock:        "for-update",
		Update:         updateRevision,
		Respell:        respellRevision(),
		Conflict:       conflictOnConflict,
		ConflictAtlas:  conflictOnConflictAtlas,
		UpsertTag:      upsertTagOnConflict,
		VersionCast:    "BIGINT",
		VersionLiteral: "dollar",
		DerivedTableAs: " AS",
		AtlasSchema:    "search-path",
	}
}

// GenericRevisionTable is what the migrator writes for an engine whose
// descriptor states no revision table: the spellings several built-in
// engines share. Each call returns new values.
func GenericRevisionTable() descriptor.RevisionTable {
	return descriptor.RevisionTable{
		Create:         createRevisions,
		CreateAtlas:    createAtlasRevisions,
		CreateTags:     createTags,
		CreateLog:      createLog,
		CreateSchema:   createSchema,
		FirstRow:       "limit",
		Update:         updateRevision,
		Respell:        respellRevision(),
		UpsertTag:      upsertTagOnDuplicateKey,
		VersionCast:    "BIGINT",
		VersionLiteral: "standard",
		DerivedTableAs: " AS",
	}
}
