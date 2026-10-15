package builtindialects

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/capabilityline"
)

// oracle is Oracle Database.
//
// Names compare case-insensitively because the renderer writes a plain name
// bare and Oracle folds it to upper case: a declared `view_count` is stored as
// VIEW_COUNT, and comparing exactly would report a difference on every run
// against a catalog Ptah itself wrote. Measured -- user_tables reports
// PTAH_FOLD for a bare CREATE and ptah_fold for a quoted one, and the two
// coexist as separate tables.
//
// Index names are schema-scoped, not table-scoped: `DROP INDEX <name>` is the
// whole statement here, with no ON clause naming a table, so two tables in one
// schema cannot both carry an index called `idx_name` (stokaro/ptah#1875).
//
// Measured on 23.26: an INSERT bound with :1 is accepted, while ? and $1 both
// answer ORA-00911, invalid character.
func oracle() descriptor.Dialect {
	return descriptor.Dialect{
		Name:    "oracle",
		Aliases: []string{"oracledb"},
		Identifiers: descriptor.Identifiers{
			IndexNamespace: "schema",
			IndexNames:     "ascii_case_insensitive",
			TableNames:     "ascii_case_insensitive",
			ColumnNames:    "ascii_case_insensitive",
		},
		Placeholder: descriptor.PlaceholderColon,

		// An Oracle banner carries two numbers, and the first is not the
		// version: "Oracle AI Database 26ai Free Release 23.26.2.0.0". The
		// version is read after "Release". The IF [NOT] EXISTS guards arrived
		// with the 23 line, and the step is the whole major: 23.0 through
		// 23.25 were never measured. Ptah models no identifier limit for
		// Oracle.
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "Oracle23",
			BannerTokens:  []string{"oracle"},
			Versions: descriptor.Versions{
				Scheme: "after-release",
				Floor:  "Oracle21",
				Steps: []descriptor.VersionStep{
					{Min: "23", Preset: "Oracle23"},
				},
				Attribution: "measured-lines",
				Measured:    capabilityline.OracleMeasured(),
				Newest:      capabilityline.Oracle23,
			},
		},

		// How a connection reaches the server.
		Driver: "oracle",

		// Measured on 23.26: LIMIT answers ORA-03049, while `OFFSET n ROWS
		// FETCH NEXT m ROWS ONLY` is accepted without an ORDER BY, so Oracle
		// gets no ORDER BY sentinel and runs on every line Ptah targets.
		Query: descriptor.Query{
			OffsetFetch: true,
		},
		Scripts: descriptor.Scripts{ClientDelimiters: true},

		// Migrations: what the migrator reads.
		DDLTransactions: "no_transaction",
		RevisionTable:   oracleRevisionTable(),

		// A boolean is 1 or 0 because the type map creates BOOLEAN as
		// NUMBER(1): 21.3 has no BOOLEAN type, and internal/oracledata pins
		// this value against the map. A bare string is read through
		// NLS_TIMESTAMP_FORMAT and refused with ORA-01843, so a moment is a
		// typed TIMESTAMP literal with its offset; measured on 23.26, DATE,
		// TIMESTAMP and TIMESTAMP WITH TIME ZONE columns all accept it. Text
		// declared for a column that holds a moment is the owner's literal,
		// because which columns hold one is the type map's answer. GENERATED
		// ALWAYS refuses an explicit value with ORA-32795.
		Literals: descriptor.Literals{
			NumericBool:     true,
			BinaryPrefix:    "HEXTORAW('",
			BinarySuffix:    "')",
			TimestampPrefix: "TIMESTAMP ",
			TimestampOffset: true,
			ByOwner:         true,
		},
		ManagedData: descriptor.ManagedData{
			IdentityRefusesInsert: descriptor.IdentityInsertGeneratedAlways,
		},
		// Oracle accepts CREATE TABLE IF NOT EXISTS on 23 and refuses it on 21
		// with ORA-00922, so the tracker is created from a PL/SQL block that
		// ignores ORA-00955, name already used; one statement for both lines
		// means a 23 server measures what a 21 server is sent. RELEASE
		// SAVEPOINT answers ORA-00900 (stokaro/ptah#3330).
		Seeds: descriptor.Seeds{
			Tracker: `BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE schema_seeds (
        seed_path VARCHAR(512) PRIMARY KEY,
        env VARCHAR(128) NOT NULL,
        checksum CHAR(64) NOT NULL,
        applied_at TIMESTAMP NOT NULL
    )';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -955 THEN
            RAISE;
        END IF;
END;`,
			Transactional: true,
			Savepoint:     descriptor.Savepoint{Create: "SAVEPOINT", Rollback: "ROLLBACK TO SAVEPOINT"},
		},
	}
}

// oracleRevisionTable is how Oracle keeps the migrator's tables.
//
// Oracle has neither BIGINT nor TEXT and takes DEFAULT only before NOT NULL.
// Measured on Oracle Free 23.26.3.0.0, one column at a time:
//
//	version BIGINT                                ORA-00902: invalid datatype
//	description TEXT                              ORA-00902: invalid datatype
//	state VARCHAR(32) NOT NULL DEFAULT 'applied'  ORA-03076: unexpected item DEFAULT
//
// Every text column Ptah can write as an empty string is nullable, because
// Oracle stores ” as NULL: a NOT NULL description would refuse a migration
// that has none, and the migrator reads the NULL back as ”. The revision text
// is a CLOB, which refuses COALESCE with ” (ORA-00932), and partial_hashes is
// a CLOB that takes the JSON document as bytes (stokaro/ptah#3298).
//
// Oracle 21 has no IF NOT EXISTS, so each CREATE TABLE runs inside a PL/SQL
// block that ignores ORA-00955, "name is already used by an existing object";
// running it twice is accepted and changes nothing, measured on Oracle Free
// 23.26.3.0.0. The statement travels as a string literal, so its quotes are
// doubled. A schema is a user, and a migrator does not create accounts.
//
// Oracle commits the transaction in progress before every schema statement --
// measured on 23.26.2.0.0 and 21.3.0.0.0, a CREATE TABLE inside an explicit
// transaction survives the ROLLBACK (stokaro/ptah#1875) -- so its writer opens
// no transaction at all and every statement commits on its own;
// TestOracleMigratorKeepsEveryStatementItRanLive measures it. Its driver
// refuses a serializable isolation level by name and takes SET TRANSACTION
// instead. It has no LIMIT (`LIMIT 1` answers ORA-03049), no AS before a
// derived table's alias (ORA-03048), and Oracle 21 takes no SELECT without
// FROM.
func oracleRevisionTable() descriptor.RevisionTable {
	return descriptor.RevisionTable{
		Create: `DECLARE
    already_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(already_exists, -955);
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE {table_escaped} (
    version NUMBER(19) PRIMARY KEY,
    description CLOB NULL,
    applied_at TIMESTAMP NOT NULL,
    state VARCHAR2(32) DEFAULT ''applied'' NOT NULL,
    applied NUMBER(10) DEFAULT 1 NOT NULL,
    total NUMBER(10) DEFAULT 1 NOT NULL,
    error CLOB NULL,
    error_stmt CLOB NULL,
    execution_time_ms NUMBER(19) DEFAULT 0 NOT NULL,
    checksum VARCHAR2(64) NULL
)';
EXCEPTION
    WHEN already_exists THEN NULL;
END;`,
		CreateAtlas: `DECLARE
    already_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(already_exists, -955);
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE {table_escaped} (
    version VARCHAR2(255) PRIMARY KEY,
    description CLOB NULL,
    type NUMBER(19) DEFAULT 2 NOT NULL,
    applied NUMBER(19) DEFAULT 0 NOT NULL,
    total NUMBER(19) DEFAULT 0 NOT NULL,
    executed_at TIMESTAMP NOT NULL,
    execution_time NUMBER(19) NOT NULL,
    error CLOB NULL,
    error_stmt CLOB NULL,
    hash VARCHAR2(255) NULL,
    partial_hashes CLOB NULL,
    operator_version VARCHAR2(255) NULL
)';
EXCEPTION
    WHEN already_exists THEN NULL;
END;`,
		CreateTags: createTags,
		CreateLog: `DECLARE
    already_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(already_exists, -955);
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE {table_escaped} (
    run_id VARCHAR2(64) NOT NULL,
    seq NUMBER(19) NOT NULL,
    operation VARCHAR2(32) NOT NULL,
    version NUMBER(19) NOT NULL,
    state VARCHAR2(32) NOT NULL,
    actor VARCHAR2(256) NULL,
    actor_source VARCHAR2(32) NOT NULL,
    checksum VARCHAR2(64) NULL,
    logged_at TIMESTAMP NOT NULL,
    error CLOB NULL,
    PRIMARY KEY (run_id, seq)
)';
EXCEPTION
    WHEN already_exists THEN NULL;
END;`,
		FirstRow:          "fetch",
		EmptyStringIsNull: true,
		Update:            updateRevision,
		Respell:           respellRevision(),
		UpsertTag:         upsertTagOnDuplicateKey,
		// Oracle has no BIGINT: `CAST('12' AS BIGINT)` answers ORA-00902.
		VersionCast:       "NUMBER(19)",
		VersionLiteral:    "standard",
		LiteralRowSource:  " FROM dual",
		SerializableInSQL: true,
	}
}
