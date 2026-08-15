package devclean

// White-box testing required: the SQL Server replay policy is an internal
// safety boundary whose token-level decisions are not observable through an
// exported API.

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/lexer"
)

func TestValidateSQLServerReplayStatement_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{
			name:      "empty statement",
			statement: "-- comment only",
		},
		{
			name:      "ordinary local table",
			statement: "CREATE TABLE dbo.accounts (id bigint PRIMARY KEY, balance decimal(18,2));",
		},
		{
			name: "unsupported kind words used as local column names",
			statement: "CREATE TABLE dbo.words (" +
				"service int, queue int, route int, contract int, " +
				"certificate int, assembly int, authorization int);",
		},
		{
			name:      "local table named openquery",
			statement: "INSERT INTO [OPENQUERY] (id) VALUES (1);",
		},
		{
			name:      "local index on identifiers named server and event",
			statement: "CREATE INDEX ix_event ON server.event (id);",
		},
		{
			name:      "local select into unqualified table",
			statement: "SELECT id INTO snapshot FROM dbo.source;",
		},
		{
			name:      "local select into schema table",
			statement: "SELECT id INTO dbo.snapshot FROM dbo.source;",
		},
		{
			name:      "local select into quoted keyword",
			statement: "SELECT id INTO [IF] FROM dbo.source;",
		},
		{
			name:      "drop external table metadata",
			statement: "DROP EXTERNAL TABLE IF EXISTS dbo.remote_data;",
		},
		{
			name:      "string literal resembling into keyword",
			statement: "SELECT 'INTO', 'otherdb.dbo.snapshot';",
		},
		{
			name:      "incomplete select into remains panic safe",
			statement: "SELECT id INTO;",
		},
		{
			name: "openquery used only as source",
			statement: "INSERT INTO dbo.local_copy (id) " +
				"SELECT id FROM OPENQUERY(remote_link, 'SELECT id FROM remote_table');",
		},
		{
			name: "openrowset used only as source",
			statement: "UPDATE dbo.local_copy SET value = source.value " +
				"FROM OPENROWSET('MSOLEDBSQL', 'Server=remote;', " +
				"'SELECT id, value FROM remote_table') AS source " +
				"WHERE dbo.local_copy.id = source.id;",
		},
		{
			name: "cte openquery used only as source",
			statement: "WITH remote_rows AS (" +
				"SELECT id, value FROM OPENQUERY(remote_link, 'SELECT id, value FROM remote_table')) " +
				"UPDATE dbo.local_copy SET value = remote_rows.value " +
				"FROM remote_rows WHERE dbo.local_copy.id = remote_rows.id;",
		},
		{
			name: "temporal table",
			statement: "CREATE TABLE dbo.temporal_data (" +
				"id int PRIMARY KEY, valid_from datetime2 GENERATED ALWAYS AS ROW START, " +
				"valid_to datetime2 GENERATED ALWAYS AS ROW END, " +
				"PERIOD FOR SYSTEM_TIME (valid_from, valid_to)) " +
				"WITH (SYSTEM_VERSIONING = ON);",
		},
		{
			name: "ordinary table with ledger column",
			statement: "CREATE TABLE dbo.ledger_entries (" +
				"id bigint PRIMARY KEY, [ledger] nvarchar(64), " +
				"CHECK ([ledger] = 'ON'));",
		},
		{
			name: "local security policy",
			statement: "CREATE SECURITY POLICY dbo.tenant_policy " +
				"ADD FILTER PREDICATE dbo.tenant_filter(tenant_id) ON dbo.accounts;",
		},
		{
			name:      "local aggregate",
			statement: "CREATE AGGREGATE dbo.concat_values (@value nvarchar(max)) RETURNS nvarchar(max);",
		},
		{
			name:      "local type",
			statement: "CREATE TYPE dbo.account_id FROM bigint NOT NULL;",
		},
		{
			name:      "local xml schema collection",
			statement: "CREATE XML SCHEMA COLLECTION dbo.documents AS N'<schema />';",
		},
		{
			name:      "local schema with authorization",
			statement: "CREATE SCHEMA app AUTHORIZATION dbo;",
		},
		{
			name:      "ordinary insert",
			statement: "INSERT INTO dbo.accounts (id, balance) VALUES (1, 10);",
		},
		{
			name:      "ordinary update",
			statement: "UPDATE dbo.accounts SET balance = 20 WHERE id = 1;",
		},
		{
			name:      "ordinary delete",
			statement: "DELETE FROM dbo.accounts WHERE id = 1;",
		},
		{
			name: "ordinary merge",
			statement: "MERGE INTO dbo.accounts AS target " +
				"USING dbo.incoming AS source ON target.id = source.id " +
				"WHEN MATCHED THEN UPDATE SET balance = source.balance;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := validateSQLServerReplayStatement(sqlServerReplayTokens(test.statement))
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidateSQLServerReplayStatement_FailurePath(t *testing.T) {
	tests := []struct {
		name          string
		statement     string
		wantOperation string
	}{
		{
			name:          "dynamic exec",
			statement:     "EXEC(N'CREATE LOGIN escaped WITH PASSWORD = ''secret''');",
			wantOperation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:          "stored procedure execute",
			statement:     "EXECUTE sys.sp_addextendedproperty @name = N'outside', @value = 1;",
			wantOperation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:          "server trigger",
			statement:     "CREATE TRIGGER reject_login ON ALL SERVER FOR LOGON AS ROLLBACK;",
			wantOperation: "ON SERVER or ON ALL SERVER",
		},
		{
			name:          "server event session",
			statement:     "CREATE EVENT SESSION replay_escape ON SERVER ADD EVENT sqlserver.error_reported;",
			wantOperation: "ON SERVER or ON ALL SERVER",
		},
		{
			name:          "disable server trigger",
			statement:     "DISABLE TRIGGER reject_login ON ALL SERVER;",
			wantOperation: "ON SERVER or ON ALL SERVER",
		},
		{
			name:          "database ddl trigger",
			statement:     "CREATE TRIGGER reject_table ON DATABASE FOR CREATE_TABLE AS ROLLBACK;",
			wantOperation: "CREATE DATABASE DDL TRIGGER",
		},
		{
			name:          "enable database ddl trigger",
			statement:     "ENABLE TRIGGER reject_table ON DATABASE;",
			wantOperation: "ENABLE DATABASE DDL TRIGGER",
		},
		{
			name:          "database event notification",
			statement:     "CREATE EVENT NOTIFICATION notify_ddl ON DATABASE FOR CREATE_TABLE TO SERVICE 'svc', 'current database';",
			wantOperation: "CREATE EVENT NOTIFICATION",
		},
		{
			name:          "cross database select into three part name",
			statement:     "SELECT id INTO otherdb.dbo.snapshot FROM dbo.source;",
			wantOperation: "cross-database SELECT INTO",
		},
		{
			name:          "cross database select into omitted schema",
			statement:     "SELECT id INTO otherdb..snapshot FROM dbo.source;",
			wantOperation: "cross-database SELECT INTO",
		},
		{
			name:          "cross server select into",
			statement:     "SELECT id INTO remote_server.otherdb.dbo.snapshot FROM dbo.source;",
			wantOperation: "cross-database SELECT INTO",
		},
		{
			name:          "cte cross database select into",
			statement:     "WITH source_rows AS (SELECT id FROM dbo.source) SELECT id INTO otherdb.dbo.snapshot FROM source_rows;",
			wantOperation: "cross-database SELECT INTO",
		},
		{
			name:          "temporary select into",
			statement:     "SELECT id INTO #snapshot FROM dbo.source;",
			wantOperation: "temporary SELECT INTO target",
		},
		{
			name:          "insert openquery destination",
			statement:     "INSERT OPENQUERY(remote_link, 'SELECT id FROM remote_table') VALUES (1);",
			wantOperation: "OPENQUERY mutation destination",
		},
		{
			name:          "insert top openrowset destination",
			statement:     "INSERT TOP (10) INTO OPENROWSET('MSOLEDBSQL', 'Server=remote;', 'SELECT id FROM remote_table') VALUES (1);",
			wantOperation: "OPENROWSET mutation destination",
		},
		{
			name:          "update openquery destination",
			statement:     "UPDATE TOP (5) OPENQUERY(remote_link, 'SELECT value FROM remote_table') SET value = 1;",
			wantOperation: "OPENQUERY mutation destination",
		},
		{
			name:          "update openrowset alias destination",
			statement:     "UPDATE RemoteRows SET value = 1 FROM OPENROWSET('MSOLEDBSQL', 'Server=remote;', 'SELECT value FROM remote_table') AS remoteRows;",
			wantOperation: "OPENROWSET mutation destination",
		},
		{
			name:          "delete openrowset destination",
			statement:     "DELETE TOP (25) PERCENT FROM OPENROWSET('MSOLEDBSQL', 'Server=remote;', 'SELECT id FROM remote_table');",
			wantOperation: "OPENROWSET mutation destination",
		},
		{
			name:          "delete openquery alias destination",
			statement:     "DELETE remote_rows FROM OPENQUERY(remote_link, 'SELECT id FROM remote_table') remote_rows;",
			wantOperation: "OPENQUERY mutation destination",
		},
		{
			name:          "merge openquery destination",
			statement:     "MERGE INTO OPENQUERY(remote_link, 'SELECT id FROM remote_table') AS target USING dbo.source ON target.id = source.id;",
			wantOperation: "OPENQUERY mutation destination",
		},
		{
			name: "cte insert openquery destination",
			statement: "WITH rows_to_send AS (SELECT id FROM dbo.source) " +
				"INSERT OPENQUERY(remote_link, 'SELECT id FROM remote_table') " +
				"SELECT id FROM rows_to_send;",
			wantOperation: "OPENQUERY mutation destination",
		},
		{
			name: "openrowset cte update destination",
			statement: "WITH remote_rows (id, value) AS (" +
				"SELECT id, value FROM OPENROWSET('MSOLEDBSQL', 'Server=remote;', " +
				"'SELECT id, value FROM remote_table')) " +
				"UPDATE remote_rows SET value = 1;",
			wantOperation: "OPENROWSET mutation destination",
		},
		{
			name: "openquery second cte delete destination",
			statement: "WITH local_rows AS (SELECT id FROM dbo.source), " +
				"remote_rows AS (SELECT id FROM OPENQUERY(remote_link, " +
				"'SELECT id FROM remote_table')) " +
				"DELETE remote_rows;",
			wantOperation: "OPENQUERY mutation destination",
		},
		{
			name: "create external table as select",
			statement: "CREATE EXTERNAL TABLE dbo.remote_export " +
				"WITH (LOCATION = '/export', DATA_SOURCE = remote_source, " +
				"FILE_FORMAT = parquet_format) AS " +
				"SELECT id FROM dbo.source;",
			wantOperation: "CREATE EXTERNAL TABLE AS SELECT",
		},
		{
			name:          "bulk insert",
			statement:     `BULK INSERT otherdb.dbo.users FROM '\\host\share\users.csv';`,
			wantOperation: "BULK INSERT external data operation",
		},
		{
			name:          "backup database",
			statement:     `BACKUP DATABASE ptah_dev TO DISK = '\\host\share\ptah.bak';`,
			wantOperation: "BACKUP external storage operation",
		},
		{
			name:          "restore database",
			statement:     `RESTORE DATABASE ptah_dev FROM DISK = '\\host\share\ptah.bak';`,
			wantOperation: "RESTORE external storage operation",
		},
		{
			name:          "DBCC server operation",
			statement:     "DBCC FREEPROCCACHE;",
			wantOperation: "DBCC server operation",
		},
		{
			name: "create external table as cte select",
			statement: "CREATE EXTERNAL TABLE dbo.remote_export " +
				"WITH (LOCATION = '/export', DATA_SOURCE = remote_source, " +
				"FILE_FORMAT = parquet_format) AS " +
				"WITH source_rows AS (SELECT id FROM dbo.source) " +
				"SELECT id FROM source_rows;",
			wantOperation: "CREATE EXTERNAL TABLE AS SELECT",
		},
		{
			name: "create external table metadata",
			statement: "CREATE EXTERNAL TABLE dbo.remote_data (" +
				"id int, [as] nvarchar(32)) WITH (" +
				"LOCATION = '/data', DATA_SOURCE = remote_source);",
			wantOperation: "CREATE EXTERNAL TABLE",
		},
		{
			name:          "local dml trigger body",
			statement:     "CREATE TRIGGER dbo.audit_insert ON dbo.accounts AFTER INSERT AS SELECT 1;",
			wantOperation: "CREATE executable stored body",
		},
		{
			name:          "local procedure body",
			statement:     "CREATE OR ALTER PROCEDURE dbo.rebuild AS DELETE FROM dbo.accounts;",
			wantOperation: "CREATE executable stored body",
		},
		{
			name:          "local function body",
			statement:     "ALTER FUNCTION dbo.answer() RETURNS int AS BEGIN RETURN 42 END;",
			wantOperation: "ALTER executable stored body",
		},
		{
			name:          "local synonym",
			statement:     "CREATE SYNONYM dbo.current_accounts FOR dbo.accounts;",
			wantOperation: "CREATE SYNONYM",
		},
		{
			name:          "create ledger table",
			statement:     "CREATE TABLE dbo.ledger_data (id int PRIMARY KEY) WITH (LEDGER = ON);",
			wantOperation: "LEDGER table",
		},
		{
			name:          "alter table to enable ledger",
			statement:     "ALTER TABLE dbo.ledger_data SET (LEDGER = ON);",
			wantOperation: "LEDGER table",
		},
		{
			name:          "create user",
			statement:     "CREATE USER replay_user WITHOUT LOGIN;",
			wantOperation: "CREATE DATABASE PRINCIPAL",
		},
		{
			name:          "create application role",
			statement:     "CREATE APPLICATION ROLE replay_role WITH PASSWORD = 'secret';",
			wantOperation: "CREATE DATABASE PRINCIPAL",
		},
		{
			name:          "alter role membership",
			statement:     "ALTER ROLE replay_role ADD MEMBER replay_user;",
			wantOperation: "ALTER DATABASE PRINCIPAL",
		},
		{
			name:          "drop user",
			statement:     "DROP USER replay_user;",
			wantOperation: "DROP DATABASE PRINCIPAL",
		},
		{
			name:          "grant permission",
			statement:     "GRANT CONTROL ON SCHEMA::dbo TO replay_user;",
			wantOperation: "permission mutation",
		},
		{
			name:          "deny permission",
			statement:     "DENY SELECT TO replay_user;",
			wantOperation: "permission mutation",
		},
		{
			name:          "revoke permission",
			statement:     "REVOKE CONNECT FROM replay_user;",
			wantOperation: "permission mutation",
		},
		{
			name:          "alter authorization",
			statement:     "ALTER AUTHORIZATION ON SCHEMA::dbo TO replay_user;",
			wantOperation: "ALTER AUTHORIZATION",
		},
		{
			name:          "create assembly",
			statement:     "CREATE ASSEMBLY replay_assembly FROM 0x00;",
			wantOperation: "CREATE ASSEMBLY",
		},
		{
			name:          "create partition function",
			statement:     "CREATE PARTITION FUNCTION replay_partition(int) AS RANGE LEFT FOR VALUES (10);",
			wantOperation: "CREATE PARTITION FUNCTION",
		},
		{
			name:          "create partition scheme",
			statement:     "CREATE PARTITION SCHEME replay_scheme AS PARTITION replay_partition ALL TO ([PRIMARY]);",
			wantOperation: "CREATE PARTITION SCHEME",
		},
		{
			name:          "create fulltext catalog",
			statement:     "CREATE FULLTEXT CATALOG replay_catalog;",
			wantOperation: "CREATE FULLTEXT CATALOG",
		},
		{
			name:          "create fulltext stoplist",
			statement:     "CREATE FULLTEXT STOPLIST replay_stoplist;",
			wantOperation: "CREATE FULLTEXT STOPLIST",
		},
		{
			name:          "create search property list",
			statement:     "CREATE SEARCH PROPERTY LIST replay_properties;",
			wantOperation: "CREATE SEARCH PROPERTY LIST",
		},
		{
			name:          "create certificate",
			statement:     "CREATE CERTIFICATE replay_certificate WITH SUBJECT = 'replay';",
			wantOperation: "CREATE CERTIFICATE",
		},
		{
			name:          "create database master key",
			statement:     "CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'secret';",
			wantOperation: "CREATE MASTER KEY",
		},
		{
			name:          "create symmetric key",
			statement:     "CREATE SYMMETRIC KEY replay_symmetric WITH ALGORITHM = AES_256 ENCRYPTION BY PASSWORD = 'secret';",
			wantOperation: "CREATE SYMMETRIC KEY",
		},
		{
			name:          "create asymmetric key",
			statement:     "CREATE ASYMMETRIC KEY replay_asymmetric WITH ALGORITHM = RSA_2048;",
			wantOperation: "CREATE ASYMMETRIC KEY",
		},
		{
			name:          "create column master key",
			statement:     "CREATE COLUMN MASTER KEY replay_column_master WITH (KEY_STORE_PROVIDER_NAME = 'provider', KEY_PATH = 'path');",
			wantOperation: "CREATE COLUMN MASTER KEY",
		},
		{
			name:          "create column encryption key",
			statement:     "CREATE COLUMN ENCRYPTION KEY replay_column_encryption WITH VALUES (COLUMN_MASTER_KEY = replay_column_master, ALGORITHM = 'RSA_OAEP', ENCRYPTED_VALUE = 0x00);",
			wantOperation: "CREATE COLUMN ENCRYPTION KEY",
		},
		{
			name:          "create event session",
			statement:     "CREATE EVENT SESSION replay_session ON DATABASE ADD EVENT sqlserver.error_reported;",
			wantOperation: "CREATE EVENT SESSION",
		},
		{
			name:          "create broker message type",
			statement:     "CREATE MESSAGE TYPE [replay_message] VALIDATION = NONE;",
			wantOperation: "CREATE MESSAGE TYPE",
		},
		{
			name:          "create broker contract",
			statement:     "CREATE CONTRACT replay_contract ([replay_message] SENT BY INITIATOR);",
			wantOperation: "CREATE CONTRACT",
		},
		{
			name:          "create broker queue",
			statement:     "CREATE QUEUE dbo.replay_queue;",
			wantOperation: "CREATE QUEUE",
		},
		{
			name:          "create broker service",
			statement:     "CREATE SERVICE replay_service ON QUEUE dbo.replay_queue (replay_contract);",
			wantOperation: "CREATE SERVICE",
		},
		{
			name:          "create broker route",
			statement:     "CREATE ROUTE replay_route WITH SERVICE_NAME = 'replay_service', ADDRESS = 'LOCAL';",
			wantOperation: "CREATE ROUTE",
		},
		{
			name:          "create remote service binding",
			statement:     "CREATE REMOTE SERVICE BINDING replay_binding TO SERVICE 'replay_service' WITH USER = replay_user;",
			wantOperation: "CREATE REMOTE SERVICE BINDING",
		},
		{
			name:          "create broker priority",
			statement:     "CREATE BROKER PRIORITY replay_priority FOR CONVERSATION SET (CONTRACT_NAME = ANY);",
			wantOperation: "CREATE BROKER PRIORITY",
		},
		{
			name:          "create resource pool",
			statement:     "CREATE RESOURCE POOL replay_pool;",
			wantOperation: "CREATE RESOURCE POOL",
		},
		{
			name:          "alter resource governor",
			statement:     "ALTER RESOURCE GOVERNOR RECONFIGURE;",
			wantOperation: "ALTER RESOURCE GOVERNOR",
		},
		{
			name:          "create workload group",
			statement:     "CREATE WORKLOAD GROUP replay_group USING replay_pool;",
			wantOperation: "CREATE WORKLOAD GROUP",
		},
		{
			name:          "create cryptographic provider",
			statement:     "CREATE CRYPTOGRAPHIC PROVIDER replay_provider FROM FILE = 'provider.dll';",
			wantOperation: "CREATE CRYPTOGRAPHIC PROVIDER",
		},
		{
			name:          "database scoped credential inherited rejection",
			statement:     "CREATE DATABASE SCOPED CREDENTIAL replay_credential WITH IDENTITY = 'identity';",
			wantOperation: "CREATE DATABASE",
		},
		{
			name:          "external data source inherited rejection",
			statement:     "CREATE EXTERNAL DATA SOURCE replay_source WITH (LOCATION = 'https://example.invalid');",
			wantOperation: "CREATE EXTERNAL",
		},
		{
			name:          "database audit specification inherited rejection",
			statement:     "CREATE DATABASE AUDIT SPECIFICATION replay_audit FOR SERVER AUDIT audit_target;",
			wantOperation: "CREATE DATABASE",
		},
		{
			name:          "database scoped configuration inherited rejection",
			statement:     "ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 1;",
			wantOperation: "ALTER DATABASE",
		},
		{
			name:          "database file inherited rejection",
			statement:     "ALTER DATABASE current_database ADD FILE (NAME = replay_file, FILENAME = 'replay.ndf');",
			wantOperation: "ALTER DATABASE",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := validateSQLServerReplayStatement(sqlServerReplayTokens(test.statement))
			c.Assert(
				err,
				qt.ErrorMatches,
				`sqlserver migration replay rejects `+
					regexp.QuoteMeta(test.wantOperation)+
					` because its effects cannot be confined to the disposable database realm`,
			)
		})
	}
}

func sqlServerReplayTokens(statement string) []lexer.Token {
	return significantTokens(statement, replayLexerOptions(platform.SQLServer))
}
