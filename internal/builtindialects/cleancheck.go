package builtindialects

// The catalog queries a clean check lists a dev database's base tables with,
// on the engines whose URL selects no schema. None binds a parameter.
const (
	// sqliteCleanTables reads sqlite_master. The ESCAPE clause matters: in
	// LIKE, `_` matches any single character, so an unescaped 'sqlite_%'
	// would also hide a user table named `sqliteXthing` and under-report the
	// database.
	//
	// rowid is the catalog's order, the order the tables were created in,
	// and the first table in it is the one the pinned binary names when it
	// refuses a dev database: with `zzz_t` created before `aaa_t` it names
	// `zzz_t`.
	sqliteCleanTables = "\n\t\t\tSELECT name" +
		"\n\t\t\tFROM sqlite_master" +
		"\n\t\t\tWHERE type = 'table'" +
		"\n\t\t\t  AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'" +
		"\n\t\t\tORDER BY rowid"
	// sqlServerCleanTables reads sys.tables. A replay empties every user
	// schema of a SQL Server dev database, so the probe reads them all and
	// returns the first table in byte order. A rehearsal empties only the
	// connected schema and is refused over a table elsewhere too, since one
	// claim serves both. is_ms_shipped leaves the server's own out.
	sqlServerCleanTables = "\n\t\t\tSELECT TOP 1 s.name, t.name" +
		"\n\t\t\tFROM sys.tables t" +
		"\n\t\t\tJOIN sys.schemas s ON s.schema_id = t.schema_id" +
		"\n\t\t\tWHERE t.is_ms_shipped = 0" +
		"\n\t\t\tORDER BY s.name COLLATE Latin1_General_BIN2, t.name COLLATE Latin1_General_BIN2"
	// clickHouseCleanTables reads system.tables. A view, a dictionary and the
	// storage a materialized view keeps in an `.inner` table are not tables
	// a reset would lose data from.
	clickHouseCleanTables = "\n\t\t\tSELECT database, name" +
		"\n\t\t\tFROM system.tables" +
		"\n\t\t\tWHERE database = currentDatabase()" +
		"\n\t\t\t  AND is_temporary = 0" +
		"\n\t\t\t  AND engine NOT IN ('View', 'MaterializedView', 'LiveView', 'WindowView', 'Dictionary')" +
		"\n\t\t\t  AND NOT startsWith(name, '.inner')" +
		"\n\t\t\tORDER BY name"
	// oracleCleanTables lists the tables the Oracle reset drops: every table
	// the connected account owns that Oracle did not generate and that is
	// not already in the recycle bin. The list is the reset's own, so an
	// account that owns tables Oracle maintains, such as SYSTEM, is refused
	// rather than emptied.
	oracleCleanTables = "\n\t\t\tSELECT USER, o.object_name" +
		"\n\t\t\tFROM user_objects o" +
		"\n\t\t\tWHERE o.object_type = 'TABLE'" +
		"\n\t\t\t  AND o.object_name NOT LIKE 'BIN$%'" +
		"\n\t\t\t  AND o.generated = 'N'" +
		"\n\t\t\tORDER BY o.object_name"
)

// mysqlSchemaAttributes reads the character set and the collation a MySQL or
// MariaDB database carries. The pinned binary prints both for an empty
// database: measured on MySQL 9.7,
// `{"name":"…","charset":"utf8mb4","collate":"utf8mb4_0900_ai_ci"}`.
const mysqlSchemaAttributes = "\n\t\t\tSELECT default_character_set_name, default_collation_name" +
	"\n\t\t\tFROM information_schema.schemata" +
	"\n\t\t\tWHERE schema_name = ?"
