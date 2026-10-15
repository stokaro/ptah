package builtindialects

// The catalog queries a development database cleanup finds Ptah's revision
// tables with, where the reader hides them by name. Each binds the schema
// first; {names} stands for the placeholders of the table names.
const (
	// postgresRevisionProbe reads pg_class. relkind 'r' and 'p' are ordinary
	// and partitioned tables: anything the writer would drop under a
	// different statement is excluded, so a same-named view or sequence
	// cannot enter the plan as a table.
	postgresRevisionProbe = "\n\t\t\tSELECT c.relname" +
		"\n\t\t\tFROM pg_class c" +
		"\n\t\t\tJOIN pg_namespace n ON n.oid = c.relnamespace" +
		"\n\t\t\tWHERE n.nspname = $1" +
		"\n\t\t\t  AND c.relkind IN ('r', 'p')" +
		"\n\t\t\t  AND c.relname IN ({names})" +
		"\n\t\t\tORDER BY c.relname"
	// mysqlRevisionProbe reads information_schema. COALESCE mirrors the
	// writer's cleanup schema: the configured schema when the connection
	// pins one, otherwise the session's database.
	mysqlRevisionProbe = "\n\t\t\tSELECT table_name" +
		"\n\t\t\tFROM information_schema.tables" +
		"\n\t\t\tWHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE())" +
		"\n\t\t\t  AND table_type = 'BASE TABLE'" +
		"\n\t\t\t  AND table_name IN ({names})" +
		"\n\t\t\tORDER BY table_name"
	// sqlServerRevisionProbe reads sys.tables.
	sqlServerRevisionProbe = "\n\t\t\tSELECT t.name" +
		"\n\t\t\tFROM sys.tables AS t" +
		"\n\t\t\tJOIN sys.schemas AS s ON s.schema_id = t.schema_id" +
		"\n\t\t\tWHERE t.is_ms_shipped = 0" +
		"\n\t\t\t  AND s.name = @p1" +
		"\n\t\t\t  AND t.name IN ({names})" +
		"\n\t\t\tORDER BY t.name"
)
