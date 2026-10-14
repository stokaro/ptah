package descriptor

// RevisionTable states how the migrator keeps its own tables on an engine:
// the revision table in Ptah's layout and in Atlas's, the tag table and the
// operation log, and the shape of the statements that read and write them.
// migration/migrator reads it and checks the enumerated values when it does.
//
// The zero value describes no revision table, and the migrator then writes
// the generic statements it writes for an engine it has no description of.
// A built-in engine states every field, including those whose value is the
// generic one, so that its description can be read on its own.
//
// A statement template names what the migrator fills in with a placeholder
// in braces:
//
//   - {table}: the table's name, schema-qualified and quoted;
//   - {table_escaped}: the same name with every single quote doubled, for a
//     statement that carries the CREATE TABLE inside a string literal;
//   - {object}: the table's unquoted, schema-qualified name as a national
//     string literal, N'schema.table';
//   - {engine}: the table-engine clause: " ENGINE = <name>" when the caller
//     names an engine, and EngineClause otherwise;
//   - {schema}, {schema_escaped} and {schema_object}: the same three forms of
//     a schema name, in CreateSchema;
//   - {assignments}: the SET list of an Update;
//   - {file} and {recorded}: the version a directory's file spells and the
//     version a revision row records, both decimal digits, in Respell.
type RevisionTable struct {
	// Create creates the revision table in Ptah's layout when it is absent.
	// A template that names no {engine} gives the table no engine clause, and
	// when CreateAtlas names none either, a configured engine is refused.
	Create string `json:"create"`
	// CreateAtlas creates the revision table in Atlas's layout when it is
	// absent.
	CreateAtlas string `json:"create_atlas"`
	// CreateTags creates the table that maps a tag to a schema version.
	CreateTags string `json:"create_tags"`
	// CreateLog creates the append-only operation log.
	CreateLog string `json:"create_log"`
	// CreateSchema creates the schema a configured revision table lives in
	// when it is absent. Empty means the migrator creates no schema: the
	// engine has none, a schema is something the migrator must not create,
	// or creating the table creates its schema.
	CreateSchema string `json:"create_schema,omitempty"`
	// EngineClause is what {engine} expands to when the caller names no
	// table engine, such as " ENGINE=InnoDB".
	EngineClause string `json:"engine_clause,omitempty"`
	// RequiredEngine is the one table engine a configured engine may name,
	// compared without regard to case, such as "InnoDB". Empty means any.
	RequiredEngine string `json:"required_engine,omitempty"`
	// QualifiedByDefault makes the migrator always qualify its tables: by the
	// configured schema, else the connection's schema, else DefaultSchema.
	// It then creates no schema named like DefaultSchema, which always
	// exists.
	QualifiedByDefault bool `json:"qualified_by_default,omitempty"`
	// AddedColumns are the definitions the migrator gives a column it adds
	// to a revision table an older layout created, keyed by column. A column
	// it lists no definition for takes the generic one.
	AddedColumns map[string]string `json:"added_columns,omitempty"`
	// FirstRow is how a read limits itself to its first row: "limit"
	// (LIMIT 1), "fetch" (FETCH FIRST 1 ROWS ONLY) or "top" (SELECT TOP (1)).
	FirstRow string `json:"first_row"`
	// EmptyStringIsNull reports that the engine stores a zero-length string
	// as NULL and keeps revision text in a type that refuses COALESCE with an
	// empty string.
	EmptyStringIsNull bool `json:"empty_string_is_null,omitempty"`
	// RowLock is how a read of every revision locks the rows it read until
	// its transaction ends: "for-update" (a FOR UPDATE suffix),
	// "table-hint" (WITH (UPDLOCK, HOLDLOCK) after the table), or empty for
	// none.
	RowLock string `json:"row_lock,omitempty"`
	// Update rewrites one revision row, chosen by version. Its placeholders
	// are {table} and {assignments}.
	Update string `json:"update"`
	// Respell gives a revision row of Ptah's layout the version spelling its
	// file has, in one or more statements run in order. Its placeholders are
	// {table}, {file} and {recorded}.
	Respell []string `json:"respell"`
	// Mutations reports that the engine changes a stored row only through an
	// asynchronous mutation and runs no transaction around revision writes,
	// so a revision row cannot be rewritten atomically.
	Mutations bool `json:"mutations,omitempty"`
	// Upserts reports that the engine has an UPSERT statement, which writes a
	// row whether or not its key exists.
	Upserts bool `json:"upserts,omitempty"`
	// Conflict is the clause that turns the INSERT of a forced revision in
	// Ptah's layout into a write that replaces the row its version has.
	// Empty means the engine has none.
	Conflict string `json:"conflict,omitempty"`
	// ConflictAtlas is Conflict for Atlas's layout.
	ConflictAtlas string `json:"conflict_atlas,omitempty"`
	// UpsertTag points a tag at a version whether or not the tag exists. It
	// binds the tag, the version and the time, in that order, and its one
	// placeholder is {table}.
	UpsertTag string `json:"upsert_tag"`
	// VersionCast is the type an Atlas version token is cast to for numeric
	// comparison, such as "BIGINT".
	VersionCast string `json:"version_cast"`
	// VersionLiteral is how a revision version is written as a literal whose
	// bytes do not depend on a session setting: "standard" ('...' with
	// doubled quotes), "dollar" (standard, or dollar-quoted when the value
	// holds a backslash), "hex" (X'...'), "backslash" (standard with each
	// backslash doubled), "national" (N'...') or "yql" (a typed YQL Utf8
	// literal).
	VersionLiteral string `json:"version_literal"`
	// LiteralRowSource follows each literal of a SELECT that names no table,
	// such as " FROM dual" where the engine takes no SELECT without FROM.
	LiteralRowSource string `json:"literal_row_source,omitempty"`
	// DerivedTableAs is the keyword before a derived table's alias, " AS",
	// or empty where the engine refuses one.
	DerivedTableAs string `json:"derived_table_as,omitempty"`
	// UnionByName reports that UNION ALL pairs columns by name rather than
	// by position, so every branch names its column.
	UnionByName bool `json:"union_by_name,omitempty"`
	// JSONAsText reports that the revision table keeps a JSON document in a
	// text column, so the migrator binds it as a string rather than as
	// bytes.
	JSONAsText bool `json:"json_as_text,omitempty"`
	// TimeAsText reports that the migrator binds a revision timestamp as
	// RFC 3339 text rather than as a time value.
	TimeAsText bool `json:"time_as_text,omitempty"`
	// TypedNulls reports that the engine refuses an untyped NULL argument,
	// so an absent text value is bound as a typed nil.
	TypedNulls bool `json:"typed_nulls,omitempty"`
	// SerializableInSQL reports that the driver refuses a serializable
	// isolation level by name, so the migrator asks for it in SQL, as the
	// first statement of the transaction.
	SerializableInSQL bool `json:"serializable_in_sql,omitempty"`
	// DeleteCountsKeys reports that a DELETE by key reports a row affected
	// whether or not a row held the key, so the migrator counts the row and
	// deletes it in one serializable transaction to learn whether it existed.
	DeleteCountsKeys bool `json:"delete_counts_keys,omitempty"`
	// AtlasSchema is when Atlas keeps its revision table in a schema of its
	// own, atlas_schema_revisions: "database" when the URL names no
	// database, "search-path" when the URL pins no search_path, or empty for
	// never.
	AtlasSchema string `json:"atlas_schema,omitempty"`
}
