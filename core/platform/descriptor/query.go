package descriptor

// Query states how an engine spells the parts of a statement the query
// builder, core/query, renders differently per engine. The zero value is the
// common spelling: every join, LIMIT and OFFSET bound as signed integers, a
// bare OFFSET where the caller gave no limit, no upsert, and every construct
// decided by the dialect rather than by the target's capabilities.
type Query struct {
	// ChecksCapabilities makes the renderer decide a WITH clause, a correlated
	// subquery, a join condition other than column equalities and an OFFSET
	// without a LIMIT by the target's capabilities, and refuse the one the
	// target lacks. Without it the renderer writes each as the dialect spells
	// it, whatever the capabilities say.
	ChecksCapabilities bool `json:"checks_capabilities,omitempty"`
	// UnsupportedJoins are the joins the engine cannot run. The renderer
	// refuses them rather than send a statement the server rejects.
	UnsupportedJoins []Join `json:"unsupported_joins,omitempty"`
	// OffsetFetch limits rows with `OFFSET n ROWS FETCH NEXT m ROWS ONLY`
	// rather than with LIMIT and OFFSET. OFFSET is written even when only a
	// limit was given, as `OFFSET 0 ROWS`.
	OffsetFetch bool `json:"offset_fetch,omitempty"`
	// FetchOrderBy is the ORDER BY term written in front of a row limit the
	// caller ordered by nothing, for an engine that refuses OFFSET and FETCH
	// without an ORDER BY, such as "(SELECT NULL)". Empty writes none.
	FetchOrderBy string `json:"fetch_order_by,omitempty"`
	// OffsetOnlyLimit is the LIMIT literal written in front of an OFFSET the
	// caller gave without a limit, for an engine that accepts OFFSET only
	// after a LIMIT, such as "-1". Empty writes the OFFSET alone.
	OffsetOnlyLimit string `json:"offset_only_limit,omitempty"`
	// UnsignedBounds binds the row limit and offset as unsigned 64-bit
	// integers, for an engine that refuses a signed one there, and refuses a
	// negative bound.
	UnsignedBounds bool `json:"unsigned_bounds,omitempty"`
	// Upsert is how the engine writes an inserted row over a row it collides
	// with. Empty means the builder writes no upsert for the engine.
	Upsert Upsert `json:"upsert,omitempty"`
	// UpsertRefusal says why the engine has no ON CONFLICT clause the builder
	// writes, such as "SQL Server has no ON CONFLICT: it expresses an upsert
	// as MERGE". The renderer refuses an ON CONFLICT clause with it. Empty
	// refuses with a sentence that names the dialect.
	UpsertRefusal string `json:"upsert_refusal,omitempty"`
	// UpdateRefusal says why the engine does not run a plain UPDATE
	// statement. When it is set, the renderer refuses every UPDATE with it.
	UpdateRefusal string `json:"update_refusal,omitempty"`
}

// Join names a kind of join the query builder writes.
type Join string

const (
	// JoinInner is an INNER JOIN.
	JoinInner Join = "inner"
	// JoinLeft is a LEFT [OUTER] JOIN.
	JoinLeft Join = "left"
	// JoinRight is a RIGHT [OUTER] JOIN.
	JoinRight Join = "right"
	// JoinFull is a FULL OUTER JOIN.
	JoinFull Join = "full"
)

// Upsert is how an engine writes an inserted row over a row it collides with.
type Upsert string

const (
	// UpsertOnConflict is `ON CONFLICT (columns) DO UPDATE SET ...` or
	// `DO NOTHING`, watching the unique index on the columns the caller named
	// and reading the proposed row as `excluded`.
	UpsertOnConflict Upsert = "on-conflict"
	// UpsertOnDuplicateKey is `ON DUPLICATE KEY UPDATE`, which fires for every
	// unique key on the table and reads the proposed row with VALUES().
	UpsertOnDuplicateKey Upsert = "on-duplicate-key"
	// UpsertInto is an `UPSERT INTO` statement, which writes each row over the
	// row with the same primary key, in place of an INSERT with a clause.
	UpsertInto Upsert = "upsert-into"
)

// Scripts states how a script of several statements is cut into them.
type Scripts struct {
	// BatchSeparator is the word that ends a batch when it stands alone on
	// its line, optionally followed by a repeat count, as GO does in SQL
	// Server's client tools. It is matched without regard to letter case.
	// Empty means the engine's clients have none.
	BatchSeparator string `json:"batch_separator,omitempty"`
	// ClientDelimiters reports whether a script may change its statement
	// terminator with a MySQL DELIMITER line or an `-- atlas:delimiter`
	// directive. An engine whose text the shared rewriter cannot read leaves
	// it false, and the directives stay in the text as written.
	ClientDelimiters bool `json:"client_delimiters,omitempty"`
}
