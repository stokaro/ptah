package descriptor

// Comparison states facts about an engine that comparing a declared schema
// with the catalog it is applied to depends on. migration/schemadiff,
// internal/indexbacking, internal/foreignkeyscope, internal/dbexprprobe and
// migration/internal/tableidentity read it and check the enumerated values
// when they do. The zero value is an engine with none of these properties.
type Comparison struct {
	// ForeignKeysOutside reports that a foreign key may name a table outside
	// the schemas a description holds, and the engine accepts it; an
	// unqualified table is then in DefaultSchema.
	ForeignKeysOutside bool `json:"foreign_keys_outside,omitempty"`
	// VerbatimTableNames reports that a table is identified by the exact
	// bytes the catalog holds rather than by the name comparison rules.
	VerbatimTableNames bool `json:"verbatim_table_names,omitempty"`
	// UniqueIndexStandsAlone reports that a unique index backs no UNIQUE
	// constraint: the two are separate objects, or the engine has no UNIQUE
	// constraint at all.
	UniqueIndexStandsAlone bool `json:"unique_index_stands_alone,omitempty"`
	// ForeignKeyBackingIndex reports that the server creates and reports an
	// index for every foreign key.
	ForeignKeyBackingIndex bool `json:"foreign_key_backing_index,omitempty"`
	// InternalIndexPrefix begins the name of an index the engine builds for
	// itself and no statement can name, such as SQLite's autoindexes.
	InternalIndexPrefix string `json:"internal_index_prefix,omitempty"`
	// ExpressionProbes is the catalog the server is asked how it stores a
	// declared expression: "pg-catalog" (CHECK, column, trigger, view and
	// routine spellings, through PostgreSQL's catalog functions),
	// "sys-catalog" (CHECK expressions, through SQL Server's), or empty for
	// none.
	ExpressionProbes string `json:"expression_probes,omitempty"`
	// ProbeViewsInSchema reports that an expression probe creates its view
	// in a schema rather than as a temporary view.
	ProbeViewsInSchema bool `json:"probe_views_in_schema,omitempty"`
	// RewritesGeneratedExpressions reports that the server stores a
	// generated column's expression in a form of its own, so only the server
	// can say what a declaration becomes.
	RewritesGeneratedExpressions bool `json:"rewrites_generated_expressions,omitempty"`
	// RewritesRoutineBodies reports that the server stores a routine body in
	// a form of its own, reflowed and qualified, rather than as written.
	RewritesRoutineBodies bool `json:"rewrites_routine_bodies,omitempty"`
	// RolePasswords reports that the catalog can say whether a role has a
	// password, which a comparison that sets one must establish first.
	RolePasswords bool `json:"role_passwords,omitempty"`
	// DefaultGeneratedKind is the storage a generated column declared
	// without one gets, such as "STORED". Empty means it has no default.
	DefaultGeneratedKind string `json:"default_generated_kind,omitempty"`
}
