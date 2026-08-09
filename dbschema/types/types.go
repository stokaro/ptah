// Package types defines the database-agnostic model of an introspected live
// schema (DBSchema and its tables, columns, indexes, and constraints) shared by
// the dbschema readers and writers and consumed by schema diffing.
package types

import (
	"context"
	"strings"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/tableref"
)

// DBSchema represents the complete schema read from a database
type DBSchema struct {
	Schemas     []DBSchemaInfo `json:"schemas"`
	Tables      []DBTable      `json:"tables"`
	Enums       []DBEnum       `json:"enums"`
	Indexes     []DBIndex      `json:"indexes"`
	Constraints []DBConstraint `json:"constraints"`
	Extensions  []DBExtension  `json:"extensions"`   // PostgreSQL extensions
	Functions   []DBFunction   `json:"functions"`    // PostgreSQL custom functions
	Sequences   []DBSequence   `json:"sequences"`    // PostgreSQL standalone sequences
	Domains     []DBDomain     `json:"domains"`      // PostgreSQL domain types
	Composites  []DBComposite  `json:"composites"`   // PostgreSQL composite types
	Ranges      []DBRange      `json:"ranges"`       // PostgreSQL range types
	Views       []DBView       `json:"views"`        // Database views
	MatViews    []DBMatView    `json:"matviews"`     // Database materialized views
	Triggers    []DBTrigger    `json:"triggers"`     // Database triggers
	RLSPolicies []DBRLSPolicy  `json:"rls_policies"` // PostgreSQL RLS policies
	Roles       []DBRole       `json:"roles"`        // PostgreSQL roles
	Grants      []DBGrant      `json:"grants"`       // PostgreSQL privilege grants

	// RolesOutOfScope lists roles that exist on the server but that this
	// description deliberately does not define, because nothing in the
	// schemas being read refers to them.
	//
	// PostgreSQL roles belong to the cluster rather than to one database, so
	// "the reader did not describe this role" and "this role does not exist"
	// are different facts, and a comparator that cannot tell them apart plans
	// CREATE ROLE for a role that is already there. Roles and RolesOutOfScope
	// partition the roles Ptah manages, so their union answers the
	// comparator's question -- does this role exist -- whatever scoping rule
	// decided the description. See stokaro/ptah#1267 and stokaro/ptah#1276.
	//
	// The partition is over managed roles, not over the whole catalog. A
	// PostgreSQL reader leaves the reserved pg_ roles and the bootstrap
	// superuser out of both lists, because Ptah manages neither in either
	// direction, so the union is not every role the server has and must not be
	// described as if it were. A desired schema that names one of them is
	// refused before anything is compared or planned, naming the role and the
	// rule, rather than compared against nothing. See stokaro/ptah#1312.
	//
	// This field is not part of the description: it carries role names from
	// outside the inspected scope, so it is never serialized and never
	// rendered. Only Roles reaches output.
	RolesOutOfScope []DBRole `json:"-"`

	// NotDescribed records what this read did not look at, so a comparator can
	// tell an object the database does not have from one the reader was never
	// asked about. The zero value claims the read covered everything; see
	// [go.5x5.cz/ptah/core/coverage].
	NotDescribed coverage.Set `json:"not_described,omitzero"`
}

// DBSchemaInfo represents a database schema/namespace.
type DBSchemaInfo struct {
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
	Charset string `json:"charset,omitempty"`
	Collate string `json:"collate,omitempty"`
}

// DBTable represents a database table
type DBTable struct {
	Name          string     `json:"name"`
	Schema        string     `json:"schema,omitempty"`
	Type          string     `json:"type"` // TABLE, VIEW, etc.
	Comment       string     `json:"comment"`
	Columns       []DBColumn `json:"columns"`
	EstimatedRows int64      `json:"estimated_rows,omitempty"` // Best-effort planner estimate from database statistics
	RLSEnabled    bool       `json:"rls_enabled"`              // Whether RLS is enabled on this table (PostgreSQL)
	Strict        bool       `json:"strict,omitempty"`         // SQLite STRICT table option
	WithoutRowID  bool       `json:"without_rowid,omitempty"`  // SQLite WITHOUT ROWID table option
}

// QualifiedName returns schema.table when Schema is set, or Name otherwise.
func (t DBTable) QualifiedName() string {
	return QualifyTableName(t.Schema, t.Name)
}

// QualifyTableName returns an unambiguous schema-qualified table reference.
func QualifyTableName(schema, table string) string {
	return tableref.Canonical(schema, table)
}

// DBColumn represents a database column.
//
// GeneratedKind / GeneratedExpression are populated by readers for dialects
// that expose generated column metadata. Schema comparison matches these fields
// when the goschema-side model also carries generated column metadata.
//
// DomainName and FormattedType are a pair. DataType reports a domain column's
// BASE type, so it alone cannot say that the declared type was a domain:
// DomainName carries that fact and FormattedType carries the spelling the
// server uses for it, schema-qualified where the search path needs that.
// Everything that answers "what type is this column declared as" -- the
// desired-state conversion, the Atlas-compatible JSON inspect output, and the
// comparator's database side -- must consult DomainName rather than infer the
// answer from DataType, which reports the base type and drops the domain's
// constraints with it. See stokaro/ptah#1242.
type DBColumn struct {
	Name               string  `json:"name"`
	DataType           string  `json:"data_type"`
	UDTName            string  `json:"udt_name"`                 // For PostgreSQL enum types
	FormattedType      string  `json:"formatted_type,omitempty"` // Server's own spelling, where the catalog cannot express it
	ColumnType         string  `json:"column_type"`              // For MySQL ENUM syntax
	IsNullable         string  `json:"is_nullable"`              // YES/NO
	ColumnDefault      *string `json:"column_default"`           // Can be NULL
	CharacterMaxLength *int    `json:"character_max_length"`     // For VARCHAR, etc.
	Charset            string  `json:"charset,omitempty"`        // MySQL/MariaDB column character set
	Collate            string  `json:"collate,omitempty"`        // MySQL/MariaDB column collation
	NumericPrecision   *int    `json:"numeric_precision"`        // For DECIMAL, etc.
	NumericScale       *int    `json:"numeric_scale"`            // For DECIMAL, etc.
	OrdinalPosition    int     `json:"ordinal_position"`
	IsAutoIncrement    bool    `json:"is_auto_increment"` // Derived field
	IsPrimaryKey       bool    `json:"is_primary_key"`    // Derived field
	IsUnique           bool    `json:"is_unique"`         // Derived field

	// DomainName names the domain a column is declared with, empty for every
	// column whose declared type is not a domain (PostgreSQL only today).
	//
	// It is a separate fact rather than something read back out of
	// FormattedType, because FormattedType is filled for arrays as well and the
	// two shapes want opposite answers: an array's spelling is a TYPE, which may
	// be compared and normalized like any other, while a domain's spelling is an
	// IDENTIFIER its author chose and must only ever be compared by identity.
	// Nothing but the catalog can tell them apart -- a domain over an array is
	// reported with data_type "ARRAY" exactly like a plain array column
	// (stokaro/ptah#1138).
	DomainName string `json:"domain_name,omitempty"`

	// DomainSchema names the schema holding that domain, empty for every column
	// whose declared type is not a domain.
	//
	// A domain's identity is (schema, name), and the name alone is not it:
	// public.status and other.status are two different types with different
	// CHECK constraints over possibly different base types. A comparator handed
	// only "status" for both calls a column that must be converted unchanged,
	// while the plan still drops the domain the desired schema no longer
	// declares -- so DROP DOMAIN ... CASCADE takes the column and its data with
	// nothing having converted it first (stokaro/ptah#1138).
	//
	// It is recorded raw, exactly as information_schema.columns.domain_schema
	// reports it, rather than blanked for the schema being read: the domain a
	// column is declared with may live in a schema the read never visits, and
	// blanking it there erases the very distinction this field exists for.
	// FormattedType cannot stand in, because the server writes a qualifier
	// there only when the search path forces one.
	DomainSchema string `json:"domain_schema,omitempty"`

	// GeneratedExpression holds the generated-column expression. Nil for plain
	// columns.
	GeneratedExpression *string `json:"generated_expression,omitempty"`
	// GeneratedKind names the generated-column kind, for example STORED,
	// VIRTUAL, MATERIALIZED, ALIAS, or EPHEMERAL. Empty for plain columns.
	GeneratedKind string `json:"generated_kind,omitempty"`

	// IdentityGeneration records an identity column's generation mode, mirroring
	// the goschema-side field: "ALWAYS" (GENERATED ALWAYS AS IDENTITY, which
	// rejects explicit inserts), "BY_DEFAULT" (GENERATED BY DEFAULT AS IDENTITY,
	// which accepts them), or "" for non-identity columns. Populated by readers
	// for dialects that expose identity metadata (currently PostgreSQL via
	// pg_attribute.attidentity).
	IdentityGeneration string `json:"identity_generation,omitempty"`
}

// DBEnum represents a database enum type (PostgreSQL)
type DBEnum struct {
	Name string `json:"name"`
	// Schema owns the enum. Readers blank it for the connection's own schema,
	// exactly as they do for tables, views and domains, so a filter or a
	// comparison reconstructs the qualified spelling from the connection's
	// default. Without it a schema-qualified `--exclude app.color` matched
	// nothing and silently kept the enum (stokaro/ptah#933).
	Schema string   `json:"schema,omitempty"`
	Values []string `json:"values"`
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (e DBEnum) QualifiedName() string { return QualifyTableName(e.Schema, e.Name) }

// DBDomain represents a PostgreSQL domain type read from the database.
type DBDomain struct {
	Name     string `json:"name"`
	Schema   string `json:"schema,omitempty"`
	BaseType string `json:"base_type"`
	NotNull  bool   `json:"not_null"`
	Default  string `json:"default,omitempty"`
	Check    string `json:"check,omitempty"`
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (d DBDomain) QualifiedName() string { return QualifyTableName(d.Schema, d.Name) }

// DBCompositeField is a single field of a composite type read from the database.
type DBCompositeField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// DBComposite represents a PostgreSQL composite type read from the database.
type DBComposite struct {
	Name   string             `json:"name"`
	Schema string             `json:"schema,omitempty"`
	Fields []DBCompositeField `json:"fields"`
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (c DBComposite) QualifiedName() string { return QualifyTableName(c.Schema, c.Name) }

// DBRange represents a PostgreSQL range type read from the database.
//
// Everything after Subtype exists so a change to an EXISTING range type can be
// detected. While the reader returned only the name and subtype, the comparator
// had nothing to compare and reported a changed range as converged
// (stokaro/ptah#931 item 2).
type DBRange struct {
	Name    string `json:"name"`
	Schema  string `json:"schema,omitempty"`
	Subtype string `json:"subtype"`
	// SubtypeOpClass is the operator class backing the subtype's ordering
	// (pg_range.rngsubopc). Always populated by the catalog, including when the
	// author never named one, so the comparator only consults it when the
	// desired schema declares one.
	SubtypeOpClass string `json:"subtype_opclass,omitempty"`
	// Collation is the subtype collation (pg_range.rngcollation), empty for
	// non-collatable subtypes.
	Collation string `json:"collation,omitempty"`
	// Canonical is the canonicalization function (pg_range.rngcanonical), empty
	// when the range has none.
	Canonical string `json:"canonical,omitempty"`
	// SubtypeDiff is the subtype difference function (pg_range.rngsubdiff),
	// empty when the range has none.
	SubtypeDiff string `json:"subtype_diff,omitempty"`
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (r DBRange) QualifiedName() string { return QualifyTableName(r.Schema, r.Name) }

// DBIndexPart represents one ordered key column in an introspected index.
type DBIndexPart struct {
	Name string `json:"name"`
	// Expr is a raw indexed expression, such as lower(name). It is mutually
	// exclusive with Name: an expression is not an identifier, and a reader
	// that reports one in Name makes the renderer quote it into a column
	// reference that does not exist.
	Expr string `json:"expr,omitempty"`
	// Operator is the operator class this key was built with, for example
	// text_pattern_ops, together with its parameters when it has any, for
	// example tsvector_ops(siglen=64). Empty means the key uses its type's
	// default class with no parameters, which is the only case where omitting
	// it from emitted DDL reproduces the index. See #1242.
	//
	// A parameterised class is carried even when it is the type's default,
	// because the class name is the only place its parameters can hang.
	Operator string `json:"operator,omitempty"`
	Desc     bool   `json:"desc,omitempty"`
	// NullsOrder is an explicit NULLS ordering for this key: "FIRST", "LAST",
	// or empty when the key uses the direction's default. PostgreSQL defaults
	// to NULLS LAST for ASC and NULLS FIRST for DESC, so only the deviating
	// spelling has to be carried.
	NullsOrder string `json:"nulls_order,omitempty"`
}

// Index NULLS ordering spellings for DBIndexPart.NullsOrder.
const (
	NullsOrderFirst = "FIRST"
	NullsOrderLast  = "LAST"
)

// DBIndex represents a database index.
//
// Most fields are dialect-neutral. The Type/Expression/Granularity trio is
// populated only by the ClickHouse reader for data-skipping indexes; other
// readers leave them at their zero values so the diff layer does not start
// emitting spurious type/granularity changes for PostgreSQL or MySQL
// indexes.
type DBIndex struct {
	Name      string   `json:"name"`
	TableName string   `json:"table_name"`
	Schema    string   `json:"schema,omitempty"`
	Columns   []string `json:"columns"`
	// Parts preserves key order and direction when the database exposes it.
	// Empty means the reader supplied only the legacy ascending Columns form.
	Parts      []DBIndexPart `json:"parts,omitempty"`
	IsUnique   bool          `json:"is_unique"`
	IsPrimary  bool          `json:"is_primary"`
	Definition string        `json:"definition"` // Full index definition
	// KeyPartsIncomplete reports that the catalog described a key part this
	// reader cannot name, so Columns (and Parts) list fewer parts than the key
	// actually has. The MySQL reader sets it for a functional key part --
	// `KEY idx ((b + 1))` -- whose information_schema.STATISTICS row carries a
	// NULL COLUMN_NAME and an EXPRESSION column that MariaDB does not have.
	//
	// A comparison must not read the key columns of such an index as the whole
	// key: it would plan a rebuild on every run for a key that never changed.
	// Everything else about the index is reported normally.
	KeyPartsIncomplete bool `json:"key_parts_incomplete,omitempty"`
	// Condition is the WHERE clause for partial indexes when the dialect
	// exposes one structurally.
	Condition string `json:"condition,omitempty"`
	// Comment is the index's own object comment, as PostgreSQL keeps it in
	// pg_description for the index relation and reports through
	// obj_description(indexrelid, 'pg_class').
	//
	// It is the index's, not the table's: COMMENT ON INDEX and COMMENT ON TABLE
	// address different objects, and an index has a slot for one on every
	// surface below this model -- goschema.Index.Comment, the Atlas-compatible
	// HCL reader's `comment` attribute, and the HCL writer's. Before #1242 this
	// field did not exist, so a comment the server reported was dropped between
	// the catalog and the model and every surface below it saw an index that
	// had none. Empty means the object carries no comment.
	Comment string `json:"comment,omitempty"`
	// NullsDistinct carries PostgreSQL UNIQUE INDEX NULLS [NOT] DISTINCT
	// state. Nil means the clause was not present in the definition.
	NullsDistinct *bool `json:"nulls_distinct,omitempty"`

	// Method is the index access method as the server spells it -- btree,
	// gin, gist, brin, hash. It is deliberately not Type: Type is the
	// ClickHouse data-skipping-index type below, a different concept that
	// happens to share a slot in the annotation surface, and overloading one
	// field with both would make a ClickHouse "bloom_filter" and a PostgreSQL
	// "gin" indistinguishable at this layer. Empty means the reader did not
	// report an access method.
	//
	// A dropped access method is not always a quiet degradation: an index on
	// a type with no btree operator class, such as point, does not replay at
	// all without it. See #1242.
	Method string `json:"method,omitempty"`
	// IncludeColumns carries PostgreSQL INCLUDE payload columns, the
	// non-key columns stored in the index for index-only scans.
	IncludeColumns []string `json:"include_columns,omitempty"`
	// StorageParams carries the index's WITH (...) storage parameters, keyed by
	// parameter name with the value as the server spells it.
	//
	// A reader records only the parameters the rest of the model can carry back
	// out again. A parameter recorded here but dropped by one of the surfaces
	// the model passes through would make every such index differ from its own
	// inspected document forever, and the rebuild that difference plans would
	// drop the parameter it was meant to protect. See #1242 and
	// docs/conformance.md for the ones a PostgreSQL reader deliberately omits.
	StorageParams map[string]string `json:"storage_params,omitempty"`

	// RequiresExtensions names the extensions this index cannot be built
	// without, as the catalog resolved them rather than as the DDL spells them.
	//
	// An index key names an operator class only when that class is not the
	// default for its type on its access method, so an index whose default
	// class comes from an extension refers to that extension with no token at
	// all. Measured on PostgreSQL 17.10: with btree_gin installed,
	// CREATE INDEX t_gin ON t USING gin (n int4_ops) over an integer column is
	// stored, and rendered back, as CREATE INDEX t_gin ON public.t USING gin
	// (n) -- and replaying that on a database without btree_gin fails with
	// `data type integer has no default operator class for access method
	// "gin"`. pg_index.indclass holds the class each key actually resolved to,
	// so the reader answers the question exactly (stokaro/ptah#1286).
	//
	// The index's access method is read the same way, from pg_class.relam. An
	// access method the catalog owns, `gin` or `gist`, belongs to no extension
	// and pins nothing; one an extension supplies, such as bloom's `bloom`,
	// resolves to that extension.
	//
	// The edge is recorded whether or not the DDL prints the class. It does print
	// a non-default one -- pg_trgm's gin_trgm_ops arrives as
	// CREATE INDEX w_trgm ON public.w USING gin (txt gin_trgm_ops) -- and that
	// index resolves to pg_trgm here as well. This field answers "which
	// extension owns what this index resolved to", not "which dependency the DDL
	// omitted".
	//
	// Empty means the reader found no such edge, which is the ordinary case:
	// core supplies the default operator class for every core type its access
	// methods index. Readers with no catalog to ask leave it unset.
	RequiresExtensions []string `json:"requires_extensions,omitempty"`

	// Type is the ClickHouse data-skipping-index type. One of
	// "minmax" / "set(N)" / "bloom_filter" / "bloom_filter(p)" /
	// "tokenbf_v1(...)" / "ngrambf_v1(...)" etc. Empty on non-ClickHouse
	// readers.
	Type string `json:"type,omitempty"`
	// Expression is the full ClickHouse skipping-index expression
	// (column reference, function call, tuple, etc.). The reader also writes
	// the expression into Columns[0] for back-compat with the existing diff
	// layer; Expression is the canonical field for richer diffing once
	// that's wired up. Empty on non-ClickHouse readers.
	Expression string `json:"expression,omitempty"`
	// Granularity is the GRANULARITY value the index was declared with.
	// Non-zero only on ClickHouse skipping indexes.
	Granularity int `json:"granularity,omitempty"`
}

// QualifiedTableName returns schema.table when Schema is set, or TableName otherwise.
func (i DBIndex) QualifiedTableName() string {
	return QualifyTableName(i.Schema, i.TableName)
}

// DBConstraint represents a database constraint
type DBConstraint struct {
	Name           string   `json:"name"`
	TableName      string   `json:"table_name"`
	Schema         string   `json:"schema,omitempty"`
	Type           string   `json:"type"` // PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, EXCLUDE
	ColumnName     string   `json:"column_name"`
	ColumnNames    []string `json:"column_names,omitempty"`
	ForeignTable   *string  `json:"foreign_table"` // For foreign keys
	ForeignSchema  string   `json:"foreign_schema,omitempty"`
	ForeignColumn  *string  `json:"foreign_column"` // For foreign keys
	ForeignColumns []string `json:"foreign_columns,omitempty"`
	DeleteRule     *string  `json:"delete_rule"`  // CASCADE, RESTRICT, etc.
	UpdateRule     *string  `json:"update_rule"`  // CASCADE, RESTRICT, etc.
	CheckClause    *string  `json:"check_clause"` // For CHECK constraints
	// NullsDistinct carries PostgreSQL UNIQUE NULLS [NOT] DISTINCT state.
	// Nil means the clause was not present in the definition.
	NullsDistinct *bool `json:"nulls_distinct,omitempty"`
	// IncludeColumns carries PostgreSQL INCLUDE columns for covering UNIQUE
	// and PRIMARY KEY constraints.
	IncludeColumns []string `json:"include_columns,omitempty"`
	// EXCLUDE constraint specific fields (PostgreSQL only)
	UsingMethod     *string `json:"using_method"`     // Index method: gist, btree, etc.
	ExcludeElements *string `json:"exclude_elements"` // Elements with operators: "room_id WITH =, during WITH &&"
	WhereCondition  *string `json:"where_condition"`  // Optional WHERE clause for EXCLUDE constraints

	// RequiresExtensions names the extensions the index backing this constraint
	// cannot be built without, read from that index exactly as
	// [DBIndex.RequiresExtensions] is.
	//
	// It is a separate field rather than a lookup because the backing index is
	// not part of the entity model: a constraint owns its index, and the
	// converter drops the index row so the constraint is rendered once. The
	// dependency is as hard to read off the text here as on a plain index, and
	// for the same reason. Measured on PostgreSQL 17.10,
	// `EXCLUDE USING gist (room WITH =, during WITH &&)` over an integer column
	// needs btree_gist and pg_get_constraintdef prints no token of it, while
	// `EXCLUDE USING gist (txt gist_trgm_ops WITH =)` needs pg_trgm and prints
	// the class, because a class is printed exactly when it is not the default
	// (stokaro/ptah#1286).
	RequiresExtensions []string `json:"requires_extensions,omitempty"`
}

// QualifiedTableName returns schema.table when Schema is set, or TableName otherwise.
func (c DBConstraint) QualifiedTableName() string {
	return QualifyTableName(c.Schema, c.TableName)
}

// QualifiedForeignTableName returns schema.table for a foreign key target.
func (c DBConstraint) QualifiedForeignTableName() string {
	if c.ForeignTable == nil {
		return ""
	}
	return QualifyTableName(c.ForeignSchema, *c.ForeignTable)
}

// ColumnNamesOrDefault returns all local constraint columns, falling back to
// the legacy single-column field for callers that have not populated slices.
func (c DBConstraint) ColumnNamesOrDefault() []string {
	if len(c.ColumnNames) > 0 {
		return c.ColumnNames
	}
	if c.ColumnName != "" {
		return []string{c.ColumnName}
	}
	return nil
}

// ForeignColumnsOrDefault returns all referenced FK columns, falling back to
// the legacy single-column field for older readers and test fixtures.
func (c DBConstraint) ForeignColumnsOrDefault() []string {
	if len(c.ForeignColumns) > 0 {
		return c.ForeignColumns
	}
	if c.ForeignColumn != nil && *c.ForeignColumn != "" {
		return []string{*c.ForeignColumn}
	}
	return nil
}

// DBExtension represents a PostgreSQL extension installed in the database
type DBExtension struct {
	Name             string  `json:"name"`              // Extension name (pg_trgm, postgis, etc.)
	Version          string  `json:"version"`           // Installed version
	Schema           string  `json:"schema"`            // Schema where extension is installed
	Relocatable      bool    `json:"relocatable"`       // Whether extension can be moved between schemas
	Comment          *string `json:"comment"`           // Extension comment/description
	DefaultVersion   *string `json:"default_version"`   // Default version available
	InstalledVersion *string `json:"installed_version"` // Currently installed version (may differ from default)

	// Provides lists the catalog names this extension supplies -- its types,
	// functions, relations, operator classes and operator families -- read from
	// pg_depend. It answers "what would stop resolving if this extension were
	// not here", which is a different question from the extension's own name and
	// usually a disjoint set of words: `isn` supplies the type `isbn`, and
	// `pgcrypto` supplies the function `gen_salt`.
	//
	// Names pg_catalog also supplies are excluded, because a document using one
	// resolves it with the extension dropped. This matters because contrib
	// extensions mostly supply overloads of core functions: unfiltered, `citext`
	// contributes `max`, `min`, `strpos`, `replace` and `split_part`, and
	// `pgcrypto` contributes `gen_random_uuid`, which core has supplied since
	// PostgreSQL 13.
	//
	// A function name pg_get_keywords() reports as a SQL keyword is excluded too,
	// because a name read out of SQL text carries no position: `hstore` supplies
	// three functions named `delete`, and `DELETE FROM audit` in a plpgsql body
	// is indistinguishable from a call to `delete(h, 'k')`. That exclusion is
	// conditional on the redundancy that makes it free -- the same extension
	// must also contribute to this list a type appearing in that function's
	// signature, so a genuine call spells the type and the type entry answers
	// for it. An extension supplying `merge(text, text)` and no type has its
	// only evidence in the name, and the name is kept.
	//
	// Empty means not measured rather than "supplies nothing" -- every extension
	// supplies something, and readers that do not consult a catalog (Go
	// annotations, YAML) leave this unset.
	Provides []string `json:"provides,omitempty"`
}

// DBSequence represents a standalone PostgreSQL sequence read from the database.
//
// Only user-declared, standalone sequences appear here. Implicit sequences that
// back SERIAL / BIGSERIAL / identity columns (those OWNED BY a column via an
// internal/auto dependency) are deliberately excluded so that declaring a plain
// SERIAL column does not surface as a spurious standalone sequence.
type DBSequence struct {
	Name      string `json:"name"`                // Sequence name
	Schema    string `json:"schema,omitempty"`    // Schema containing the sequence
	DataType  string `json:"data_type,omitempty"` // Underlying integer type (e.g. "bigint")
	Start     *int64 `json:"start,omitempty"`     // START WITH value
	Increment *int64 `json:"increment,omitempty"` // INCREMENT BY value
	MinValue  *int64 `json:"min_value,omitempty"` // MINVALUE bound
	MaxValue  *int64 `json:"max_value,omitempty"` // MAXVALUE bound
	Cache     *int64 `json:"cache,omitempty"`     // CACHE size
	Cycle     bool   `json:"cycle"`               // Whether the sequence uses CYCLE
	OwnedBy   string `json:"owned_by,omitempty"`  // Owning table.column, if any
	Comment   string `json:"comment,omitempty"`   // Sequence comment/description
}

// QualifiedName returns schema.sequence when Schema is set, or Name otherwise.
func (s DBSequence) QualifiedName() string {
	return QualifyTableName(s.Schema, s.Name)
}

// DBInfo contains connection and metadata information
type DBInfo struct {
	Dialect             string                  `json:"dialect"` // postgres, mysql, mariadb
	Version             string                  `json:"version"`
	Schema              string                  `json:"schema"`               // public, database name, etc.
	URL                 string                  `json:"url"`                  // database connection URL (for reference)
	Capabilities        capability.Capabilities `json:"capabilities"`         // resolved from Dialect + Version for live connections
	IdentifierSemantics identifier.Semantics    `json:"identifier_semantics"` // catalog identifier metadata and static rules
}

// SchemaReader interface for reading database schemas
type SchemaReader interface {
	ReadSchema() (*DBSchema, error)
}

// SchemaExecutor executes SQL statements produced by schema operations.
//
// ExecuteSQL accepts a context and an optional slice of arguments that are
// bound as native driver parameters, mirroring database/sql's ExecContext.
// Use placeholders (`?` or the dialect-native form such as `$1`/`$2` for
// PostgreSQL) instead of interpolating values into the SQL string; this
// prevents the SQL injection class of bugs that the no-args signature used
// to invite (see issue #130). Identifiers (table/column names) cannot be
// parameterized — route them through a validated escape helper instead.
type SchemaExecutor interface {
	ExecuteSQL(ctx context.Context, sql string, args ...any) error
	IsDryRun() bool
}

// SchemaWriter writes schemas to databases.
type SchemaWriter interface {
	SchemaExecutor
	// DropAllTables removes supported user objects from the writer's configured
	// schema or database. Implementations must refuse cleanup when dependencies
	// or database-global ownership prevent them from confining the destructive
	// effect to that scope. The context governs object discovery and DDL.
	// Implementations may use a short, bounded cleanup context after
	// cancellation to restore connection-local settings before returning.
	DropAllTables(ctx context.Context) error
	BeginTransaction(ctx context.Context) (SchemaTransaction, error)
	SetDryRun(dryRun bool)
}

// SchemaTransaction executes schema changes inside a database transaction.
//
// It deliberately owns transaction lifecycle instead of storing the active
// transaction on the parent SchemaWriter. That keeps dialect writers reentrant:
// concurrent callers may begin independent transactions without racing over
// shared writer state.
type SchemaTransaction interface {
	SchemaExecutor
	Commit() error
	Rollback() error
}

// DBFunction represents a PostgreSQL custom function read from the database
type DBFunction struct {
	Name string `json:"name"` // Function name
	// Schema owns the function. Readers blank it for the connection's own
	// schema, the same convention tables, views and domains follow, so a
	// filter reconstructs the qualified spelling from the connection's
	// default. Without it a schema-qualified `--exclude app.fn_app` matched
	// nothing and silently kept the function (stokaro/ptah#933).
	Schema     string `json:"schema,omitempty"`
	Parameters string `json:"parameters"` // Function parameters (e.g., "tenant_id_param TEXT")
	Returns    string `json:"returns"`    // Return type (e.g., "VOID", "TEXT")
	Language   string `json:"language"`   // Function language (e.g., "plpgsql", "sql")
	Security   string `json:"security"`   // Security context (e.g., "DEFINER", "INVOKER")
	Volatility string `json:"volatility"` // Function volatility (e.g., "STABLE", "IMMUTABLE", "VOLATILE")
	Body       string `json:"body"`       // Function body/implementation
	Comment    string `json:"comment"`    // Function comment/description
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (f DBFunction) QualifiedName() string { return QualifyTableName(f.Schema, f.Name) }

// DBView represents a database view read from the database.
type DBView struct {
	Name        string `json:"name"`         // View name
	Schema      string `json:"schema"`       // Schema where the view is defined
	Body        string `json:"body"`         // SELECT query used as the view definition
	CheckOption string `json:"check_option"` // NONE, LOCAL, CASCADED, or dialect equivalent
	Comment     string `json:"comment"`      // View comment/description
}

// QualifiedName returns schema.view when Schema is set, or Name otherwise.
func (v DBView) QualifiedName() string {
	return QualifyTableName(v.Schema, v.Name)
}

// DBMatView represents a PostgreSQL materialized view read from the database.
type DBMatView struct {
	Name            string `json:"name"`             // Materialized view name
	Schema          string `json:"schema"`           // Schema where the materialized view is defined
	Body            string `json:"body"`             // SELECT query used as the materialized view definition
	RefreshStrategy string `json:"refresh_strategy"` // Ptah-managed refresh policy; database introspection defaults to manual
	Comment         string `json:"comment"`          // Materialized view comment/description
}

// QualifiedName returns schema.materialized_view when Schema is set, or Name otherwise.
func (v DBMatView) QualifiedName() string {
	return QualifyTableName(v.Schema, v.Name)
}

// DBTrigger represents a database trigger read from the database.
type DBTrigger struct {
	Name    string `json:"name"`    // Trigger name
	Schema  string `json:"schema"`  // Schema where the trigger is defined
	Table   string `json:"table"`   // Target table
	Timing  string `json:"timing"`  // BEFORE, AFTER, or INSTEAD OF
	Event   string `json:"event"`   // INSERT, UPDATE, DELETE, or TRUNCATE
	ForEach string `json:"for"`     // ROW or STATEMENT
	Body    string `json:"body"`    // Trigger body
	Comment string `json:"comment"` // Trigger comment/description
}

// QualifiedTable returns schema.table when Schema is set, or Table otherwise.
func (t DBTrigger) QualifiedTable() string {
	return QualifyTableName(t.Schema, t.Table)
}

// DBRLSPolicy represents a PostgreSQL RLS policy read from the database
type DBRLSPolicy struct {
	Name                string `json:"name"`                  // Policy name
	Table               string `json:"table"`                 // Target table name
	PolicyFor           string `json:"policy_for"`            // Operations policy applies to (e.g., "ALL", "SELECT")
	ToRoles             string `json:"to_roles"`              // Target roles (e.g., "app_user", "PUBLIC")
	UsingExpression     string `json:"using_expression"`      // USING clause expression
	WithCheckExpression string `json:"with_check_expression"` // WITH CHECK clause expression
	Comment             string `json:"comment"`               // Policy comment/description
}

// DBRole represents a PostgreSQL role read from the database
type DBRole struct {
	Name        string `json:"name"`         // Role name
	Login       bool   `json:"login"`        // Whether role can login
	Superuser   bool   `json:"superuser"`    // Whether role is superuser
	CreateDB    bool   `json:"create_db"`    // Whether role can create databases
	CreateRole  bool   `json:"create_role"`  // Whether role can create other roles
	Inherit     bool   `json:"inherit"`      // Whether role inherits privileges
	Replication bool   `json:"replication"`  // Whether role can initiate replication
	HasPassword bool   `json:"has_password"` // Whether role has a password set
	Comment     string `json:"comment"`      // Role comment/description
}

// DBGrant represents a PostgreSQL privilege grant read from the database.
type DBGrant struct {
	Role       string `json:"role"`                 // Role receiving the privilege
	Privilege  string `json:"privilege"`            // Granted privilege, e.g. SELECT or USAGE
	ObjectType string `json:"object_type"`          // TABLE, SCHEMA, or SEQUENCE
	Schema     string `json:"schema,omitempty"`     // Schema containing the target object
	ObjectName string `json:"object_name"`          // Target table or schema name
	WithOption bool   `json:"with_option"`          // Whether the grant has WITH GRANT OPTION
	GrantedBy  string `json:"granted_by,omitempty"` // Grantor role
}

// QualifiedTarget returns schema.object for table grants and the schema name
// itself for schema grants.
func (g DBGrant) QualifiedTarget() string {
	if strings.EqualFold(g.ObjectType, "SCHEMA") {
		return g.ObjectName
	}
	return QualifyTableName(g.Schema, g.ObjectName)
}
