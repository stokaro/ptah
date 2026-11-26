// Package catalog defines the database-agnostic model of an introspected live
// schema (Database and its tables, columns, indexes, and constraints) shared by
// the dbschema readers and writers and consumed by schema diffing.
//
// The name says which side of the comparison this is. A schema Ptah reads from
// a server is the CURRENT state; what an authoring source declares is the
// DESIRED one, and that lives in core/goschema. Under the old name the two
// were both `types`, one of them nested under `dbschema/`, and a reader had to
// know which import a file had picked to tell them apart -- while the type
// named `Database` here is a catalog and the type named `DBSchema` was not a
// schema (stokaro/ptah#2246 section 2.1).
//
// The DB prefix went with it. It repeated the package for every type, so
// `types.DBTable` said "table" twice and `catalog.Table` says it once.
package catalog

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/normalize"
	"ptah.run/internal/tableref"
)

// Database is the complete description one schema read produced: the CURRENT
// side of a schema comparison, where the desired side is core/goschema. The
// dbschema readers fill it from a live server; any other [SchemaReader]
// implementation, stokaro/ptah-testkit's fakes included, constructs it
// directly.
//
// The Schema field the object types here carry follows one convention: a
// reader blanks it for an object in the read's default schema and fills it
// for an object outside that schema. Empty therefore does not mean
// "no schema" -- it means the connection's default, which
// ServerInfo.IdentifierSemantics.DefaultSchema names and from which the
// qualified spelling is reconstructed. It is a convention rather than an
// invariant: a reader is free to fill the default schema instead of blanking
// it, so a consumer tolerates both spellings rather than relying on the
// blank. Both sides of a comparison then have to build their object keys the
// same way, through the QualifiedName methods, which delegate to
// [QualifyTableName]. Keying the two sides differently is how a synced table
// turns into a phantom CREATE and DROP pair (stokaro/ptah#1244,
// stokaro/ptah#1991). FeatureObjects use the identities defined by their
// model owners; those identities do not inherit this convention.
//
// NotDescribed records what the read deliberately did not look at, which is a
// different fact from an object being absent; see that field.
type Database struct {
	// ReservedNames preserves provider-owned storage exclusions during comparison.
	ReservedNames []ReservedName `json:"reserved_names,omitempty"`
	// FeatureObjects holds individually named feature objects, including table-owned children.
	FeatureObjects schemaext.Objects `json:"feature_objects,omitzero"`
	// FeatureCoverage records the model definitions and scopes this source actually describes.
	FeatureCoverage schemaext.Coverage `json:"feature_coverage,omitzero"`
	// PlanningContextObjects holds objects that are context for planning
	// only, and PlanningContextCoverage what the read established about them
	// across the database. Two reads fill them. A provider's filtered read
	// fills both with the objects of its planning context kinds that it left
	// out of its scope. The ClickHouse read fills PlanningContextObjects with
	// the definition of each row policy it records in FeatureCoverage as not
	// described, such as one on a view, which the down migration of a plan
	// that drops the view creates again. A comparison never reads them, so
	// they neither appear nor disappear in a diff.
	PlanningContextObjects  schemaext.Objects  `json:"planning_context_objects,omitzero"`
	PlanningContextCoverage schemaext.Coverage `json:"planning_context_coverage,omitzero"`
	// Facets carries typed settings owned by feature providers.
	Facets      schemaext.Facets   `json:"facets,omitzero"`
	Schemas     []Schema           `json:"schemas"`
	Tables      []Table            `json:"tables"`
	Enums       []Enum             `json:"enums"`
	Indexes     []Index            `json:"indexes"`
	Constraints []Constraint       `json:"constraints"`
	Functions   []Function         `json:"functions"` // PostgreSQL custom functions
	Sequences   []Sequence         `json:"sequences"` // PostgreSQL standalone sequences
	Views       []View             `json:"views"`     // Database views
	MatViews    []MaterializedView `json:"matviews"`  // Database materialized views

	Triggers    []Trigger   `json:"triggers"`     // Database triggers
	RLSPolicies []RLSPolicy `json:"rls_policies"` // PostgreSQL RLS policies
	Roles       []Role      `json:"roles"`        // PostgreSQL roles
	Grants      []Grant     `json:"grants"`       // PostgreSQL privilege grants
	// ObjectOwners are the owners of the objects this read covers, one row per
	// object, on the engines that have an owner to report.
	//
	// They are observations, never declarations. A comparison reads the
	// owner of a view or a materialized view here only where the desired
	// schema declares one for it, and a plan that drops and creates such an
	// object again reads here the owner to give it back. Nothing turns a row
	// into a declared owner or a declared role: an inspect writes no owner,
	// and treating ownership as part of the description is what made an
	// inspect describe the connecting superuser and plan a CREATE ROLE for it
	// (stokaro/ptah#1950).
	ObjectOwners []ObjectOwner `json:"object_owners,omitempty"`

	// RoleMemberships are the role-in-role edges the server reports: who
	// inherits whose privileges. An endpoint may be outside the managed roles.
	// Analysis reads these edges independently of role definitions. Engines
	// with declared memberships also compare them when coverage permits it. A
	// saved plan's source fingerprint covers the part its plan depends on.
	RoleMemberships []RoleMembership `json:"role_memberships,omitempty"`

	// WideAccess is the access to every relation, or to every relation of one
	// schema, that no object grant states, which the security analysis reads
	// to measure a role's reach. Nil means the reader does not establish it,
	// and an empty non-nil slice means it read it and found none.
	WideAccess []WideAccess `json:"-"`

	// WideGrants are the table privileges a role holds on every table, or on
	// every table of one schema, through a grant that names no table, such as
	// MySQL's `GRANT SELECT ON *.*`, Oracle's SELECT ANY TABLE, or Oracle's
	// SELECT ANY TABLE ON SCHEMA. A REVOKE on one table takes none of them.
	// Nil means the reader does not establish them, and an empty non-nil slice
	// means it read them and found none.
	WideGrants []WideGrant `json:"-"`

	// WideGrantsPartial reports that the read was refused some of the grants
	// WideGrants describes: an Oracle account that may read DBA_SYS_PRIVS and
	// not DBA_SCHEMA_PRIVS reads the grants on every table and not those on
	// one schema. Each grant WideGrants holds is held, and a grant missing
	// from it may be held too.
	WideGrantsPartial bool `json:"-"`

	// RoutinePrivileges are the privileges granted on the routines of the
	// databases the read describes, to every account, where a reader reads
	// them for a plan that replaces a routine. The comparison does not read
	// them. See [RoutinePrivileges].
	RoutinePrivileges RoutinePrivileges `json:"-"`

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
	RolesOutOfScope []Role `json:"-"`

	// RolesOutOfScopeReason is why the reader left RolesOutOfScope out of the
	// description, in that reader's words: a clause that completes
	// "because", which names no role. It is empty where the reader states no
	// reason, and the note that counts the roles then words the cause from
	// the dialect. Like RolesOutOfScope it is never serialized.
	RolesOutOfScopeReason string `json:"-"`

	// CharsetDefaultCollations is a server fact: the collation the server
	// gives a column or a table that names a character set and no collation,
	// keyed by the character set's name in lower case. A MySQL or MariaDB
	// read takes it from information_schema.CHARACTER_SETS, where MariaDB
	// reports the defaults its character_set_collations setting chooses:
	// utf8mb4 maps to utf8mb4_0900_ai_ci on MySQL 8.4 and to
	// utf8mb4_uca1400_ai_ci on MariaDB 11.8. On MySQL the utf8mb4 entry is
	// the session's default_collation_for_utf8mb4, which CHARACTER_SETS does
	// not follow. Other readers leave it nil.
	//
	// It describes no object, so it is never serialized. A comparison that
	// does not find a character set here leaves a column that inherits from
	// it uncompared.
	CharsetDefaultCollations map[string]string `json:"-"`

	// CharsetAliases is a server fact: the character set each alias names in
	// the session the read ran in, keyed by the alias in lower case. A MySQL
	// or MariaDB read asks the server what utf8 names: utf8mb3 on MySQL 8.4,
	// and on MariaDB utf8mb3 where old_mode holds UTF8_IS_UTF8MB3, its
	// default, and utf8mb4 where it does not. Other readers leave it nil, and
	// a comparison then reads the aliases the dialect's descriptor states.
	// Like CharsetDefaultCollations, it is never serialized.
	CharsetAliases map[string]string `json:"-"`

	// NotDescribed records what this read did not look at, so a comparator can
	// tell an object the database does not have from one the reader was never
	// asked about. The zero value claims the read covered everything; see
	// [ptah.run/core/coverage].
	NotDescribed coverage.Set `json:"not_described,omitzero"`

	// UnregisteredVirtualTables lists the SQLite virtual tables this read found
	// whose module the reading build does not register.
	//
	// It is a fact about the reading build, not about the schema: the same
	// database read by a build that registers the module yields an empty list.
	// So it is observation context, kept on the read beside DatabasePath, and
	// not part of any table's SQLite owner facets, which describe the table.
	//
	// It is the description's own statement that part of this database could
	// not be classified. SQLite reports a table as `shadow` only when the
	// module that owns it is loaded and claims the name, so with the module
	// absent a virtual table is still recognized as virtual while its private
	// storage arrives as ordinary user tables. Tables therefore holds objects
	// that are not user tables, and no field on any of them says so -- the fact
	// belongs to the read, not to a row.
	//
	// A comparator that ignores this plans DROP TABLE for a module's storage.
	// Measured on stokaro/ptah#1028: against an fts4 database this build cannot
	// load, `ptah schema apply` planned and executed five drops at exit 0 and
	// `MATCH` went from returning a row to `SQL logic error`.
	//
	// It is separate from Tables on purpose, because narrowing survives it. The
	// tables at risk are not the virtual table an operator would exclude, so a
	// selection that removes the virtual table leaves every dangerous row in
	// place; a signal carried on the excluded row would vanish exactly when it
	// matters. See [ptah.run/internal/sqlitevirtual].
	//
	// It is never serialized: the same database read by a build that registers
	// the module yields an empty list, so this describes the reader rather than
	// the database, and a description that carried it would not be portable.
	UnregisteredVirtualTables []VirtualTable `json:"-"`

	// UnreadTables lists the tables this read found and does not describe,
	// each with the reader's reason, such as a YDB replica table no
	// replication writes any more. A table named here is one the database
	// holds, so a comparison refuses a declaration at it rather than plan a
	// CREATE TABLE the database would reject.
	//
	// It is a fact about the read, not part of the description, so it is never
	// serialized: a document made from this read names nothing about these
	// tables, and a filter keeps them, since narrowing the description cannot
	// make the database lose a table.
	UnreadTables []UnreadTable `json:"-"`

	// UnmodeledObjects lists the objects this read found of a kind Ptah does not
	// model, and the kinds it could not list, for a report to name. Nothing
	// compares them, so a difference never comes from one and no plan touches
	// one.
	//
	// It is a fact about the read, not part of the description, so it is never
	// serialized: no document can declare such an object, and a document that
	// recorded one would limit nothing. A filter keeps the list, since narrowing
	// the description cannot make the database lose the object.
	UnmodeledObjects []UnmodeledObject `json:"-"`

	// RemovalBlockers lists the objects this read found outside the model that
	// name a table, or some of its columns, and block their removal, such as a
	// Spanner change stream. A plan reads them to refuse a drop the database
	// would refuse; nothing compares them, so a difference never comes from
	// one.
	//
	// It is a fact about the read, not part of the description, so it is never
	// serialized: no document can declare such an object, and a document made
	// from this read names nothing about it. A filter keeps the list, since
	// narrowing the description cannot make the database lose the object.
	RemovalBlockers []RemovalBlocker `json:"-"`

	// OutOfScope holds what an exclude selector took out of this description
	// that the database keeps beside the tables the description holds: the
	// columns, indexes and constraints of those tables, and every trigger and
	// view, as the read reported them. An object out of scope is kept as it
	// is, and a comparison never reads one.
	//
	// A target that changes a table by writing a new one and copying the
	// rows into it, as SQLite does, writes the new table from the
	// declaration, which the selector left without these parts. Its plan
	// reads them here to carry each part into the new table unchanged, and
	// refuses where it cannot. A view or a trigger of another table that
	// names the table must still work after the copy takes its name, so the
	// plan reads those too. It is a fact about the read, not part of the
	// description, so it is never serialized.
	OutOfScope TableParts `json:"-"`

	// DatabasePath is the absolute path of the database this read describes,
	// such as /local, on a target whose statements name some objects by an
	// absolute path rather than relative to the connection: YDB, whose ALTER
	// SEQUENCE takes the sequence behind a Serial column only by its absolute
	// path, and whose GRANT takes the database itself the same way, and on its
	// older lines an object at the database root too. A plan built against
	// this read names those objects with it.
	//
	// An executable provider supplies its exact session database binding,
	// including a selected dev realm. Empty from readers that supply no such
	// context, and from a description that did not come from a live read. It
	// describes where the database is rather than what it holds, so it is never
	// serialized: a description applied elsewhere uses the target's binding.
	DatabasePath string `json:"-"`

	// CurrentAccount is the account the reading connection ran as, such as
	// the MySQL family's CURRENT_USER(): a fact about the reading connection,
	// not about the database. A plan applied through the same connection
	// creates objects as this account, so beside [Function.Definer] and
	// [View.Definer] it tells a replacement that keeps a routine's or a view's
	// definer from one that changes it.
	//
	// Empty from a reader that does not record it, and from a description that
	// did not come from a live read. Like DatabasePath, it is never
	// serialized: the same database read by another account describes the
	// same schema.
	CurrentAccount string `json:"-"`

	// CurrentDatabase is the database the reading connection selected, where
	// the read describes that one database and its objects carry no schema,
	// as the MySQL family's DATABASE() names it. An unqualified name in the
	// read is in this database, and the grants the read reports are those on
	// it. Empty from a read of a whole server, from a reader that does not
	// record it, and from a description that did not come from a live read.
	// Like CurrentAccount, it is never serialized.
	CurrentDatabase string `json:"-"`

	// ConnectedRole is the role the reading connection ran as, spelled the way
	// [RoleMembership] spells a role, which on the MySQL family leaves out the
	// host that [Database.CurrentAccount] carries. A plan applied through the
	// same connection holds what this role holds, so the memberships it
	// reaches are part of what a saved plan's source fingerprint covers.
	//
	// Empty from a reader that reads no memberships or does not record it, and
	// from a description that did not come from a live read. Like
	// CurrentAccount, it is never serialized: the same database read by another
	// role describes the same schema.
	ConnectedRole string `json:"-"`
	// ConnectedRoleSuperuser reports whether the PostgreSQL connection acts as
	// the owner when it revokes an object's privileges.
	ConnectedRoleSuperuser bool `json:"-"`
	// StoredNames reports that every name is the spelling the engine's catalog
	// stores, which is what a live read reports, rather than the spelling a
	// declaration wrote. It matters only on an engine whose descriptor states an
	// UnquotedFold: there a stored name that is a plain identifier and not in
	// the fold's case was created quoted, so no unquoted name reaches it, and
	// Ptah names it quoted when it compares, converts or renders the read.
	// Without it the same name is taken as written, as a schema predicted from
	// a declaration writes it, and folds. The Oracle reader sets it.
	StoredNames bool `json:"stored_names,omitempty"`
	// Predicted reports that this description was converted from a
	// declaration, as a prediction of what a server builds from it, rather
	// than read from a server. A comparison fills in what a declaration leaves
	// to the server, such as the kind of a generated column declared without
	// one, only on a predicted description. On a read, an empty value is what
	// the server reported, and a guess there could hide a difference. It is a
	// fact about where the description came from, so it is never serialized.
	Predicted bool `json:"-"`
}

// TableParts are parts of tables, each naming the table it belongs to, and
// the views beside them. A column belongs to the table that holds it in
// Tables, and such a table holds only those columns. The zero value holds no
// part.
type TableParts struct {
	Tables      []Table      `json:"tables,omitempty"`
	Indexes     []Index      `json:"indexes,omitempty"`
	Constraints []Constraint `json:"constraints,omitempty"`
	Triggers    []Trigger    `json:"triggers,omitempty"`
	Views       []View       `json:"views,omitempty"`
}

// IsEmpty reports whether p holds no part.
func (p TableParts) IsEmpty() bool {
	return len(p.Tables) == 0 && len(p.Indexes) == 0 && len(p.Constraints) == 0 && len(p.Triggers) == 0 && len(p.Views) == 0
}

// VirtualTable identifies one SQLite virtual table and the module that owns
// it, for the lists that talk about virtual tables rather than describe them.
type VirtualTable struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Module string `json:"module"`
}

// Schema represents a database schema/namespace.
type Schema struct {
	// Facets carries typed settings owned by feature providers.
	Facets  schemaext.Facets `json:"facets,omitzero"`
	Name    string           `json:"name"`
	Comment string           `json:"comment,omitempty"`
	Charset string           `json:"charset,omitempty"`
	Collate string           `json:"collate,omitempty"`
}

// Table represents a database table
//
// EstimatedRows and RowStatsUnknown are a pair, and reading EstimatedRows
// alone is a mistake: PostgreSQL spells "nobody has ever counted this table"
// as pg_class.reltuples = -1, which floors to the same 0 a genuinely empty
// table reports. RowStatsUnknown carries that distinction so a caller choosing
// between a blocking and a non-blocking index build can fail safe instead of
// reading an absent statistic as an empty table. Readers that never collect
// row statistics leave both fields zero, which claims nothing.
//
// Partitioned marks a PostgreSQL declaratively partitioned parent (relkind
// 'p'). information_schema.tables reports it as an ordinary BASE TABLE, so
// Type cannot carry it, and the distinction decides which statements are legal:
// PostgreSQL rejects CREATE INDEX CONCURRENTLY and DROP INDEX CONCURRENTLY on
// a partitioned relation with SQLSTATE 0A000.
//
// Parents names the tables this one is a partition of or inherits from, each
// as [QualifyTableName] spells it. A plan that removes both drops this table
// first: a DROP TABLE of a partitioned parent takes its partitions, so a later
// drop of one finds nothing, and a parent of plain inheritance cannot be dropped
// while a child holds it. A reader that does not ask leaves it empty.
type Table struct {
	// Facets carries typed settings owned by feature providers.
	Facets  schemaext.Facets `json:"facets,omitzero"`
	Name    string           `json:"name"`
	Schema  string           `json:"schema,omitempty"`
	Type    string           `json:"type"` // TABLE, VIEW, etc.
	Comment string           `json:"comment"`
	Charset string           `json:"charset,omitempty"` // MySQL/MariaDB default character set for columns declared without one
	Collate string           `json:"collate,omitempty"` // MySQL/MariaDB default collation
	// RowFormat is the MySQL-family TABLES.ROW_FORMAT, which decides whether
	// MySQL retains an index KEY_BLOCK_SIZE hint.
	RowFormat       string   `json:"row_format,omitempty"`
	Columns         []Column `json:"columns"`
	EstimatedRows   int64    `json:"estimated_rows,omitempty"`    // Best-effort planner estimate from database statistics
	RowStatsUnknown bool     `json:"row_stats_unknown,omitempty"` // The database reports no usable row statistics; EstimatedRows is not a row count
	Partitioned     bool     `json:"partitioned,omitempty"`       // PostgreSQL declaratively partitioned parent (pg_class.relkind = 'p')
	Parents         []string `json:"parents,omitempty"`           // Tables this one is a partition of or inherits from; see the type's comment
	RLSEnabled      bool     `json:"rls_enabled"`                 // Whether RLS is enabled on this table (PostgreSQL)
	// RLSForced reports pg_class.relforcerowsecurity: the table's policies
	// apply to its owner as well. It is independent of RLSEnabled, which
	// leaves the owner exempt (stokaro/ptah#3121).
	RLSForced bool `json:"rls_forced,omitempty"`
	// Projections are the projections of a ClickHouse table, in the order the
	// server's CREATE TABLE lists them. No declaration states one and no
	// comparison reads them: a table rebuild carries them into the new table.
	// A reader for another engine leaves it empty.
	Projections []Projection `json:"projections,omitempty"`
	// Assumptions are the ASSUME constraints of a ClickHouse table, in the
	// order the server's CREATE TABLE lists them. The server checks no row
	// against one; the optimizer takes it as true. No declaration states one
	// and no comparison reads them: a table rebuild carries them into the new
	// table. A reader for another engine leaves it empty.
	Assumptions []Assumption `json:"assumptions,omitempty"`
}

// Assumption is one ASSUME constraint of a ClickHouse table. Name is the
// constraint's name, unquoted. Expression is what follows ASSUME in the
// server's CREATE TABLE, verbatim, which ALTER TABLE ... ADD CONSTRAINT takes
// back as it is.
type Assumption struct {
	Name       string `json:"name"`
	Expression string `json:"expression"`
}

// Projection is one projection of a ClickHouse table. Name is the projection's
// name, unquoted. Definition is what follows the name in the server's CREATE
// TABLE, verbatim: `(SELECT * ORDER BY name)`, with `WITH SETTINGS (...)` after
// it where the projection has settings, or `INDEX name TYPE basic` for an index
// projection. ALTER TABLE ... ADD PROJECTION takes the name and the definition
// as they are.
type Projection struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
}

// RawType is the type spelling a comparator holds a desired schema against.
//
// FormattedType comes first because it is the only field that survives an array
// or a domain, and because the desired side reads the same field -- see
// goSchemaFieldType in internal/convert/dbschematogo. The reader fills it from
// the server's own format_type for exactly those two shapes and leaves it empty
// for every other column.
//
// With ColumnType and UDTName first the two sides read different fields for the
// same column, and the comparator reported a change between a database and
// ITSELF. Measured on PostgreSQL 17, `ptah-compat schema diff` with --from and
// --to naming one database, seven phantom rows:
//
//	arrays.a_bit          type: _bit    -> bit(8)[]
//	arrays.a_char         type: _bpchar -> character(5)[]
//	arrays.a_cube         type: _cube   -> cube[]
//	arrays.a_enum         type: _status -> status[]
//	arrays.a_varchar      type: varchar -> character varying(100)[]
//	arrays.a_varchar_dim  type: varchar -> character varying(100)[]
//	scalars.c_tags        type: text    -> tags
//
// Every one of them proposed an ALTER COLUMN ... TYPE to the type the column
// already had. None survive this (stokaro/ptah#1138).
//
// The width lives in a field of its own, which is the other half of the answer:
// a PostgreSQL read reports `code varchar(50)` as DataType "character varying"
// with CharacterMaxLength 50, so a caller reading DataType alone holds a type
// with no width and reports an ALTER for every varchar column that never
// changed (stokaro/ptah#1662).
//
// It is a method on the column rather than a helper beside one comparator
// because both comparators have to ask the same question and get the same
// answer.
//
// What this string may then be USED for is not uniform, and the difference is
// the whole of #1138's comparator half. An array's spelling is a type. A
// domain's spelling is the identifier its author chose, and a caller comparing
// types keeps it away from type normalization for that reason.
func (c Column) RawType() string {
	rawType := strings.TrimSpace(c.FormattedType)
	if rawType == "" {
		rawType = strings.TrimSpace(c.ColumnType)
	}
	if rawType == "" && c.UDTName != "" {
		rawType = strings.TrimSpace(c.UDTName)
	}
	if rawType == "" {
		rawType = strings.TrimSpace(c.DataType)
	}
	if strings.Contains(rawType, "(") {
		return rawType
	}
	return withTypeSize(rawType, c)
}

// defaultCatalogTimePrecision is the fractional-seconds precision of a temporal
// column that declares none, on the engines whose catalog reports it in a field of
// its own.
const defaultCatalogTimePrecision = 6

// withTimePrecision writes the precision a catalog reports for a temporal column
// into the type's name, behind its first word, so `timestamp without time zone` reads
// `timestamp(3) without time zone`. A catalog that keeps the precision apart keeps it
// for every column, so the default is left out: written out, a read of an ordinary
// column would differ from the document that built it. A nil precision and any other
// type stand as they are.
func withTimePrecision(typeName string, precision *int) string {
	if precision == nil || *precision == defaultCatalogTimePrecision {
		return typeName
	}
	first, rest, _ := strings.Cut(typeName, " ")
	switch strings.ToLower(first) {
	case "timestamp", "timestamptz", "time", "timetz", "interval":
	default:
		return typeName
	}
	if rest != "" {
		rest = " " + rest
	}
	return fmt.Sprintf("%s(%d)%s", first, *precision, rest)
}

// withTypeSize appends the width or precision the catalog keeps in a field of
// its own, for the families that have one.
//
// The bit families are here for a reason worth stating: without them
// `ptah schema inspect` wrote a `bit(4)` column as `bit`, and replaying that
// document into a fresh database produced `bit(1)` -- three bits of every value
// gone, measured on PostgreSQL 17.11. A `bit varying(8)` column came back
// unlimited, and applying the document to the SOURCE database removed the
// declared width from the live column. PostgreSQL keeps both widths in the same
// `character_maximum_length` the varchar family uses, and the reader already
// carries it (stokaro/ptah#2034).
func withTypeSize(rawType string, column Column) string {
	// The fold is normalize.Type's, not a second list of spellings: `character
	// varying` and `varchar` are one type and only that package says so.
	switch normalize.Type(rawType) {
	case "timestamp", "timestamptz", "time", "timetz", "interval":
		return withTimePrecision(rawType, column.DatetimePrecision)
	case "varchar", "char", "bit", "bit varying":
		if column.CharacterMaxLength != nil {
			return fmt.Sprintf("%s(%d)", rawType, *column.CharacterMaxLength)
		}
	case "decimal":
		if column.NumericPrecision == nil {
			return rawType
		}
		if column.NumericScale != nil {
			return fmt.Sprintf("%s(%d,%d)", rawType, *column.NumericPrecision, *column.NumericScale)
		}
		return fmt.Sprintf("%s(%d)", rawType, *column.NumericPrecision)
	}
	return rawType
}

// Populated reports whether the table holds rows: true when EstimatedRows
// counts at least one, and when RowStatsUnknown says the database reports no
// usable statistics, so a caller choosing how to build an index on the table
// assumes rows rather than none. It is false for the zero value, which is also
// what a reader that collects no row statistics reports. The answer changes
// when an empty table gains rows or a table loses all of them, and not while
// writes and analyzes move the estimate of a table that holds rows.
func (t Table) Populated() bool {
	return t.RowStatsUnknown || t.EstimatedRows > 0
}

// QualifiedName returns schema.table when Schema is set, or Name otherwise.
func (t Table) QualifiedName() string {
	return QualifyTableName(t.Schema, t.Name)
}

// QualifyTableName returns an unambiguous schema-qualified table reference:
// the string the QualifiedName methods in this package delegate to, and the
// key both sides of a schema comparison must build object names through.
//
// The format is deterministic: the same pair always yields the same string,
// and the two parts stay distinguishable in it. An empty schema yields the
// table part alone; otherwise the two are joined with a dot, and a part that
// would otherwise be misread -- one carrying a dot, or a quoting character of
// its own -- is quoted, so a table named "tenant.data" with no schema and a
// table named "data" in schema "tenant" produce different keys.
//
// The escaping is this package's own, not any one dialect's quoting rules.
// Build keys through this function rather than joining the parts by hand, and
// compare them rather than parsing them back apart.
func QualifyTableName(schema, table string) string {
	return tableref.Canonical(schema, table)
}

// Column represents a database column.
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
type Column struct {
	// Facets carries typed settings owned by feature providers.
	Facets   schemaext.Facets `json:"facets,omitzero"`
	Name     string           `json:"name"`
	DataType string           `json:"data_type"`
	UDTName  string           `json:"udt_name"` // For PostgreSQL enum types
	// UDTSchema is the schema that holds the column's type, as
	// information_schema reports it: pg_catalog for a built-in type, and the
	// schema of an enum, domain or extension type. Empty from a reader that
	// does not report it.
	UDTSchema          string  `json:"udt_schema,omitempty"`
	FormattedType      string  `json:"formatted_type,omitempty"` // Server's own spelling, where the catalog cannot express it
	ColumnType         string  `json:"column_type"`              // For MySQL ENUM syntax
	IsNullable         string  `json:"is_nullable"`              // YES/NO
	ColumnDefault      *string `json:"column_default"`           // Can be NULL
	CharacterMaxLength *int    `json:"character_max_length"`     // For VARCHAR, etc.
	// Collate is the collation the column declares, empty where the column
	// takes the one it inherits: its table's default on MySQL and MariaDB,
	// its type's on PostgreSQL and CockroachDB (a domain's own, C for name,
	// or the database default), the database's on SQL Server, and BINARY on
	// SQLite. A column that restates what it inherits reads as one that does
	// not, because the server keeps nothing that tells the two apart. On
	// PostgreSQL only COLLATE "default" restates the database default this
	// way: a column naming the collation the database default is built from,
	// such as "en_US.utf8", reads as declared, because the catalog stores
	// that collation rather than "default". Oracle holds none for a character
	// column that names none, which the catalog calls USING_NLS_COMP, and for
	// one that takes its table's DEFAULT COLLATION. ClickHouse reports none.
	Collate string `json:"collate,omitempty"`
	// EffectiveCollate is the collation the column compares with, declared
	// or inherited, and the one every output written from a read prints.
	// MySQL and MariaDB name it for every character column. PostgreSQL and
	// CockroachDB name it as information_schema.COLUMNS does, empty for the
	// database default. SQLite and SQL Server read no inherited name, so
	// there it is the declared one, empty for BINARY and for the database's
	// collation. Oracle names it for every column that holds a collation other
	// than USING_NLS_COMP, its table's default included. A description that is not a read, such as a schema file or
	// a provider's answer, sets both fields to the collation it names.
	EffectiveCollate string `json:"effective_collate,omitempty"`
	// Comment is the column's own comment, where the target has one and the
	// reader reads it. It is empty for a column nobody commented, which is why
	// it is omitted from the encoding rather than written as "".
	Comment string `json:"comment,omitempty"`
	// StorageClauses are the clauses of a ClickHouse column's definition that
	// say how the server stores the column and that no declaration states --
	// CODEC, STATISTICS, TTL and SETTINGS -- each with its keyword, as the
	// server's CREATE TABLE spells it and in its order. No comparison reads
	// them: a table rebuild carries the codec into the new table and refuses a
	// column with any of the others. A reader for another engine leaves it
	// empty.
	StorageClauses []string `json:"storage_clauses,omitempty"`
	// NotNullConstraintName is the name the target keeps for this column's NOT
	// NULL, where it keeps one as an addressable catalog object.
	//
	// PostgreSQL 18 records one row per NOT NULL in pg_constraint with
	// contype 'n', keyed to the column through conkey. PostgreSQL 17 stores
	// nothing, so this is empty there and the read is not even attempted --
	// see the capability gate on the reader's projection.
	//
	// PostgreSQL 18 names EVERY NOT NULL and provides no catalog flag
	// separating an author-supplied name from a generated one, so a faithful
	// read returns all of them. Suppressing names that look generated would
	// silently lose a supplied name that happens to use the same pattern
	// (stokaro/ptah#2161).
	NotNullConstraintName string `json:"not_null_constraint_name,omitempty"`
	NumericPrecision      *int   `json:"numeric_precision"` // For DECIMAL, etc.
	NumericScale          *int   `json:"numeric_scale"`     // For DECIMAL, etc.
	// DatetimePrecision is the fractional-seconds precision of a timestamp,
	// time, or interval column, as information_schema.datetime_precision
	// reports it: the declared (p), or the type's default when none was
	// declared. PostgreSQL rewrites a column whose precision shrinks and not
	// one whose precision grows, which is why the lint baseline needs it.
	DatetimePrecision *int `json:"datetime_precision,omitempty"`
	OrdinalPosition   int  `json:"ordinal_position"`
	IsAutoIncrement   bool `json:"is_auto_increment"` // Derived field
	IsPrimaryKey      bool `json:"is_primary_key"`    // Derived field
	IsUnique          bool `json:"is_unique"`         // Derived field
	// NotNullFromKey reports that the column is NOT NULL only because its
	// primary key makes it so: the column states no NOT NULL of its own, and
	// dropping the key makes it nullable. Oracle keeps a stated NOT NULL as a
	// separate constraint and the key's as none, so only its reader can say.
	// It is false where the target keeps no such difference, and for a
	// nullable column.
	NotNullFromKey bool `json:"not_null_from_key,omitempty"`

	// TypeIsDeclaredText records that DataType must be written back as it
	// stands, because canonicalizing it would describe a different column.
	//
	// The name records where the fact was first found. SQLite keeps the
	// declaration and derives an affinity from it at use time, so `VARCHAR(80)`
	// comes back as `VARCHAR(80)` and means TEXT -- the text an author wrote,
	// stored verbatim. A description that rewrote it to `TEXT` replayed as a
	// different table, and the comparison planned a rebuild that changed
	// nothing (stokaro/ptah#2040).
	//
	// The contract is broader than that origin, and two more engines need it
	// for a reason of their own: their catalog answers with a NATIVE type whose
	// name Ptah's portable mapping also uses for something else.
	//
	//   - SQL Server: `varchar` is one byte per character. The portable mapping
	//     turns a declared VARCHAR into NVARCHAR, which is two, so a native
	//     varchar re-rendered bare comes back Unicode (stokaro/ptah#2147).
	//   - ClickHouse: `DateTime` is second precision and four bytes wide. The
	//     portable mapping turns a declared DATETIME into DateTime64(3), which
	//     is millisecond precision and eight (stokaro/ptah#2142).
	//
	// PostgreSQL, MySQL, MariaDB and Oracle do not set it: their catalogs
	// answer with types whose names the portable mapping leaves alone, so there
	// is nothing for the flag to protect.
	//
	// A renderer reads it to decide whether it may canonicalize the spelling.
	// It must not.
	TypeIsDeclaredText bool `json:"type_is_declared_text,omitempty"`

	// DomainName names the domain a column is declared with, empty for every
	// column whose declared type is not a domain. The PostgreSQL reader fills
	// it, and the Oracle 23 reader does for a domain its read describes.
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
	// VIRTUAL, PERSISTED, MATERIALIZED, ALIAS, or EPHEMERAL. Empty for plain
	// columns.
	GeneratedKind string `json:"generated_kind,omitempty"`

	// IdentityGeneration records an identity column's generation mode, mirroring
	// the goschema-side field: "ALWAYS" (GENERATED ALWAYS AS IDENTITY, which
	// rejects explicit inserts), "BY_DEFAULT" (GENERATED BY DEFAULT AS IDENTITY,
	// which accepts them), or "" for non-identity columns. The PostgreSQL reader
	// fills it from pg_attribute.attidentity and the Oracle reader from
	// ALL_TAB_IDENTITY_COLS.GENERATION_TYPE; other readers leave it empty.
	IdentityGeneration string `json:"identity_generation,omitempty"`

	// IdentityStart and IdentityIncrement are the first value an identity
	// column issues and the step between values, as the catalog spells them.
	//
	// They are strings rather than numbers because that is what the model they
	// feed uses, and because the value is not always an int64: SQL Server
	// allows DECIMAL(38,0) IDENTITY, whose seed does not fit one. Carrying the
	// digits keeps a reader from having to choose between rounding and
	// refusing.
	//
	// Empty for a non-identity column, and for an identity column whose reader
	// does not report them. The YDB reader reports them for a Serial column
	// whose sequence starts or steps at something other than 1, and leaves
	// each empty where it is 1, the value a sequence nobody altered has.
	IdentityStart     string `json:"identity_start,omitempty"`
	IdentityIncrement string `json:"identity_increment,omitempty"`

	// SequenceRestart is the value the last RESTART of a YDB Serial column's
	// sequence moved it to, and empty for a sequence nobody restarted and for
	// every other column.
	//
	// It is read because YDB replays that restart on every later ALTER
	// SEQUENCE. Measured on 25.1.4.7 and 26.2.1.14: after `RESTART WITH 500`
	// and a row holding 500, `ALTER SEQUENCE ... INCREMENT BY 1` moves the next
	// value back to 500, and the next insert fails with `Conflict with
	// existing key`. A plan refuses to alter a sequence that carries one.
	SequenceRestart string `json:"sequence_restart,omitempty"`

	// OwnedSequence names the sequence that is part of this column: the one a
	// serial column draws its default from, or an identity column's own. The
	// sequence is in the column's schema, and it is not described in
	// [Database.Sequences], because writing the column back creates it.
	//
	// It is the name the catalog holds, which is the name the column was
	// created with: renaming the table or the column leaves the sequence's
	// name as it was. A grant on the sequence names it this way, so this is
	// where a grant finds the column it belongs to.
	//
	// Empty for every other column, including one drawing from a sequence the
	// description carries on its own. The PostgreSQL-family readers fill it;
	// other readers leave it empty.
	OwnedSequence string `json:"owned_sequence,omitempty"`
}

// Enum represents a database enum type (PostgreSQL)
type Enum struct {
	// Facets carries typed settings owned by feature providers.
	Facets schemaext.Facets `json:"facets,omitzero"`
	Name   string           `json:"name"`
	// Schema owns the enum. Readers blank it for the connection's own schema,
	// exactly as they do for tables, views and domains, so a filter or a
	// comparison reconstructs the qualified spelling from the connection's
	// default. Without it a schema-qualified `--exclude app.color` matched
	// nothing and silently kept the enum (stokaro/ptah#933).
	Schema string   `json:"schema,omitempty"`
	Values []string `json:"values"`
	// Comment is the type's own comment, as obj_description reports it.
	Comment string `json:"comment,omitempty"`
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (e Enum) QualifiedName() string { return QualifyTableName(e.Schema, e.Name) }

// IndexPart represents one ordered key column in an introspected index.
type IndexPart struct {
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
	// Prefix is the number of leading characters this key indexes, which MySQL
	// spells `INDEX (notes(20))`. Empty means the key covers the whole column.
	//
	// It is REQUIRED for a BLOB or TEXT column there -- MySQL refuses an index
	// on one without a length -- so a key that loses it produces a description
	// that cannot be applied (stokaro/ptah#2112).
	Prefix string `json:"prefix,omitempty"`
}

// Index NULLS ordering spellings for IndexPart.NullsOrder.
const (
	NullsOrderFirst = "FIRST"
	NullsOrderLast  = "LAST"
)

// Index represents a database index.
//
// Most fields are dialect-neutral. Expression is populated only by the
// ClickHouse reader, for a data-skipping index's key expression; that index's
// type and granularity are an observed facet the ClickHouse owner reads.
type Index struct {
	// ManagedBy identifies the feature object that creates and removes this
	// index. Zero means no feature ownership is reported. The reader retains
	// the index metadata, but an export describes it through its owner, and a
	// comparison does not remove it when no standalone index is declared.
	ManagedBy objectidentity.ID `json:"managed_by,omitzero"`
	// Facets carries typed settings owned by feature providers.
	Facets    schemaext.Facets `json:"facets,omitzero"`
	Name      string           `json:"name"`
	TableName string           `json:"table_name"`
	Schema    string           `json:"schema,omitempty"`
	Columns   []string         `json:"columns"`
	// Parts preserves key order and direction when the database exposes it.
	// Empty means the reader supplied only the legacy ascending Columns form.
	Parts      []IndexPart `json:"parts,omitempty"`
	IsUnique   bool        `json:"is_unique"`
	IsPrimary  bool        `json:"is_primary"`
	Definition string      `json:"definition"` // Full index definition
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
	// surface below this model -- schemamodel.Index.Comment, the Atlas-compatible
	// HCL reader's `comment` attribute, and the HCL writer's. Without this
	// field a comment the server reports is dropped between the catalog and the
	// model, and every surface below it sees an index that has none. Empty
	// means the object carries no comment. On MySQL and MariaDB it is
	// STATISTICS.INDEX_COMMENT, the COMMENT clause after the key's parts.
	Comment string `json:"comment,omitempty"`
	// Invisible reports an index the optimizer does not use:
	// STATISTICS.IS_VISIBLE NO on MySQL, STATISTICS.IGNORED YES on MariaDB,
	// and a definition ending in NOT VISIBLE on CockroachDB.
	Invisible bool `json:"invisible,omitempty"`
	// NullsDistinct carries PostgreSQL UNIQUE INDEX NULLS [NOT] DISTINCT
	// state. Nil means the clause was not present in the definition.
	NullsDistinct *bool `json:"nulls_distinct,omitempty"`

	// Method is the index access method as the server spells it -- btree,
	// gin, gist, brin, hash. A ClickHouse data-skipping-index type is a
	// different concept that happens to share a slot in the annotation
	// surface; it travels in the owner's facet, so a ClickHouse "bloom_filter"
	// and a PostgreSQL "gin" stay distinguishable at this layer. Empty means the
	// reader did not report an access method.
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

	// Requires names the objects this index cannot be built without, under a
	// common kind, so the model names them without importing their owners:
	// the extensions it needs, as [objectidentity.KindExtension] identities of
	// their exact names, sorted. They are the catalog's answer, not the DDL's
	// spelling.
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
	Requires []objectidentity.ID `json:"requires,omitempty"`

	// PartitionAttached reports that this index is a partition's copy of an
	// index on its partitioned parent, attached to that parent index rather
	// than standing on its own.
	//
	// PostgreSQL creates one such index per partition whenever an index is
	// created on a partitioned parent, names it itself
	// (events_2026_tenant_idx), and refuses to drop it alone:
	// DROP INDEX "events_2026_tenant_idx" answers `cannot drop index
	// events_2026_tenant_idx because index idx_events_tenant requires it`
	// (SQLSTATE 2BP01). It is the index equivalent of a constraint's backing
	// index -- a real catalog row that no standalone statement addresses --
	// and a comparison that reads it as an ordinary index plans a DROP the
	// server refuses.
	//
	// The parent index is not marked: it is the object the DDL names, and on
	// PostgreSQL it is relkind 'I' while a partition's copy is relkind 'i'.
	// Only readers that can ask the catalog (pg_inherits over index
	// relations) set this; everything else leaves it false.
	PartitionAttached bool `json:"partition_attached,omitempty"`

	// HashShardBuckets is the bucket count of a CockroachDB index built USING
	// HASH, and 0 for every other index. See [Constraint.HashShardBuckets] for
	// what the description carries instead of the sharding.
	HashShardBuckets int `json:"hash_shard_buckets,omitempty"`
}

// QualifiedTableName returns schema.table when Schema is set, or TableName otherwise.
func (i Index) QualifiedTableName() string {
	return QualifyTableName(i.Schema, i.TableName)
}

// Constraint represents one database constraint -- a PRIMARY KEY, FOREIGN
// KEY, UNIQUE, or CHECK row, as Type spells it. FeatureObjects carries owned
// constraints such as PostgreSQL EXCLUDE.
//
// The column lists exist in two spellings. ColumnName and ForeignColumn are
// the legacy single-column fields; ColumnNames and ForeignColumns are the
// multi-column slices, and a reader may have populated either. Consumers must
// read through [Constraint.ColumnNamesOrDefault] and
// [Constraint.ForeignColumnsOrDefault], which merge the two; a fake reader
// should fill the slices. Code reading one spelling directly works against
// every fixture that happens to use that spelling and silently misses the
// other.
type Constraint struct {
	// Facets carries typed settings owned by feature providers. On a
	// PRIMARY KEY constraint they are the settings of the table's primary
	// key, such as the MySQL family's KEY_BLOCK_SIZE hint.
	Facets         schemaext.Facets `json:"facets,omitzero"`
	Name           string           `json:"name"`
	TableName      string           `json:"table_name"`
	Schema         string           `json:"schema,omitempty"`
	Type           string           `json:"type"` // PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
	ColumnName     string           `json:"column_name"`
	ColumnNames    []string         `json:"column_names,omitempty"`
	ForeignTable   *string          `json:"foreign_table"` // For foreign keys
	ForeignSchema  string           `json:"foreign_schema,omitempty"`
	ForeignColumn  *string          `json:"foreign_column"` // For foreign keys
	ForeignColumns []string         `json:"foreign_columns,omitempty"`
	DeleteRule     *string          `json:"delete_rule"` // CASCADE, RESTRICT, etc.
	UpdateRule     *string          `json:"update_rule"` // CASCADE, RESTRICT, etc.
	// Inherited reports that the table holds this constraint from another table:
	// a partition's copy of its parent's key or foreign key, or a check it
	// inherits. Only the other table's statements change it, and PostgreSQL answers
	// `cannot drop inherited constraint` to one on this table. Only readers that
	// can ask the catalog (pg_constraint) set it.
	Inherited bool `json:"inherited,omitempty"`
	// Deferrable reports a constraint whose check may be postponed to the end
	// of a transaction, read from pg_constraint.condeferrable on PostgreSQL
	// and from ALL_CONSTRAINTS.DEFERRABLE on Oracle, where a CHECK may defer
	// too.
	Deferrable bool `json:"deferrable,omitempty"`
	// Disabled reports a constraint the server keeps and checks no row
	// against, as Oracle's DISABLE does, read from ALL_CONSTRAINTS.STATUS.
	// No declaration states a disabled constraint, so a disabled CHECK or
	// foreign key whose definition matches the declared one is planned
	// enabled in place, where the target takes ENABLE VALIDATE CONSTRAINT.
	Disabled bool `json:"disabled,omitempty"`
	// GeneratedName reports that the server chose the constraint's name
	// because its declaration gave none, as Oracle names an unnamed CHECK
	// SYS_C008933. A comparison pairs such a CHECK with an unnamed declared
	// CHECK on the same table whose condition is the same, so the declaration
	// that created it plans nothing on the next run. The Oracle reader sets it.
	GeneratedName bool `json:"generated_name,omitempty"`
	// Initially is the default timing of a deferrable check -- "deferred" or
	// "immediate" -- read from pg_constraint.condeferred. It is empty for a
	// constraint that is not deferrable, so a schema that never asked for one
	// reads back exactly as it did before (stokaro/ptah#1624).
	Initially string `json:"initially,omitempty"`
	// Match is a foreign key's MATCH type: "FULL" or "PARTIAL", and empty for
	// MATCH SIMPLE, which PostgreSQL reports as `s` in pg_constraint.confmatchtype
	// and MySQL as `NONE` in REFERENTIAL_CONSTRAINTS.MATCH_OPTION.
	Match string `json:"match,omitempty"`
	// NotEnforced reports a CHECK or a foreign key the server keeps and does
	// not check: pg_constraint.conenforced false on PostgreSQL 18, and
	// TABLE_CONSTRAINTS.ENFORCED `NO` on MySQL.
	NotEnforced bool `json:"not_enforced,omitempty"`
	// WithoutOverlaps reports a PRIMARY KEY or UNIQUE constraint whose last
	// column is kept from overlapping between rows that agree on the others:
	// pg_constraint.conperiod true on PostgreSQL 18, which pg_get_constraintdef
	// prints as `WITHOUT OVERLAPS` after that column.
	WithoutOverlaps bool `json:"without_overlaps,omitempty"`
	// Period reports a temporal foreign key: pg_constraint.conperiod true on a
	// foreign key, printed as `PERIOD` before the last referencing and the last
	// referenced column.
	Period bool `json:"period,omitempty"`
	// NotValid reports a CHECK or foreign key the server has not validated
	// against the rows the table held when it was added, as
	// pg_get_constraintdef marks it with NOT VALID and Oracle's
	// ALL_CONSTRAINTS.VALIDATED with NOT VALIDATED. A constraint declared NOT
	// ENFORCED is not reported here, though PostgreSQL 18.6 records it with
	// convalidated false, because VALIDATE CONSTRAINT refuses it.
	NotValid    bool    `json:"not_valid,omitempty"`
	CheckClause *string `json:"check_clause"` // For CHECK constraints
	// ColumnLevelCheck identifies a MariaDB CHECK whose catalog LEVEL is
	// Column. Its column is ColumnName; DROP CONSTRAINT cannot remove it.
	ColumnLevelCheck bool `json:"column_level_check,omitempty"`
	// NullsDistinct carries PostgreSQL UNIQUE NULLS [NOT] DISTINCT state.
	// Nil means the clause was not present in the definition.
	NullsDistinct *bool `json:"nulls_distinct,omitempty"`
	// IncludeColumns carries PostgreSQL INCLUDE columns for covering UNIQUE
	// and PRIMARY KEY constraints.
	IncludeColumns []string `json:"include_columns,omitempty"`
	// UsingMethod is the index access method of a PRIMARY KEY, when reported.
	UsingMethod *string `json:"using_method"`
	// ReferencedIndex is the index a foreign key depends on, on its referenced
	// table, where a reader records it: pg_constraint.conindid on PostgreSQL.
	// Empty where the reader records none. It is a reader-only execution fact:
	// two keys may cover the referenced columns, and only this says which of
	// them the server refuses to drop while the foreign key exists.
	ReferencedIndex string `json:"-"`

	// Requires names the objects the index backing this constraint cannot be
	// built without, read from that index exactly as [Index.Requires] is.
	//
	// It is a separate field rather than a lookup because the backing index is
	// not part of the entity model: a constraint owns its index, and the
	// converter drops the index row so the constraint is rendered once. The
	// dependency metadata survives conversion even when the backing index is
	// omitted from the declaration.
	Requires []objectidentity.ID `json:"requires,omitempty"`

	// Comment is the constraint's description, empty when it has none. A
	// reader fills it where the target stores one against the constraint:
	// PostgreSQL and the engines that share its catalog, through
	// obj_description(oid, 'pg_constraint').
	Comment string `json:"comment,omitempty"`

	// HashShardBuckets is the bucket count of a CockroachDB key built USING
	// HASH, and 0 for every other constraint.
	//
	// A hash-sharded key spans a hidden virtual column CockroachDB adds, such
	// as crdb_internal_id_shard_16, before or after the declared ones. The
	// reader leaves that column out of ColumnNames, as it leaves every hidden
	// column out of a table, so the description is the key as declared, and
	// records the sharding here. No schema source can declare USING HASH, so
	// the description applied elsewhere builds the key unsharded, and the read
	// surfaces say so (stokaro/ptah#3771).
	HashShardBuckets int `json:"hash_shard_buckets,omitempty"`
}

// QualifiedTableName returns schema.table when Schema is set, or TableName otherwise.
func (c Constraint) QualifiedTableName() string {
	return QualifyTableName(c.Schema, c.TableName)
}

// QualifiedForeignTableName returns schema.table for a foreign key target.
func (c Constraint) QualifiedForeignTableName() string {
	if c.ForeignTable == nil {
		return ""
	}
	return QualifyTableName(c.ForeignSchema, *c.ForeignTable)
}

// ColumnNamesOrDefault returns all local constraint columns, falling back to
// the legacy single-column field for callers that have not populated slices.
func (c Constraint) ColumnNamesOrDefault() []string {
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
func (c Constraint) ForeignColumnsOrDefault() []string {
	if len(c.ForeignColumns) > 0 {
		return c.ForeignColumns
	}
	if c.ForeignColumn != nil && *c.ForeignColumn != "" {
		return []string{*c.ForeignColumn}
	}
	return nil
}

// Sequence represents a standalone PostgreSQL sequence read from the database.
//
// Only user-declared, standalone sequences appear here. Implicit sequences that
// back SERIAL / BIGSERIAL / identity columns (those OWNED BY a column via an
// internal/auto dependency) are deliberately excluded so that declaring a plain
// SERIAL column does not surface as a spurious standalone sequence.
type Sequence struct {
	// Facets carries typed settings owned by feature providers.
	Facets    schemaext.Facets `json:"facets,omitzero"`
	Name      string           `json:"name"`                // Sequence name
	Schema    string           `json:"schema,omitempty"`    // Schema containing the sequence
	DataType  string           `json:"data_type,omitempty"` // Underlying integer type (e.g. "bigint")
	Start     *int64           `json:"start,omitempty"`     // START WITH value
	Increment *int64           `json:"increment,omitempty"` // INCREMENT BY value
	MinValue  *int64           `json:"min_value,omitempty"` // MINVALUE bound
	MaxValue  *int64           `json:"max_value,omitempty"` // MAXVALUE bound
	Cache     *int64           `json:"cache,omitempty"`     // CACHE size
	Cycle     bool             `json:"cycle"`               // Whether the sequence uses CYCLE
	OwnedBy   string           `json:"owned_by,omitempty"`  // Owning column, if any, as table.column or schema.table.column; a part that would read back as another name, such as one holding a dot, is double-quoted
	Comment   string           `json:"comment,omitempty"`   // Sequence comment/description
}

// QualifiedName returns schema.sequence when Schema is set, or Name otherwise.
func (s Sequence) QualifiedName() string {
	return QualifyTableName(s.Schema, s.Name)
}

// NameFolding is how a server compares database and table names where a
// setting each server is started with decides it; see
// [ServerInfo.NameFolding].
type NameFolding string

const (
	// NameFoldingUnknown is folding no connection read. It is the zero value.
	NameFoldingUnknown NameFolding = ""
	// NameFoldingFolds is a server that compares database and table names
	// without regard to case: lower_case_table_names 1 or 2. FA and fa name
	// one database there.
	NameFoldingFolds NameFolding = "folds"
	// NameFoldingExact is a server that keeps database and table names apart
	// by case: lower_case_table_names 0. FA and fa are two databases there.
	NameFoldingExact NameFolding = "exact"
)

// ServerInfo is the identity of one connected server as
// dbschema.ConnectToDatabase resolved it: which product answered, which
// version, which schema unqualified names mean there, and the capability set
// and identifier semantics that follow from those answers.
type ServerInfo struct {
	// DatabaseContext is the selected provider's established interpretation of
	// authored and execution database roots. It contains no connection address.
	DatabaseContext schemaext.DatabaseContext `json:"database_context,omitzero"`
	// Provider binds an explicitly selected executable to its negotiated
	// contract. Built-in connections leave it zero. Saved plans compare it
	// before reading or writing the target.
	Provider ProviderIdentity `json:"provider,omitzero"`
	// Dialect is the canonical target selected by the application runtime.
	// Built-in names are declared by core/platform. Where products share one
	// wire protocol it names the product the connection actually reached
	// rather than the URL's scheme, so a caller must not assume the two agree:
	// a postgres:// connection can report cockroachdb or yugabytedb,
	// and a mysql:// connection to MariaDB reports mariadb.
	Dialect string `json:"dialect"`
	Version string `json:"version"`
	// Schema is the schema this connection resolves unqualified names
	// against, and the value a blank Schema field means throughout a
	// [Database] this connection read. It is the connected server's own notion
	// of a current schema, so its spelling is per-dialect and is not "public"
	// everywhere: it is a database name on the engines whose schemas are
	// databases, main on SQLite, the connected user on Oracle. Where the
	// dialect has a selectable schema, a URL naming one selects it here.
	Schema string `json:"schema"`

	// WholeServer reports a connection to a whole MySQL or MariaDB server:
	// one opened from a URL that names no database, which
	// dbschema.ConnectToServer accepts. Schema is empty then, a read of the
	// connection describes every user database with each object under the
	// database it is in, and a comparison against it creates and drops
	// databases. A ServerInfo built without a connection leaves it false: an
	// empty Schema alone does not make a server.
	WholeServer bool `json:"whole_server,omitempty"`

	// NameFolding is how the server compares database and table names, on a
	// dialect whose servers each decide it by a setting: MySQL and MariaDB,
	// under lower_case_table_names. A MySQL or MariaDB connection reads the
	// setting and records [NameFoldingFolds] or [NameFoldingExact]. On a
	// server that folds names, a comparison matches a name the schema spells
	// in another case to the server's spelling of it, and refuses two names
	// the schema declares that the server reads as one.
	//
	// The zero value is [NameFoldingUnknown], and a ServerInfo built without a
	// connection holds it. A MySQL-family comparison with folding unknown
	// refuses a database or table name that matches another name only without
	// regard to case, because the plan for the pair depends on the setting;
	// every other name compares as bytes. Other dialects do not read it.
	NameFolding NameFolding `json:"name_folding,omitempty"`

	// DefaultIntSize is the width in bytes CockroachDB gives a column
	// declared INT or INTEGER without one, read from the session's
	// default_int_size when the connection is made. It is 8 unless the
	// session sets it, and 0 where the dialect has no such setting or the
	// connection did not read it; a comparison takes 0 on CockroachDB as 8,
	// the server's own default. PostgreSQL gives those spellings 4 bytes
	// always.
	DefaultIntSize int `json:"default_int_size,omitempty"`

	// URL is the database connection URL the connection was opened from, with
	// whatever credentials it carried. Callers that reconnect to the same
	// target read it -- a dev-database URL, a second session for an online DDL
	// tool, a realm-scope decision that depends on the URL's path.
	//
	// It is excluded from JSON because every other field here is tagged, and a
	// struct whose tags invite marshalling must not carry one field that turns
	// a marshal into a credential disclosure: `json.Marshal(conn.Info())` is
	// the obvious thing to do with a ServerInfo, and it must not write the
	// database password into whatever the caller does with the bytes. Marshal
	// [ServerInfo.RedactedURL] instead -- it names the same target with the
	// secrets removed (stokaro/ptah#2246).
	URL string `json:"-"`

	// RedactedURL is URL with every secret it carries replaced, suitable for a
	// log line, an error message or a serialized report. It occupies the `url`
	// JSON name so a consumer that marshals ServerInfo still learns which target
	// the description came from.
	RedactedURL string `json:"url"`

	// CollationNames lists the collation sorts the connected server names, in
	// upper case, where the connector reads them. Oracle's connector reads them
	// from V$NLS_VALID_VALUES, which a plan checks a declared column collation
	// against before it runs a statement. It is nil where the dialect has no
	// such list, and where the read failed, in which case the server answers a
	// name itself.
	CollationNames []string `json:"-"`

	Capabilities        capability.Capabilities `json:"capabilities"`         // resolved from Dialect + Version for live connections
	IdentifierSemantics identifier.Semantics    `json:"identifier_semantics"` // catalog identifier metadata and static rules

	// CapabilityNote says what a non-version-specific resolution actually
	// planned, and is empty when the version selected a measured release line.
	//
	// It carries the same sentence the typed --server-version path already
	// prints, from the same producer, because a live connection asks the same
	// question: this server is newer than anything measured, or it fell in a
	// gap, or its dialect has no ladder to spend a version on. Reporting it
	// only for the typed path is how planning against an unmodeled server reads
	// as planning against a modeled one (stokaro/ptah#916).
	CapabilityNote string `json:"capability_note,omitempty"`
}

// SchemaReader reads a live database schema, in the two forms this package
// offers every other database call in: a context-free one and a Context one.
//
// ReadSchemaContext governs every catalog query the read issues with the
// provided context: canceling it, or letting its deadline pass, makes the read
// return promptly with an error rather than running the remaining queries.
//
// The context is part of the contract rather than a convenience. A schema read
// is dozens of round trips against a server that may be slow or unreachable,
// and it is what the migration generator's own documented context contract
// bottoms out in, so a caller with no way to say "stop" has no way to bound
// either one (stokaro/ptah#2246).
//
// ReadSchema is that same read under context.Background(), and is what a caller
// with no context to hand writes. It is the pairing database/sql itself uses
// and that dbschema.DatabaseConnection already follows -- Exec beside
// ExecContext, Query beside QueryContext, QueryRow beside QueryRowContext -- so
// a reader implements both and the choice is the caller's. Prefer
// ReadSchemaContext: only it can be stopped.
//
// Both are part of the interface rather than one being a helper beside it,
// because an implementation of SchemaReader can live outside this module.
// stokaro/ptah-testkit is one such consumer: a separate module that builds
// against the last published release, so the read it issues has to be spelled
// a way that release carries.
type SchemaReader interface {
	ReadSchema() (*Database, error)
	ReadSchemaContext(ctx context.Context) (*Database, error)
}

// RehearsalSchemaReader optionally reads the schema with observed environment
// objects outside its writable scope. A rehearsal uses these facts to compare
// its starting state without recreating objects that already match. The read
// must preserve unknown observations and perform no writes. Its result grants
// no mutation authority: callers must validate every generated baseline
// statement against the rehearsal's execution scope before executing any.
type RehearsalSchemaReader interface {
	ReadRehearsalSchemaContext(ctx context.Context) (*Database, error)
}

// SchemaExecutor executes SQL statements produced by schema operations.
//
// ExecuteSQL accepts a context and an optional slice of arguments that are
// bound as native driver parameters, mirroring database/sql's ExecContext.
// Use placeholders (`?` or the dialect-native form such as `$1`/`$2` for
// PostgreSQL) instead of interpolating values into the SQL string; this
// prevents the SQL injection class of bugs a no-args signature invites (see
// issue #130). Identifiers (table/column names) cannot be
// parameterized — route them through a validated escape helper instead.
//
// IsDryRun reports whether the executor is in dry-run mode. In that mode no
// statement reaches the server: ExecuteSQL reports success without executing
// anything. The mode is selected on a writer with [SchemaWriter.SetDryRun];
// IsDryRun is what lets a layer that wraps or replaces an executor carry the
// mode onto the replacement rather than silently losing it.
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
	// Under dry-run mode it drops nothing and performs no destructive work.
	DropAllTables(ctx context.Context) error
	// BeginTransaction starts a database transaction and returns a
	// transaction-scoped executor for it. Under dry-run mode no server
	// transaction is begun: the returned SchemaTransaction is itself a
	// dry-run executor whose Commit and Rollback succeed without touching
	// the server.
	BeginTransaction(ctx context.Context) (SchemaTransaction, error)
	// SetDryRun turns dry-run mode on or off for this writer and for every
	// transaction it begins afterward. In dry-run mode every mutating call
	// reports success without sending anything to the server -- see the
	// contract on [SchemaExecutor]. Set the mode before beginning a
	// transaction: whether a transaction already in flight observes a later
	// change is the dialect's business, and must not be relied on either way.
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

// CancellationRollback reports evidence from the transaction's own lifetime,
// after Rollback has been attempted. A true result means cancellation of the
// context that began this transaction ended it without a commit attempt. It
// does not imply that schema statements with implicit commits were undone.
//
// An execution RPC or statement context can be canceled while its transaction
// stays open. Implementations must not infer this evidence from that context.
// Transactions without this interface provide no cancellation-based evidence;
// callers must use the result of Rollback itself.
type CancellationRollback interface {
	SchemaTransaction
	RolledBackByCancellation() bool
}

// CommitRollback reports evidence from the transaction's own lifetime, after
// Commit has failed. A true result means the transaction's owner confirmed that
// the commit ended the transaction by rolling it back, so nothing it executed
// took effect: a provider answered the commit with ROLLED_BACK. A commit whose
// outcome is unknown never reports true.
//
// Transactions without this interface provide no such evidence; callers must
// then treat a failed commit by its error alone.
type CommitRollback interface {
	SchemaTransaction
	RolledBackAtCommit() bool
}

// Function represents a custom function read from the database.
type Function struct {
	// Facets carries typed settings owned by feature providers.
	Facets schemaext.Facets `json:"facets,omitzero"`
	Name   string           `json:"name"` // Function name
	// Kind separates a function from a procedure. Empty means function, which
	// is what every description written before procedures existed meant.
	Kind string `json:"kind,omitempty"`
	// Schema owns the function. Readers blank it for the connection's own
	// schema, the same convention tables, views and domains follow, so a
	// filter reconstructs the qualified spelling from the connection's
	// default. Without it a schema-qualified `--exclude app.fn_app` matched
	// nothing and silently kept the function (stokaro/ptah#933).
	Schema     string `json:"schema,omitempty"`
	Parameters string `json:"parameters"` // Full declaration parameters (e.g., "tenant_id_param TEXT DEFAULT 'public'")
	// IdentityArguments is PostgreSQL's canonical input-argument type list for
	// identifying an overload in ALTER or DROP FUNCTION. It deliberately stays
	// separate from Parameters: declaration parameters may contain names,
	// defaults, and OUT-only arguments that are not part of a function's drop
	// identity. Nil means a reader did not capture the identity; a non-nil empty
	// string is the valid identity of a zero-input or OUT-only function. This
	// reader-only execution fact is not part of serialized schema descriptions.
	IdentityArguments *string `json:"-"`
	// Definer is the account a SQL SECURITY DEFINER routine runs as, where a
	// reader records it: the MySQL family's DEFINER. Empty where the reader
	// records none. It is a reader-only execution fact: on a target whose plan
	// replaces a routine by DROP and CREATE
	// ([capability.RoutineReplacementResetsDefiner]), recreating a routine of
	// another definer silently changes the principal its body runs as, which
	// the comparison refuses with [Database.CurrentAccount].
	Definer    string `json:"-"`
	Returns    string `json:"returns"`    // Return type (e.g., "VOID", "TEXT")
	Language   string `json:"language"`   // Function language (e.g., "plpgsql", "sql")
	Security   string `json:"security"`   // Security context (e.g., "DEFINER", "INVOKER")
	Volatility string `json:"volatility"` // Function volatility (e.g., "STABLE", "IMMUTABLE", "VOLATILE")
	// Settings are the routine's own configuration settings, each `name=value`
	// with the value in SQL, quoted as pg_get_functiondef prints it:
	// `work_mem='8MB'`, `search_path='public', 'pg_catalog'`. PostgreSQL keeps
	// them in pg_proc.proconfig unquoted; a dialect without such a facility
	// leaves this empty.
	Settings []string `json:"settings,omitempty"`
	Body     string   `json:"body"`    // Function body/implementation
	Comment  string   `json:"comment"` // Function comment/description

	// ReturnsTable reports a function whose result is a table, as the reader
	// establishes it from the catalog's object type rather than from
	// [Function.Returns]: a SQL Server inline or multi-statement table-valued
	// function, which SQL Server grants the privileges of a table on. Only the
	// SQL Server reader sets it. Every other reader leaves it false, a
	// PostgreSQL function declared RETURNS TABLE (...) included, and so does a
	// function converted from a declaration rather than read from a database.
	// Returns alone does not imply it: Oracle reports `table` as the return
	// type of a function returning a nested-table collection, which takes the
	// privileges of a function.
	ReturnsTable bool `json:"returns_table,omitempty"`
}

// QualifiedName returns schema.name when Schema is set, or Name otherwise.
func (f Function) QualifiedName() string { return QualifyTableName(f.Schema, f.Name) }

// Signature returns the argument list that selects this routine among the
// overloads of its name, the list a DROP or ALTER addresses it by.
//
// It flattens [Function.DropIdentity] for a caller that compares the text and
// has no use for the difference between a routine that takes no arguments and
// one nothing established an identity for. A caller that writes a statement
// needs that difference and asks DropIdentity.
func (f Function) Signature() string {
	if identity := f.DropIdentity(); identity != nil {
		return *identity
	}
	return ""
}

// DropIdentity returns the argument list that addresses this routine in a DROP
// or ALTER statement, or nil when nothing established one.
//
// It is IdentityArguments when a reader captured it, and Parameters otherwise.
// Only the PostgreSQL reader captures an identity, so on every other dialect the
// answer is the declaration parameters as the catalog reports them.
//
// Nil and an empty list are different answers, and a statement spells them
// differently. An empty list is the identity of a routine that takes no
// arguments, and `DROP FUNCTION f()` names that routine exactly; nil means
// nothing said, so only the bare name can be written. PostgreSQL refuses a bare
// name whenever it is overloaded, with `function name "f" is not unique`, so a
// zero-argument overload reached through the flattened answer could not be
// dropped at all.
func (f Function) DropIdentity() *string {
	if f.IdentityArguments != nil {
		return f.IdentityArguments
	}
	if f.Parameters == "" {
		return nil
	}
	return &f.Parameters
}

// View represents a database view read from the database.
type View struct {
	// Facets carries typed settings owned by feature providers.
	Facets      schemaext.Facets `json:"facets,omitzero"`
	Name        string           `json:"name"`         // View name
	Schema      string           `json:"schema"`       // Schema where the view is defined
	Body        string           `json:"body"`         // SELECT query used as the view definition
	CheckOption string           `json:"check_option"` // NONE, LOCAL, CASCADED, or dialect equivalent
	Comment     string           `json:"comment"`      // View comment/description
	// Security is the MySQL or MariaDB view's SQL SECURITY mode.
	Security string `json:"security,omitempty"`
	// Definer is the account a view under SQL SECURITY DEFINER runs as, where
	// a reader records it: the MySQL family's DEFINER, spelled user@host. Empty
	// where the reader records none. Like [Function.Definer] it is a
	// reader-only execution fact: a CREATE OR REPLACE VIEW that names no
	// DEFINER makes the account that runs it the definer, so replacing a view
	// of another definer changes whose privileges it runs with, which the
	// plan refuses with [Database.CurrentAccount].
	Definer string `json:"-"`
	// Attributes carries the view's own WITH clause -- SQL Server's
	// SCHEMABINDING and VIEW_METADATA -- uppercased, in the order the server
	// wrote them.
	//
	// They belong to the view rather than to its body: SCHEMABINDING binds it
	// to the tables it names, so they cannot be altered under it, and an
	// indexed view requires it. The reader cuts the header off to get at the
	// body, and nothing kept this, so a replayed view came back unbound with
	// no diagnostic anywhere (stokaro/ptah#2125).
	Attributes []string `json:"attributes,omitempty"`

	// Options is the view's own options other than its check option, as the
	// server holds them, `name=value`: PostgreSQL's security_invoker and
	// security_barrier. The value is the spelling the server stored, so
	// `security_invoker = on` reads back as security_invoker=on; a comparison
	// reads it through [ptah.run/core/schemamodel.View.Options]' rules rather
	// than as text.
	Options []string `json:"options,omitempty"`
}

// QualifiedName returns schema.view when Schema is set, or Name otherwise.
func (v View) QualifiedName() string {
	return QualifyTableName(v.Schema, v.Name)
}

// MaterializedView represents a materialized view read from the database.
// Settings an engine attaches to one, such as a ClickHouse refresh schedule,
// are observations in Facets.
type MaterializedView struct {
	// Facets carries typed settings owned by feature providers.
	Facets  schemaext.Facets `json:"facets,omitzero"`
	Name    string           `json:"name"`    // Materialized view name
	Schema  string           `json:"schema"`  // Schema where the materialized view is defined
	Body    string           `json:"body"`    // SELECT query used as the materialized view definition
	Comment string           `json:"comment"` // Materialized view comment/description

	// Options is the materialized view's storage parameters as the server
	// holds them, `name=value`, such as fillfactor or an autovacuum setting;
	// see [View.Options].
	Options []string `json:"options,omitempty"`

	// Unpopulated reports that the server holds the view without rows, as
	// CREATE MATERIALIZED VIEW ... WITH NO DATA leaves it, where a reader
	// records it: PostgreSQL's pg_class.relispopulated. The definition the
	// server reports has no WITH NO DATA clause, so a description of the view
	// writes the clause by this, and a view created again from it is empty
	// too. It is a reader-only fact and not part of a serialized description.
	Unpopulated bool `json:"-"`
}

// QualifiedName returns schema.materialized_view when Schema is set, or Name otherwise.
func (v MaterializedView) QualifiedName() string {
	return QualifyTableName(v.Schema, v.Name)
}

// Trigger represents a database trigger read from the database.
type Trigger struct {
	// Facets carries typed settings owned by feature providers.
	Facets  schemaext.Facets `json:"facets,omitzero"`
	Name    string           `json:"name"`    // Trigger name
	Schema  string           `json:"schema"`  // Schema where the trigger is defined
	Table   string           `json:"table"`   // Target table
	Timing  string           `json:"timing"`  // BEFORE, AFTER, or INSTEAD OF
	Event   string           `json:"event"`   // INSERT, UPDATE, DELETE, or TRUNCATE
	ForEach string           `json:"for"`     // ROW or STATEMENT
	Body    string           `json:"body"`    // Trigger body
	Comment string           `json:"comment"` // Trigger comment/description

	// When is the condition of the trigger's WHEN clause, without its
	// parentheses, as the server prints it back. Empty means none.
	When string `json:"when,omitempty"`
	// OldTable and NewTable name the transition tables of a REFERENCING
	// clause. Empty means the trigger declares none.
	OldTable string `json:"old_table,omitempty"`
	NewTable string `json:"new_table,omitempty"`

	// Definition is the statement that created the trigger, as the server
	// stores it, on an engine that keeps one: SQLite's sqlite_schema.sql. A
	// plan that has to create the trigger again exactly as it stands, because
	// it is out of scope, runs it. Empty from a reader that does not keep one.
	// It is a reader-only fact and never serialized.
	Definition string `json:"-"`

	// ExecuteFunction is the name of the function the trigger runs, as the
	// catalog reports it, qualified with the function's schema when that is not
	// the schema the read resolves names in.
	//
	// PostgreSQL has no inline trigger body: `CREATE TRIGGER ... EXECUTE
	// FUNCTION f()` always names a function, and Ptah writes one per trigger
	// when the declaration carries a body. Reading only that function's source
	// and discarding its NAME made every trigger look like one Ptah owns, so a
	// function shared by several triggers was described once per trigger and
	// replayed as that many copies (stokaro/ptah#2210).
	ExecuteFunction string `json:"execute_function,omitempty"`
	// FunctionArguments are the text values stored in pg_trigger.tgargs.
	FunctionArguments []string `json:"function_arguments,omitempty"`
}

// QualifiedTable returns schema.table when Schema is set, or Table otherwise.
func (t Trigger) QualifiedTable() string {
	return QualifyTableName(t.Schema, t.Table)
}

// RLSPolicy represents a PostgreSQL RLS policy read from the database
type RLSPolicy struct {
	Name                string `json:"name"`                  // Policy name
	Table               string `json:"table"`                 // Target table name
	PolicyFor           string `json:"policy_for"`            // Operations policy applies to (e.g., "ALL", "SELECT")
	ToRoles             string `json:"to_roles"`              // Target roles (e.g., "app_user", "PUBLIC")
	UsingExpression     string `json:"using_expression"`      // USING clause expression
	WithCheckExpression string `json:"with_check_expression"` // WITH CHECK clause expression
	Comment             string `json:"comment"`               // Policy comment/description
	// Restrictive reports AS RESTRICTIVE, read from pg_policy.polpermissive.
	// A permissive policy widens access and a restrictive one narrows it, so
	// a comparison that ignores the flag calls two policies equal when one
	// grants what the other withholds (stokaro/ptah#3121).
	Restrictive bool `json:"restrictive"`
}

// Role represents a PostgreSQL role read from the database
type Role struct {
	// Facets carries typed settings owned by feature providers.
	Facets        schemaext.Facets  `json:"facets,omitzero"`
	Name          string            `json:"name"`           // Role name
	Login         bool              `json:"login"`          // Whether role can login
	Superuser     bool              `json:"superuser"`      // Whether role is superuser
	CreateDB      bool              `json:"create_db"`      // Whether role can create databases
	CreateRole    bool              `json:"create_role"`    // Whether role can create other roles
	Inherit       bool              `json:"inherit"`        // Whether role inherits privileges
	Replication   bool              `json:"replication"`    // Whether role can initiate replication
	PasswordState RolePasswordState `json:"password_state"` // What the reader established about password presence
	Comment       string            `json:"comment"`        // Role comment/description

	// Group is YDB's: it reports a group, a principal CREATE GROUP made, which
	// never logs in and is the only kind with members. False is a role, which
	// on YDB is a user. Every other reader reports roles of one kind and
	// leaves it false; omitempty keeps their serialization byte-identical.
	Group bool `json:"group,omitempty"`

	// AttributesUnknown marks a role the reader knows exists and whose
	// attributes it did not read, so every other field but Name is a zero
	// value rather than a fact. An executable provider names the roles outside
	// its read's scope and describes none of their attributes, and only such a
	// role, in [Database.RolesOutOfScope], carries it. A comparison plans no
	// change to its attributes, since nothing says they differ. It is not
	// part of the description and is never serialized.
	AttributesUnknown bool `json:"-"`
}

// ObjectOwner is the owner of one schema object.
//
// Kind is the object kind in Ptah's own vocabulary -- table, view,
// materialized view, sequence, schema -- rather than the catalog's letter, so a
// consumer need not know pg_class.relkind to read it.
type ObjectOwner struct {
	// Kind is the object kind.
	Kind string `json:"kind"`
	// Schema is the schema the object lives in, empty for a schema itself.
	Schema string `json:"schema,omitempty"`
	// Name is the object's name.
	Name string `json:"name"`
	// Owner is the role that owns it.
	Owner string `json:"owner"`
	// OwnerCanLogin reports whether that role can log in; nil means that the
	// read did not establish it. A known value is read from the
	// catalog beside the owner rather than inferred from the described roles:
	// the owner of everything on a default PostgreSQL database is the
	// bootstrap superuser, which Ptah deliberately does not describe, and an
	// analysis that had to look it up in the role list would go quiet exactly
	// where the question matters.
	OwnerCanLogin *bool `json:"owner_can_login"`
}

// RoleMembership is one role-in-role edge. The membership options determine
// whether Member inherits Role's privileges or may switch to Role.
//
// Either endpoint may be outside the roles Ptah manages. Keeping the edge
// lets access analysis follow privileges without declaring those principals.
type RoleMembership struct {
	// Role is the role whose privileges are granted.
	Role string `json:"role"`
	// Member is the role that receives them.
	Member string `json:"member"`
	// AdminOption reports whether Member may grant Role onward.
	AdminOption bool `json:"admin_option"`
	// InheritOption and SetOption report the per-membership options on
	// PostgreSQL 16 and later. Nil means the server did not report an option.
	InheritOption *bool `json:"inherit_option,omitempty"`
	SetOption     *bool `json:"set_option,omitempty"`
}

// WideGrant is one table privilege a role holds on every table through one
// grant wider than the database a read describes.
type WideGrant struct {
	// Role is the grantee, as the read reports it. PUBLIC gives the
	// privilege to every role.
	Role string `json:"role"`
	// Privilege is the table privilege the grant gives, as a GRANT on a table
	// names it: SELECT for both `GRANT SELECT ON *.*` and SELECT ANY TABLE.
	Privilege string `json:"privilege"`
	// Grant is what the server grants, in the words a REVOKE of it uses:
	// "SELECT ON *.*", "SELECT ANY TABLE" or "SELECT ANY TABLE ON SCHEMA APP".
	Grant string `json:"grant"`
	// Schema is the one schema whose tables the grant covers, as the schema
	// an Oracle `GRANT SELECT ANY TABLE ON SCHEMA app` names, in the read's
	// spelling. It is empty when the grant covers every table.
	Schema string `json:"schema,omitempty"`
}

// RoutinePrivileges is what a read found granted on routines, kept for a plan
// that replaces a routine by DROP and CREATE.
//
// On MySQL and MariaDB with the default automatic_sp_privileges, DROP
// PROCEDURE and DROP FUNCTION delete every row of mysql.procs_priv for the
// routine, whoever the grantee is, and the CREATE brings back only the row it
// grants the account that runs it. A plan that replaces a routine for a new
// body grants the others again after the CREATE. They are what the server
// holds, not what a declaration grants, so they cover accounts a schema does
// not manage, and nothing compares them. With automatic_sp_privileges off the
// DROP keeps every row, and the read reports none.
//
// The zero value gives nothing back and refuses nothing. It is what a read
// that found no such privilege reports, and what a description that did not
// come from a live read, or from a reader with no such catalog, holds.
type RoutinePrivileges struct {
	// Grants is one entry per account and routine, in the order the read
	// reported them.
	Grants []RoutineGrant `json:"grants,omitempty"`
	// Refused reports that the server refused the read, so the privileges on
	// a routine are not known. A plan that would replace a routine refuses
	// rather than take privileges nobody can see.
	Refused bool `json:"refused,omitempty"`
}

// Clone returns a copy of r that shares no slice with it.
func (r RoutinePrivileges) Clone() RoutinePrivileges {
	clone := RoutinePrivileges{Refused: r.Refused}
	if r.Grants == nil {
		return clone
	}
	clone.Grants = make([]RoutineGrant, len(r.Grants))
	for i, grant := range r.Grants {
		grant.Privileges = slices.Clone(grant.Privileges)
		clone.Grants[i] = grant
	}
	return clone
}

// RoutineGrant is the privileges one account holds on one stored routine, as
// the MySQL family records them in mysql.procs_priv.
type RoutineGrant struct {
	// User and Host name the account, as `'user'@'host'` names it in a GRANT.
	User string `json:"user"`
	Host string `json:"host"`
	// Role reports a MariaDB role. MariaDB records a role with an empty Host
	// and refuses a GRANT that names it with one, so a GRANT names it by User
	// alone. A MySQL role is an account like any other and keeps its Host.
	Role bool `json:"role,omitempty"`
	// Schema is the database that holds the routine, spelled as the read
	// spells the database it describes.
	Schema string `json:"schema"`
	// Name is the routine's name, as the GRANT that wrote the row spelled it.
	// A routine name is compared without case on every MySQL and MariaDB
	// server, so it may differ in case from the routine's own.
	Name string `json:"name"`
	// Type is FUNCTION or PROCEDURE.
	Type string `json:"type"`
	// Privileges are the privileges held, in upper case: EXECUTE, ALTER
	// ROUTINE, or both. Empty when the account holds only the grant option.
	Privileges []string `json:"privileges,omitempty"`
	// WithOption reports that the account may grant what it holds on the
	// routine.
	WithOption bool `json:"with_option,omitempty"`
	// ReaderMayGrant reports that the account the read ran as may grant the
	// row again once the routine is replaced: it holds the grant option and
	// the row's privileges on the routine's database, or on every database. A
	// privilege it holds on the routine itself does not count, because the
	// DROP takes that too.
	ReaderMayGrant bool `json:"reader_may_grant,omitempty"`
}

// WideAccess is a role's access to every relation of a database that no grant
// on an object states: membership in a PostgreSQL role that reads or writes
// all data, or a privilege MySQL or MariaDB grants on every database or on a
// database name pattern that reaches the database. It is read for the security
// analysis, which measures how much of a database a role reaches, and no
// comparison or plan reads it.
type WideAccess struct {
	// Role is the role that holds the access.
	Role string `json:"role"`
	// Read reports access to read every relation.
	Read bool `json:"read,omitempty"`
	// Write reports access to change the rows of every relation.
	Write bool `json:"write,omitempty"`
	// Source names where the access comes from, such as pg_read_all_data, or
	// a grant in the words a REVOKE of it uses, such as "SELECT ON *.*" or
	// "SELECT ON `ab%`.*".
	Source string `json:"source"`
	// Schema is the one schema or database whose relations the access
	// covers, in the read's spelling, such as the database a grant on a
	// database name pattern reaches. It is empty when the access covers every
	// relation the read describes.
	Schema string `json:"schema,omitempty"`
	// SwitchOnly reports access Role holds only after it switches to the
	// predefined role, which a PostgreSQL 16 membership with INHERIT FALSE
	// states. A member of Role that can only inherit from it does not reach
	// it.
	SwitchOnly bool `json:"switch_only,omitempty"`
}

// Grant represents a privilege grant read from the database.
type Grant struct {
	Role       string `json:"role"`                 // Role receiving the privilege; PUBLIC for every role
	Privilege  string `json:"privilege"`            // Granted privilege, e.g. SELECT or USAGE
	ObjectType string `json:"object_type"`          // TABLE, SCHEMA, SEQUENCE, FUNCTION, or PROCEDURE
	Schema     string `json:"schema,omitempty"`     // Schema containing the target object
	ObjectName string `json:"object_name"`          // Target table, schema, or routine name
	WithOption bool   `json:"with_option"`          // Whether the grant has WITH GRANT OPTION
	GrantedBy  string `json:"granted_by,omitempty"` // Grantor role
	// ObjectOwner is the owner observed for this grant's target. It is reading
	// context: a PostgreSQL read names it to decide which grantor a superuser
	// can revoke, and a SQL Server read to tell the row a GRANT or a DENY run
	// with CONTROL takes the place of, which names the owner as its grantor,
	// from a row another principal gave. It is empty where the read names no
	// owner.
	ObjectOwner string `json:"-"`

	// Arguments are a routine target's input argument types as the catalog
	// spells them (oidvectortypes of pg_proc.proargtypes), such as
	// `uuid` or `integer[], text`. A routine's identity includes them. Empty
	// for every other target.
	Arguments string `json:"arguments,omitempty"`

	// Implicit marks a privilege the object holds by default rather than by a
	// GRANT: PostgreSQL lets PUBLIC execute a routine whose pg_proc.proacl is
	// NULL, meaning nobody has granted or revoked anything on it. A routine's
	// owner also has an implicit row. If it revoked its own EXECUTE, the row
	// carries IsPartialRevoke so a comparison can distinguish that state from
	// an unread ACL. A description leaves implicit grants out.
	//
	// On a relation, a sequence or a schema, an implicit row is a privilege
	// PostgreSQL gives the object's owner, whether the ACL is NULL or spells it
	// out; see [Grant.OwnerDefault] and [Grant.OwnerRevoked].
	Implicit bool `json:"implicit,omitempty"`

	// IsPartialRevoke marks a row that SUBTRACTS from a broader grant rather
	// than adding one. On ClickHouse, `GRANT SELECT ON db.* TO r` followed by
	// `REVOKE SELECT ON db.t FROM r` leaves two rows in system.grants, the
	// second with is_partial_revoke = 1, and SHOW GRANTS prints both lines. A
	// SQL Server DENY reads the same way, and so does the REVOKE row SQL Server
	// keeps for a column revoked under a grant on its whole table.
	//
	// Ptah does not manage that shape — a grant minus exceptions has no
	// declaration to compare against — so nothing plans one. The field exists
	// so the reader reports the row instead of dropping it, which is what lets
	// a live validation refuse a managed role whose effective privileges are
	// quietly narrower than its grant row alone would say.
	//
	// PostgreSQL reports it together with Implicit, for a privilege an object's
	// owner revoked from itself: what it subtracts from is what ownership gives.
	// Ptah manages that shape, as a revoke.
	IsPartialRevoke bool `json:"is_partial_revoke,omitempty"`

	// Denied marks a SQL Server DENY row, which IsPartialRevoke marks too. A
	// DENY withholds the privilege from the grantee and from every member of
	// it, whatever grant, schema permission or role membership gives it
	// otherwise. Ptah manages it as
	// [ptah.run/core/schemamodel.Database.DeniedGrants]. The REVOKE row SQL
	// Server keeps for a column revoked under a grant on its whole table is a
	// partial revoke and not a DENY.
	Denied bool `json:"denied,omitempty"`

	// Column names the column of the table target this privilege is limited
	// to, as pg_attribute.attacl, the MySQL family's
	// information_schema.COLUMN_PRIVILEGES or SQL Server's
	// sys.database_permissions records it. Empty for a privilege on the whole
	// object. On PostgreSQL and the MySQL family a column privilege and the
	// table privilege of the same name are two rows. On SQL Server the table
	// privilege takes the place of the column rows, and a table privilege with
	// a column row of another state under it holds for the other columns only,
	// so it is reported as the privilege on each of them.
	Column string `json:"column,omitempty"`
}

// OwnerDefault reports whether the row is a privilege an object's owner holds
// by default. No statement wrote it and a new object has it, so a description
// and a plan that creates the object again leave it out. A routine's implicit
// PUBLIC EXECUTE is not an owner privilege.
func (g Grant) OwnerDefault() bool {
	return g.Implicit && !g.IsPartialRevoke && (!g.onRoutine() || (g.Role != "PUBLIC" && g.Privilege == "EXECUTE"))
}

// OwnerRevoked reports whether the row is a default owner privilege the owner
// revoked from itself.
func (g Grant) OwnerRevoked() bool {
	return g.Implicit && g.IsPartialRevoke && (!g.onRoutine() || (g.Role != "PUBLIC" && g.Privilege == "EXECUTE"))
}

func (g Grant) onRoutine() bool {
	return strings.EqualFold(g.ObjectType, "FUNCTION") || strings.EqualFold(g.ObjectType, "PROCEDURE")
}

// QualifiedTarget returns schema.object for table, sequence and routine
// grants, and the schema name itself for schema grants. A routine's argument
// types are not part of it; see [Grant.Arguments].
func (g Grant) QualifiedTarget() string {
	if strings.EqualFold(g.ObjectType, "SCHEMA") {
		return g.ObjectName
	}
	return QualifyTableName(g.Schema, g.ObjectName)
}
