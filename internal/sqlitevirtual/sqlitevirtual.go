// Package sqlitevirtual refuses a schema comparison that would treat a live
// SQLite virtual table as an ordinary one.
//
// The desired side of a comparison comes from one of two very different
// places, and the difference decides everything here.
//
// No Ptah desired-state DOCUMENT can declare a virtual table. Go annotations,
// HCL, YAML and native `.sql` schema files have no syntax for it, and the
// native SQL parser says so out loud: feeding it `ptah db read` output for a
// database holding one fails with `unsupported CREATE target: VIRTUAL`. But
// `schema diff` also accepts a DATABASE URL as its desired side, and that
// catalog is read by the same reader, so a desired table can perfectly well be
// virtual. Conflating the two refused two identical databases as
// `cannot convert one kind into the other`, naming the desired side ordinary
// when it was the same FTS5 index.
//
// So each name at least one side calls virtual is classified, and only the
// answers a plan cannot express are refused:
//
//   - VIRTUAL LIVE, UNDECLARED. The comparator reads silence as intent and
//     plans `DROP TABLE "docs"`. Measured: `ptah schema apply --auto-approve`
//     against a desired state naming only the other table deleted an FTS5 index
//     and its three rows, leaving `no such table: docs`, with `PRAGMA
//     table_list` down from seven rows to one. A document could not have asked
//     for the table to be kept, so this is deletion the operator cannot
//     decline. Refused, waivable by [AllowDropEnvVar].
//   - VIRTUAL ON ONE SIDE, ORDINARY ON THE OTHER. Two kinds of object under one
//     name. Treating them as equal leaves an incompatible object in place while
//     every surface reports the schema synced; comparing their columns plans
//     statements SQLite refuses on a virtual table. Refused unconditionally.
//   - VIRTUAL ON BOTH, DIFFERENT DECLARATION. SQLite has no ALTER VIRTUAL
//     TABLE, so converging this means dropping and recreating, which destroys
//     the index contents. Refused unconditionally rather than planned or
//     silently called equal.
//   - VIRTUAL ON BOTH, SAME DECLARATION. Nothing to do, and nothing to refuse.
//   - VIRTUAL DESIRED, ABSENT LIVE. An addition, which already works: the
//     module declaration reaches the renderer and
//     `CREATE VIRTUAL TABLE "docs" USING fts5(title, body);` is planned.
//
// See stokaro/ptah#1028.
//
// The first refusal removes a capability, and AGENTS.md ("Compatibility never
// removes a capability. Constitute it, do not discard it.") does not allow that
// to be the end of the story. The two directions get different escapes because
// they are different requests:
//
//   - to keep the table, exclude it from the comparison. `--exclude docs`
//     already does this on every verb that compares, measured as
//     `Schema is synced, no changes to be made.` with the catalog untouched;
//   - to drop it, [AllowDropEnvVar] plans the drop exactly as before. It is an
//     environment variable rather than a flag for the reason
//     [go.5x5.cz/ptah/internal/reservedrole.AllowEnvVar] gives: the conformance
//     cli-surface tier asserts that ptah-compat registers exactly the flags the
//     pinned Atlas community binary registers.
//
// The opt-in covers only the undeclared removal. A kind collision and a changed
// declaration stay refused however it is set, because no value of an
// environment variable makes the planner able to convert one object into
// another.
package sqlitevirtual

import (
	"fmt"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/envbool"
)

// AllowDropEnvVar plans the removal of a live virtual table the desired state
// does not declare, restoring what Ptah did before the refusal existed.
//
// Setting it never makes anything succeed that the engine would refuse: the
// statement planned is the `DROP TABLE` SQLite has always accepted for a
// virtual table, which also destroys the module's shadow tables and the index
// contents. It only decides whether Ptah is willing to plan it unasked.
const AllowDropEnvVar = "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP"

var allowDrop = envbool.New(AllowDropEnvVar, false)

// DropAllowed reports whether the opt-in lifts the removal refusal.
//
// Unset keeps the refusal and a valid false spelling keeps it too; an empty or
// unparsable value is a configuration error rather than a silent refusal.
func DropAllowed() (bool, error) {
	return allowDrop.Resolve()
}

// ValidateToggle resolves [AllowDropEnvVar] for a comparison on this dialect,
// and nothing else.
//
// It exists because [ValidateComparison] needs both scoped schemas, which a
// caller only has after selection has run -- and selection can return first. A
// `schema diff` whose --include matched neither side returned before the
// variable was ever parsed, so a malformed value stayed dormant on exactly the
// runs an operator is already debugging. Callers resolve it as soon as the
// dialect is known; ValidateComparison resolves it again, which is free and
// keeps the direct caller honest.
func ValidateToggle(dialect string) error {
	if platform.NormalizeDialect(dialect) != platform.SQLite {
		return nil
	}
	_, err := DropAllowed()
	return err
}

// ValidateExplicitURLToggle resolves [AllowDropEnvVar] when databaseURL
// already identifies SQLite. Invalid and empty URLs are left to the owning
// command's normal URL validation so this preflight changes only the ordering
// of a known SQLite configuration error.
//
// Command adapters call this before loading project configuration. An explicit
// target URL, such as --db-url or --dev-url, already selects the SQLite
// subsystem, so malformed project config must not mask the malformed boolean
// value that subsystem owns. They still call [ValidateToggle] after merging
// project defaults, which covers a URL selected by project configuration.
func ValidateExplicitURLToggle(databaseURL string) error {
	dialect, err := atlasurl.DialectFromURL(databaseURL)
	if err != nil {
		return nil
	}
	return ValidateToggle(dialect)
}

// Table is one live virtual table and the module declaration that owns it.
type Table struct {
	Schema    string
	Name      string
	Module    string
	Arguments string
}

func (t Table) String() string {
	if t.Schema == "" {
		return fmt.Sprintf("%q (module %s)", t.Name, t.Module)
	}
	return fmt.Sprintf("%q.%q (module %s)", t.Schema, t.Name, t.Module)
}

// ValidateComparison refuses a comparison whose database side holds a virtual
// table, naming every offending table, its module, and the way out.
//
// It is called at the seams that already return an error and that every verb
// which compares a live database goes through. A comparison the desired state
// cannot express is refused there rather than planned badly here.
func ValidateComparison(dialect string, desired *goschema.Database, database *types.DBSchema) error {
	if platform.NormalizeDialect(dialect) != platform.SQLite {
		// A non-SQLite target has no virtual tables to classify, so this
		// subsystem is not invoked on that run and must not fail a MySQL plan
		// for a malformed value of its variable. Same boundary as
		// stokaro/ptah#1334: validate on every invocation of the subsystem that
		// owns the variable, and on no others.
		return nil
	}
	// Resolved before anything is scanned, so EVERY SQLite comparison refuses a
	// malformed value -- including the healthy pipeline that holds no virtual
	// table today. Resolving it after the scan instead left a typo'd opt-in
	// dormant: the operator believes they enabled the drop, nothing says
	// otherwise, and the value is first parsed on the day a virtual table
	// appears. Same boundary as
	// [go.5x5.cz/ptah/internal/reservedrole.ValidateDeclared], which resolves
	// after the dialect gate and before the roles are scanned.
	dropAllowed, err := DropAllowed()
	if err != nil {
		return err
	}

	semantics := identifier.ForDialect(dialect)
	sides := pairSides(desired, database, semantics)
	var collisions, transitions, removals []Table
	for _, side := range sides {
		switch {
		case side.live.virtual && !side.declared:
			// The desired state says nothing about it. When that side is a
			// document it could not have said anything, so the silence is not
			// intent: this is the data-loss path.
			removals = append(removals, side.table())
		case side.wanted.virtual && !side.present:
			// An addition. The module declaration reaches the renderer and
			// CREATE VIRTUAL TABLE is planned, so there is nothing to refuse.
		case side.live.virtual != side.wanted.virtual:
			// Both sides hold the name, and it is a virtual table on one and an
			// ordinary table on the other. Two kinds of object, one name, and
			// no statement converts one into the other.
			collisions = append(collisions, side.table())
		case !side.declarationsMatch(semantics):
			// Both virtual, different declaration. SQLite has no
			// ALTER VIRTUAL TABLE, so converging this means dropping and
			// recreating, which destroys the index contents. Refused rather
			// than planned, and refused rather than passed off as equal.
			transitions = append(transitions, side.table())
		}
	}

	if len(collisions) > 0 {
		return fmt.Errorf(
			"%w: %s is a virtual table on one side of the comparison and an ordinary table on the other: %s;"+
				" Ptah cannot convert one kind into the other, and comparing their columns would plan"+
				" statements SQLite refuses on a virtual table;"+
				" rename one of them, or exclude the name from the comparison",
			ptaherr.ErrUnsupportedFeature,
			quotedNames(collisions),
			describe(collisions),
		)
	}
	if len(transitions) > 0 {
		return fmt.Errorf(
			"%w: virtual %s %s %s declared differently on the two sides;"+
				" SQLite has no ALTER VIRTUAL TABLE, so converging %s means dropping and recreating %s,"+
				" which destroys the index contents, and Ptah does not parse module arguments to tell"+
				" an equivalent declaration from a changed one;"+
				" exclude %s from the comparison, or recreate %s deliberately",
			ptaherr.ErrUnsupportedFeature,
			noun(len(transitions)),
			quotedNames(transitions),
			verb(len(transitions)),
			pronoun(len(transitions)),
			pronoun(len(transitions)),
			quotedNames(transitions),
			pronoun(len(transitions)),
		)
	}
	if dropAllowed || len(removals) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%w: the database has virtual %s %s that the desired schema does not name;"+
			" a schema document cannot declare a virtual table at all, so the absence is not a request to drop"+
			" %s, and planning the removal would delete the index and everything in it;"+
			" exclude %s from the comparison to leave %s in place, or set %s=1 to plan the drop anyway",
		ptaherr.ErrUnsupportedFeature,
		noun(len(removals)),
		names(removals),
		pronoun(len(removals)),
		quotedNames(removals),
		pronoun(len(removals)),
		AllowDropEnvVar,
	)
}

// declaration is one side's view of a table name.
type declaration struct {
	virtual   bool
	module    string
	arguments string
}

// pairedSide is one table name seen from both sides of a comparison.
//
// The desired side is not always a document. `schema diff` accepts a database
// URL for it, and that catalog is read by the same reader, so a DESIRED table
// can itself be virtual -- which makes "the desired state declares this name"
// and "the desired state declares an ordinary table" two different facts. A
// check that conflated them refused two identical databases with
// `cannot convert one kind into the other`, naming the desired side as ordinary
// when it was the same FTS5 index. See stokaro/ptah#1028.
type pairedSide struct {
	schema string
	name   string
	// present reports that the DATABASE holds a table of this name, and
	// declared that the DESIRED side names it. Both are needed: a desired
	// virtual table with no live counterpart is an addition, which works, and
	// reading its absence as an ordinary table made the validator refuse one.
	present  bool
	declared bool
	live     declaration
	wanted   declaration
}

func (s pairedSide) table() Table {
	module := s.live.module
	if module == "" {
		module = s.wanted.module
	}
	return Table{Schema: s.schema, Name: s.name, Module: module}
}

// declarationsMatch compares two virtual-table declarations.
//
// The module is an identifier, which SQLite resolves case-insensitively, so it
// is folded the same way a table name is. The arguments are compared verbatim:
// they are not SQL, only the module interprets them, and normalizing whitespace
// would equate `tokenize = 'a  b'` with `tokenize = 'a b'`, which are two
// different tokenizers. Two catalogs written by the same statement carry the
// same bytes, so the common case matches; anything else is reported rather than
// guessed at.
func (s pairedSide) declarationsMatch(semantics identifier.Semantics) bool {
	return semantics.TableIdentityKey(s.live.module) == semantics.TableIdentityKey(s.wanted.module) &&
		s.live.arguments == s.wanted.arguments
}

// pairSides joins the two sides of the comparison on table identity, keeping
// only the names at least one side calls a virtual table.
func pairSides(
	desired *goschema.Database,
	database *types.DBSchema,
	semantics identifier.Semantics,
) []pairedSide {
	sides := make(map[string]*pairedSide)
	at := func(schema, name string) *pairedSide {
		key := identity(schema, name, semantics)
		if side, ok := sides[key]; ok {
			return side
		}
		side := &pairedSide{schema: schema, name: name}
		sides[key] = side
		return side
	}

	if database != nil {
		for _, table := range database.Tables {
			side := at(table.Schema, table.Name)
			side.present = true
			side.live = declaration{
				virtual:   table.VirtualModule != "",
				module:    table.VirtualModule,
				arguments: table.VirtualArguments,
			}
		}
	}
	if desired != nil {
		for _, table := range desired.Tables {
			side := at(table.Schema, table.Name)
			side.declared = true
			side.wanted = declaration{
				virtual:   table.VirtualModule != "",
				module:    table.VirtualModule,
				arguments: table.VirtualArguments,
			}
		}
	}

	paired := make([]pairedSide, 0, len(sides))
	for _, side := range sides {
		if !side.live.virtual && !side.wanted.virtual {
			continue
		}
		paired = append(paired, *side)
	}
	sort.Slice(paired, func(i, j int) bool {
		if paired[i].schema != paired[j].schema {
			return paired[i].schema < paired[j].schema
		}
		return paired[i].name < paired[j].name
	})
	return paired
}

// describe renders each table as the side that calls it virtual sees it.
func describe(tables []Table) string {
	return names(tables)
}

func verb(count int) string {
	if count == 1 {
		return "is"
	}
	return "are"
}

// Names renders a table list as the quoted names a diagnostic prints, in the
// order Tables produced them.
func Names(tables []Table) []string {
	rendered := make([]string, 0, len(tables))
	for _, table := range tables {
		rendered = append(rendered, table.String())
	}
	return rendered
}

// Tables lists the virtual tables a database schema holds, in a stable order.
func Tables(database *types.DBSchema) []Table {
	if database == nil {
		return nil
	}
	var virtual []Table
	for _, table := range database.Tables {
		if table.VirtualModule == "" {
			continue
		}
		virtual = append(virtual, Table{
			Schema:    table.Schema,
			Name:      table.Name,
			Module:    table.VirtualModule,
			Arguments: table.VirtualArguments,
		})
	}
	sort.Slice(virtual, func(i, j int) bool {
		if virtual[i].Schema != virtual[j].Schema {
			return virtual[i].Schema < virtual[j].Schema
		}
		return virtual[i].Name < virtual[j].Name
	})
	return virtual
}

// identity is the comparator's own table identity, built from
// [identifier.Semantics] rather than from a second spelling of the rule.
//
// SQLite matches table names case-insensitively, so `CREATE TABLE DOCS`
// collides with a virtual `docs` and a comparison that missed it would refuse
// nothing and plan the ALTER anyway. But the folding is ASCII ONLY, which
// `strings.ToLower` is not: measured on a real database, `CREATE VIRTUAL TABLE
// "Ä"` and `CREATE TABLE "ä"` both succeed and `PRAGMA table_list` reports two
// tables, while `CREATE TABLE DOCS` beside `docs` fails with
// `table DOCS already exists`. Folding Unicode here would invent a collision
// the engine does not see and refuse a comparison that has nothing wrong with
// it.
func identity(schema, name string, semantics identifier.Semantics) string {
	if schema == "" {
		schema = semantics.DefaultSchema
	}
	return semantics.TableIdentityKey(schema) +
		"\x00" + semantics.TableIdentityKey(name)
}

func names(tables []Table) string {
	rendered := make([]string, 0, len(tables))
	for _, table := range tables {
		rendered = append(rendered, table.String())
	}
	return strings.Join(rendered, ", ")
}

func quotedNames(tables []Table) string {
	rendered := make([]string, 0, len(tables))
	for _, table := range tables {
		rendered = append(rendered, fmt.Sprintf("%q", table.Name))
	}
	return strings.Join(rendered, ", ")
}

func noun(count int) string {
	if count == 1 {
		return "table"
	}
	return "tables"
}

func pronoun(count int) string {
	if count == 1 {
		return "it"
	}
	return "them"
}
