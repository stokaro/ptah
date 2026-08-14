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
//
// # When the module is not registered
//
// Everything above assumes the description is right about which tables are
// tables, and where the module is missing it is not. SQLite marks a table
// `shadow` only while the module that owns it is loaded, so a build without
// `fts4` describes that module's five storage tables as ordinary user tables --
// and a desired state that does not name them reads as a request to drop them.
// Measured: `--exclude docs`, the remedy the removal refusal printed, planned
// and executed five `DROP TABLE` statements at exit 0 and left `MATCH`
// answering `SQL logic error`.
//
// So a comparison over such a database is refused BEFORE any of the cases above
// are classified, and the refusal does not suggest an exclusion, because the
// tables at risk are not the one an operator would exclude and Ptah cannot list
// them without the module. It is waivable by
// [AllowUnregisteredModuleEnvVar], which restores what Ptah did before.
//
// It fires only when the plan could act on such a table -- when some live table
// is one the desired side does not name, or names with a different column list.
// That is decidable without identifying the module's tables, which is the whole
// difficulty, and it is what keeps a narrowed comparison such as
// `--include users` running: nothing in it is droppable or alterable, so
// nothing can be destroyed however badly Ptah has misclassified it.
//
// Adding a virtual table whose module is absent is refused separately and with
// no opt-in, because the `CREATE VIRTUAL TABLE` a plan would carry fails with
// `no such module`. That check belongs to the addition branch alone: two states
// that both already hold the table plan no such statement.
package sqlitevirtual

import (
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/envbool"
	"go.5x5.cz/ptah/internal/sqlitemodule"
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

// AllowUnregisteredModuleEnvVar compares a database holding a virtual table
// whose module this build does not register, treating that module's private
// storage as the ordinary tables it appears to be.
//
// It exists because the refusal it lifts removes a capability, and AGENTS.md
// ("Compatibility never removes a capability. Constitute it, do not discard
// it.") does not allow that to be the end of the story. Comparing such a
// database is something Ptah did before, and an operator who knows their
// database -- who knows the module keeps no shadow tables, or has already
// excluded every one of them by name -- can still ask for it.
//
// Setting it does not make Ptah able to classify anything. It grants exactly
// one thing: permission to plan from a description Ptah has said it cannot
// vouch for. The measured consequence of doing that on an fts4 database was
// five `DROP TABLE` statements at exit 0 and an index that stopped answering
// `MATCH`, so the default is the refusal and the opt-in is deliberate.
//
// It is separate from [AllowDropEnvVar] because the two answer different
// questions. That one says "yes, drop the virtual table I can see"; this one
// says "yes, plan against tables I have been told may not be tables". Folding
// them together would let an operator who wanted the first silently accept the
// second.
//
// It is an environment variable rather than a flag for the reason
// [go.5x5.cz/ptah/internal/reservedrole.AllowEnvVar] gives: the conformance
// cli-surface tier asserts that ptah-compat registers exactly the flags the
// pinned Atlas community binary registers.
const AllowUnregisteredModuleEnvVar = "PTAH_SQLITE_ALLOW_UNREGISTERED_VIRTUAL_MODULE"

var allowUnregisteredModule = envbool.New(AllowUnregisteredModuleEnvVar, false)

// UnregisteredModuleAllowed reports whether the opt-in lifts the refusal to
// compare a database Ptah could not fully classify.
//
// Unset keeps the refusal and a valid false spelling keeps it too; an empty or
// unparsable value is a configuration error rather than a silent refusal.
func UnregisteredModuleAllowed() (bool, error) {
	return allowUnregisteredModule.Resolve()
}

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
	if _, err := DropAllowed(); err != nil {
		return err
	}
	// Both toggles this package owns are resolved here, for the same reason
	// either is resolved early: a typo in one of them must be reported on every
	// SQLite comparison, not only on the runs that reach the condition it
	// governs. An operator who misspells the unregistered-module opt-in and is
	// told nothing believes the refusal is unconditional.
	_, err := UnregisteredModuleAllowed()
	return err
}

// ValidateExplicitURLToggle resolves [AllowDropEnvVar] when databaseURL
// already identifies SQLite. Invalid and empty URLs are left to the owning
// command's normal URL validation so this preflight changes only the ordering
// of a known SQLite configuration error.
//
// Native commands call this before loading project configuration. An explicit
// target URL, such as --db-url or replay --dev-url, already selects the SQLite
// subsystem, so malformed project config must not mask the malformed boolean
// value that subsystem owns. They still call [ValidateToggle] after merging
// project defaults, which covers a URL selected by ptah.yaml.
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
	registered, err := sqlitemodule.Registered()
	if err != nil {
		return err
	}
	semantics := identifier.ForDialect(dialect)
	// Asked before anything is classified, because it decides whether the
	// classification means anything. Every branch below reads `this table is
	// ordinary` off the description; where a module is missing that answer was
	// never SQLite's, and the tables it is wrong about are not the ones an
	// operator can see or exclude.
	if err := validateDatabaseIsClassifiable(desired, database, registered, semantics); err != nil {
		return err
	}

	sides := pairSides(desired, database, semantics)
	var collisions, transitions, removals, uncreatable []Table
	for _, side := range sides {
		switch {
		case side.live.virtual && !side.declared:
			// The desired state says nothing about it. When that side is a
			// document it could not have said anything, so the silence is not
			// intent: this is the data-loss path.
			removals = append(removals, side.table())
		case side.wanted.virtual && !side.present:
			// An addition: the module declaration reaches the renderer and
			// CREATE VIRTUAL TABLE is planned. That statement is the only thing
			// a missing module makes impossible, so this is the ONE branch the
			// desired-side check belongs in.
			//
			// Checking every desired virtual table instead refused a diff of two
			// databases that both already hold the same fts4 index, where no
			// CREATE is planned at all and the diagnostic's claimed mid-apply
			// failure cannot happen -- and it did so even with
			// [AllowUnregisteredModuleEnvVar] set, so the opt-in did not restore
			// the comparison it promises. That is the same conflation of "the
			// desired state names this" with "the desired state adds this" that
			// this package's doc comment records from stokaro/ptah#1028, one
			// level along.
			if !registered.Registers(side.wanted.module) {
				uncreatable = append(uncreatable, side.table())
			}
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

	if len(uncreatable) > 0 {
		return refuseUncreatableAdditions(uncreatable, registered)
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

// validateDatabaseIsClassifiable refuses a comparison whose DATABASE side holds
// a virtual table this build cannot load the module for.
//
// The failure it prevents is silent and destructive. SQLite marks a module's
// shadow tables as `shadow` only while the module is loaded, so with it absent
// the module's private storage is described as ordinary user tables -- and a
// desired state that does not name them is read as a request to drop them.
// Measured on stokaro/ptah#1028 against an fts4 database: five `DROP TABLE`
// statements planned and executed at exit 0, after which `MATCH` reported
// `SQL logic error`. Refused, waivable by [AllowUnregisteredModuleEnvVar],
// because comparing such a database is something an operator could do before.
//
// It fires only when the comparison can actually act on such a table -- when
// some live table in it is one the desired side does not name, or names with a
// different column list. That condition is decidable without knowing WHICH
// tables are the module's, which is the whole difficulty: the planner only
// touches a live table that is missing from the desired state or described
// differently there, so if no live table is in either position, nothing is
// planned against any of them however badly Ptah has misclassified them.
//
// Scoping is what makes the distinction matter. Measured: `--include users`
// against an fts4 database narrows the comparison to `users` alone and plans
// nothing -- `Schemas are synced, no changes to be made.` -- yet an
// unconditional refusal rejected it at exit 2 and sent the operator to the
// opt-in for a run that was already safe. `--exclude docs` on the same database
// is the opposite case and must still be refused, because it leaves the
// module's five storage tables in the comparison with nothing naming them.
//
// The residue is stated rather than hidden: a desired state that DOES name such
// a table, with a different shape, can still plan an ALTER against module
// storage. Bounding that would mean knowing which tables are the module's, and
// nothing here can. This check bounds the destruction that was measured.
//
// The desired side is NOT checked here. Its only impossible statement is the
// `CREATE VIRTUAL TABLE` an addition plans, which is a question about one
// paired name rather than about the description as a whole, so it is asked
// where the pairing already decides that a name is an addition. See the
// `side.wanted.virtual && !side.present` branch in [ValidateComparison].
func validateDatabaseIsClassifiable(
	desired *goschema.Database,
	database *types.DBSchema,
	registered sqlitemodule.Set,
	semantics identifier.Semantics,
) error {
	// Resolved before the side is scanned, so a malformed opt-in is reported on
	// every SQLite comparison rather than only the ones holding an unregistered
	// module. [ValidateToggle] already resolved it at the seam; doing it again
	// is free and keeps this function honest on its own.
	unregisteredAllowed, err := UnregisteredModuleAllowed()
	if err != nil {
		return err
	}
	if unregisteredAllowed {
		return nil
	}
	unclassified := liveUnregistered(database, registered)
	if len(unclassified) == 0 {
		return nil
	}
	if !someLiveTableCouldChange(desired, database, semantics) {
		return nil
	}
	return fmt.Errorf(
		"%w: the database holds virtual %s %s whose %s this build of Ptah does not register;"+
			" SQLite marks a module's shadow tables as shadow only while the module is loaded, so Ptah"+
			" cannot tell that module's private storage from ordinary tables, and comparing this"+
			" description would plan DROP TABLE for whichever of those tables the desired schema does"+
			" not name; excluding %s does not protect the index, because the tables at risk are the"+
			" module's own storage rather than %s, and Ptah cannot list them without the module;"+
			" this build registers %s;"+
			" read this database with a build that registers %s, or set %s=1 to compare them as"+
			" ordinary tables and accept those drops",
		ptaherr.ErrUnsupportedFeature,
		noun(len(unclassified)),
		names(unclassified),
		moduleNoun(len(distinctModules(unclassified))),
		quotedNames(unclassified),
		pronoun(len(unclassified)),
		registered.String(),
		modulesOf(unclassified),
		AllowUnregisteredModuleEnvVar,
	)
}

// refuseUncreatableAdditions refuses the virtual tables a plan would CREATE and
// this build cannot.
//
// It fires only for a name the desired side adds -- virtual there, absent from
// the database -- because that is the only shape whose plan carries
// `CREATE VIRTUAL TABLE ... USING <module>`, which this build answers with
// `no such module: <module>`. Measured mid-apply on stokaro/ptah#1028, after
// the plan had been printed and auto-approved, with the target left half
// converged.
//
// There is no opt-in, for the same reason a kind collision has none: no value
// of an environment variable makes a module exist. That is also why the check
// must not fire for a name both sides already hold -- there the statement never
// appears, so an unwaivable refusal would take away a comparison an operator
// can legitimately run and that [AllowUnregisteredModuleEnvVar] is supposed to
// restore.
func refuseUncreatableAdditions(wanted []Table, registered sqlitemodule.Set) error {
	return fmt.Errorf(
		"%w: the desired schema adds virtual %s %s whose %s this build of Ptah does not register;"+
			" creating %s means the statement `CREATE VIRTUAL TABLE ... USING %s`, which this build"+
			" answers with `no such module: %s`, so the comparison can only produce a plan that stops"+
			" part of the way through applying it;"+
			" this build registers %s;"+
			" apply this schema with a build that registers %s",
		ptaherr.ErrUnsupportedFeature,
		noun(len(wanted)),
		names(wanted),
		moduleNoun(len(distinctModules(wanted))),
		pronoun(len(wanted)),
		wanted[0].Module,
		wanted[0].Module,
		registered.String(),
		modulesOf(wanted),
	)
}

// someLiveTableCouldChange reports whether the comparison holds a live table
// the plan could act on -- one the desired side does not name, or names with a
// different column list.
//
// Both halves matter and they were found one after the other. The first is the
// `DROP TABLE` measured on stokaro/ptah#1028. The second was raised in review:
// two database-backed states can name the same storage table and describe it
// differently, and the comparator then plans `ALTER TABLE` -- or on SQLite the
// rebuild that stands in for one -- against module-owned storage, which
// corrupts the index exactly as thoroughly as dropping it.
//
// Column NAMES are compared, and their order, not their types. That is
// deliberate. The question this answers is "could the plan touch this table",
// and a false yes costs an operator a refusal they did not need -- which is the
// defect the reachability gate was added to fix. Type spellings are where two
// models legitimately differ (`INTEGER` against `integer`), and normalizing
// them here would be a second, drifting copy of the comparator's own rules. A
// type-only change to a table that is really module storage is the residue, and
// it is a narrow one: both sides of such a comparison are read by the same
// reader, which gives the same type text for the same column.
//
// It joins on the comparator's own table identity rather than on a second
// spelling of the rule, for the reason [identity] gives: SQLite folds ASCII
// only, and a Unicode fold would call `"Ä"` and `"ä"` one table when the engine
// reports two.
//
// A nil desired state names nothing, so every live table is undeclared and the
// answer is true for any non-empty database. That is the same reading
// [pairSides] gives it, and it keeps a nil desired state from being mistaken
// for one that declared everything.
func someLiveTableCouldChange(
	desired *goschema.Database,
	database *types.DBSchema,
	semantics identifier.Semantics,
) bool {
	if database == nil {
		return false
	}
	declared := desiredColumnsByTable(desired, semantics)
	for _, table := range database.Tables {
		wanted, ok := declared[identity(table.Schema, table.Name, semantics)]
		if !ok {
			return true
		}
		if !slices.Equal(wanted, liveColumnNames(table, semantics)) {
			return true
		}
	}
	return false
}

// desiredColumnsByTable indexes the desired side's column names by table
// identity. A declared table with no fields maps to an empty list, which is
// what a virtual table looks like on both sides and must compare equal.
func desiredColumnsByTable(
	desired *goschema.Database,
	semantics identifier.Semantics,
) map[string][]string {
	declared := make(map[string][]string)
	if desired == nil {
		return declared
	}
	columnsByStruct := make(map[string][]string)
	for _, field := range desired.Fields {
		columnsByStruct[field.StructName] = append(
			columnsByStruct[field.StructName],
			semantics.TableIdentityKey(field.Name),
		)
	}
	for _, table := range desired.Tables {
		key := identity(table.Schema, table.Name, semantics)
		declared[key] = columnsByStruct[table.StructName]
	}
	return declared
}

func liveColumnNames(table types.DBTable, semantics identifier.Semantics) []string {
	names := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		names = append(names, semantics.TableIdentityKey(column.Name))
	}
	return names
}

// liveUnregistered lists the database side's unclassifiable virtual tables.
//
// It reads two sources and unions them, because they answer at different times
// and only one of them survives narrowing:
//
//   - [types.DBSchema.UnregisteredVirtualTables] is the reader's own statement,
//     recorded before any selection ran. It is the source that still speaks
//     after `--exclude docs` has removed the virtual table, which is exactly
//     the run that plans the drops.
//   - the tables still in the description are checked directly, so a DBSchema
//     built by something other than the SQLite reader -- a test, a future
//     producer -- cannot walk past this by leaving the field empty. A zero
//     value must not read as "every module is present".
func liveUnregistered(database *types.DBSchema, registered sqlitemodule.Set) []Table {
	if database == nil {
		return nil
	}
	seen := make(map[string]struct{})
	var unclassified []Table
	add := func(schema, name, module string) {
		if module == "" || registered.Registers(module) {
			return
		}
		if _, ok := seen[schema+"\x00"+name]; ok {
			return
		}
		seen[schema+"\x00"+name] = struct{}{}
		unclassified = append(unclassified, Table{Schema: schema, Name: name, Module: module})
	}
	for _, table := range database.UnregisteredVirtualTables {
		add(table.Schema, table.Name, table.Module)
	}
	for _, table := range database.Tables {
		add(table.Schema, table.Name, table.VirtualModule)
	}
	sortTables(unclassified)
	return unclassified
}

// distinctModules lists the modules of a table list once each, in a stable
// order. The tables are what an operator recognizes; the modules are what a
// different build would have to register, and two tables can share one.
func distinctModules(tables []Table) []string {
	var modules []string
	for _, table := range tables {
		if !slices.Contains(modules, table.Module) {
			modules = append(modules, table.Module)
		}
	}
	sort.Strings(modules)
	return modules
}

// modulesOf renders the distinct modules the way a diagnostic prints them.
func modulesOf(tables []Table) string {
	return strings.Join(distinctModules(tables), ", ")
}

// moduleNoun agrees with the number of DISTINCT modules rather than the number
// of tables, because two tables of one module are still one missing module and
// "whose modules" would then name a set of size one.
func moduleNoun(count int) string {
	if count == 1 {
		return "module"
	}
	return "modules"
}

func sortTables(tables []Table) {
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Schema != tables[j].Schema {
			return tables[i].Schema < tables[j].Schema
		}
		return tables[i].Name < tables[j].Name
	})
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

// ReportUnclassified writes a note naming the virtual tables whose module the
// reading build could not load, and nothing at all when there are none.
//
// It belongs on the read surfaces, which refuse nothing: `ptah db read` and
// `schema inspect` produce a description, and for these databases that
// description is wrong in a way the reader can name and the operator cannot
// see. Measured on an fts4 database, `ptah db read` emits the virtual table
// correctly and then five `CREATE TABLE` statements for the module's own
// storage; replayed, those five collide with the index the first statement
// creates. A reader shown that output with nothing said has no way to know
// which half is real.
//
// It reports the tables and the module rather than a count, the opposite of
// [go.5x5.cz/ptah/internal/rolescope.ReportUndescribed]. That note withholds
// names because they come from outside the inspected scope and can belong to
// another tenant; these names are in the document the operator is already
// looking at, and the note is useless without saying which of the statements
// below it are the suspect ones.
//
// w may be nil, which is how the inspect surfaces spell "no diagnostics
// stream"; the note is then dropped rather than panicking. Write errors are
// dropped too: a diagnostic that fails to print must not fail a read that
// succeeded.
func ReportUnclassified(w io.Writer, schema *types.DBSchema) {
	if w == nil || schema == nil || len(schema.UnregisteredVirtualTables) == 0 {
		return
	}
	// A document with no tables in it has no statements to warn about, and
	// selection can produce one: `--include` naming something the database does
	// not have renders nothing, and a note beside an empty rendering describes
	// a document that does not exist.
	if len(schema.Tables) == 0 {
		return
	}

	// Which virtual tables the document still contains decides WHICH note, not
	// whether there is one. Selection runs before this, and the two projections
	// it can produce need opposite things said:
	//
	//   - the virtual table survived: name it, because the reader can see it
	//     and the tables around it are the module's;
	//   - the virtual table was projected out -- `--exclude docs` -- but the
	//     module's storage was not, because nothing marked it. Naming `docs`
	//     here would send the reader looking for a statement that is not in the
	//     document, so the note says what is true instead: this description was
	//     narrowed, and Ptah cannot tell whether the module's tables are still
	//     in it. Suppressing it entirely was worse, and was raised in review:
	//     replaying that document creates module-private tables with nothing
	//     said at all.
	semantics := identifier.ForDialect(platform.SQLite)
	present := make(map[string]struct{}, len(schema.Tables))
	for _, table := range schema.Tables {
		present[identity(table.Schema, table.Name, semantics)] = struct{}{}
	}
	rendered := make([]Table, 0, len(schema.UnregisteredVirtualTables))
	all := make([]Table, 0, len(schema.UnregisteredVirtualTables))
	for _, table := range schema.UnregisteredVirtualTables {
		named := Table{Schema: table.Schema, Name: table.Name, Module: table.Module}
		all = append(all, named)
		if _, ok := present[identity(table.Schema, table.Name, semantics)]; ok {
			rendered = append(rendered, named)
		}
	}
	if len(rendered) == 0 {
		fmt.Fprintf(w,
			"note: this description was narrowed, and the database it came from uses %s %s this build"+
				" does not register. SQLite could not mark the tables %s owns, so Ptah cannot tell"+
				" whether any of the ordinary tables below are the module's private storage. Applying"+
				" them would create tables the module creates itself.\n",
			moduleNoun(len(distinctModules(all))),
			modulesOf(all),
			pronounFor(len(distinctModules(all))),
		)
		return
	}
	fmt.Fprintf(w,
		"note: virtual %s %s %s a module this build does not register, so SQLite could not mark the"+
			" tables that %s owns and this description reports them as ordinary tables. Applying it"+
			" creates tables the module creates itself, and comparing it can plan their removal. Set"+
			" %s=1 to compare anyway.\n",
		noun(len(rendered)),
		names(rendered),
		useVerb(len(rendered)),
		modulesOf(rendered),
		AllowUnregisteredModuleEnvVar,
	)
}

// pronounFor agrees with a module count the way [pronoun] agrees with a table
// count.
func pronounFor(count int) string {
	if count == 1 {
		return "it"
	}
	return "they"
}

func useVerb(count int) string {
	if count == 1 {
		return "uses"
	}
	return "use"
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
