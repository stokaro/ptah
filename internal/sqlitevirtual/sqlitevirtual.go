// Package sqlitevirtual refuses a schema comparison that would treat a live
// SQLite virtual table as an ordinary one.
//
// The desired side of a comparison comes from one of two very different
// places, and the difference decides everything here.
//
// A desired state can name a virtual table two ways. A native `.sql` schema
// file may declare `CREATE VIRTUAL TABLE ... USING ...`, which is what makes
// `ptah db read` output readable back by the tool that wrote it; and
// `schema diff` accepts a DATABASE URL as its desired side, read by the same
// reader that produced it. Go annotations, HCL and YAML still have no syntax
// for one.
//
// Conflating a virtual table with an ordinary one refused two identical
// databases as `cannot convert one kind into the other`, naming the desired
// side ordinary when it was the same FTS5 index.
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
// It fires only when a plan can act on such a table, and that question is asked
// in three places: only part of it is answerable before the comparison runs, and
// a generated migration is two plans rather than one.
//
//   - Here: some live table is one the desired side does not name, which is
//     exactly the comparator's removal set.
//   - Afterwards, in [ValidatePlannedChanges]: the diff removes or rebuilds
//     something.
//   - Where a rollback is planned beside the migration, in
//     [ValidatePlannedRollback]: the REVERSED diff does. Reversal turns changes
//     SQLite performs in place into changes it does not, so an up file that is
//     one `ALTER TABLE ... ADD COLUMN` -- which the second check admits, and
//     should -- can be published beside a down file that rebuilds the module's
//     storage.
//
// None of the three needs to identify the module's tables, which is the whole
// difficulty, and together they keep a narrowed comparison such as
// `--include users` running -- nothing in it is dropped or rebuilt, so nothing
// can be destroyed however badly Ptah has misclassified it.
//
// The first two questions are about statements a caller may still filter, so
// both take the caller's [Policy]: a project that skips `drop_table` deletes
// every table drop from the diff before it is planned, and a refusal keyed on a
// drop that will never be rendered is a refusal for something that cannot
// happen. The third does not, because by then the filtering has already run;
// see [ValidatePlannedRollback].
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
	"go.5x5.cz/ptah/internal/planner/objectlookup"
	"go.5x5.cz/ptah/internal/planner/sqliterebuild"
	"go.5x5.cz/ptah/internal/sqlitemodule"
	"go.5x5.cz/ptah/migration/diffpolicy"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// AllowDropEnvVar plans the removal of a live virtual table the desired state
// does not declare, restoring what Ptah did before the refusal existed.
//
// Setting it never makes anything succeed that the engine would refuse: the
// statement planned is the `DROP TABLE` SQLite has always accepted for a
// virtual table, which also destroys the module's shadow tables and the index
// contents. It only decides whether Ptah is willing to plan it unasked.
const AllowDropEnvVar = "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP"

// It is [go.5x5.cz/ptah/internal/envbool.Retained]: a true value restores the
// `DROP TABLE` the pinned community binary plans for a SQLite virtual table
// anyway, so refusing it in strict mode would move Ptah further from the
// oracle rather than closer.
var allowDrop = envbool.New(AllowDropEnvVar, false, envbool.Retained)

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

// It is [go.5x5.cz/ptah/internal/envbool.Retained], and the argument is if
// anything stronger than its sibling's. The refusal it lifts is Ptah's own: the
// pinned community binary has no notion of a module this build cannot classify
// and plans the drops regardless. A true value therefore restores oracle
// behavior rather than adding a capability beyond it, so gating it would make
// strict mode the one place Ptah is stricter than the binary it exists to
// match. The parse is still owed: a malformed value would otherwise stay
// dormant until a comparison happens to meet an unregistered module, which is
// precisely the run an operator is already debugging. See stokaro/ptah#1028.
var allowUnregisteredModule = envbool.New(AllowUnregisteredModuleEnvVar, false, envbool.Retained)

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

// Policy is what the caller will do to the diff between the comparison and the
// plan, as far as this guard's predictions are concerned.
//
// Every refusal in this package is a claim about a statement: "planning the
// removal would delete the index", "comparing this description would plan DROP
// TABLE", "the plan changes docs_content". A caller whose diff policy deletes
// those statements again makes the claim false, and refusing on it is the
// over-refusal this package has now had to fix three times -- once for
// `--include users`, once for two databases holding the same index, and once
// here.
//
// The zero value promises nothing, so a seam that says nothing keeps the
// strictest reading and a new caller that forgets to pass its policy refuses
// more rather than less.
type Policy struct {
	// SkipDropTable reports that the caller removes every table drop from the
	// diff before planning it -- `diff.skip: [drop_table]` in ptah.yaml, and
	// `diff { skip { drop_table = true } }` in an Atlas project file.
	//
	// It carries the dependent removals with it, which is what makes the whole
	// table safe to discount rather than only its DROP TABLE: both
	// implementations of this policy also drop the index, constraint, trigger,
	// RLS and grant removals belonging to a table they keep
	// ([go.5x5.cz/ptah/migration/diffpolicy.Apply] and
	// [go.5x5.cz/ptah/internal/atlasschema.ApplyDiffPolicy]). A table that is
	// not dropped and whose dependent removals are gone is not rebuilt either.
	//
	// It does NOT cover a rebuild. `skip drop_table` filters removals, not
	// modifications, so a table both sides name and describe differently is
	// still dropped and recreated by the SQLite planner -- which is why the
	// post-diff half of the guard keeps running under this policy and only its
	// removal input is discounted.
	SkipDropTable bool

	// SkipDropColumn reports that the caller removes every column drop from the
	// diff before planning it -- `diff.skip: [drop_column]` in ptah.yaml.
	//
	// A column removal is one of the shapes SQLite has no ALTER for, so
	// [sqliterebuild.NeedsTableRebuild] reports a rebuild for any table diff
	// carrying one, and a rebuild is what [ValidatePlannedChanges] refuses. A
	// caller that deletes the column drop again leaves a table diff the planner
	// converges without a rebuild, or with nothing at all.
	//
	// Measured: `ptah migrations generate` with
	// `diff.skip: [drop_table, drop_column]` against an fts4 database, dropping
	// one column from an ordinary `users` table, was refused at exit 2 -- while
	// the same run with the opt-in set exited 0 and wrote no migration file at
	// all, because the policy had emptied the plan.
	SkipDropColumn bool

	// SkipDropIndex reports that the caller removes every standalone index drop
	// from the diff before planning it -- `diff.skip: [drop_index]` in
	// ptah.yaml.
	//
	// It is standalone drops only. An index REPLACEMENT -- dropped and
	// recreated under the same name -- is kept by both implementations of this
	// policy, so it is kept here too, and the table it is aimed at stays
	// counted.
	SkipDropIndex bool
}

// skipSet is this policy in the vocabulary the callers filter with, so the
// prediction and the filtering cannot disagree about what a skip removes.
//
// [diffpolicy.ChangeKind] has a fourth member, `drop_enum`, and it is
// deliberately absent: SQLite has no enum type, nothing in a SQLite diff
// populates EnumsRemoved, and this gate reads no enum field. The census in
// TestDiffPolicySkipKindsAreClassified fails if a fifth kind appears without
// being placed on one side of that line.
func (p Policy) skipSet() diffpolicy.SkipSet {
	var kinds []diffpolicy.ChangeKind
	if p.SkipDropTable {
		kinds = append(kinds, diffpolicy.DropTable)
	}
	if p.SkipDropColumn {
		kinds = append(kinds, diffpolicy.DropColumn)
	}
	if p.SkipDropIndex {
		kinds = append(kinds, diffpolicy.DropIndex)
	}
	return diffpolicy.NewSkipSet(kinds...)
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
//
// policy is what the caller will do to the diff before it is planned; see
// [Policy]. The refusals that predict a `DROP TABLE` stand down when the caller
// has said it removes every one of them.
func ValidateComparison(
	dialect string,
	desired *goschema.Database,
	database *types.DBSchema,
	policy Policy,
) error {
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
	if err := validateDatabaseIsClassifiable(desired, database, registered, semantics, policy); err != nil {
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
			//
			// Unless the caller deletes the statement again. `skip drop_table`
			// filters this table out of TablesRemoved before the plan is
			// rendered, so the `DROP TABLE "docs"` this refusal exists to
			// prevent is never emitted, and refusing sends an operator to an
			// opt-in for a plan that drops nothing. Measured with
			// `diff.skip: [drop_table]` in ptah.yaml: both opt-ins set,
			// `ptah schema apply` reports `Schema is synced, no changes to be
			// made.` at exit 0 -- and without them it exited 2.
			if policy.SkipDropTable {
				continue
			}
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
//
// The whole condition is about a DROP TABLE, so a caller whose [Policy] skips
// table drops is answered by [ValidatePlannedChanges] alone. That is not a hole:
// the rebuild half of the harm is exactly what the post-diff gate reads, and
// `skip drop_table` does not filter a modification.
func validateDatabaseIsClassifiable(
	desired *goschema.Database,
	database *types.DBSchema,
	registered sqlitemodule.Set,
	semantics identifier.Semantics,
	policy Policy,
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
	// Asked AFTER the opt-in is resolved, deliberately: a malformed value must
	// still be a configuration error on the projects that configure a diff
	// policy, or the typo stays dormant on exactly the runs this branch lets
	// through (stokaro/ptah#1334).
	if policy.SkipDropTable {
		return nil
	}
	unclassified := liveUnregistered(database, registered)
	if len(unclassified) == 0 {
		return nil
	}
	if !someLiveTableIsUndeclared(desired, database, semantics) {
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

// ValidatePlannedChanges refuses a plan that would change a table in a database
// whose modules this build cannot load.
//
// It is the second half of the same guard, and it exists because the first half
// cannot be completed where it stands. [ValidateComparison] runs before
// anything is compared, so it can only ask questions it can answer without the
// comparator: "is this live table missing from the desired state" is one of
// those, and it is exactly the comparator's removal set. "Would this table be
// CHANGED" is not. Two database-backed states can name the same table and
// differ in a column's type, nullability, default, generated expression, or a
// table constraint, and every one of those makes the SQLite planner rebuild the
// table -- drop, recreate, copy -- which destroys a module's storage as
// thoroughly as dropping it outright. Answering that here means a second copy
// of the comparator's rules, free to drift from the rules that actually decide.
//
// So it is asked afterwards, of the comparator's own answer. A diff that drops
// or rebuilds nothing cannot destroy the module's storage, whatever Ptah
// believes that storage to be, and a diff that drops or rebuilds something in a
// database Ptah cannot classify might be destroying it.
//
// The set is destructive-change rather than "anything changed", and the
// difference is a whole class of change: `ALTER TABLE ... ADD COLUMN` is a
// statement SQLite has, so a table whose only change is added columns is
// neither dropped nor rebuilt and is not counted, and neither is a table that
// only GAINS an index or a trigger. What is counted beside a drop and a rebuild
// is a removal from a table: an index dropped or replaced on it, a trigger
// dropped or replaced on it. Those reach the plan without the table being
// touched, and Ptah can no more tell whose index it is than whose table it is.
// See [tablesTouchedBy], which asks the planner's own rebuild predicate rather
// than a second spelling of it.
//
// Callers pass the same database description they passed to
// [ValidateComparison], and the same [Policy]. The diff reaching this seam is
// the comparator's answer rather than the plan -- every caller that filters it
// does so afterwards -- so the policy is how this gate learns which of the
// changes it can see will still be there when the SQL is rendered.
//
// A database with no unclassifiable module returns nil whatever the diff says,
// so this is inert for every other SQLite comparison and for every other
// dialect.
func ValidatePlannedChanges(
	dialect string,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	policy Policy,
) error {
	if platform.NormalizeDialect(dialect) != platform.SQLite || diff == nil {
		return nil
	}
	// The objects this plan CREATES are its own, so the plan and the creation
	// set are the same diff. A rollback is the one case where they differ; see
	// [ValidatePlannedRollback].
	return refusePlanTouchingUnclassifiedStorage(
		database,
		tablesTouchedBy(diff, createdBy(diff), policy),
		forwardPlan,
	)
}

// ValidatePlannedRollback refuses the ROLLBACK of a plan that would change a
// table in a database whose modules this build cannot load.
//
// A generated migration is two artifacts, and only one of them had a gate. The
// down file is planned from the reverse of the forward diff, and reversal turns
// changes SQLite expresses in place into changes it does not: an added column
// comes back as a removed one, and [sqliterebuild.NeedsTableRebuild] reports a
// rebuild for that -- drop, recreate, copy. So the exemption
// [ValidatePlannedChanges] grants an add-column-only diff, which is correct for
// the forward direction, admitted a rollback that rebuilds the table.
//
// Reproduced through [go.5x5.cz/ptah/migration/generator.PlanMigration] on an
// fts4 database this build cannot load, with a programmatic desired schema
// naming the module's storage and one extra column on `docs_content`. The up
// file was the single statement
//
//	ALTER TABLE "docs_content" ADD COLUMN "spurious" TEXT;
//
// and the down file beside it was
//
//	CREATE TABLE "__ptah_rebuild_docs_content" (...);
//	INSERT INTO "__ptah_rebuild_docs_content" (...) SELECT ... FROM "docs_content";
//	DROP TABLE "docs_content";
//	ALTER TABLE "__ptah_rebuild_docs_content" RENAME TO "docs_content";
//
// written at exit 0 with no refusal. Restoring the pre-exemption gate refused
// the same run at the comparison, which is where the gap came from: the
// exemption is right about the forward statement and says nothing about the
// file generated beside it.
//
// It asks the same question of the same predicate, and differs from
// [ValidatePlannedChanges] in exactly one input: the creation set. An object a
// migration CREATES cannot be one the module already owns, so the rollback that
// removes it again destroys nothing -- and the objects a ROLLBACK removes are
// the ones the FORWARD direction created, which the reverse diff records as
// removals. Reading the reverse diff's own additions instead would refuse
// `migrations generate` for adding an ordinary table, an index or a trigger to
// a database that happens to hold an unloadable module, which is the
// over-refusal this package has now fixed three times arriving a fourth way.
// See [createdBy] for why "created" is narrower than "added".
//
// No [Policy] is taken, because at this seam there is nothing left to promise:
// both callers filter the forward diff through their diff policy BEFORE the
// reverse is derived from it (see [go.5x5.cz/ptah/migration/generator] and
// [go.5x5.cz/ptah/internal/atlasmigrate]), and nothing filters the reverse
// afterwards. A caller that grew a later filter would be refused for a
// statement it deletes, which is the safe direction and the one the zero Policy
// already stands for.
func ValidatePlannedRollback(
	dialect string,
	database *types.DBSchema,
	forward *difftypes.SchemaDiff,
	reverse *difftypes.SchemaDiff,
) error {
	if platform.NormalizeDialect(dialect) != platform.SQLite || forward == nil || reverse == nil {
		return nil
	}
	return refusePlanTouchingUnclassifiedStorage(
		database,
		tablesTouchedBy(reverse, createdBy(forward), Policy{}),
		rollbackPlan,
	)
}

// created names the objects a migration brings into existence, which is the one
// class of object its rollback can remove without removing anything the
// database had before.
//
// It is deliberately narrower than "added". An index dropped and recreated
// under one name is a REPLACEMENT of the object that was there, not a new
// object, and so is a trigger; the rollback of a replacement puts the earlier
// definition back over the module's own, which is a removal like any other. So
// an addition counts as a creation only when the same migration does not also
// remove it.
type created struct {
	tables   []string
	indexes  []difftypes.IndexRef
	triggers []difftypes.TriggerRef
}

// createdBy reads the creation set off the diff that performs the creations.
//
// For a forward plan that is the plan's own diff, and the index and trigger
// halves are inert there: an entry this gate counts is in the diff's removals,
// which is exactly what disqualifies it from being a creation. They matter for
// a rollback, where the removals ARE the forward direction's additions.
func createdBy(diff *difftypes.SchemaDiff) created {
	semantics := diffSemantics(diff)
	set := created{tables: diff.TablesAdded}
	removedIndexes := diff.IndexRemovals()
	for _, ref := range diff.IndexAdditions() {
		if containsIndexRef(removedIndexes, ref, semantics) {
			continue
		}
		set.indexes = append(set.indexes, ref)
	}
	for _, ref := range diff.TriggersAdded {
		if containsTriggerRef(diff.TriggersRemoved, ref, semantics) {
			continue
		}
		set.triggers = append(set.triggers, ref)
	}
	return set
}

// containsIndexRef joins two index references on the object they name rather
// than on their spelling, for the reason [objectlookup] gives: the two sides of
// a reversal carry the comparator's spelling and the declaration's, and SQLite
// folds ASCII case besides.
func containsIndexRef(
	refs []difftypes.IndexRef,
	ref difftypes.IndexRef,
	semantics identifier.Semantics,
) bool {
	return slices.ContainsFunc(refs, func(candidate difftypes.IndexRef) bool {
		return objectlookup.Same(candidate.Name, ref.Name, semantics) &&
			objectlookup.Same(candidate.TableName, ref.TableName, semantics)
	})
}

func containsTriggerRef(
	refs []difftypes.TriggerRef,
	ref difftypes.TriggerRef,
	semantics identifier.Semantics,
) bool {
	return slices.ContainsFunc(refs, func(candidate difftypes.TriggerRef) bool {
		return objectlookup.Same(candidate.TriggerName, ref.TriggerName, semantics) &&
			objectlookup.Same(candidate.TableName, ref.TableName, semantics)
	})
}

// planSubject is how a refusal names the artifact whose statements were
// counted, and why one of them can be refused when the other was not.
//
// The two directions must not share one noun. A run whose whole forward plan is
// `ALTER TABLE "docs_content" ADD COLUMN "spurious" TEXT` and whose refusal
// said "the plan changes docs_content" would send an operator looking for a
// statement that is not there.
type planSubject struct {
	// changes is the clause naming the artifact and its verb. The refusal
	// continues with the tables, so it ends where a list belongs.
	changes string
	// reversal explains, for a rollback, how a forward plan this gate admitted
	// reaches here at all. It is empty for the forward plan, whose statements
	// need no such explanation.
	reversal string
}

var (
	forwardPlan  = planSubject{changes: "the plan changes"}
	rollbackPlan = planSubject{
		changes: "the rollback generated beside this migration changes",
		reversal: " the forward statements are ones SQLite performs in place, but reversing them" +
			" for the down file turns an added column into a removed one, which SQLite has no ALTER" +
			" for and converges by rebuilding the table;",
	}
)

// refusePlanTouchingUnclassifiedStorage is the one refusal both directions
// raise, so the rule cannot be stated twice and drift.
func refusePlanTouchingUnclassifiedStorage(
	database *types.DBSchema,
	touched []string,
	subject planSubject,
) error {
	if len(touched) == 0 {
		return nil
	}
	allowed, err := UnregisteredModuleAllowed()
	if err != nil {
		return err
	}
	if allowed {
		return nil
	}
	registered, err := sqlitemodule.Registered()
	if err != nil {
		return err
	}
	unclassified := liveUnregistered(database, registered)
	if len(unclassified) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%w: %s %s in a database that holds virtual %s %s whose %s this build of Ptah"+
			" does not register;%s SQLite marks a module's shadow tables as shadow only while the module"+
			" is loaded, so Ptah cannot tell that module's private storage from ordinary tables, and"+
			" dropping or rebuilding one of them destroys the index it belongs to, while dropping or"+
			" replacing an index or trigger one of them carries removes machinery the module may be"+
			" the one maintaining;"+
			" this build registers %s;"+
			" apply this change with a build that registers %s, or set %s=1 to plan it against tables"+
			" Ptah cannot vouch for",
		ptaherr.ErrUnsupportedFeature,
		subject.changes,
		quotedStrings(touched),
		noun(len(unclassified)),
		names(unclassified),
		moduleNoun(len(distinctModules(unclassified))),
		subject.reversal,
		registered.String(),
		modulesOf(unclassified),
		AllowUnregisteredModuleEnvVar,
	)
}

// tablesTouchedBy lists the tables the diff would drop, rebuild, or strip an
// object from, in a stable order.
//
// It has to be a SUPERSET of what the SQLite planner acts on, and the planner
// derives its rebuild set from more than the obvious two fields. Beside
// TablesModified it calls existingTablesWithConstraintChanges, which reads
// ConstraintsAddedWithTables and ConstraintsRemovedWithTables: a table whose
// columns are unchanged and whose constraint changed is recorded only at schema
// level, and SQLite has no ALTER for a constraint, so that table is rebuilt --
// drop, recreate, copy -- exactly like any other. Reading the two table fields
// alone let that through, which review caught.
//
// The planner also emits statements against a table that is neither dropped nor
// rebuilt: `removeIndexes` renders `DROP INDEX` for every IndexesRemoved entry
// whose table is not being rebuilt, `removeTriggers` renders `DROP TRIGGER` for
// every TriggersRemoved entry, and `modifyTriggers` replaces a TriggersModified
// one. Those are removals from a table this gate cannot classify, and Ptah can
// no more tell a module's own index from an operator's than it can tell the
// module's storage from an ordinary table -- that inability is the premise of
// this whole guard. Reproduced before the fields were read here: against an
// fts4 database this build cannot load, with both sides naming the module's
// storage,
//
//	ptah schema diff --from sqlite://live.db --to sqlite://desired.db
//
// planned `DROP INDEX IF EXISTS "docs_content_title_idx";` at exit 0, and the
// trigger fixture planned `DROP TRIGGER IF EXISTS "docs_content_guard";` at
// exit 0, with this gate seeing an empty touched set both times.
//
// ADDITIONS are not counted, here as everywhere else in this function. A
// `CREATE INDEX` or `CREATE TRIGGER` removes nothing, so the harm this gate
// names cannot come from one.
//
// It is coarser than the planner's own derivation wherever being coarser is
// free, because refusing a table the planner does not rebuild is safe while
// missing one is the defect. It is NOT coarser where the extra refusal costs a
// capability, and three exclusions mark those places. All three are sound in
// the other direction too:
//
//   - a table the MIGRATION creates cannot be one the module already owns, so
//     an added table carries none of this risk, and counting it would refuse
//     the ordinary case of adding a table with a constraint beside an index
//     Ptah cannot classify. The set is a parameter rather than the diff's own
//     TablesAdded because the rollback direction records the same tables as
//     removals: reading the reverse diff's additions there would refuse every
//     `migrations generate` that adds a table to a database holding an
//     unloadable module;
//   - a change the caller's [Policy] deletes again before anything is rendered
//     is not in the plan at all. Counting one refused a `schema apply` whose
//     whole plan the policy had already emptied;
//   - a table whose only change is COLUMNS ADDED is neither dropped nor
//     rebuilt. SQLite expresses that with `ALTER TABLE ... ADD COLUMN`, which
//     rewrites no row and touches no other object, so this gate's whole
//     premise -- "drop or rebuild destroys the index" -- does not apply to it.
//     Counting it refused `schema diff --include users` against an fts4
//     database whose plan was the single statement
//     `ALTER TABLE "users" ADD COLUMN "email" TEXT;`, and sent the operator to
//     an opt-in that also permits the drops.
//
// The second is keyed on the caller's [Policy] rather than on anything in the
// diff, because the diff at this seam is the comparator's answer and not yet
// the plan -- and it is applied by handing the diff to the caller's own filter,
// [diffpolicy.ApplyForDialect], rather than by reading the policy a second time
// here. That matters beyond tidiness: `skip drop_column` empties
// TablesModified[i].ColumnsRemoved, which is one of the fields
// [sqliterebuild.NeedsTableRebuild] reads, so a second reading would have had
// to reimplement the predicate's inputs as well as the filter. Measured before
// the filter ran here: `ptah migrations generate` with
// `diff.skip: [drop_table, drop_column]` against an fts4 database was refused
// at exit 2 for a plan that, obtained with the opt-in, wrote no file at all.
// The third is keyed on [sqliterebuild.NeedsTableRebuild], the same predicate
// `planTableRebuilds` selects with, so the two cannot drift.
//
// The residue the third exclusion leaves is measured rather than assumed, and
// it is not destruction. `ALTER TABLE docs_content ADD COLUMN spurious TEXT`
// on a live fts4 index left every row in place and `docs MATCH 'brown'` still
// answering 1, and only refused further writes (`INSERT INTO docs` reported
// `SQL logic error`); dropping the added column again restored them, with the
// rows still there. Reaching even that needs a desired state that NAMES the
// module's storage table, which is the residue [ValidateComparison] already
// records. A rebuild, by contrast, is drop-recreate-copy and takes the index
// with it for good.
//
// makes names what this MIGRATION brings into existence, which is what the
// first exclusion is really about. For a forward plan that is the diff's own
// creations; for a rollback it is the forward direction's, because the reverse
// diff records those same objects as the removals the down file performs. See
// [ValidatePlannedRollback] and [createdBy].
func tablesTouchedBy(diff *difftypes.SchemaDiff, makes created, policy Policy) []string {
	// The exclusion is an identity question, not a string one. The created set
	// carries the comparator's spelling while a constraint's TableName comes
	// from the declaration or the catalog, so one may say `main.t` where the
	// other says `t`, and SQLite folds ASCII case besides. A raw lookup answers
	// "different object" for one object -- the shape stokaro/ptah#1351 came
	// from -- and the cost here is a false refusal of a safe addition. The
	// planner asks the same question through the same helper.
	semantics := diffSemantics(diff)

	// The caller's own filter, applied to a copy, rather than a second reading
	// of its policy. Everything below is asked of what will still be in the
	// plan; the exclusions further down are still asked of the ORIGINAL diff,
	// because a filtered-away field can no longer say what it excluded.
	planned, _ := diffpolicy.ApplyForDialect(diff, policy.skipSet(), platform.SQLite)

	touched := slices.Clone(planned.TablesRemoved)
	for _, table := range planned.TablesModified {
		// Asked through the planner's own predicate rather than a second
		// spelling of it, so a change to what SQLite can express in place is
		// made once and both readers see it.
		if !sqliterebuild.NeedsTableRebuild(table) {
			continue
		}
		touched = append(touched, table.TableName)
	}
	for _, constraint := range planned.ConstraintsAddedWithTables {
		touched = append(touched, constraint.TableName)
	}
	for _, constraint := range planned.ConstraintsRemovedWithTables {
		touched = append(touched, constraint.TableName)
	}
	// The objects a table CARRIES, which the planner drops and replaces without
	// dropping or rebuilding the table itself. The owning table is what is
	// counted: it is the thing this gate can weigh against the module's
	// storage, and the index or trigger name says nothing about whose it is.
	//
	// An object this migration created is discounted at the REF, before its
	// owning table joins the list -- dropping an index the migration built
	// removes nothing the database had, while its table may well carry another
	// index that a genuine removal would take. Excluding the table instead
	// would let one created index cover every removal aimed at it.
	for _, ref := range planned.IndexesRemoved {
		if containsIndexRef(makes.indexes, ref, semantics) {
			continue
		}
		touched = append(touched, ref.TableName)
	}
	for _, ref := range planned.TriggersRemoved {
		if containsTriggerRef(makes.triggers, ref, semantics) {
			continue
		}
		touched = append(touched, ref.TableName)
	}
	for _, modified := range planned.TriggersModified {
		touched = append(touched, modified.TableName)
	}

	kept := touched[:0]
	for _, name := range touched {
		if objectlookup.Contains(makes.tables, name, semantics) {
			continue
		}
		// Asked through the same identity helper as the addition exclusion, and
		// against diff.TablesRemoved rather than the accumulated list, so a
		// constraint whose host is being dropped is discounted with the drop.
		// The spellings really do differ: TablesRemoved carries the
		// comparator's, a constraint's TableName the declaration's or the
		// catalog's (stokaro/ptah#1351).
		if policy.SkipDropTable && objectlookup.Contains(diff.TablesRemoved, name, semantics) {
			continue
		}
		kept = append(kept, name)
	}
	slices.Sort(kept)
	return slices.Compact(kept)
}

// diffSemantics is the identifier rule the diff was produced under, falling
// back to SQLite's own when the comparison recorded none.
//
// [difftypes.SchemaDiff.IdentifierSemantics] is absent for a dialect-only
// comparison. This gate only runs on SQLite, so the fallback is the dialect's
// rule rather than a conservative guess.
func diffSemantics(diff *difftypes.SchemaDiff) identifier.Semantics {
	if diff.IdentifierSemantics != nil {
		return *diff.IdentifierSemantics
	}
	return identifier.ForDialect(platform.SQLite)
}

func quotedStrings(values []string) string {
	rendered := make([]string, 0, len(values))
	for _, value := range values {
		rendered = append(rendered, fmt.Sprintf("%q", value))
	}
	return strings.Join(rendered, ", ")
}

// someLiveTableIsUndeclared reports whether the comparison holds a live table
// the desired side does not name -- the shape that plans a `DROP TABLE`.
//
// This is the half of the harm that is decidable BEFORE the comparison runs,
// and it is decidable exactly: the comparator's removal set is the live tables
// the desired state leaves out, so this predicate and `TablesRemoved` answer the
// same question. The other half -- a table both sides name and describe
// differently, which SQLite converges by rebuilding it -- is not decidable here
// without a second copy of the comparator's rules, so it is asked afterwards
// against the comparator's own answer. See [ValidatePlannedChanges].
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
func someLiveTableIsUndeclared(
	desired *goschema.Database,
	database *types.DBSchema,
	semantics identifier.Semantics,
) bool {
	if database == nil {
		return false
	}
	declared := make(map[string]struct{})
	if desired != nil {
		for _, table := range desired.Tables {
			declared[identity(table.Schema, table.Name, semantics)] = struct{}{}
		}
	}
	for _, table := range database.Tables {
		if _, ok := declared[identity(table.Schema, table.Name, semantics)]; !ok {
			return true
		}
	}
	return false
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
