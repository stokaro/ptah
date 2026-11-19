package atlasfilter

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/internal/featureselect"
	"ptah.run/internal/readscope"
	"ptah.run/internal/schemaselection"
)

// Exclusion is the exclude selectors of one migration comparison, applied the
// same way to every state that comparison reads: the desired schema, the
// database a migration is planned against or the replay of a migration
// directory standing in for it, and the shadow database read back after the
// migration ran there.
//
// Every planner that writes a migration under an exclusion goes through this
// type -- `ptah migrations generate` and `ptah migrations plan`, the shadow
// verification behind them, and `ptah-compat migrate diff` -- so the sides of
// one comparison cannot disagree about what a selector names. Excluded from
// one side only, an object comes back as a CREATE or a DROP of exactly what
// the selector was written to protect.
//
// The zero value excludes nothing: every method returns its input unchanged.
type Exclusion struct {
	selections selectext.Set
	patterns   []string
	// comparison serves the target's comparison rules, when the runtime
	// NewExclusion took serves them: the database side reads the rule for the
	// index a server builds for a foreign key from them.
	comparison schemacomparison.Runtime
}

// NewExclusion returns the exclusion patterns select under the scope
// selections runtime serves.
//
// Patterns take the grammar of `--exclude`: Atlas-style selectors, repeated or
// comma-separated. Patterns that name nothing -- none, or only blanks and
// commas -- return the zero Exclusion without consulting runtime. Otherwise
// runtime must implement [selectext.Runtime], which *ptah.run/engine.Runtime
// does; one that does not is refused with an error wrapping
// [selectext.ErrUnselected]. A malformed selector is refused here, before any
// state is read; a selector whose depth only the connection can judge is
// refused when a state is filtered.
func NewExclusion(runtime any, patterns []string) (Exclusion, error) {
	if !slices.ContainsFunc(patterns, NamesSelector) {
		return Exclusion{}, nil
	}
	selector, ok := runtime.(selectext.Runtime)
	if !ok {
		return Exclusion{}, fmt.Errorf("%w: exclude selectors need a runtime that serves scope selections", selectext.ErrUnselected)
	}
	selections := selector.Selections()
	if err := ValidateExcludeSelectors(patterns, selections); err != nil {
		return Exclusion{}, err
	}
	comparison, _ := runtime.(schemacomparison.Runtime)
	return Exclusion{selections: selections, patterns: slices.Clone(patterns), comparison: comparison}, nil
}

// NamesSelector reports whether one value of a selector flag or list holds at
// least one selector: a value of blanks and commas names none. Every reader
// that decides whether a selection is present asks it, so a value one of them
// reads as empty cannot carry a selector for another.
func NamesSelector(value string) bool {
	return strings.Trim(value, ", \t") != ""
}

// Excludes reports whether the exclusion names any selector.
func (e Exclusion) Excludes() bool {
	return len(e.patterns) != 0
}

// ReadContext marks ctx with the object kinds the selectors leave out whole,
// so a schema read under it does not have to answer for them; see
// [KindsLeftOut]. It is the context every read of a compared state runs
// under.
func (e Exclusion) ReadContext(ctx context.Context) context.Context {
	if !e.Excludes() {
		return ctx
	}
	return readscope.Without(ctx, KindsLeftOut(e.selections, e.patterns, nil)...)
}

// Naming is how one filtered state names its objects to an exclude pattern.
//
// Both fields have to be stated, because they answer different questions:
// DefaultSchema says where an unqualified object lives, and RealmRelative says
// what a pattern's leading segment names. Every side of one comparison takes
// the same RealmRelative, or one list of selectors names different objects on
// each side.
type Naming struct {
	// DefaultSchema owns the objects the state leaves unqualified: the
	// connection's schema, or the one the desired state declares.
	DefaultSchema string
	// RealmRelative counts a pattern from the realm, so `public.notes.body`
	// names a column, rather than from DefaultSchema, where the schema slot is
	// filled and the same pattern has one part too many. It is
	// [Scope.RealmRelativePatterns] for a caller that holds no Scope.
	RealmRelative bool
	// Dialect is [Scope.Dialect] for a caller that holds no Scope.
	Dialect string
	// LiveKeys is [Scope.LiveKeys] for a caller that holds no Scope. Only
	// [Exclusion.Desired] reads it.
	LiveKeys LiveKeys
}

// ConnectionNaming is the Naming of a state read over the connection info
// describes: info.Schema owns what the read leaves unqualified, and a URL that
// names no schema describes the realm, so its patterns are realm-relative.
// It is the count `schema apply`, `schema inspect` and `migrate diff` take
// from the same URL, so one selector list serves all of them.
func ConnectionNaming(info catalog.ServerInfo) Naming {
	return Naming{
		DefaultSchema: info.Schema,
		RealmRelative: schemaselection.Realm(info.Dialect, info.URL, info.Schema),
		Dialect:       info.Dialect,
	}
}

// patternScope is the schema a pattern's parts are counted against: nothing
// when the patterns are realm-relative.
func (n Naming) patternScope() string {
	if n.RealmRelative {
		return ""
	}
	return n.DefaultSchema
}

// Desired returns a copy of db without the objects the selectors name, as
// [ExcludeGeneratedReport] filters it, counting each pattern as naming says.
func (e Exclusion) Desired(db *schemamodel.Database, naming Naming) (*schemamodel.Database, ExcludeReport, error) {
	if !e.Excludes() {
		return db, ExcludeReport{}, nil
	}
	return excludeGenerated(e.selections, db, e.patterns, naming.patternScope(), naming.DefaultSchema, naming.Dialect, naming.LiveKeys, featureselect.Bindings{})
}

// Observed returns a copy of db without the objects the selectors name, as
// [ExcludeDatabaseReport] filters it, counting each pattern as naming says.
func (e Exclusion) Observed(db *catalog.Database, naming Naming) (*catalog.Database, ExcludeReport, error) {
	if !e.Excludes() {
		return db, ExcludeReport{}, nil
	}
	target := databaseTarget{dialect: naming.Dialect}
	if e.comparison != nil && naming.Dialect != "" {
		rules, err := e.comparison.ComparisonRules(naming.Dialect)
		if err != nil {
			return nil, ExcludeReport{}, fmt.Errorf("read the comparison rules of %s: %w", naming.Dialect, err)
		}
		target.foreignKeyIndexes = rules.ForeignKeyIndexes
	}
	return excludeDatabase(e.selections, db, e.patterns, naming.patternScope(), naming.DefaultSchema, target, featureselect.Bindings{})
}

// ReportUnmatched writes one warning to w naming the selectors that matched
// nothing in any of reports -- one report for each state the comparison
// filtered -- and writes nothing when every selector matched somewhere or w
// is nil. A selector that names an object on one side only matched, because
// that is what an excluded CREATE or DROP looks like; see
// [UnmatchedAcrossStates]. The warning names the source sources records for
// each selector.
func ReportUnmatched(w io.Writer, sources ExcludeSources, reports ...ExcludeReport) {
	selectors := UnmatchedAcrossStates(reports...)
	if w == nil || len(selectors) == 0 {
		return
	}
	fmt.Fprintf(w, "Warning: %s.\n", (&UnmatchedExcludeError{Selectors: selectors, Sources: sources, Stored: StoredSpellings(reports...)}).Error())
}
