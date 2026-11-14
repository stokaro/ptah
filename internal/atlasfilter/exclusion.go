package atlasfilter

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/internal/readscope"
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
	return Exclusion{selections: selections, patterns: slices.Clone(patterns)}, nil
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

// Desired returns a copy of db without the objects the selectors name, as
// [ExcludeGeneratedReport] filters it. defaultSchema owns the objects db
// leaves unqualified, and is the connection's default schema.
func (e Exclusion) Desired(db *schemamodel.Database, defaultSchema string) (*schemamodel.Database, ExcludeReport, error) {
	if !e.Excludes() {
		return db, ExcludeReport{}, nil
	}
	return ExcludeGeneratedReport(e.selections, db, e.patterns, defaultSchema)
}

// Observed returns a copy of db without the objects the selectors name, as
// [ExcludeDatabaseReport] filters it. defaultSchema owns the objects the read
// left unqualified, and is the connection's default schema.
func (e Exclusion) Observed(db *catalog.Database, defaultSchema string) (*catalog.Database, ExcludeReport, error) {
	if !e.Excludes() {
		return db, ExcludeReport{}, nil
	}
	return ExcludeDatabaseReport(e.selections, db, e.patterns, defaultSchema)
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
	fmt.Fprintf(w, "Warning: %s.\n", (&UnmatchedExcludeError{Selectors: selectors, Sources: sources}).Error())
}
