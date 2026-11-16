package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/internal/atlasfilter"
)

// Inclusion is a positive selection over both sides of a comparison that plans
// a migration: the selectors `schema apply --include` takes, with the same
// meaning. An object it does not select is on neither side, so the migration
// neither creates nor drops it, and a table it selects keeps the indexes,
// constraints, triggers and policies an exclusion leaves it.
//
// One type serves every verb that plans a migration under it -- `ptah
// migrations generate`, the shadow verification behind it, and `ptah-compat
// migrate diff` -- so the sides of one comparison select the same objects.
//
// The zero value selects everything: every method returns its input
// unchanged.
type Inclusion struct {
	runtime   selectext.Runtime
	selectors []string
	source    string
}

// NewInclusion returns the inclusion selectors make under the scope selections
// runtime serves.
//
// Selectors take the grammar of `--include`, repeated or comma-separated.
// Selectors that name nothing -- none, or only blanks and commas -- return the
// zero Inclusion without consulting runtime. Otherwise runtime must implement
// [selectext.Runtime]; one that does not is refused with an error wrapping
// [selectext.ErrUnselected]. A malformed selector is refused here, before any
// state is read.
func NewInclusion(runtime any, selectors []string) (Inclusion, error) {
	if !slices.ContainsFunc(selectors, atlasfilter.NamesSelector) {
		return Inclusion{}, nil
	}
	selector, ok := runtime.(selectext.Runtime)
	if !ok {
		return Inclusion{}, fmt.Errorf("%w: include selectors need a runtime that serves scope selections", selectext.ErrUnselected)
	}
	if err := atlasfilter.ValidateIncludeSelectors(selectors, selector.Selections()); err != nil {
		return Inclusion{}, err
	}
	return Inclusion{runtime: selector, selectors: slices.Clone(selectors)}, nil
}

// WrittenAs returns a copy of the inclusion whose refusal of an empty selection
// names source as where the selectors were written, such as an environment
// variable on a command that registers no --include flag. Empty names
// --include.
func (i Inclusion) WrittenAs(source string) Inclusion {
	i.source = source
	return i
}

// Includes reports whether the inclusion names any selector.
func (i Inclusion) Includes() bool {
	return len(i.selectors) != 0
}

// ReadContext marks ctx with the object kinds the selectors leave out whole,
// as [LiveSelection.ReadContext] does.
func (i Inclusion) ReadContext(ctx context.Context) context.Context {
	if !i.Includes() {
		return ctx
	}
	return i.selection().ReadContext(ctx, i.runtime.Selections())
}

// Apply keeps on both sides what the selectors select, on a connection of
// dialect, counting each pattern as naming says: the count the verb's
// exclusion takes, so one selector list names the same objects in both. Selectors
// that select nothing on either side return the
// [atlasfilter.EmptySelectionError]: a migration planned over nothing would
// read as synced. current and desired are not changed.
func (i Inclusion) Apply(
	ctx context.Context,
	dialect string,
	naming atlasfilter.Naming,
	current *catalog.Database,
	desired *schemamodel.Database,
) (*catalog.Database, *schemamodel.Database, error) {
	return i.apply(ctx, countedOn{dialect: dialect, naming: naming, empty: refuseEmpty}, current, desired)
}

// ApplyAfterChange is [Inclusion.Apply] for a comparison made after the
// change the plan made, such as a verification that replays it: selectors
// that select nothing on either side return the two empty projections, because
// a plan may drop every object it selects. A selector the grammar or the scope
// rejects, and a selected object that depends on an unselected one, are still
// refused.
func (i Inclusion) ApplyAfterChange(
	ctx context.Context,
	dialect string,
	naming atlasfilter.Naming,
	current *catalog.Database,
	desired *schemamodel.Database,
) (*catalog.Database, *schemamodel.Database, error) {
	return i.apply(ctx, countedOn{dialect: dialect, naming: naming, empty: compareEmpty}, current, desired)
}

func (i Inclusion) apply(
	ctx context.Context,
	on countedOn,
	current *catalog.Database,
	desired *schemamodel.Database,
) (*catalog.Database, *schemamodel.Database, error) {
	if !i.Includes() {
		return current, desired, nil
	}
	current, desired, err := i.selection().applyCounted(ctx, i.runtime, on, current, desired)
	var empty *atlasfilter.EmptySelectionError
	if errors.As(err, &empty) && i.source != "" {
		return nil, nil, &atlasfilter.EmptySelectionError{Selectors: empty.Selectors, Source: i.source}
	}
	return current, desired, err
}

func (i Inclusion) selection() LiveSelection {
	return LiveSelection{Include: i.selectors}
}
