package engine

import (
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
)

// registerSelections freezes every provider's scope selections into one set,
// under the same ownership rule as HCL: an extension names its provider as
// the owner, and the provider owns the desired codec of every model it
// selects.
func (r *Runtime) registerSelections(providers []Provider) error {
	var extensions []selectext.Extension
	for _, provider := range providers {
		for _, extension := range provider.Selections {
			if extension.Owner != provider.ID {
				return fmt.Errorf("%w: provider %q registers scope selections owned by %q", ErrInvalidRegistration, provider.ID, extension.Owner)
			}
			for _, selection := range extension.Selections {
				if !r.ownsCodec(provider.ID, selection.Kind, schemaext.Desired) {
					return fmt.Errorf("%w: provider %q selects model %q without owning its desired codec",
						ErrInvalidRegistration, provider.ID, selection.Kind)
				}
			}
			extensions = append(extensions, extension)
		}
	}
	set, err := selectext.NewSet(extensions...)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRegistration, err)
	}
	r.selections = set
	return nil
}

// Selections returns the scope selections of the runtime's providers as one
// frozen set. A runtime whose providers contribute none returns a selected
// set without owners.
func (r *Runtime) Selections() selectext.Set {
	return r.selections
}
