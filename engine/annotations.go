package engine

import (
	"fmt"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
)

// registerAnnotations freezes every provider's Go annotation extensions into
// one set. An extension names its provider as its owner, and the provider owns
// the desired codec of every model the extension produces, so a parse cannot
// declare a model no selected codec can capture.
func (r *Runtime) registerAnnotations(providers []Provider) error {
	var extensions []annotation.Extension
	for _, provider := range providers {
		for _, extension := range provider.Annotations {
			if extension.Owner != provider.ID {
				return fmt.Errorf("%w: provider %q registers annotations owned by %q", ErrInvalidRegistration, provider.ID, extension.Owner)
			}
			for _, kind := range extension.Kinds {
				if !r.ownsCodec(provider.ID, kind, schemaext.Desired) {
					return fmt.Errorf("%w: provider %q declares model %q from annotations without owning its desired codec",
						ErrInvalidRegistration, provider.ID, kind)
				}
			}
			extensions = append(extensions, extension)
		}
	}
	set, err := annotation.NewSet(extensions...)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRegistration, err)
	}
	r.annotations = set
	return nil
}

// Annotations returns the Go annotation extensions of the runtime's providers
// as one frozen set. A runtime whose providers contribute none returns a
// selected set without owners, which parses the frontend's own directives.
func (r *Runtime) Annotations() annotation.Set {
	return r.annotations
}

// registerYAML freezes every provider's YAML extensions into one set, under
// the rules [Runtime.registerAnnotations] applies.
func (r *Runtime) registerYAML(providers []Provider) error {
	var extensions []yamlext.Extension
	for _, provider := range providers {
		for _, extension := range provider.YAML {
			if extension.Owner != provider.ID {
				return fmt.Errorf("%w: provider %q registers YAML claims owned by %q", ErrInvalidRegistration, provider.ID, extension.Owner)
			}
			for _, kind := range extension.Kinds {
				if !r.ownsCodec(provider.ID, kind, schemaext.Desired) {
					return fmt.Errorf("%w: provider %q claims model %q for YAML without owning its desired codec",
						ErrInvalidRegistration, provider.ID, kind)
				}
			}
			extensions = append(extensions, extension)
		}
	}
	set, err := yamlext.NewSet(extensions...)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRegistration, err)
	}
	r.yaml = set
	return nil
}

// YAML returns the YAML extensions of the runtime's providers as one frozen
// set. A runtime whose providers contribute none returns a selected set
// without owners.
func (r *Runtime) YAML() yamlext.Set {
	return r.yaml
}
