package engine

import (
	"fmt"
	"slices"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
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
	if r.annotations, err = set.WithCoverageVocabulary(r.vocabulary); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRegistration, err)
	}
	return nil
}

// registerCoverageKinds freezes every provider's coverage kinds, in-process
// coverage kinds and format limits into the runtime's vocabulary. Two
// providers claiming one kind, a provider claiming a common kind, a kind
// registered both ways, and a format limit on a kind the provider does not
// register are refused.
func (r *Runtime) registerCoverageKinds(providers []Provider) error {
	var kinds, process []coverage.Kind
	var limits []coverage.FormatLimit
	for _, provider := range providers {
		kinds = append(kinds, provider.CoverageKinds...)
		process = append(process, provider.ProcessCoverageKinds...)
		for _, limit := range provider.CoverageFormatLimits {
			for _, kind := range limit.Kinds {
				if !slices.Contains(provider.CoverageKinds, kind) {
					return fmt.Errorf("%w: provider %q states a %s format limit on coverage kind %q, which it does not register",
						ErrInvalidRegistration, provider.ID, limit.Format, kind)
				}
			}
			limits = append(limits, limit)
		}
	}
	vocabulary, err := coverage.NewVocabulary(kinds...)
	if err == nil {
		vocabulary, err = vocabulary.WithProcess(process...)
	}
	if err == nil {
		vocabulary, err = vocabulary.WithFormatLimits(limits...)
	}
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRegistration, err)
	}
	r.vocabulary = vocabulary
	return nil
}

// CoverageVocabulary returns the coverage kinds the runtime's documents may
// name: the common kinds and those its providers register. A header or a Go
// notdescribed declaration naming any other kind is refused by name.
func (r *Runtime) CoverageVocabulary() coverage.Vocabulary {
	return r.vocabulary
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

// registerHCL freezes every provider's HCL extensions into one set, under
// the same ownership rule as YAML: an extension names its provider as the
// owner, and the provider owns the desired codec of every model it claims.
func (r *Runtime) registerHCL(providers []Provider) error {
	var extensions []hclext.Extension
	for _, provider := range providers {
		for _, extension := range provider.HCL {
			if extension.Owner != provider.ID {
				return fmt.Errorf("%w: provider %q registers HCL blocks owned by %q", ErrInvalidRegistration, provider.ID, extension.Owner)
			}
			for _, kind := range slices.Concat(extension.Kinds, extension.Unrecorded) {
				if !r.ownsCodec(provider.ID, kind, schemaext.Desired) {
					return fmt.Errorf("%w: provider %q claims model %q for HCL without owning its desired codec",
						ErrInvalidRegistration, provider.ID, kind)
				}
			}
			extensions = append(extensions, extension)
		}
	}
	set, err := hclext.NewSet(extensions...)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRegistration, err)
	}
	r.hcl = set
	return nil
}

// HCL returns the HCL extensions of the runtime's providers as one frozen
// set. A runtime whose providers contribute none returns a selected set
// without owners.
func (r *Runtime) HCL() hclext.Set {
	return r.hcl
}

// registerSQL freezes every provider's SQL extensions into one set. An
// extension must name its provider as the owner and read only targets that
// provider registers, under their canonical names, or targets it reads
// statements or facets of its own on; see [Runtime.readsObjects] and
// [Runtime.readsFacets].
func (r *Runtime) registerSQL(providers []Provider) error {
	var extensions []sqlext.Extension
	for _, provider := range providers {
		for _, extension := range provider.SQL {
			if extension.Owner != provider.ID {
				return fmt.Errorf("%w: provider %q registers SQL syntax owned by %q", ErrInvalidRegistration, provider.ID, extension.Owner)
			}
			for _, target := range extension.Targets {
				selected, found := r.targets[target]
				if !found || selected.name != target || (selected.owner != provider.ID &&
					!r.readsObjects(provider.ID, extension, target) && !r.readsFacets(provider.ID, extension, target)) {
					return fmt.Errorf("%w: provider %q registers SQL syntax for target %q it does not register", ErrInvalidRegistration, provider.ID, target)
				}
			}
			extensions = append(extensions, extension)
		}
	}
	set, err := sqlext.NewSet(extensions...)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRegistration, err)
	}
	r.sql = set
	return nil
}

// readsObjects reports whether owner may read statements on target, a
// target another provider registers: the extension declares objects through statements or table child sites, and every object kind they declare is owner's and served on
// target. The PostgreSQL extension owner reads CREATE EXTENSION so on every
// PostgreSQL-family target. Other inline syntax and a grammar of a whole
// document require the target owner or a registered facet service.
func (r *Runtime) readsObjects(owner string, extension sqlext.Extension, target string) bool {
	if extension.Grammar != nil || len(extension.Patterns) == 0 || len(extension.Kinds) == 0 {
		return false
	}
	for _, pattern := range extension.Patterns {
		if pattern.Site != sqlext.Statement && pattern.Site != sqlext.TableElement && pattern.Site != sqlext.AlterOperation {
			return false
		}
	}
	for _, kind := range extension.Kinds {
		if !r.ownsCodec(owner, kind, schemaext.Desired) || !r.serves(target, kind) {
			return false
		}
	}
	return true
}

// readsFacets reports whether owner may read syntax on target, a target
// another provider registers, that only sets facets of the owner's: every
// pattern stands at a site whose value carries facets alone, and every facet
// kind the extension declares is the owner's and compared on target. The MySQL
// owner reads a MariaDB column's character set and ON UPDATE clause so.
func (r *Runtime) readsFacets(owner string, extension sqlext.Extension, target string) bool {
	if len(extension.Patterns) == 0 || len(extension.Facets) == 0 {
		return false
	}
	for _, pattern := range extension.Patterns {
		if !sqlext.FacetSite(pattern.Site) {
			return false
		}
	}
	for _, kind := range extension.Facets {
		if !r.ownsCodec(owner, kind, schemaext.Desired) || !r.servesFacet(target, kind) {
			return false
		}
	}
	return true
}

// SQL returns the SQL extensions of the runtime's providers as one frozen
// set. A runtime whose providers contribute none returns a selected set
// without owners.
func (r *Runtime) SQL() sqlext.Set {
	return r.sql
}
