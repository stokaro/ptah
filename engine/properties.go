package engine

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// PropertySource assigns a batch service to source property definitions for one
// canonical target and source format. Each target/format/key has one owner.
// The provider must own the desired model codec for every declared kind.
type PropertySource struct {
	Target      string
	Format      schemaext.PropertyFormat
	Definitions []schemaext.PropertyDefinition
	Service     schemaext.PropertyService
}

type propertyKey struct {
	target string
	format schemaext.PropertyFormat
	kind   schemaext.Kind
}

func (r *Runtime) registerPropertySource(owner string, declaration PropertySource) error {
	target, found := r.targets[declaration.Target]
	if !found || target.name != declaration.Target || !schemaext.Kind(declaration.Format).Valid() ||
		len(declaration.Definitions) == 0 || declaration.Service == nil || nilService(declaration.Service) {
		return fmt.Errorf("%w: incomplete property source registration for %q", ErrInvalidRegistration, declaration.Target)
	}
	keys := make(map[string]bool)
	absorbed := make(map[schemaext.CommonAttribute]bool)
	for _, registered := range r.propertyServices {
		if registered.Target == declaration.Target && registered.Format == declaration.Format {
			for _, definition := range registered.Definitions {
				for _, key := range definition.Keys {
					keys[key] = true
				}
				for _, absorption := range definition.Absorbs {
					absorbed[absorption.Attribute] = true
				}
			}
		}
	}
	for _, definition := range declaration.Definitions {
		if err := r.validatePropertyDefinition(owner, definition, keys); err != nil {
			return err
		}
		if err := validateAbsorptions(declaration.Format, definition, absorbed); err != nil {
			return err
		}
		key := propertyKey{declaration.Target, declaration.Format, definition.Kind}
		if _, duplicate := r.properties[key]; duplicate {
			return fmt.Errorf("%w: duplicate property source for %q/%q/%q", ErrInvalidRegistration, key.target, key.format, key.kind)
		}
		r.properties[key] = len(r.propertyServices)
	}
	declaration.Definitions = clonePropertyDefinitions(declaration.Definitions)
	r.propertyServices = append(r.propertyServices, declaration)
	return nil
}

func (r *Runtime) validatePropertyDefinition(owner string, definition schemaext.PropertyDefinition, keys map[string]bool) error {
	owned := slices.ContainsFunc(r.codecs.Definitions(), func(model schemaext.CodecIdentity) bool {
		return model.Kind == definition.Kind && model.Representation == schemaext.Desired && model.Owner == owner
	})
	if !owned || len(definition.Keys) == 0 {
		return fmt.Errorf("%w: missing owned desired codec or property keys for %q", ErrInvalidRegistration, definition.Kind)
	}
	for _, key := range definition.Keys {
		if !propertyName(key) || keys[key] {
			return fmt.Errorf("%w: invalid or duplicate source property %q", ErrInvalidRegistration, key)
		}
		keys[key] = true
	}
	return nil
}

// validateAbsorptions requires each absorbed attribute to belong to the
// format, to land in one of the definition's own keys, and to have one owner
// per target and format.
func validateAbsorptions(format schemaext.PropertyFormat, definition schemaext.PropertyDefinition, absorbed map[schemaext.CommonAttribute]bool) error {
	for _, absorption := range definition.Absorbs {
		attributeFormat, known := absorption.Attribute.Format()
		if !known || attributeFormat != format || !slices.Contains(definition.Keys, absorption.Key) || absorbed[absorption.Attribute] {
			return fmt.Errorf("%w: invalid or duplicate absorption of %q into %q", ErrInvalidRegistration, absorption.Attribute, absorption.Key)
		}
		absorbed[absorption.Attribute] = true
	}
	return nil
}

func propertyName(name string) bool {
	for part := range strings.SplitSeq(name, ".") {
		if part == "" {
			return false
		}
		for i, char := range part {
			if (char >= 'a' && char <= 'z') || (i > 0 && (char == '_' || (char >= '0' && char <= '9'))) {
				continue
			}
			return false
		}
	}
	return name != ""
}

func clonePropertyDefinitions(definitions []schemaext.PropertyDefinition) []schemaext.PropertyDefinition {
	result := slices.Clone(definitions)
	for i := range result {
		result[i].Keys = slices.Clone(result[i].Keys)
		result[i].Absorbs = slices.Clone(result[i].Absorbs)
	}
	return result
}

// PropertyDefinitions returns independent ownership declarations in registration
// order. Missing formats wrap ErrUnsupportedFeature, including on an empty
// corpus. Target aliases are resolved before looking up definitions.
func (r *Runtime) PropertyDefinitions(targetName string, format schemaext.PropertyFormat) ([]schemaext.PropertyDefinition, error) {
	selected, found := r.lookup(targetName)
	if !found {
		return nil, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, targetName)
	}
	var result []schemaext.PropertyDefinition
	for _, source := range r.propertyServices {
		if source.Target == selected.name && source.Format == format {
			result = append(result, clonePropertyDefinitions(source.Definitions)...)
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("%w: no property format %q for %q", ptaherr.ErrUnsupportedFeature, format, selected.name)
	}
	return result, nil
}

// PropertyFormats returns the selected target's supported source formats in
// sorted order. A known target with no property services returns an empty list;
// an unknown target returns ErrUnsupportedDialect. The result is independent.
func (r *Runtime) PropertyFormats(targetName string) ([]schemaext.PropertyFormat, error) {
	selected, found := r.lookup(targetName)
	if !found {
		return nil, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, targetName)
	}
	var formats []schemaext.PropertyFormat
	for _, source := range r.propertyServices {
		if source.Target == selected.name {
			formats = append(formats, source.Format)
		}
	}
	slices.Sort(formats)
	return slices.Compact(formats), nil
}

func propertyContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: property conversion requires a context", schemaext.ErrInvalidValue)
	}
	return ctx.Err()
}

func (r *Runtime) propertyBatches(ctx context.Context, targetName string, format schemaext.PropertyFormat, kinds []schemaext.Kind) (string, [][]int, error) {
	if err := propertyContext(ctx); err != nil {
		return "", nil, err
	}
	if _, err := r.PropertyDefinitions(targetName, format); err != nil {
		return "", nil, err
	}
	selected, _ := r.lookup(targetName) // PropertyDefinitions validated the target.
	batches := make([][]int, len(r.propertyServices))
	for index, kind := range kinds {
		service, found := r.properties[propertyKey{selected.name, format, kind}]
		if !found {
			return "", nil, fmt.Errorf("%w: no property source for %q/%q/%q", ptaherr.ErrUnsupportedFeature, selected.name, format, kind)
		}
		batches[service] = append(batches[service], index)
	}
	return selected.name, batches, nil
}

func (r *Runtime) snapshotPropertyFragment(target string, format schemaext.PropertyFormat, fragment schemaext.PropertyFragment) (schemaext.PropertyFragment, error) {
	service, found := r.properties[propertyKey{target, format, fragment.Kind}]
	if !found {
		return schemaext.PropertyFragment{}, fmt.Errorf("%w: unregistered property fragment kind %q", schemaext.ErrInvalidValue, fragment.Kind)
	}
	definitions := r.propertyServices[service].Definitions
	index := slices.IndexFunc(definitions, func(definition schemaext.PropertyDefinition) bool { return definition.Kind == fragment.Kind })
	for key, value := range fragment.Properties {
		if !slices.Contains(definitions[index].Keys, key) || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
			return schemaext.PropertyFragment{}, fmt.Errorf("%w: invalid or unowned property %q for %q", schemaext.ErrInvalidValue, key, fragment.Kind)
		}
	}
	return fragment.Clone(), nil
}
