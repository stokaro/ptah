package atlasfilter

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

// ownedExtension is one of the extension owner's objects as the filters read
// it: its name and the schema it is installed into, declared or observed.
type ownedExtension struct {
	object schemaext.Object
	name   string
	schema string
}

// ownedExtensions lists the extension owner's objects in identity order.
func ownedExtensions(objects schemaext.Objects) ([]ownedExtension, error) {
	all, err := objects.All()
	if err != nil {
		return nil, err
	}
	var owned []ownedExtension
	for _, object := range all {
		extension := ownedExtension{object: object, name: pgextension.Name(object.Ref)}
		switch value := object.Value.(type) {
		case *pgextension.DesiredExtension:
			extension.schema = value.Schema
		case *pgextension.ObservedExtension:
			extension.schema = value.Schema
		default:
			continue
		}
		owned = append(owned, extension)
	}
	return owned, nil
}

// filterExtensionFeatures is [exclusionState.filterExtensions] over the
// extension owner's objects, on either side of a comparison: an extension an
// exclusion names, or whose installation schema is excluded, leaves with its
// coverage, and a version an exclusion names is left to the server.
func (s *exclusionState) filterExtensionFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage, error) {
	owned, err := ownedExtensions(objects)
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	excluded := make(map[objectidentity.Key]bool)
	for _, extension := range owned {
		names := extensionNameCandidates(s.effectiveExtensionSchema(extension.schema), extension.name)
		if s.matches("extension", names...) || s.extensionSchemaExcluded(extension.schema) {
			excluded[extension.object.Ref.Key()] = true
			continue
		}
		if !s.matchesField("extension", "version", names...) {
			continue
		}
		if objects, err = objects.Replace(withoutVersion(extension.object)); err != nil {
			return schemaext.Objects{}, schemaext.Coverage{}, err
		}
	}
	keep := func(ref objectidentity.ID) bool { return !excluded[ref.Key()] }
	return objects.Select(keep), coverage.SelectSubjects(keep), nil
}

// withoutVersion returns object with its version left to the server.
func withoutVersion(object schemaext.Object) schemaext.Object {
	switch value := object.Value.(type) {
	case *pgextension.DesiredExtension:
		value.Version = ""
	case *pgextension.ObservedExtension:
		value.Version = ""
	}
	return object
}

// selectExtensionFeatures is the scope's projection of the common extension
// list over the extension owner's objects: every extension rides along when
// the scope keeps extensions as support or a non-extension resource matched,
// and otherwise only the extensions the selectors name stay.
func (s *scopeSelection) selectExtensionFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage, error) {
	owned, err := ownedExtensions(objects)
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	support := s.extensionSupport || s.nonExtensionMatched
	dropped := make(map[objectidentity.Key]bool)
	for _, extension := range owned {
		if !s.selectedExtension(extension.schema, extension.name) && !support {
			dropped[extension.object.Ref.Key()] = true
		}
	}
	keep := func(ref objectidentity.ID) bool { return !dropped[ref.Key()] }
	return objects.Select(keep), coverage.SelectSubjects(keep), nil
}

// ownedExtensionSchemas lists the installation schema of each of the
// extension owner's objects, as the scope keeps a schema an extension is in.
func (s *scopeSelection) ownedExtensionSchemas(objects schemaext.Objects) ([]string, error) {
	owned, err := ownedExtensions(objects)
	if err != nil {
		return nil, err
	}
	schemas := make([]string, 0, len(owned))
	for _, extension := range owned {
		schemas = append(schemas, s.effectiveExtensionSchema(extension.schema))
	}
	return schemas, nil
}
