package schemaprep

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// TypeDefinitions projects common types into the vocabulary shared by type
// owners. Object names are decoded components; their case and punctuation
// remain part of their identity.
func TypeDefinitions(builder objectidentity.Builder, domains []schemamodel.Domain, composites []schemamodel.CompositeType) []schemaext.DefinedType {
	var result []schemaext.DefinedType
	for _, domain := range domains {
		id := builder.TablePartsVerbatim(domain.Schema, domain.Name)
		id.Kind = objectidentity.KindDomain
		result = append(result, schemaext.DefinedType{ID: id, References: []string{domain.BaseType}})
	}
	for _, composite := range composites {
		id := builder.TablePartsVerbatim(composite.Schema, composite.Name)
		id.Kind = objectidentity.KindComposite
		var refs []string
		for _, field := range composite.Fields {
			refs = append(refs, field.Type)
		}
		result = append(result, schemaext.DefinedType{ID: id, References: refs})
	}
	return result
}
