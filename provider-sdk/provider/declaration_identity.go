package provider

import protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

// declarationIdentityMatches binds the receipt subject to both spellings of
// the value's owner. A child appends its parent to the namespace path; no
// delimiter parsing or payload interpretation participates in this binding.
func declarationIdentityMatches(request *protocol.PlanDeclarationsRequest, object *protocol.ComparisonRecord) bool {
	for _, normalized := range []bool{false, true} {
		owner := object.GetValue().GetValue().GetOwner()
		if normalized {
			owner = object.GetValue().GetNormalizedOwner()
		}
		part := func(value *protocol.ComparisonPart) string {
			if normalized {
				return value.GetNormalized()
			}
			return value.GetSource()
		}
		subject := object.GetSubject()
		bound := &protocol.Reference{Engine: request.GetEngine(), Kind: subject.GetKind(), Database: part(subject.GetDatabase()),
			Catalog: part(subject.GetCatalog()), Schema: part(subject.GetSchema()), Name: part(subject.GetName())}
		for _, component := range subject.GetPath() {
			bound.Path = append(bound.Path, part(component))
		}
		if parent := part(subject.GetParent()); parent != "" {
			bound.Path = append(bound.Path, parent)
		}
		for _, component := range subject.GetSignature() {
			bound.Overload = append(bound.Overload, part(component))
		}
		if subject.GetSchema().GetDefaulted() && owner.GetSchema() == "" {
			defaultSchema := request.GetIdentifiers().GetDefaultSchema()
			if normalized {
				defaultSchema = sqlFold(request.GetIdentifiers().GetRules().GetTableNames(), defaultSchema)
			}
			if bound.Schema == defaultSchema {
				bound.Schema = ""
			}
		}
		if compareReferences(bound, owner) != 0 {
			return false
		}
	}
	return true
}
