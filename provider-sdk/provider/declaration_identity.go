package provider

import (
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

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

// declaredObjectIdentityMatches binds a declared object's subject to its value
// owner. A value converted from a read keeps the owner the read gave it,
// bound to the database the read was of, while the host names the subject
// relative to that database. The value's database context proves the
// binding, so an owner database that is the context's is set aside before the
// comparison; any other owner database, or none in the context, refuses.
func declaredObjectIdentityMatches(request *protocol.PlanDeclarationsRequest, record *protocol.ComparisonRecord) bool {
	owner := record.GetValue().GetValue().GetOwner()
	subject := record.GetSubject().GetDatabase()
	if subject.GetSource() != "" || subject.GetNormalized() != "" || owner.GetDatabase() == "" {
		return declarationIdentityMatches(request, record)
	}
	database := record.GetValue().GetValue().GetContext().GetDatabase()
	if database == "" || owner.GetDatabase() != database || record.GetValue().GetNormalizedOwner().GetDatabase() != database {
		return false
	}
	bound := proto.CloneOf(record)
	bound.Value.Value.Owner.Database, bound.Value.NormalizedOwner.Database = "", ""
	return declarationIdentityMatches(request, bound)
}
