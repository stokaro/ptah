package provider

import (
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

func validateObservedAccess(engine *protocol.Engine, kinds, complete commonKinds, observed *protocol.ObservedCommonObjects) error {
	if kinds[protocol.CommonObjectSupport_ROLE_MEMBERSHIP] {
		for _, role := range observed.GetRoles() {
			if len(role.GetMemberOf()) != 0 {
				return commonObjectError("independent memberships cannot be mixed with role member_of")
			}
		}
	}
	seenMemberships := make(map[[2]string]bool)
	for _, membership := range observed.GetMemberships() {
		if !complete[protocol.CommonObjectSupport_ROLE_MEMBERSHIP] {
			return commonObjectError("a membership the read did not cover")
		}
		key := [2]string{membership.GetGroup(), membership.GetMember()}
		if strings.TrimSpace(key[0]) == "" || strings.TrimSpace(key[1]) == "" || seenMemberships[key] {
			return commonObjectError("a membership needs nonempty principals and a unique pair")
		}
		seenMemberships[key] = true
	}
	var owners []*protocol.Reference
	for _, owner := range observed.GetObjectOwners() {
		if !complete[protocol.CommonObjectSupport_OBJECT_OWNER] {
			return commonObjectError("an object owner the read did not cover")
		}
		if !validOwnedObject(owner.GetObject(), engine.GetId()) || strings.TrimSpace(owner.GetOwner()) == "" {
			return commonObjectError("an object owner needs an engine object and a principal")
		}
		identity := foldedOwnerReference(engine, owner.GetObject())
		if slices.ContainsFunc(owners, func(other *protocol.Reference) bool { return compareReferences(other, identity) == 0 }) {
			return commonObjectError("an object has repeated or conflicting owners")
		}
		owners = append(owners, identity)
	}
	return nil
}

func validOwnedObject(object *protocol.Reference, engine string) bool {
	if object == nil || object.GetEngine() != engine {
		return false
	}
	return validGrantTarget(object)
}

func foldedOwnerReference(engine *protocol.Engine, object *protocol.Reference) *protocol.Reference {
	identity := proto.CloneOf(object)
	rules := engine.GetIdentifiers()
	identity.Database = sqlFold(rules.GetTableNames(), identity.Database)
	identity.Catalog = sqlFold(rules.GetTableNames(), identity.Catalog)
	if identity.Schema == "" {
		identity.Schema = engine.GetDefaultSchema()
	}
	identity.Schema = sqlFold(rules.GetTableNames(), identity.Schema)
	for i, part := range identity.Path {
		identity.Path[i] = sqlFold(rules.GetTableNames(), part)
	}
	comparison := rules.GetTableNames()
	switch identity.Kind {
	case "column":
		comparison = rules.GetColumnNames()
	case "index":
		comparison = rules.GetIndexNames()
	}
	identity.Name = sqlFold(comparison, identity.Name)
	return identity
}
