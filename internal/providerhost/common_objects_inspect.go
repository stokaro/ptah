package providerhost

import (
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
)

// commonCoverageKinds is the coverage kind a read records for a common kind it
// did not inspect, so a comparison plans no removal of what nobody read. A
// kind without one, a materialized view or a comment, has no record a
// comparison would read, and the host refuses a read that leaves it out.
var commonCoverageKinds = map[protocol.CommonObjectSupport_Kind]coverage.Kind{
	protocol.CommonObjectSupport_VIEW:            coverage.View,
	protocol.CommonObjectSupport_SEQUENCE:        coverage.Sequence,
	protocol.CommonObjectSupport_ROLE:            coverage.Role,
	protocol.CommonObjectSupport_GRANT:           coverage.Grant,
	protocol.CommonObjectSupport_OBJECT_OWNER:    coverage.ObjectOwner,
	protocol.CommonObjectSupport_ROLE_MEMBERSHIP: coverage.RoleMembership,
	// A read that leaves the namespaces uninspected says nothing of the
	// schemas the declaration names, so the comparison withholds them; see
	// compare.DeclaredNamespaces.
	protocol.CommonObjectSupport_SCHEMA: coverage.Schema,
}

// commonObjects places what a read reports of the common objects beyond
// tables into result: namespaces, views, sequences, roles and their
// memberships, grants, and the comments of the objects the read reports. The
// SDK has checked the reply against the kinds the engine declares; a comment
// on an object the read does not hold, a namespace of another database, and
// an uninspected kind no coverage record can express, are refused here.
func (r reader) commonObjects(result *catalog.Database, observed *protocol.ObservedCommonObjects) error {
	if err := recordCommonCoverage(result, observed); err != nil {
		return err
	}
	for _, namespace := range observed.GetNamespaces() {
		identity := namespace.GetIdentity()
		if identity.GetDatabase() != "" && (r.session == nil || identity.GetDatabase() != r.session.remote.GetDatabase()) {
			return unsupported(fmt.Sprintf("namespace %q of another database than the session's", identity.GetName()))
		}
		result.Schemas = append(result.Schemas, catalog.Schema{Name: identity.GetName()})
	}
	for _, view := range observed.GetViews() {
		identity := view.GetIdentity()
		if identity.GetKind() == "materialized_view" {
			if view.GetCheckOption() != "" || len(view.GetAttributes()) != 0 {
				return unsupported(fmt.Sprintf("a check option or attributes of materialized view %q", identity.GetName()))
			}
			result.MatViews = append(result.MatViews, catalog.MaterializedView{Name: identity.GetName(), Schema: identity.GetSchema(), Body: view.GetBody()})
			continue
		}
		result.Views = append(result.Views, catalog.View{Name: identity.GetName(), Schema: identity.GetSchema(), Body: view.GetBody(),
			CheckOption: view.GetCheckOption(), Attributes: slices.Clone(view.GetAttributes())})
	}
	for _, sequence := range observed.GetSequences() {
		result.Sequences = append(result.Sequences, observedSequence(sequence))
	}
	for _, role := range observed.GetRoles() {
		result.Roles = append(result.Roles, catalog.Role{Name: role.GetName(), Login: role.GetLogin(), Superuser: role.GetSuperuser(), CreateDB: role.GetCreateDb(),
			CreateRole: role.GetCreateRole(), Inherit: role.GetInherit(), Replication: role.GetReplication(), Group: role.GetGroup(),
			PasswordState: observedPassword[role.GetPassword()]})
		for _, group := range role.GetMemberOf() {
			result.RoleMemberships = append(result.RoleMemberships, catalog.RoleMembership{Role: group, Member: role.GetName()})
		}
	}
	// The wire names a role out of scope and carries none of its attributes.
	for _, name := range observed.GetRolesOutOfScope() {
		result.RolesOutOfScope = append(result.RolesOutOfScope, catalog.Role{Name: name, AttributesUnknown: true})
	}
	result.RolesOutOfScopeReason = observed.GetRolesOutOfScopeReason()
	for _, membership := range observed.GetMemberships() {
		result.RoleMemberships = append(result.RoleMemberships, catalog.RoleMembership{Role: membership.GetGroup(), Member: membership.GetMember()})
	}
	for _, owner := range observed.GetObjectOwners() {
		object := owner.GetObject()
		if err := r.ownerBinding(object); err != nil {
			return err
		}
		result.ObjectOwners = append(result.ObjectOwners, catalog.ObjectOwner{
			Kind: object.GetKind(), Schema: object.GetSchema(), Name: object.GetName(), Owner: owner.GetOwner(),
			OwnerCanLogin: copyPlanningPointer(owner.OwnerCanLogin),
		})
	}
	for _, grant := range observed.GetGrants() {
		result.Grants = append(result.Grants, observedGrants(grant)...)
	}
	for _, comment := range observed.GetComments() {
		if err := placeComment(result, comment.GetObject(), comment.GetText()); err != nil {
			return err
		}
	}
	return nil
}

func recordCommonCoverage(result *catalog.Database, observed *protocol.ObservedCommonObjects) error {
	// Absence of a coverage row cannot establish an empty access catalog.
	// These independent facts may be absent from an older provider contract.
	for _, kind := range []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_OBJECT_OWNER, protocol.CommonObjectSupport_ROLE_MEMBERSHIP} {
		if !slices.ContainsFunc(observed.GetCoverage(), func(entry *protocol.CommonObjectCoverage) bool { return entry.GetKind() == kind }) {
			result.NotDescribed = result.NotDescribed.With(coverage.Refused(commonCoverageKinds[kind]))
		}
	}
	for _, entry := range observed.GetCoverage() {
		if entry.GetKnowledge().GetState() == protocol.ModelKnowledge_COMPLETE {
			continue
		}
		kind, recorded := commonCoverageKinds[entry.GetKind()]
		if !recorded {
			return unsupported(fmt.Sprintf("a read that leaves %s objects uninspected", strings.ToLower(entry.GetKind().String())))
		}
		result.NotDescribed = result.NotDescribed.With(coverage.Refused(kind))
	}
	return nil
}

func (r reader) ownerBinding(object *protocol.Reference) error {
	if object.GetDatabase() != "" && (r.session == nil || object.GetDatabase() != r.session.remote.GetDatabase()) {
		return unsupported("object ownership outside the exact session database")
	}
	if object.GetCatalog() != "" || len(object.GetPath()) != 0 || len(object.GetOverload()) != 0 {
		return unsupported("object ownership with catalog, parent path, or overload coordinates")
	}
	return nil
}

var observedPassword = map[protocol.ObservedRole_Password]catalog.RolePasswordState{
	protocol.ObservedRole_PASSWORD_UNKNOWN: catalog.RolePasswordUnknown,
	protocol.ObservedRole_PASSWORD_NONE:    catalog.RolePasswordAbsent,
	protocol.ObservedRole_PASSWORD_SET:     catalog.RolePasswordPresent,
}

func observedSequence(sequence *protocol.ObservedSequence) catalog.Sequence {
	identity := sequence.GetIdentity()
	result := catalog.Sequence{Name: identity.GetName(), Schema: identity.GetSchema(), DataType: sequence.GetDataType(),
		Start: copyPlanningPointer(sequence.Start), Increment: copyPlanningPointer(sequence.Increment), MinValue: copyPlanningPointer(sequence.MinValue),
		MaxValue: copyPlanningPointer(sequence.MaxValue), Cache: copyPlanningPointer(sequence.Cache), Cycle: sequence.GetCycle()}
	if owner := sequence.GetOwnedBy(); owner != nil {
		result.OwnedBy = owner.GetPath()[0] + "." + owner.GetName()
	}
	return result
}

// observedGrants is one catalog row for each column a grant names, or one for
// the whole object, as the shared readers report a column privilege.
func observedGrants(grant *protocol.ObservedGrant) []catalog.Grant {
	target := grant.GetTarget()
	row := catalog.Grant{Role: grant.GetRole(), Privilege: grant.GetPrivilege(), ObjectType: strings.ToUpper(target.GetKind()),
		Schema: target.GetSchema(), ObjectName: target.GetName(), WithOption: grant.GetWithGrantOption(), GrantedBy: grant.GetGrantedBy(),
		Arguments: strings.Join(target.GetOverload(), ", "), Implicit: grant.GetImplicit()}
	if len(grant.GetColumns()) == 0 {
		return []catalog.Grant{row}
	}
	rows := make([]catalog.Grant, len(grant.GetColumns()))
	for i, column := range grant.GetColumns() {
		rows[i] = row
		rows[i].Column = column
	}
	return rows
}

// placeComment gives the comment to the object of the read it names.
func placeComment(result *catalog.Database, object *protocol.Reference, text string) error {
	find, known := commentTargets[object.GetKind()]
	var target *string
	if known {
		target = find(result, object)
	}
	if target == nil {
		return unsupported(fmt.Sprintf("a comment on %s %q, which the read does not report", object.GetKind(), object.GetName()))
	}
	*target = text
	return nil
}

// commentTargets find, by the kind a comment names, the comment field of the
// object of the read it names, or nil for one the read does not report. A
// column and an index have their table as the only path component.
var commentTargets = map[string]func(*catalog.Database, *protocol.Reference) *string{
	"table": func(result *catalog.Database, object *protocol.Reference) *string {
		return field(result.Tables, func(table catalog.Table) bool {
			return table.Schema == object.GetSchema() && table.Name == object.GetName()
		},
			func(table *catalog.Table) *string { return &table.Comment })
	},
	"column": func(result *catalog.Database, object *protocol.Reference) *string {
		columns := field(result.Tables, func(table catalog.Table) bool {
			return table.Schema == object.GetSchema() && table.Name == object.GetPath()[0]
		},
			func(table *catalog.Table) *[]catalog.Column { return &table.Columns })
		if columns == nil {
			return nil
		}
		return field(*columns, func(column catalog.Column) bool { return column.Name == object.GetName() }, func(column *catalog.Column) *string { return &column.Comment })
	},
	"index": func(result *catalog.Database, object *protocol.Reference) *string {
		return field(result.Indexes, func(index catalog.Index) bool {
			return index.Schema == object.GetSchema() && index.TableName == object.GetPath()[0] && index.Name == object.GetName()
		}, func(index *catalog.Index) *string { return &index.Comment })
	},
	"view": func(result *catalog.Database, object *protocol.Reference) *string {
		return field(result.Views, func(view catalog.View) bool {
			return view.Schema == object.GetSchema() && view.Name == object.GetName()
		},
			func(view *catalog.View) *string { return &view.Comment })
	},
	"materialized_view": func(result *catalog.Database, object *protocol.Reference) *string {
		return field(result.MatViews, func(view catalog.MaterializedView) bool {
			return view.Schema == object.GetSchema() && view.Name == object.GetName()
		},
			func(view *catalog.MaterializedView) *string { return &view.Comment })
	},
	"sequence": func(result *catalog.Database, object *protocol.Reference) *string {
		return field(result.Sequences, func(sequence catalog.Sequence) bool {
			return sequence.Schema == object.GetSchema() && sequence.Name == object.GetName()
		},
			func(sequence *catalog.Sequence) *string { return &sequence.Comment })
	},
	"role":  roleCommentTarget,
	"group": roleCommentTarget,
	"schema": func(result *catalog.Database, object *protocol.Reference) *string {
		return field(result.Schemas, func(schema catalog.Schema) bool { return schema.Name == object.GetName() },
			func(schema *catalog.Schema) *string { return &schema.Comment })
	},
}

func roleCommentTarget(result *catalog.Database, object *protocol.Reference) *string {
	return field(result.Roles, func(role catalog.Role) bool { return role.Name == object.GetName() }, func(role *catalog.Role) *string { return &role.Comment })
}

// field is the field of the first element of values that matches, or nil
// where none does.
func field[T, F any](values []T, matches func(T) bool, of func(*T) *F) *F {
	if i := slices.IndexFunc(values, matches); i >= 0 {
		return of(&values[i])
	}
	return nil
}
