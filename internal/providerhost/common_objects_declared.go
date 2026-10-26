package providerhost

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/internal/tableref"
)

// commonKindNames are the kinds of the common-object wire as a refusal names
// them.
var commonKindNames = map[protocol.CommonObjectSupport_Kind]string{
	protocol.CommonObjectSupport_VIEW:              "views",
	protocol.CommonObjectSupport_MATERIALIZED_VIEW: "materialized views",
	protocol.CommonObjectSupport_SEQUENCE:          "sequences",
	protocol.CommonObjectSupport_ROLE:              "roles",
	protocol.CommonObjectSupport_GRANT:             "grants",
	protocol.CommonObjectSupport_COMMENT:           "comments",
}

// commonKinds reports the kinds of common objects the engine carries, none
// unless the provider and the engine negotiated provider.CommonObjectsFeature.
func (t *target) commonKinds() map[protocol.CommonObjectSupport_Kind]bool {
	kinds := make(map[protocol.CommonObjectSupport_Kind]bool)
	if !t.negotiated(provider.CommonObjectsFeature) {
		return kinds
	}
	for _, kind := range t.engine.GetCommonObjects().GetKinds() {
		kinds[kind] = true
	}
	return kinds
}

// declaredCommonObjects is the declaration of the common objects beyond
// tables in source, on the wire, refusing by name a kind the engine does not
// carry before anything is sent. A comment travels in the field of the object
// it belongs to, so a commented object needs the comment kind too.
func (t *target) declaredCommonObjects(source *schemamodel.Database) (*protocol.DeclaredCommonObjects, error) {
	kinds := t.commonKinds()
	require := func(kind protocol.CommonObjectSupport_Kind, present bool) error {
		if present && !kinds[kind] {
			return unsupported(fmt.Sprintf("%s on the %s engine, whose provider does not carry them", commonKindNames[kind], t.engine.GetId()))
		}
		return nil
	}
	commented := slices.ContainsFunc(source.Views, func(view schemamodel.View) bool { return view.Comment != "" }) ||
		slices.ContainsFunc(source.MaterializedViews, func(view schemamodel.MaterializedView) bool { return view.Comment != "" }) ||
		slices.ContainsFunc(source.Sequences, func(sequence schemamodel.Sequence) bool { return sequence.Comment != "" }) ||
		slices.ContainsFunc(source.Roles, func(role schemamodel.Role) bool { return role.Comment != "" })
	for _, check := range []error{
		require(protocol.CommonObjectSupport_VIEW, len(source.Views) > 0),
		require(protocol.CommonObjectSupport_MATERIALIZED_VIEW, len(source.MaterializedViews) > 0),
		require(protocol.CommonObjectSupport_SEQUENCE, len(source.Sequences) > 0),
		require(protocol.CommonObjectSupport_ROLE, len(source.Roles) > 0),
		require(protocol.CommonObjectSupport_GRANT, len(source.Grants)+len(source.RevokedGrants) > 0),
		require(protocol.CommonObjectSupport_COMMENT, commented),
	} {
		if check != nil {
			return nil, check
		}
	}
	declared := &protocol.DeclaredCommonObjects{}
	for _, view := range source.Views {
		if !view.Facets.IsZero() {
			return nil, unsupported(fmt.Sprintf("settings of view %q", view.Name))
		}
		schema, name := splitQualified(view.Name)
		declared.Views = append(declared.Views, &protocol.SQLView{Schema: schema, Name: name, Body: view.Body, WithCheck: view.WithCheck, Comment: view.Comment,
			Attributes: slices.Clone(view.Attributes), DependsOn: slices.Clone(view.DependsOn)})
	}
	for _, view := range source.MaterializedViews {
		if !view.Facets.IsZero() {
			return nil, unsupported(fmt.Sprintf("settings of materialized view %q", view.Name))
		}
		schema, name := splitQualified(view.Name)
		declared.Views = append(declared.Views, &protocol.SQLView{Schema: schema, Name: name, Body: view.Body, Materialized: true, Comment: view.Comment,
			DependsOn: slices.Clone(view.DependsOn)})
	}
	for _, sequence := range source.Sequences {
		if !sequence.Facets.IsZero() || sequence.IfNotExists {
			return nil, unsupported(fmt.Sprintf("settings or IF NOT EXISTS of sequence %q", sequence.Name))
		}
		declared.Sequences = append(declared.Sequences, declaredSequence(sequence))
	}
	for _, role := range source.Roles {
		if !role.Facets.IsZero() {
			return nil, unsupported(fmt.Sprintf("settings of role %q", role.Name))
		}
		declared.Roles = append(declared.Roles, declaredRole(role))
	}
	for _, grants := range []struct {
		list    []schemamodel.Grant
		revoked bool
	}{{source.Grants, false}, {source.RevokedGrants, true}} {
		for _, grant := range grants.list {
			wire, err := t.declaredGrant(grant)
			if err != nil {
				return nil, err
			}
			wire.Revoked = grants.revoked
			declared.Grants = append(declared.Grants, wire)
		}
	}
	return declared, nil
}

// splitQualified splits a name the model may qualify with its schema.
func splitQualified(name string) (schema, object string) {
	if ref, valid := tableref.Parse(name); valid {
		return ref.Schema, ref.Name
	}
	return "", name
}

func declaredSequence(sequence schemamodel.Sequence) *protocol.SQLSequence {
	return &protocol.SQLSequence{Schema: sequence.Schema, Name: sequence.Name, AsType: sequence.AsType,
		Start: copyPlanningPointer(sequence.Start), Increment: copyPlanningPointer(sequence.Increment), MinValue: copyPlanningPointer(sequence.MinValue),
		MaxValue: copyPlanningPointer(sequence.MaxValue), Cache: copyPlanningPointer(sequence.Cache), Cycle: sequence.Cycle, OwnedBy: sequence.OwnedBy,
		Comment: sequence.Comment}
}

// declaredRole says that the declaration gives the role a password and never
// carries the password itself.
func declaredRole(role schemamodel.Role) *protocol.SQLRole {
	return &protocol.SQLRole{Name: role.Name, Login: role.Login, Superuser: role.Superuser, CreateDb: role.CreateDB, CreateRole: role.CreateRole,
		Inherit: role.Inherit, Replication: role.Replication, Comment: role.Comment, Group: role.Group, MemberOf: slices.Clone(role.MemberOf),
		PasswordDeclared: role.Password != ""}
}

// declaredGrant names a grant's target by the kind of object it is, as the SQL
// schema reading names it (see sqlGrant).
func (t *target) declaredGrant(grant schemamodel.Grant) (*protocol.SQLGrant, error) {
	target := &protocol.Reference{Engine: t.engine.GetId()}
	switch {
	case grant.OnDatabase:
		target.Kind = "database"
	case grant.OnSchema != "":
		target.Kind, target.Name = "schema", grant.OnSchema
	case grant.OnSequence != "":
		target.Kind = "sequence"
		target.Schema, target.Name = splitQualified(grant.OnSequence)
	case grant.OnRoutine != "":
		target.Kind = strings.ToLower(grant.RoutineKind)
		if target.Kind == "" {
			target.Kind = "routine"
		}
		target.Schema, target.Name = splitQualified(grant.OnRoutine)
		if grant.RoutineArguments != "" {
			target.Overload = strings.Split(grant.RoutineArguments, ", ")
		}
	case grant.OnObject != "":
		var err error
		target, err = t.grantTarget(grant.ObjectKind, grant.OnObject)
		if err != nil {
			return nil, err
		}
	case grant.OnTable != "":
		target.Kind = "table"
		target.Schema, target.Name = splitQualified(grant.OnTable)
	default:
		return nil, unsupported(fmt.Sprintf("a grant to %q without a target", grant.Role))
	}
	return &protocol.SQLGrant{Role: grant.Role, Privileges: slices.Clone(grant.Privileges), Target: target, WithGrantOption: grant.WithOption,
		Comment: grant.Comment, Columns: slices.Clone(grant.Columns)}, nil
}

// commonSpeller spells the desired common objects as the provider's engine
// does, through PrepareCommonObjects, for the comparison alone; see
// engine.Target.Spelling.
type commonSpeller struct{ target *target }

// LowerDesired returns a copy of the desired schema whose views, materialized
// views, sequences, roles and grants hold the values the provider prepared,
// each entry keeping its identity. A schema that declares none is returned as
// it is, with no call.
func (s commonSpeller) LowerDesired(ctx context.Context, request schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
	t := s.target
	if request.Target != t.engine.GetId() {
		return nil, fmt.Errorf("%w: spelling target changed", schemapreparation.ErrInvalid)
	}
	declared, err := t.declaredCommonObjects(request.Desired)
	if err != nil {
		return nil, err
	}
	if len(declared.GetViews())+len(declared.GetSequences())+len(declared.GetRoles())+len(declared.GetGrants()) == 0 {
		return request.Desired, nil
	}
	identifiers, err := comparisonIdentifiers(request.Semantics)
	if err != nil {
		return nil, err
	}
	if err := request.Capabilities.Validate(); err != nil {
		return nil, err
	}
	remote := &protocol.PrepareCommonObjectsRequest{Engine: t.engine.GetId(), Identifiers: identifiers, Capabilities: make(map[string]bool),
		TargetFeatures: featureSet(t.engine.GetFeatures()), Declared: declared}
	for name, value := range request.Capabilities.All() {
		remote.Capabilities[string(name)] = value
	}
	reply, err := t.host.client.PrepareCommonObjects(ctx, remote)
	if err != nil {
		return nil, err
	}
	return spelledCommonObjects(request.Desired, reply.GetPrepared()), nil
}

// spelledCommonObjects copies source with the prepared values in place, entry
// by entry: the views, then the materialized views, as declaredCommonObjects
// lists them, and the grants, then the revocations.
func spelledCommonObjects(source *schemamodel.Database, prepared *protocol.DeclaredCommonObjects) *schemamodel.Database {
	spelled := *source
	spelled.Views = slices.Clone(source.Views)
	for i := range spelled.Views {
		view := prepared.GetViews()[i]
		spelled.Views[i].Body, spelled.Views[i].WithCheck, spelled.Views[i].Attributes = view.GetBody(), view.GetWithCheck(), slices.Clone(view.GetAttributes())
	}
	spelled.MaterializedViews = slices.Clone(source.MaterializedViews)
	for i := range spelled.MaterializedViews {
		spelled.MaterializedViews[i].Body = prepared.GetViews()[len(source.Views)+i].GetBody()
	}
	spelled.Sequences = slices.Clone(source.Sequences)
	for i := range spelled.Sequences {
		sequence := prepared.GetSequences()[i]
		target := &spelled.Sequences[i]
		target.AsType, target.Cycle, target.OwnedBy = sequence.GetAsType(), sequence.GetCycle(), sequence.GetOwnedBy()
		target.Start, target.Increment, target.MinValue = copyPlanningPointer(sequence.Start), copyPlanningPointer(sequence.Increment), copyPlanningPointer(sequence.MinValue)
		target.MaxValue, target.Cache = copyPlanningPointer(sequence.MaxValue), copyPlanningPointer(sequence.Cache)
	}
	spelled.Roles = slices.Clone(source.Roles)
	for i := range spelled.Roles {
		role := prepared.GetRoles()[i]
		target := &spelled.Roles[i]
		target.Login, target.Superuser, target.CreateDB, target.CreateRole = role.GetLogin(), role.GetSuperuser(), role.GetCreateDb(), role.GetCreateRole()
		target.Inherit, target.Replication, target.MemberOf = role.GetInherit(), role.GetReplication(), slices.Clone(role.GetMemberOf())
	}
	grants := func(list []schemamodel.Grant, offset int) []schemamodel.Grant {
		result := slices.Clone(list)
		for i := range result {
			grant := prepared.GetGrants()[offset+i]
			result[i].Privileges, result[i].WithOption, result[i].Columns = slices.Clone(grant.GetPrivileges()), grant.GetWithGrantOption(), slices.Clone(grant.GetColumns())
		}
		return result
	}
	spelled.Grants = grants(source.Grants, 0)
	spelled.RevokedGrants = grants(source.RevokedGrants, len(source.Grants))
	return &spelled
}
