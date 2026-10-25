package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// CommonObjectsFeature switches on the common-object wire: views,
// materialized views, sequences, roles, grants and comments travel in common
// messages in inspection replies, validation, planning and rendering, and
// PrepareCommonObjects gives a declaration in the engine's spelling. It is a
// two-way feature. A provider lists it, and each supporting engine lists it
// beside Engine.common_objects, the kinds it carries. Contract 25.3.
const CommonObjectsFeature = "schema.common-objects"

// CommonObjectPreparer returns a declaration of common objects in the
// engine's own spelling, offline. It may change how a value is spelled, such
// as a view body or a privilege name, and never what an entry is. A failure is
// an error and exposes no prefix.
type CommonObjectPreparer interface {
	PrepareCommonObjects(context.Context, *protocol.PrepareCommonObjectsRequest) (*protocol.PreparedCommonObjects, error)
}

// commonKinds is the set of kinds a negotiated engine carries.
type commonKinds map[protocol.CommonObjectSupport_Kind]bool

func commonObjectError(reason string) error {
	return fmt.Errorf("%w: common objects: %s", ErrProtocol, reason)
}

// validateCommonObjectSupport checks an engine's declaration: present exactly
// when the engine lists the feature, which needs the provider's, and holding
// known kinds once each.
func validateCommonObjectSupport(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), CommonObjectsFeature)
	if listed && !slices.Contains(description.GetFeatures(), CommonObjectsFeature) {
		return commonObjectError("an engine lists the feature without the provider")
	}
	support := engine.GetCommonObjects()
	if listed != (support != nil) {
		return commonObjectError("an engine declares its kinds exactly when it lists the feature")
	}
	if support == nil {
		return nil
	}
	if len(support.GetKinds()) == 0 {
		return commonObjectError("an engine declares no kind")
	}
	for i, kind := range support.GetKinds() {
		if _, known := protocol.CommonObjectSupport_Kind_name[int32(kind)]; !known || kind == protocol.CommonObjectSupport_KIND_UNSPECIFIED ||
			slices.Contains(support.GetKinds()[:i], kind) {
			return commonObjectError("an unknown or repeated kind")
		}
	}
	return nil
}

// engineCommonKinds returns the kinds a negotiated engine carries, or none
// when the host did not list the feature.
func engineCommonKinds(engine *protocol.Engine) commonKinds {
	kinds := make(commonKinds)
	for _, kind := range engine.GetCommonObjects().GetKinds() {
		kinds[kind] = true
	}
	return kinds
}

func (k commonKinds) require(kind protocol.CommonObjectSupport_Kind) error {
	if !k[kind] {
		return commonObjectError(fmt.Sprintf("the engine does not carry %s objects", strings.ToLower(kind.String())))
	}
	return nil
}

// viewKinds maps the reference kind that names a view to its common kind.
var viewKinds = map[string]protocol.CommonObjectSupport_Kind{
	"view":              protocol.CommonObjectSupport_VIEW,
	"materialized_view": protocol.CommonObjectSupport_MATERIALIZED_VIEW,
}

// declaredViewKind is the reference kind of a declared view, which its
// materialized flag selects.
func declaredViewKind(view *protocol.SQLView) string {
	if view.GetMaterialized() {
		return "materialized_view"
	}
	return "view"
}

// roleReferenceKind is the reference kind that names a declared role or group.
func roleReferenceKind(role *protocol.SQLRole) string {
	if role.GetGroup() {
		return "group"
	}
	return "role"
}

// commentCarried refuses a comment on an engine that carries no comments.
func (k commonKinds) commentCarried(comment string) error {
	if comment != "" {
		return k.require(protocol.CommonObjectSupport_COMMENT)
	}
	return nil
}

// validateDeclaredCommonObjects checks a declaration against the kinds an
// engine carries and the identifier rules that make two names one.
func validateDeclaredCommonObjects(identifiers *protocol.ComparisonIdentifiers, kinds commonKinds, declared *protocol.DeclaredCommonObjects) error {
	rules := identifiers.GetRules()
	qualified := func(schema, name string) (string, string) {
		if schema == "" {
			schema = identifiers.GetDefaultSchema()
		}
		return sqlFold(rules.GetTableNames(), schema), sqlFold(rules.GetTableNames(), name)
	}
	relations := make(sqlNames)
	for _, view := range declared.GetViews() {
		if err := kinds.require(viewKinds[declaredViewKind(view)]); err != nil {
			return err
		}
		if strings.TrimSpace(view.GetName()) == "" || strings.TrimSpace(view.GetBody()) == "" || !relations.add(qualified(view.GetSchema(), view.GetName())) {
			return commonObjectError("a view needs a unique name and a body")
		}
		if err := kinds.commentCarried(view.GetComment()); err != nil {
			return err
		}
	}
	sequences := make(sqlNames)
	for _, sequence := range declared.GetSequences() {
		if err := validateDeclaredSequence(kinds, sequence); err != nil {
			return err
		}
		if !sequences.add(qualified(sequence.GetSchema(), sequence.GetName())) {
			return commonObjectError("a repeated sequence")
		}
	}
	roles := make(map[string]bool)
	for _, role := range declared.GetRoles() {
		if err := validateDeclaredRole(kinds, role); err != nil {
			return err
		}
		if roles[role.GetName()] {
			return commonObjectError("a repeated role")
		}
		roles[role.GetName()] = true
	}
	for _, grant := range declared.GetGrants() {
		if err := validateDeclaredGrant(kinds, grant); err != nil {
			return err
		}
	}
	return nil
}

func validateDeclaredSequence(kinds commonKinds, sequence *protocol.SQLSequence) error {
	if err := kinds.require(protocol.CommonObjectSupport_SEQUENCE); err != nil {
		return err
	}
	if strings.TrimSpace(sequence.GetName()) == "" || (sequence.Increment != nil && sequence.GetIncrement() == 0) {
		return commonObjectError("a sequence needs a name and a nonzero increment")
	}
	return kinds.commentCarried(sequence.GetComment())
}

func validateDeclaredRole(kinds commonKinds, role *protocol.SQLRole) error {
	if err := kinds.require(protocol.CommonObjectSupport_ROLE); err != nil {
		return err
	}
	if strings.TrimSpace(role.GetName()) == "" {
		return commonObjectError("a role needs a name")
	}
	if err := validateMemberships(role.GetName(), role.GetMemberOf()); err != nil {
		return err
	}
	return kinds.commentCarried(role.GetComment())
}

// validateMemberships refuses an empty or repeated group name and a role that
// is a member of itself.
func validateMemberships(role string, groups []string) error {
	for i, group := range groups {
		if strings.TrimSpace(group) == "" || group == role || slices.Contains(groups[:i], group) {
			return commonObjectError("a role is a member of an empty, repeated or its own group")
		}
	}
	return nil
}

func validateDeclaredGrant(kinds commonKinds, grant *protocol.SQLGrant) error {
	if err := kinds.require(protocol.CommonObjectSupport_GRANT); err != nil {
		return err
	}
	if grant == nil || strings.TrimSpace(grant.GetRole()) == "" || len(grant.GetPrivileges()) == 0 ||
		slices.ContainsFunc(grant.GetPrivileges(), func(privilege string) bool { return strings.TrimSpace(privilege) == "" }) {
		return commonObjectError("a grant needs a role and privileges")
	}
	if !validGrantTarget(grant.GetTarget()) {
		return commonObjectError("a grant needs a target")
	}
	return validateColumnList(grant.GetColumns())
}

// validGrantTarget requires a kind, and a name for every target but the
// database, whose grant names the database the schema is applied to.
func validGrantTarget(target *protocol.Reference) bool {
	return target != nil && strings.TrimSpace(target.GetKind()) != "" && (target.GetName() != "" || target.GetKind() == "database")
}

func validateColumnList(columns []string) error {
	for i, column := range columns {
		if strings.TrimSpace(column) == "" || slices.Contains(columns[:i], column) {
			return commonObjectError("an empty or repeated column")
		}
	}
	return nil
}

// validateObservedCommonObjects checks what a read reports: one coverage
// entry for each kind the engine carries, objects only of a kind the read
// covered completely, and well-formed, unique objects.
func validateObservedCommonObjects(engine *protocol.Engine, observed *protocol.ObservedCommonObjects) error {
	kinds := engineCommonKinds(engine)
	if observed == nil {
		if len(kinds) != 0 {
			return commonObjectError("a read of an engine that carries common objects needs their coverage")
		}
		return nil
	}
	if len(kinds) == 0 {
		return commonObjectError("the engine carries no common object")
	}
	complete, err := validateCommonCoverage(kinds, observed.GetCoverage())
	if err != nil {
		return err
	}
	for _, check := range []func() error{
		func() error { return validateObservedRelations(engine, complete, observed) },
		func() error { return validateObservedRoles(complete, observed) },
		func() error { return validateObservedGrants(complete, observed.GetGrants()) },
		func() error { return validateObservedComments(engine.GetId(), complete, observed.GetComments()) },
	} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func validateCommonCoverage(kinds commonKinds, coverage []*protocol.CommonObjectCoverage) (commonKinds, error) {
	complete := make(commonKinds)
	seen := make(commonKinds)
	for _, entry := range coverage {
		if !kinds[entry.GetKind()] || seen[entry.GetKind()] {
			return nil, commonObjectError("coverage of an undeclared or repeated kind")
		}
		seen[entry.GetKind()] = true
		switch entry.GetKnowledge().GetState() {
		case protocol.ModelKnowledge_COMPLETE:
			complete[entry.GetKind()] = true
		case protocol.ModelKnowledge_UNINSPECTED:
			if !comparisonReason(entry.GetKnowledge().GetReason()) {
				return nil, commonObjectError("an uninspected kind needs a reason")
			}
		default:
			return nil, commonObjectError("a kind is complete or uninspected")
		}
	}
	if len(seen) != len(kinds) {
		return nil, commonObjectError("a declared kind has no coverage")
	}
	return complete, nil
}

func validateObservedRelations(engine *protocol.Engine, complete commonKinds, observed *protocol.ObservedCommonObjects) error {
	rules := engine.GetIdentifiers()
	key := func(ref *protocol.Reference) (string, string) {
		return sqlFold(rules.GetTableNames(), ref.GetSchema()), sqlFold(rules.GetTableNames(), ref.GetName())
	}
	relations := make(sqlNames)
	for _, view := range observed.GetViews() {
		identity := view.GetIdentity()
		kind, known := viewKinds[identity.GetKind()]
		if !validValueReference(identity, engine.GetId()) || !known || strings.TrimSpace(view.GetBody()) == "" {
			return commonObjectError("a view needs an identity and a body")
		}
		if !complete[kind] {
			return commonObjectError("a view of a kind the read did not cover")
		}
		if !relations.add(key(identity)) {
			return commonObjectError("a repeated view")
		}
	}
	sequences := make(sqlNames)
	for _, sequence := range observed.GetSequences() {
		identity := sequence.GetIdentity()
		if !complete[protocol.CommonObjectSupport_SEQUENCE] {
			return commonObjectError("a sequence the read did not cover")
		}
		if !validValueReference(identity, engine.GetId()) || identity.GetKind() != "sequence" || (sequence.Increment != nil && sequence.GetIncrement() == 0) {
			return commonObjectError("a sequence needs an identity and a nonzero increment")
		}
		if owner := sequence.GetOwnedBy(); owner != nil && (!validValueReference(owner, engine.GetId()) || owner.GetKind() != "column" || len(owner.GetPath()) != 1) {
			return commonObjectError("an owning column needs its table")
		}
		if !sequences.add(key(identity)) {
			return commonObjectError("a repeated sequence")
		}
	}
	return nil
}

func validateObservedRoles(complete commonKinds, observed *protocol.ObservedCommonObjects) error {
	roles := make(map[string]bool)
	for _, role := range observed.GetRoles() {
		if !complete[protocol.CommonObjectSupport_ROLE] {
			return commonObjectError("a role the read did not cover")
		}
		if strings.TrimSpace(role.GetName()) == "" || roles[role.GetName()] {
			return commonObjectError("a role needs a unique name")
		}
		roles[role.GetName()] = true
		if _, known := protocol.ObservedRole_Password_name[int32(role.GetPassword())]; !known {
			return commonObjectError("an unknown password state")
		}
		if err := validateMemberships(role.GetName(), role.GetMemberOf()); err != nil {
			return err
		}
	}
	outside := make(map[string]bool)
	for _, name := range observed.GetRolesOutOfScope() {
		if !complete[protocol.CommonObjectSupport_ROLE] || strings.TrimSpace(name) == "" || roles[name] || outside[name] {
			return commonObjectError("a role out of scope is empty, repeated, described or not covered")
		}
		outside[name] = true
	}
	return nil
}

func validateObservedGrants(complete commonKinds, grants []*protocol.ObservedGrant) error {
	seen := make(map[string]bool)
	for _, grant := range grants {
		if !complete[protocol.CommonObjectSupport_GRANT] {
			return commonObjectError("a grant the read did not cover")
		}
		if strings.TrimSpace(grant.GetRole()) == "" || strings.TrimSpace(grant.GetPrivilege()) == "" || !validGrantTarget(grant.GetTarget()) {
			return commonObjectError("a grant needs a role, a privilege and a target")
		}
		if err := validateColumnList(grant.GetColumns()); err != nil {
			return err
		}
		identity := proto.CloneOf(grant)
		identity.GrantedBy, identity.Implicit, identity.WithGrantOption = "", false, false
		key, err := proto.MarshalOptions{Deterministic: true}.Marshal(identity)
		if err != nil {
			return commonObjectError("a grant that does not encode: " + err.Error())
		}
		if seen[string(key)] {
			return commonObjectError("a repeated grant")
		}
		seen[string(key)] = true
	}
	return nil
}

func validateObservedComments(engine string, complete commonKinds, comments []*protocol.ObjectComment) error {
	seen := make([]*protocol.Reference, 0, len(comments))
	for _, comment := range comments {
		if !complete[protocol.CommonObjectSupport_COMMENT] {
			return commonObjectError("a comment the read did not cover")
		}
		if err := validateCommentedObject(engine, comment.GetObject()); err != nil {
			return err
		}
		if comment.GetText() == "" {
			return commonObjectError("an observed comment needs its text")
		}
		if slices.ContainsFunc(seen, func(other *protocol.Reference) bool { return compareReferences(other, comment.GetObject()) == 0 }) {
			return commonObjectError("a repeated comment")
		}
		seen = append(seen, comment.GetObject())
	}
	return nil
}

// validateCommentedObject requires an object of the engine, and a column or an
// index to name its table as the only path component.
func validateCommentedObject(engine string, object *protocol.Reference) error {
	if !validValueReference(object, engine) {
		return commonObjectError("a comment needs its object")
	}
	if (object.GetKind() == "column" || object.GetKind() == "index") && len(object.GetPath()) != 1 {
		return commonObjectError("a column or index comment needs its table")
	}
	return nil
}

// commonObjectEngine returns the negotiated engine a request names and the
// kinds it carries. It refuses an engine without the wire.
func commonObjectEngine(description *protocol.Description, name string, targetFeatures []string) (*protocol.Engine, commonKinds, error) {
	if !slices.Contains(description.GetFeatures(), CommonObjectsFeature) {
		return nil, nil, commonObjectError("the wire was not negotiated")
	}
	engine, _, err := valueTarget(description, name, targetFeatures)
	if err != nil {
		return nil, nil, err
	}
	kinds := engineCommonKinds(engine)
	if len(kinds) == 0 {
		return nil, nil, commonObjectError("the engine carries no common object")
	}
	return engine, kinds, nil
}

// ValidatePrepareCommonObjectsRequest checks a preparation batch before owner
// code runs: a negotiated engine that carries the declared kinds, complete
// identifier facts, and well-formed, unique declarations.
func ValidatePrepareCommonObjectsRequest(description *protocol.Description, request *protocol.PrepareCommonObjectsRequest) error {
	_, kinds, err := commonObjectEngine(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	if err := validateComparisonContext(&protocol.CompareModelsRequest{Identifiers: request.GetIdentifiers(), Capabilities: request.GetCapabilities()},
		&protocol.ModelComparison{Placement: protocol.ModelComparison_OBJECTS}); err != nil {
		return err
	}
	return validateDeclaredCommonObjects(request.GetIdentifiers(), kinds, request.GetDeclared())
}

// ValidatePreparedCommonObjects requires a complete reply with every entry of
// the request in its order and with its identity: a view's schema, name and
// kind, a sequence's schema and name, a role's name and kind, and a grant's
// role, target and whether it is revoked. Only the spelling of values changes.
func ValidatePreparedCommonObjects(description *protocol.Description, request *protocol.PrepareCommonObjectsRequest, result *protocol.PreparedCommonObjects) error {
	if err := ValidatePrepareCommonObjectsRequest(description, request); err != nil {
		return err
	}
	if !result.GetComplete() {
		return commonObjectError("incomplete preparation")
	}
	if err := validatePreparedIdentities(request.GetDeclared(), result.GetPrepared()); err != nil {
		return err
	}
	_, kinds, _ := commonObjectEngine(description, request.GetEngine(), request.GetTargetFeatures())
	return validateDeclaredCommonObjects(request.GetIdentifiers(), kinds, result.GetPrepared())
}

// validatePreparedIdentities requires a prepared declaration to hold the
// request's entries in their order, each with its identity.
func validatePreparedIdentities(before, after *protocol.DeclaredCommonObjects) error {
	if len(before.GetViews()) != len(after.GetViews()) || len(before.GetSequences()) != len(after.GetSequences()) ||
		len(before.GetRoles()) != len(after.GetRoles()) || len(before.GetGrants()) != len(after.GetGrants()) {
		return commonObjectError("preparation changed the number of entries")
	}
	for i, view := range before.GetViews() {
		prepared := after.GetViews()[i]
		if prepared.GetSchema() != view.GetSchema() || prepared.GetName() != view.GetName() || prepared.GetMaterialized() != view.GetMaterialized() {
			return commonObjectError("preparation changed a view's identity")
		}
	}
	for i, sequence := range before.GetSequences() {
		prepared := after.GetSequences()[i]
		if prepared.GetSchema() != sequence.GetSchema() || prepared.GetName() != sequence.GetName() {
			return commonObjectError("preparation changed a sequence's identity")
		}
	}
	for i, role := range before.GetRoles() {
		prepared := after.GetRoles()[i]
		if prepared.GetName() != role.GetName() || prepared.GetGroup() != role.GetGroup() {
			return commonObjectError("preparation changed a role's identity")
		}
	}
	for i, grant := range before.GetGrants() {
		prepared := after.GetGrants()[i]
		if prepared.GetRole() != grant.GetRole() || prepared.GetRevoked() != grant.GetRevoked() || !proto.Equal(prepared.GetTarget(), grant.GetTarget()) {
			return commonObjectError("preparation changed a grant's identity")
		}
	}
	return nil
}

// The attributes an alteration names as changed.
var (
	sequenceAttributes = []string{"as_type", "start", "increment", "min_value", "max_value", "cache", "cycle", "owned_by"}
	roleAttributes     = []string{"login", "password", "superuser", "create_db", "create_role", "inherit", "replication"}
)

func validateChangedAttributes(changed, allowed []string) error {
	for i, attribute := range changed {
		if !slices.Contains(allowed, attribute) || slices.Contains(changed[:i], attribute) {
			return commonObjectError("an unknown or repeated changed attribute")
		}
	}
	return nil
}

// validateCommonObjectChange checks one typed change against the kinds the
// engine carries, and requires the change's object to name its subject.
func validateCommonObjectChange(engine string, kinds commonKinds, object *protocol.Reference, change *protocol.CommonObjectChange) error {
	subject, err := commonChangeSubject(engine, kinds, change)
	if err != nil {
		return err
	}
	if compareReferences(object, subject) != 0 {
		return commonObjectError("a change's object differs from its subject")
	}
	return nil
}

// commonChangeSubject validates the operand of a change and returns the
// object it changes.
func commonChangeSubject(engine string, kinds commonKinds, change *protocol.CommonObjectChange) (*protocol.Reference, error) {
	for _, family := range []func(string, commonKinds, *protocol.CommonObjectChange) (*protocol.Reference, bool, error){
		relationChangeSubject, principalChangeSubject, accessChangeSubject,
	} {
		if subject, matched, err := family(engine, kinds, change); matched {
			return subject, err
		}
	}
	return nil, commonObjectError("a change without an operation")
}

// named builds the reference of an object of the engine.
func named(engine, kind, schema, name string) *protocol.Reference {
	return &protocol.Reference{Engine: engine, Kind: kind, Schema: schema, Name: name}
}

// relationChangeSubject handles the views and sequences, and reports whether
// the change is one of theirs.
func relationChangeSubject(engine string, kinds commonKinds, change *protocol.CommonObjectChange) (*protocol.Reference, bool, error) {
	switch operation := change.GetOperation().(type) {
	case *protocol.CommonObjectChange_CreateView:
		view := operation.CreateView.GetView()
		reference := declaredViewKind(view)
		if err := kinds.require(viewKinds[reference]); err != nil {
			return nil, true, err
		}
		if strings.TrimSpace(view.GetName()) == "" || strings.TrimSpace(view.GetBody()) == "" {
			return nil, true, commonObjectError("a view needs a name and a body")
		}
		return named(engine, reference, view.GetSchema(), view.GetName()), true, kinds.commentCarried(view.GetComment())
	case *protocol.CommonObjectChange_DropView:
		ref := operation.DropView
		kind, known := viewKinds[ref.GetKind()]
		if !validValueReference(ref, engine) || !known {
			return nil, true, commonObjectError("a view removal needs the view")
		}
		return ref, true, kinds.require(kind)
	case *protocol.CommonObjectChange_CreateSequence:
		sequence := operation.CreateSequence
		return named(engine, "sequence", sequence.GetSchema(), sequence.GetName()), true, validateDeclaredSequence(kinds, sequence)
	case *protocol.CommonObjectChange_AlterSequence:
		alteration := operation.AlterSequence
		ref := alteration.GetSequence()
		if !validValueReference(ref, engine) || ref.GetKind() != "sequence" || (len(alteration.GetChanged()) == 0 && !alteration.GetRestart()) {
			return nil, true, commonObjectError("a sequence alteration needs the sequence and what changes")
		}
		if err := validateDeclaredSequence(kinds, alteration.GetDesired()); err != nil {
			return nil, true, err
		}
		return ref, true, validateChangedAttributes(alteration.GetChanged(), sequenceAttributes)
	case *protocol.CommonObjectChange_DropSequence:
		ref := operation.DropSequence
		if !validValueReference(ref, engine) || ref.GetKind() != "sequence" {
			return nil, true, commonObjectError("a sequence removal needs the sequence")
		}
		return ref, true, kinds.require(protocol.CommonObjectSupport_SEQUENCE)
	default:
		return nil, false, nil
	}
}

// principalChangeSubject handles the roles and their memberships.
func principalChangeSubject(engine string, kinds commonKinds, change *protocol.CommonObjectChange) (*protocol.Reference, bool, error) {
	switch operation := change.GetOperation().(type) {
	case *protocol.CommonObjectChange_CreateRole:
		role := operation.CreateRole.GetRole()
		if err := validateDeclaredRole(kinds, role); err != nil {
			return nil, true, err
		}
		if (operation.CreateRole.GetPassword() != "") != role.GetPasswordDeclared() {
			return nil, true, commonObjectError("a role carries a password exactly when it declares one")
		}
		return named(engine, roleReferenceKind(role), "", role.GetName()), true, nil
	case *protocol.CommonObjectChange_AlterRole:
		subject, err := roleAlterationSubject(engine, kinds, operation.AlterRole)
		return subject, true, err
	case *protocol.CommonObjectChange_DropRole:
		ref := operation.DropRole
		if !validValueReference(ref, engine) || (ref.GetKind() != "role" && ref.GetKind() != "group") {
			return nil, true, commonObjectError("a role removal needs the role")
		}
		return ref, true, kinds.require(protocol.CommonObjectSupport_ROLE)
	case *protocol.CommonObjectChange_AddMember:
		subject, err := membershipSubject(engine, kinds, operation.AddMember)
		return subject, true, err
	case *protocol.CommonObjectChange_RemoveMember:
		subject, err := membershipSubject(engine, kinds, operation.RemoveMember)
		return subject, true, err
	default:
		return nil, false, nil
	}
}

// accessChangeSubject handles the grants, the revocations and the comments.
func accessChangeSubject(engine string, kinds commonKinds, change *protocol.CommonObjectChange) (*protocol.Reference, bool, error) {
	switch operation := change.GetOperation().(type) {
	case *protocol.CommonObjectChange_Grant:
		subject, err := grantSubject(kinds, operation.Grant)
		return subject, true, err
	case *protocol.CommonObjectChange_Revoke:
		subject, err := grantSubject(kinds, operation.Revoke)
		return subject, true, err
	case *protocol.CommonObjectChange_SetComment:
		comment := operation.SetComment
		if err := kinds.require(protocol.CommonObjectSupport_COMMENT); err != nil {
			return nil, true, err
		}
		return comment.GetObject(), true, validateCommentedObject(engine, comment.GetObject())
	default:
		return nil, false, nil
	}
}

func roleAlterationSubject(engine string, kinds commonKinds, alteration *protocol.RoleAlteration) (*protocol.Reference, error) {
	role := alteration.GetDesired()
	if err := validateDeclaredRole(kinds, role); err != nil {
		return nil, err
	}
	if len(alteration.GetChanged()) == 0 {
		return nil, commonObjectError("a role alteration needs what changes")
	}
	if err := validateChangedAttributes(alteration.GetChanged(), roleAttributes); err != nil {
		return nil, err
	}
	if alteration.GetPassword() != "" && !slices.Contains(alteration.GetChanged(), "password") {
		return nil, commonObjectError("a password travels only when it changes")
	}
	return named(engine, roleReferenceKind(role), "", role.GetName()), nil
}

// grantSubject validates a grant or a revocation, whose operation says which
// it is, and returns the object whose privileges it changes.
func grantSubject(kinds commonKinds, grant *protocol.SQLGrant) (*protocol.Reference, error) {
	if err := validateDeclaredGrant(kinds, grant); err != nil {
		return nil, err
	}
	if grant.GetRevoked() {
		return nil, commonObjectError("a grant change says by its operation whether it revokes")
	}
	return grant.GetTarget(), nil
}

func membershipSubject(engine string, kinds commonKinds, membership *protocol.RoleMembership) (*protocol.Reference, error) {
	if err := kinds.require(protocol.CommonObjectSupport_ROLE); err != nil {
		return nil, err
	}
	if strings.TrimSpace(membership.GetGroup()) == "" || strings.TrimSpace(membership.GetMember()) == "" || membership.GetGroup() == membership.GetMember() {
		return nil, commonObjectError("a membership needs a group and another member")
	}
	return named(engine, "group", "", membership.GetGroup()), nil
}
