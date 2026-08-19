// Package schemachange is the typed semantic change from ADR 0001, for the
// foreign-key slice stokaro/ptah#1350 selects.
//
// # Why it exists
//
// `migration/schemadiff/types.SchemaDiff` describes a change as a name in a
// per-family slice. `TablesAdded []string` says a table appeared and nothing
// about what is in it, which is why the planner's signature is
//
//	GenerateSchemaDiffAST(diff *types.SchemaDiff, generated *goschema.Database, dialect string)
//
// -- the second parameter is where the planner recovers what the diff dropped.
// A planner that can read the desired description can also disagree with the
// diff it was handed, and nothing detects that it did.
//
// A [Change] here carries what its consumers need: the identity of the object,
// the state before and after, which properties changed, the evidence for the
// decision, the target facts it cannot be planned without, its risk, whether it
// is reversible, and where the object came from. A planner built on it needs no
// second parameter, and that is the boundary ADR 0001 decision 8 draws.
package schemachange

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/schemastate"
)

// Operation is what a change does to an object.
type Operation string

// The operations the slice needs. Modify is separate from a remove-and-add pair
// on purpose: the existing comparator expresses a changed foreign key as a
// removal and an addition of one name, and every planner then has to work out
// that the two belong together. A modification that says so cannot be split by
// a later stage that only sees one half.
const (
	Add    Operation = "add"
	Remove Operation = "remove"
	Modify Operation = "modify"
)

// Reversibility says whether a change can be undone from what the plan knows.
type Reversibility string

// The three answers, and the third is the point. A change that cannot be undone
// says so rather than being handed a rollback somebody invented.
const (
	Reversible   Reversibility = "reversible"
	Irreversible Reversibility = "irreversible"
	// ReversibleWithData marks a change whose rollback is expressible but whose
	// success depends on data the plan cannot see: re-adding a foreign key
	// succeeds only if no row violates it in the meantime.
	ReversibleWithData Reversibility = "reversible_with_data"
)

// Risk is how much a change can cost if it is wrong.
type Risk string

// The levels. Destructive is not a synonym for high: dropping a foreign key
// destroys a guarantee rather than data, and re-adding it can fail on data that
// accumulated while it was gone.
const (
	RiskLow           Risk = "low"
	RiskGuaranteeLoss Risk = "guarantee_loss"
	RiskDataDependent Risk = "data_dependent"
)

// Status says whether a change can be planned at all.
type Status string

// The three states. Blocked and Undecidable are separate because they ask for
// different things: a blocked change needs a target that can host it, and an
// undecidable one needs a measurement nobody has taken.
const (
	Planned     Status = "planned"
	Blocked     Status = "blocked"
	Undecidable Status = "undecidable"
)

// Change is one typed semantic change to one object.
type Change struct {
	// ID identifies the object, through the shared model. It is a value, not a
	// name: a later stage never re-parses it.
	ID objectidentity.ID
	// Operation is what happens to the object.
	Operation Operation
	// Before and After are the object's state on each side. Exactly one is nil
	// for an Add or a Remove; both are set for a Modify.
	Before *schemastate.ForeignKey
	After  *schemastate.ForeignKey
	// Changed names the properties a Modify changes, so a diagnostic can say
	// what moved without the reader diffing two structs.
	Changed []string
	// Evidence is why this change exists, in the terms an operator disputes it
	// in: which side carried what.
	Evidence string
	// RequiredFacts are the capability keys the change cannot be planned
	// without.
	RequiredFacts []capability.Capability
	// MissingFacts are the required facts the target does not have. Non-empty
	// exactly when Status is Blocked or Undecidable.
	MissingFacts []capability.Capability
	// Risk and Reversibility are properties of the change, not of the
	// statements it renders to.
	Risk          Risk
	Reversibility Reversibility
	// Status says whether the change can be planned.
	Status Status
	// Diagnostic explains a Blocked or Undecidable status in one sentence.
	Diagnostic string
	// Provenance is where the object was read from, carried so a diagnostic can
	// name the file or catalog relation responsible.
	Provenance schemastate.Provenance
}

// String renders a change for a diagnostic, never for comparison.
func (c Change) String() string {
	return fmt.Sprintf("%s %s", c.Operation, c.ID)
}

// Compare produces the typed changes that take current to desired.
//
// Both states must be normalized against the same profile. Comparing an
// unnormalized state is refused rather than tolerated: the referential-action
// default is applied in normalization, so an unnormalized side reports a
// modification for every foreign key whose ON DELETE nobody wrote.
func Compare(current, desired *schemastate.State, profile schemastate.Profile) ([]Change, error) {
	if err := requireComparable(current, desired); err != nil {
		return nil, err
	}
	changes := make([]Change, 0)
	desiredKeys := make(map[objectidentity.Key]schemastate.Object)

	for _, object := range desired.OfKind(objectidentity.KindConstraint) {
		if object.ForeignKey == nil {
			continue
		}
		desiredKeys[object.ID.Key()] = object
		existing, found := current.Get(object.ID)
		if !found || existing.ForeignKey == nil {
			changes = append(changes, decide(additionChange(object), profile, desired))
			continue
		}
		if changed := changedProperties(*existing.ForeignKey, *object.ForeignKey); len(changed) > 0 {
			changes = append(changes, decide(modificationChange(object, existing, changed), profile, desired))
		}
	}

	for _, object := range current.OfKind(objectidentity.KindConstraint) {
		if object.ForeignKey == nil {
			continue
		}
		if _, wanted := desiredKeys[object.ID.Key()]; wanted {
			continue
		}
		changes = append(changes, decide(removalChange(object), profile, current))
	}
	policyChanges, err := comparePolicies(current, desired, profile)
	if err != nil {
		return nil, err
	}
	changes = append(changes, policyChanges...)
	grantChanges, err := compareGrants(current, desired, profile)
	if err != nil {
		return nil, err
	}
	changes = append(changes, grantChanges...)
	return changes, nil
}

// compareGrants compares the privilege grants both sides carry.
//
// Two rules make a grant removal different from a table removal, and both are
// about whose privilege it is.
//
// A grant held by a role Ptah does not manage is not Ptah's to revoke: the
// description was never describing that role's privileges, so its silence is
// not a request to take them away. And a description that declined to describe
// the role family did not look at all, which is the same silence one level up.
// A removal that ignores either revokes access nobody asked to revoke.
func compareGrants(current, desired *schemastate.State, profile schemastate.Profile) ([]Change, error) {
	if err := schemastate.RequireScope(desired, objectidentity.KindGrant); err != nil {
		return nil, fmt.Errorf("the desired schema: %w", err)
	}
	if err := schemastate.RequireScope(desired, objectidentity.KindRole); err != nil {
		return nil, fmt.Errorf("the desired schema: %w", err)
	}
	changes := make([]Change, 0)
	declared := make(map[objectidentity.Key]schemastate.Object)
	for _, object := range desired.OfKind(objectidentity.KindGrant) {
		declared[object.ID.Key()] = object
		existing, found := current.Get(object.ID)
		if !found || existing.Grant == nil {
			changes = append(changes, decide(grantAddition(object), profile, desired))
			continue
		}
		if existing.Grant.WithGrant != object.Grant.WithGrant {
			changes = append(changes, decide(grantOptionChange(object, existing), profile, desired))
		}
	}
	for _, object := range current.OfKind(objectidentity.KindGrant) {
		if object.Grant == nil {
			continue
		}
		if _, wanted := declared[object.ID.Key()]; wanted {
			continue
		}
		changes = append(changes, grantRemoval(object, desired, profile))
	}
	return changes, nil
}

// grantRemoval decides whether a grant the desired schema does not carry is one
// it is asking to revoke.
func grantRemoval(object schemastate.Object, desired *schemastate.State, profile schemastate.Profile) Change {
	role := object.Grant.Role
	if !managesRole(desired, role) {
		return withheldGrantRemoval(object, fmt.Sprintf(
			"%s is not revoked: the desired schema does not manage role %q, so its privileges "+
				"are not this schema's to take away", object.ID, role))
	}
	if !schemastate.DescribesObject(desired, roleIdentity(desired, role)) {
		return withheldGrantRemoval(object, fmt.Sprintf(
			"%s is not revoked: the desired schema declares role %q not-described, so its "+
				"silence about that role's privileges is not a request to revoke them",
			object.ID, role))
	}
	change := Change{
		ID: object.ID, Operation: Remove,
		Evidence:      "held in the database by a managed role and absent from the desired schema",
		RequiredFacts: schemastate.RequiredFacts(),
		Risk:          RiskGuaranteeLoss, Reversibility: Reversible,
		Provenance: object.Provenance,
	}
	return decide(change, profile, desired)
}

func withheldGrantRemoval(object schemastate.Object, diagnostic string) Change {
	return Change{
		ID: object.ID, Operation: Remove,
		Evidence: "held in the database and not described by the desired schema",
		Risk:     RiskGuaranteeLoss, Reversibility: Reversible,
		Status:     Undecidable,
		Provenance: object.Provenance,
		Diagnostic: diagnostic,
	}
}

// managesRole reports whether the desired schema carries the role as an object
// it owns.
func managesRole(desired *schemastate.State, role string) bool {
	for _, object := range desired.OfKind(objectidentity.KindRole) {
		if object.ID.Name.Normalized == foldRoleName(desired, role) {
			return true
		}
	}
	return false
}

// roleIdentity returns the identity of a role as the desired state spells it,
// so a coverage question is asked about the same object the state carries.
func roleIdentity(desired *schemastate.State, role string) objectidentity.ID {
	for _, object := range desired.OfKind(objectidentity.KindRole) {
		if object.ID.Name.Normalized == foldRoleName(desired, role) {
			return object.ID
		}
	}
	return objectidentity.ID{Kind: objectidentity.KindRole}
}

// foldRoleName folds a role name with the target's own rules, so the two sides
// resolve one role to one name.
func foldRoleName(desired *schemastate.State, role string) string {
	return desired.Profile().Semantics.TableIdentityKey(strings.TrimSpace(role))
}

func grantAddition(object schemastate.Object) Change {
	return Change{
		ID: object.ID, Operation: Add,
		Evidence:      "declared by the desired schema and not held in the database",
		RequiredFacts: schemastate.RequiredFacts(),
		Risk:          RiskLow, Reversibility: Reversible,
		Provenance: object.Provenance,
	}
}

func grantOptionChange(object, existing schemastate.Object) Change {
	return Change{
		ID: object.ID, Operation: Modify, Changed: []string{"with grant option"},
		Evidence: fmt.Sprintf("both sides hold it and they disagree about WITH GRANT OPTION: "+
			"the database has %t and the desired schema declares %t",
			existing.Grant.WithGrant, object.Grant.WithGrant),
		RequiredFacts: schemastate.RequiredFacts(),
		Risk:          RiskGuaranteeLoss, Reversibility: Reversible,
		Provenance: object.Provenance,
	}
}

// comparePolicies compares the row-level-security policies both sides carry.
//
// A removal is planned only for a policy the DESIRED description claims to
// describe. A description that declined the policy family, or this policy in
// it, is silent rather than empty, and reading that silence as absence is how a
// partial read becomes a drop (stokaro/ptah#1028, #1664).
func comparePolicies(current, desired *schemastate.State, profile schemastate.Profile) ([]Change, error) {
	if err := schemastate.RequireScope(desired, objectidentity.KindPolicy); err != nil {
		return nil, fmt.Errorf("the desired schema: %w", err)
	}
	changes := make([]Change, 0)
	declared := make(map[objectidentity.Key]schemastate.Object)
	for _, object := range desired.OfKind(objectidentity.KindPolicy) {
		declared[object.ID.Key()] = object
		existing, found := current.Get(object.ID)
		if !found || existing.Policy == nil {
			changes = append(changes, decide(policyAddition(object), profile, desired))
			continue
		}
		if changed := changedPolicyProperties(*existing.Policy, *object.Policy); len(changed) > 0 {
			changes = append(changes, decide(policyModification(object, existing, changed), profile, desired))
		}
	}
	for _, object := range current.OfKind(objectidentity.KindPolicy) {
		if object.Policy == nil {
			continue
		}
		if _, wanted := declared[object.ID.Key()]; wanted {
			continue
		}
		if !schemastate.DescribesObject(desired, object.ID) {
			changes = append(changes, withheldRemoval(object))
			continue
		}
		changes = append(changes, decide(policyRemoval(object), profile, current))
	}
	return changes, nil
}

// withheldRemoval reports a removal the desired description did not authorize,
// as an undecidable change rather than as nothing.
//
// Nothing is the dangerous answer: an operator reading a plan that omits the
// policy cannot tell "the description keeps it" from "the description never
// looked".
func withheldRemoval(object schemastate.Object) Change {
	return Change{
		ID:            object.ID,
		Operation:     Remove,
		Before:        nil,
		Evidence:      "present in the database and not described by the desired schema",
		Risk:          RiskGuaranteeLoss,
		Reversibility: ReversibleWithData,
		Status:        Undecidable,
		Provenance:    object.Provenance,
		Diagnostic: fmt.Sprintf(
			"%s is not removed: the desired schema declares it not-described, so its silence "+
				"is not a request to drop it", object.ID),
	}
}

func policyAddition(object schemastate.Object) Change {
	return Change{
		ID: object.ID, Operation: Add,
		Evidence:      "declared by the desired schema and absent from the database",
		RequiredFacts: schemastate.RequiredFacts(),
		Risk:          RiskLow, Reversibility: Reversible,
		Provenance: object.Provenance,
	}
}

func policyRemoval(object schemastate.Object) Change {
	return Change{
		ID: object.ID, Operation: Remove,
		Evidence:      "present in the database and absent from the desired schema",
		RequiredFacts: schemastate.RequiredFacts(),
		Risk:          RiskGuaranteeLoss, Reversibility: ReversibleWithData,
		Provenance: object.Provenance,
	}
}

func policyModification(object, existing schemastate.Object, changed []string) Change {
	return Change{
		ID: object.ID, Operation: Modify, Changed: changed,
		Evidence:      "both sides declare it and they disagree about " + strings.Join(changed, ", "),
		RequiredFacts: schemastate.RequiredFacts(),
		Risk:          RiskGuaranteeLoss, Reversibility: Reversible,
		Provenance: object.Provenance,
	}
}

// changedPolicyProperties reports which properties of two policies differ, in a
// fixed order so a diagnostic reads the same each run.
func changedPolicyProperties(before, after schemastate.Policy) []string {
	changed := make([]string, 0)
	for _, property := range []struct {
		name        string
		left, right string
	}{
		{"command", before.Command, after.Command},
		{"role", before.Role, after.Role},
		{"using", before.Using, after.Using},
		{"with check", before.WithCheck, after.WithCheck},
	} {
		if property.left != property.right {
			changed = append(changed, property.name)
		}
	}
	return changed
}

// requireComparable refuses the two states a comparison must not be given.
func requireComparable(current, desired *schemastate.State) error {
	for name, state := range map[string]*schemastate.State{"current": current, "desired": desired} {
		if !state.Normalized() {
			return fmt.Errorf("%w: the %s schema", schemastate.ErrUnnormalized, name)
		}
		if err := schemastate.RequireScope(state, objectidentity.KindConstraint); err != nil {
			return fmt.Errorf("the %s schema: %w", name, err)
		}
	}
	return nil
}

// additionChange describes a foreign key the desired schema has and the
// database does not.
func additionChange(object schemastate.Object) Change {
	return Change{
		ID:            object.ID,
		Operation:     Add,
		After:         object.ForeignKey,
		Evidence:      "declared by the desired schema and absent from the database",
		RequiredFacts: schemastate.RequiredFacts(),
		// Adding a foreign key fails on rows that already violate it, and the
		// plan cannot see rows. That is a property of the change, not a
		// property of the statement it renders to.
		Risk:          RiskDataDependent,
		Reversibility: Reversible,
		Provenance:    object.Provenance,
	}
}

// removalChange describes a foreign key the database has and the desired schema
// does not.
func removalChange(object schemastate.Object) Change {
	return Change{
		ID:            object.ID,
		Operation:     Remove,
		Before:        object.ForeignKey,
		Evidence:      "present in the database and absent from the desired schema",
		RequiredFacts: schemastate.RequiredFacts(),
		// Dropping one destroys a guarantee rather than data, and re-adding it
		// can fail on rows that accumulated while it was gone.
		Risk:          RiskGuaranteeLoss,
		Reversibility: ReversibleWithData,
		Provenance:    object.Provenance,
	}
}

// modificationChange describes a foreign key both sides have and disagree
// about.
func modificationChange(object, existing schemastate.Object, changed []string) Change {
	return Change{
		ID:            object.ID,
		Operation:     Modify,
		Before:        existing.ForeignKey,
		After:         object.ForeignKey,
		Changed:       changed,
		Evidence:      "both sides declare it and they disagree about " + strings.Join(changed, ", "),
		RequiredFacts: schemastate.RequiredFacts(),
		Risk:          RiskDataDependent,
		Reversibility: ReversibleWithData,
		Provenance:    object.Provenance,
	}
}

// decide sets a change's status from the target facts the profile has.
//
// A change whose required fact the target lacks is Blocked with the key in the
// diagnostic. That is the whole point of naming the fact: the operator learns
// which measurement is missing rather than that something failed.
func decide(change Change, profile schemastate.Profile, side *schemastate.State) Change {
	missing := profile.MissingFacts(change.RequiredFacts)
	if len(missing) > 0 {
		change.Status = Blocked
		change.MissingFacts = missing
		change.Diagnostic = fmt.Sprintf(
			"%s cannot be planned for %s: the target does not have %s",
			change, profile.Dialect, capabilityList(missing))
		return change
	}
	if reason, ok := unmetPrecondition(change, profile, side); !ok {
		change.Status = Blocked
		change.Diagnostic = reason
		return change
	}
	change.Status = Planned
	return change
}

// unmetPrecondition reports a fact about the SCHEMA, rather than about the
// target, that stops a change being planned.
//
// The referenced-columns-unique rule is the one the slice needs. It is checked
// here rather than in the renderer because a blocked change with a diagnostic
// is what an operator can act on: a renderer that refuses produces an error
// with no change attached, so nothing says which declaration to fix.
func unmetPrecondition(
	change Change,
	profile schemastate.Profile,
	side *schemastate.State,
) (reason string, ok bool) {
	key := change.After
	if key == nil || !profile.UniqueReferenceRequired() {
		return "", true
	}
	referenced, found := side.Get(key.ReferencedTable)
	if !found {
		return fmt.Sprintf("%s references %s, which this schema does not describe", change, key.ReferencedTable), false
	}
	if schemastate.ReferencedColumnsAreUnique(referenced, *key) {
		return "", true
	}
	return fmt.Sprintf(
		"%s cannot be planned for %s: it references %s (%s), which no unique constraint covers, "+
			"and %s refuses a foreign key whose referenced columns are not a key",
		change, profile.Dialect, key.ReferencedTable,
		strings.Join(key.ReferencedColumns, ", "), profile.Dialect), false
}

func capabilityList(facts []capability.Capability) string {
	names := make([]string, 0, len(facts))
	for _, fact := range facts {
		names = append(names, string(fact))
	}
	return strings.Join(names, ", ")
}

// changedProperties reports which properties of two foreign keys differ, in a
// fixed order.
//
// The order is fixed rather than derived from a map walk, because this list
// reaches an operator through a diagnostic and a list that reorders between two
// runs over one input is a list nobody can diff.
func changedProperties(before, after schemastate.ForeignKey) []string {
	changed := make([]string, 0)
	if !slices.Equal(before.Columns, after.Columns) {
		changed = append(changed, "columns")
	}
	if before.ReferencedTable.Key() != after.ReferencedTable.Key() {
		changed = append(changed, "referenced table")
	}
	if !slices.Equal(before.ReferencedColumns, after.ReferencedColumns) {
		changed = append(changed, "referenced columns")
	}
	if before.OnDelete.Normalized != after.OnDelete.Normalized {
		changed = append(changed, "on delete")
	}
	if before.OnUpdate.Normalized != after.OnUpdate.Normalized {
		changed = append(changed, "on update")
	}
	return changed
}
