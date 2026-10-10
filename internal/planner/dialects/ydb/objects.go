package ydb

import (
	"strings"

	"ptah.run/core/platform/capability"
	"ptah.run/migration/schemadiff/difftypes"
)

// objectChange is one family of objects a diff may carry and the refusal a
// YDB plan answers it with.
type objectChange struct {
	present bool
	refusal func() error
}

// refuseObjects refuses every family of objects the YDB planner does not plan:
// the ones YDB has no counterpart for, by their capability key, and the ones a
// later phase implements, by that phase. Views are planned where the target
// has [capability.Views] and refused by the key where it does not.
//
// The named families come first so a refusal says what it refused. The last
// check is a catch-all over [difftypes.SchemaDiff.HasChanges]: with the tables,
// indexes, comments, views, async replications, transfers, feature changes
// and access changes this planner handles taken out,
// a diff that still reports a change carries an unhandled family. Planning
// nothing for it would report the database synced.
func (p *Planner) refuseObjects(diff *difftypes.SchemaDiff) error {
	for _, change := range p.objectChanges(diff) {
		if change.present {
			return change.refusal()
		}
	}
	rest := *diff
	// Selected owners must account for every feature change before the common
	// graph can be scheduled. Unknown kinds fail at that dispatch boundary.
	rest.FeatureChanges = nil
	rest.TablesAdded, rest.TablesRemoved, rest.TablesModified = nil, nil, nil
	rest.IndexesAdded, rest.IndexesRemoved = nil, nil
	rest.IndexesRenamed, rest.IndexPartitioningChanged, rest.IndexCommentsChanged = nil, nil, nil
	rest.ObjectCommentsChanged = withoutPlannedComments(diff)
	rest.ViewsAdded, rest.ViewsRemoved, rest.ViewsModified = nil, nil, nil
	rest.RolesAdded, rest.RolesRemoved, rest.RolesModified = nil, nil, nil
	rest.RoleMembershipsAdded, rest.RoleMembershipsRemoved = nil, nil
	rest.GrantsAdded, rest.GrantsRemoved, rest.GrantOptionsAdded, rest.GrantOptionsRevoked = nil, nil, nil, nil
	rest.DefaultPrivilegesAdded, rest.DefaultPrivilegesRemoved = nil, nil
	rest.DefaultPrivilegeOptionsAdded, rest.DefaultPrivilegeOptionsRevoked = nil, nil
	rest.AsyncReplicationsAdded, rest.AsyncReplicationsRemoved, rest.AsyncReplicationsModified = nil, nil, nil
	rest.TransfersAdded, rest.TransfersRemoved, rest.TransfersModified = nil, nil, nil
	if rest.HasChanges() {
		return refuseFact("the plan", "it changes objects the YDB planner does not plan")
	}
	return nil
}

// objectChanges lists the families in the order a refusal is reported.
func (p *Planner) objectChanges(diff *difftypes.SchemaDiff) []objectChange {
	keyed := func(key capability.Capability, feature, subject string) func() error {
		return func() error { return p.keyed(key, feature, subject) }
	}
	return []objectChange{
		{len(diff.EnumsAdded)+len(diff.EnumsRemoved)+len(diff.EnumsModified) > 0,
			keyed(capability.EnumCustomType, "enum type", "the plan changes an enum type")},
		{len(diff.DomainsAdded)+len(diff.DomainsRemoved)+len(diff.DomainsModified) > 0,
			keyed(capability.DomainTypes, "domain", "the plan changes a domain")},
		{len(diff.CompositeTypesAdded)+len(diff.CompositeTypesRemoved)+len(diff.CompositeTypesModified) > 0,
			keyed(capability.CompositeTypes, "composite type", "the plan changes a composite type")},
		{len(diff.RangesAdded)+len(diff.RangesRemoved)+len(diff.RangesModified) > 0,
			keyed(capability.RangeTypes, "range type", "the plan changes a range type")},
		{len(diff.ExtensionsAdded)+len(diff.ExtensionsRemoved)+len(diff.ExtensionsModified) > 0,
			func() error { return refuseFact("the plan changes an extension", "YDB has no extensions") }},
		{len(diff.FunctionsAdded)+len(diff.FunctionsRemoved)+len(diff.ProceduresRemoved)+len(diff.FunctionsModified) > 0,
			keyed(capability.Functions, "routine", "the plan changes a function or a procedure")},
		{len(diff.SequencesAdded)+len(diff.SequencesRemoved)+len(diff.SequencesModified) > 0,
			keyed(capability.Sequences, "sequence", "the plan changes a sequence")},
		{viewChanges(diff) && !p.caps.Has(capability.Views),
			keyed(capability.Views, "view", "the plan changes a view")},
		{len(diff.MaterializedViewsAdded)+len(diff.MaterializedViewsRemoved)+len(diff.MaterializedViewsModified) > 0,
			keyed(capability.MaterializedViews, "materialized view", "the plan changes a materialized view")},
		{len(diff.TriggersAdded)+len(diff.TriggersRemoved)+len(diff.TriggersModified) > 0,
			keyed(capability.Triggers, "trigger", "the plan changes a trigger")},
		{len(diff.RLSPoliciesAdded)+len(diff.RLSPoliciesRemoved)+len(diff.RLSPoliciesModified)+
			len(diff.RLSEnabledTablesAdded)+len(diff.RLSEnabledTablesRemoved)+len(diff.RLSForceChanged) > 0,
			keyed(capability.RowLevelSecurity, "row-level security", "the plan changes row-level security")},
		{len(diff.SynonymsAdded)+len(diff.SynonymsRemoved)+len(diff.SynonymsModified) > 0,
			func() error { return refuseFact("the plan changes a synonym", "YDB has no synonyms") }},
		{len(diff.ExtendedPropertiesAdded)+len(diff.ExtendedPropertiesRemoved)+len(diff.ExtendedPropertiesModified) > 0,
			func() error {
				return refuseFact("the plan changes an extended property", "extended properties are SQL Server's")
			}},
		{len(diff.IndexVisibilityChanged) > 0,
			keyed(capability.InvisibleIndexes, "invisible index", "the plan changes whether an index is visible")},
		{len(diff.ConstraintsValidated) > 0,
			keyed(capability.AddConstraintNotValid, "constraint validation", "the plan validates a constraint")},
		{len(diff.ConstraintsAdded) > 0,
			func() error {
				return p.constraintRefusal(diff.ConstraintsAdded[0].Type, diff.ConstraintsAdded[0].Name, "adding")
			}},
		{len(diff.ConstraintsRemoved) > 0,
			func() error {
				return p.constraintRefusal(diff.ConstraintsRemoved[0].Type, diff.ConstraintsRemoved[0].Name, "dropping")
			}},
	}
}

// constraintRefusal names the key a constraint change of this type needs. A
// change to the key is a change to the table's identity, which YDB never makes
// in place; the other kinds do not exist.
func (p *Planner) constraintRefusal(constraintType, name, verb string) error {
	subject := verb + " constraint " + name
	switch strings.ToUpper(strings.TrimSpace(constraintType)) {
	case "PRIMARY KEY":
		return p.rebuildable(capability.PrimaryKeyAlterable, "key change", subject+", the primary key")
	case "UNIQUE":
		return p.keyed(capability.UniqueConstraints, "UNIQUE constraint", subject+" (declare a unique index instead)")
	case "CHECK":
		return p.keyed(capability.CheckConstraints, "CHECK constraint", subject)
	case "FOREIGN KEY":
		return p.keyed(capability.ForeignKeys, "foreign key", subject)
	default:
		return refuseFact(subject, "YDB has no "+constraintType+" constraint")
	}
}

func (p *Planner) refuseDeclaredObjectChanges(diff *difftypes.SchemaDiff) error {
	if err := p.refuseComments(diff); err != nil {
		return err
	}
	if err := p.refuseObjects(diff); err != nil {
		return err
	}
	return nil
}
