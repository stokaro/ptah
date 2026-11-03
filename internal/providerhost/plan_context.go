package providerhost

import (
	"fmt"
	"slices"

	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/planext"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// planningContext retains unchanged prepared tables beside the captures each
// change carries. Changed operands are authoritative; preparation supplies the
// other parents an owner may need to interpret references.
func (p commonPlanner) planningContext(diff *difftypes.SchemaDiff) (featureplan.Request, map[objectidentity.Key]string, error) {
	request := featureplan.Request{Target: p.target, Identifiers: diff.EffectiveIdentifierSemantics(p.target),
		Capabilities: p.options.Capabilities, Changes: slices.Clone(diff.FeatureChanges), DatabasePath: diff.CurrentDatabasePath,
		Objects: featureplan.ObjectContext{
			Desired: schemaext.ObjectState{Objects: diff.Features.DesiredObjects, Coverage: diff.Features.DesiredCoverage},
			Current: schemaext.ObjectState{Objects: diff.Features.CurrentObjects, Coverage: diff.Features.CurrentCoverage},
			Context: schemaext.ObjectState{Objects: diff.Features.ContextObjects, Coverage: diff.Features.ContextCoverage},
		},
		Types: featureplan.TypeContext{Desired: schemaext.DefinedTypes(diff.Features.DesiredObjects), Current: schemaext.DefinedTypes(diff.Features.CurrentObjects)}}
	builder := objectidentity.NewBuilder(request.Identifiers)
	policy, err := p.policy(builder)
	if err != nil {
		return featureplan.Request{}, nil, err
	}
	request.Policy = policy
	parents := newPlanningParents(builder, p.assessParents)
	if diff.TablePreparation != nil {
		for _, table := range diff.TablePreparation.Prepared {
			name := tableref.CanonicalExact(table.Desired.Table.Schema, table.Desired.Table.Name)
			if err := parents.add(featureplan.Table{Subject: table.Subject, Desired: table.Desired, Current: table.Current}, name); err != nil {
				return featureplan.Request{}, nil, err
			}
		}
	}
	ownedIndexes := p.ownsIndexes()
	added, err := indexesByTable(builder, diff.IndexesAdded)
	if err != nil {
		return featureplan.Request{}, nil, err
	}
	for _, creation := range diff.TablesAdded {
		declared := schemacapture.TableDeclaration{Table: creation.Table, Fields: creation.Fields, Enums: creation.Enums, Constraints: creation.Constraints,
			OwnedObjects: creation.OwnedObjects, FeatureCoverage: creation.FeatureCoverage}
		subject := builder.TableParts(creation.Table.Schema, creation.Table.Name)
		if ownedIndexes {
			// The diff carries a new table's indexes beside it, not in it. The
			// owner creates them with the table, so the declaration holds them.
			declared.Indexes = added[subject.Key()]
		}
		table := featureplan.Table{Subject: subject, Action: featureplan.CreateTable, Desired: declared}
		if err := parents.add(table, creation.Name); err != nil {
			return featureplan.Request{}, nil, err
		}
	}
	for _, modification := range diff.TablesModified {
		table := featureplan.Table{Subject: builder.TableParts(modification.Desired.Table.Schema, modification.Desired.Table.Name), Action: featureplan.AlterTable,
			Desired: modification.Desired, Current: modification.Current}
		if err := parents.add(table, modification.TableName); err != nil {
			return featureplan.Request{}, nil, err
		}
		request.Changes = append(request.Changes, modification.FeatureChanges...)
	}
	for _, removal := range diff.TablesRemoved {
		observed := removal.Current.Table
		table := featureplan.Table{Subject: builder.TableParts(observed.Schema, observed.Name), Action: featureplan.DropTable, Current: removal.Current}
		if err := parents.add(table, removal.Name); err != nil {
			return featureplan.Request{}, nil, err
		}
	}
	if ownedIndexes {
		if err := parents.alterForIndexes(builder, diff); err != nil {
			return featureplan.Request{}, nil, err
		}
	}
	request.Tables = parents.tables
	return request, parents.names, nil
}

// indexesByTable groups added indexes by the key of the table they belong to.
func indexesByTable(builder objectidentity.Builder, changes difftypes.IndexChanges) (map[objectidentity.Key][]schemamodel.Index, error) {
	result := make(map[objectidentity.Key][]schemamodel.Index)
	for _, change := range changes {
		key, err := tableKey(builder, change.TableName)
		if err != nil {
			return nil, err
		}
		result[key] = append(result[key], change.Index)
	}
	return result, nil
}

func tableKey(builder objectidentity.Builder, name string) (objectidentity.Key, error) {
	ref, valid := tableref.Parse(name)
	if !valid {
		return objectidentity.Key{}, fmt.Errorf("%w: provider planning index names table %q", schemaext.ErrInvalidValue, name)
	}
	return builder.TableParts(ref.Schema, ref.Name).Key(), nil
}

// policy is the operator's planning policy as the provider's planner receives
// it. The words of a rebuild request are set whether or not the operator
// asked, because a refusal is what an operator who did not ask reads.
func (p commonPlanner) policy(builder objectidentity.Builder) (featureplan.Policy, error) {
	var statements featureplan.StatementStyle
	switch p.options.Statements {
	case planext.StatementStyleAnnotated:
		statements = featureplan.StatementStyleAnnotated
	case planext.StatementStyleExact:
		statements = featureplan.StatementStyleExact
	default:
		return featureplan.Policy{}, fmt.Errorf("%w: unknown statement style %d", schemaext.ErrInvalidValue, p.options.Statements)
	}
	builds, err := policyIndexes(builder, p.options.ConcurrentIndexRefs)
	if err != nil {
		return featureplan.Policy{}, err
	}
	drops, err := policyIndexes(builder, p.options.ConcurrentIndexDropRefs)
	if err != nil {
		return featureplan.Policy{}, err
	}
	return featureplan.Policy{
		Statements:                   statements,
		AllowTableRebuild:            p.options.AllowTableRebuild,
		TableRebuildRequest:          p.options.TableRebuildWords(),
		OnlineAlter:                  p.options.OnlineAlter,
		ConcurrentIndexBuilds:        p.options.ConcurrentIndexes,
		ConcurrentIndexBuildSubjects: builds,
		ConcurrentIndexDrops:         p.options.ConcurrentIndexDrops,
		ConcurrentIndexDropSubjects:  drops,
		OmitNullBackfill:             p.options.OmitNullBackfill,
	}, nil
}

// policyIndexes names each index once, under the plan's identifier rules.
func policyIndexes(builder objectidentity.Builder, refs []difftypes.IndexRef) ([]objectidentity.ID, error) {
	var result []objectidentity.ID
	seen := make(map[objectidentity.Key]bool, len(refs))
	for _, ref := range refs {
		table, valid := tableref.Parse(ref.TableName)
		if !valid || ref.Name == "" {
			return nil, fmt.Errorf("%w: concurrent index %q of table %q", schemaext.ErrInvalidValue, ref.Name, ref.TableName)
		}
		index := builder.IndexParts(table.Schema, table.Name, ref.Name)
		if !seen[index.Key()] {
			seen[index.Key()] = true
			result = append(result, index)
		}
	}
	return result, nil
}
