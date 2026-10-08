// Package ydbplan plans YDB-owned feature changes against captured parent state.
// It contributes typed operations to the shared graph and performs no database I/O.
package ydbplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// Service lowers changefeed changes into stream and backing-topic operations.
// All stream drops precede additions on the same table to respect YDB's stream
// limit. Topic mutations follow stream replacement. The zero value is ready for
// concurrent use; target facts and complete operands come from each request.
type Service struct{}

type tableChanges struct {
	featureplan.Table
	TableName      string
	FeatureChanges []schemaext.ChangeRecord
}

// PlanFeatures validates captured stream state before contributing any operations.
// A host-selected table rebuild owns its streams; those changes are validated
// and explicitly accounted for without a second emitter. A failure or canceled
// context discards the entire result, including already planned tables.
func (Service) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != platform.YDB {
		return featureplan.Result{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !request.Identifiers.Equal(identifier.ForDialect(platform.YDB)) {
		return featureplan.Result{}, fmt.Errorf("%w: invalid YDB planning identifier semantics", schemaext.ErrInvalidValue)
	}
	parents, err := planParents(request)
	if err != nil {
		return featureplan.Result{}, err
	}
	tables, err := groupChanges(request)
	if err != nil {
		return featureplan.Result{}, err
	}
	result := featureplan.Result{Complete: true, Parents: parents, Changes: make([]featureplan.ChangePlan, len(request.Changes))}
	planned := make(map[objectidentity.Key]featureplan.ChangePlan)
	for i, table := range tables {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		if err := validateTableChanges(table, request.Capabilities); err != nil {
			return featureplan.Result{}, err
		}
		if table.Action == featureplan.RebuildTable {
			for _, change := range table.FeatureChanges {
				planned[change.Subject.Key()] = featureplan.ChangePlan{Subject: change.Subject, Kind: change.Value.Kind(), Strategy: "restore through the parent rebuild"}
			}
			continue
		}
		contribution, changes, err := changefeedContribution(table.TableName, table.FeatureChanges, i)
		if err != nil {
			return featureplan.Result{}, err
		}
		result.Contributions = append(result.Contributions, contribution)
		for _, change := range changes {
			planned[change.Subject.Key()] = change
		}
	}
	for i, change := range request.Changes {
		result.Changes[i] = planned[change.Subject.Key()]
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func groupChanges(request featureplan.Request) ([]tableChanges, error) {
	byParent := make(map[objectidentity.Key]tableChanges)
	for _, table := range request.Tables {
		if _, exists := byParent[table.Subject.Key()]; exists {
			return nil, refuseFact(table.Subject.String(), "duplicate captured parent")
		}
		byParent[table.Subject.Key()] = tableChanges{Table: table, TableName: table.Desired.Table.QualifiedName()}
	}
	for _, change := range request.Changes {
		parent := objectidentity.ID{Kind: objectidentity.KindTable, Catalog: change.Subject.Catalog, Schema: change.Subject.Schema, Name: change.Subject.Parent}
		table, found := byParent[parent.Key()]
		if !found {
			return nil, refuseFact(change.Subject.String(), "feature changes require a captured parent table")
		}
		table.FeatureChanges = append(table.FeatureChanges, change)
		byParent[parent.Key()] = table
	}
	var tables []tableChanges
	for _, table := range byParent {
		if len(table.FeatureChanges) > 0 {
			tables = append(tables, table)
		}
	}
	slices.SortFunc(tables, func(a, b tableChanges) int { return schemaext.CompareRefs(a.Subject, b.Subject) })
	return tables, nil
}
