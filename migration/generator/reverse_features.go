package generator

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/migration/schemadiff/difftypes"
)

// reverseFeatureChanges sends the accepted changes as one contextual batch.
// The host retains each assessment and applies complete owner projections only
// to its captured state; a desired document is not a post-execution snapshot.
func reverseFeatureChanges(ctx context.Context, forward, reverse *difftypes.SchemaDiff, dialect string, caps capability.Capabilities, runtime Runtime) ([]schemaext.Reversal, error) {
	changes := slices.Clone(forward.FeatureChanges)
	for _, table := range forward.TablesModified {
		changes = append(changes, table.FeatureChanges...)
	}
	recovery, err := runtime.ReverseChanges(ctx, schemaext.ReversalRequest{
		Target: dialect, Identifiers: forward.EffectiveIdentifierSemantics(dialect), Capabilities: caps, Changes: changes,
	})
	if err != nil {
		return nil, err
	}
	if len(recovery) != len(changes) {
		return nil, fmt.Errorf("%w: reversal returned an incomplete batch", schemaext.ErrInvalidValue)
	}
	for i := range changes {
		if recovery[i].Change.Subject != changes[i].Subject {
			return nil, fmt.Errorf("%w: reversal changed a captured subject", schemaext.ErrInvalidValue)
		}
	}
	offset := len(forward.FeatureChanges)
	reverse.FeatureChanges = reversedRecords(recovery[:offset])
	semantics := forward.EffectiveIdentifierSemantics(dialect)
	projected := make(map[string]schemacapture.TableObservation, len(forward.TablesModified))
	for i, table := range forward.TablesModified {
		count := len(table.FeatureChanges)
		results := recovery[offset : offset+count]
		reverse.TablesModified[i].FeatureChanges = reversedRecords(results)
		current, err := reverseFeatureTableState(ctx, forward, table, results, dialect, caps, runtime)
		if err != nil {
			return nil, err
		}
		reverse.TablesModified[i].Current = current
		projected[semantics.QualifiedTableIdentityKey(table.TableName)] = current
		offset += count
	}
	for _, observed := range forward.ObservedConstraintHosts {
		if current, found := projected[semantics.QualifiedTableIdentityKey(observed.Table.QualifiedName())]; found {
			// Both captures must include the same accepted column and feature
			// changes, without sharing mutable state between their consumers.
			reverse.ObservedConstraintHosts = append(reverse.ObservedConstraintHosts, current.Clone())
			continue
		}
		// A constraint-only rebuild needs the same projected operand as a
		// table change. A missing projection remains a missing capture and
		// the target planner refuses a rebuild that requires it.
		table := difftypes.TableDiff{TableName: observed.Table.QualifiedName(), Current: observed}
		current, err := reverseFeatureTableState(ctx, forward, table, nil, dialect, caps, runtime)
		if err != nil {
			return nil, err
		}
		reverse.ObservedConstraintHosts = append(reverse.ObservedConstraintHosts, current)
	}
	return recovery, nil
}

func recoveryNotes(recovery []schemaext.Reversal) []ast.Node {
	var notes []ast.Node
	for _, result := range recovery {
		// Quote every external string: even a multiline subject must stay on
		// one SQL comment line on renderers that write CommentNode verbatim.
		notes = append(notes, ast.NewComment("Rollback of "+strconv.Quote(result.Change.Subject.String())+": "+strconv.Quote(result.Strategy)+"."))
		for _, limitation := range result.Limitations {
			notes = append(notes, ast.NewComment("Recovery limit: "+strconv.Quote(limitation)))
		}
	}
	return notes
}

func reversedRecords(recovery []schemaext.Reversal) []schemaext.ChangeRecord {
	var records []schemaext.ChangeRecord
	for _, result := range recovery {
		records = append(records, result.Change)
	}
	return records
}

func reverseFeatureTableState(
	ctx context.Context, forward *difftypes.SchemaDiff, table difftypes.TableDiff,
	recovery []schemaext.Reversal, dialect string, caps capability.Capabilities, runtime Runtime,
) (schemacapture.TableObservation, error) {
	if !table.Current.HasTable() {
		if len(recovery) == 0 {
			return schemacapture.TableObservation{}, nil
		}
		return schemacapture.TableObservation{}, fmt.Errorf("cannot reverse features of %q without a captured table", table.TableName)
	}
	semantics := forward.EffectiveIdentifierSemantics(dialect)
	if pendingTableProjection(forward, table, semantics) {
		if len(recovery) > 0 {
			return schemacapture.TableObservation{}, fmt.Errorf("cannot reverse feature changes of %q without a projected common table state", table.TableName)
		}
		return schemacapture.TableObservation{}, nil
	}
	current := table.Current.Clone()
	columns, err := projectColumns(ctx, table, dialect, semantics, runtime)
	if err != nil {
		return schemacapture.TableObservation{}, err
	}
	current.Table.Columns = columns
	indexes, err := projectIndexes(ctx, forward, table.Current, dialect, semantics, runtime)
	if err != nil {
		return schemacapture.TableObservation{}, err
	}
	current.Indexes = indexes
	constraints, err := projectConstraints(forward, table.Current, semantics)
	if err != nil {
		return schemacapture.TableObservation{}, err
	}
	current.Constraints = constraints
	current, unavailable, err := projectConstraintEffects(ctx, forward, table.Current, current, dialect, caps, runtime)
	if err != nil {
		return schemacapture.TableObservation{}, err
	}
	if unavailable != "" {
		if len(recovery) > 0 {
			return schemacapture.TableObservation{}, fmt.Errorf("cannot reverse feature changes of %q: %s", table.TableName, unavailable)
		}
		return schemacapture.TableObservation{}, nil
	}
	if table.CommentChange != nil {
		current.Table.Comment = table.CommentChange.Desired
	}
	if len(recovery) == 0 {
		return current, nil
	}
	var projections []schemaext.ObjectProjection
	for _, result := range recovery {
		if len(result.ForwardState) != 1 || result.ForwardState[0].Placement != schemaext.ObjectPlacement {
			return schemacapture.TableObservation{}, fmt.Errorf("%w: table reversal requires a named object projection", schemaext.ErrInvalidValue)
		}
		projections = append(projections, schemaext.ObjectProjection{Subject: result.Change.Subject, Value: result.ForwardState[0].Value})
	}
	state, err := runtime.Codecs().ProjectObjects(ctx, schemaext.ObjectState{
		Objects: table.Current.OwnedObjects, Coverage: table.Current.FeatureCoverage,
	}, projections)
	if err != nil {
		return schemacapture.TableObservation{}, err
	}
	current.OwnedObjects, current.FeatureCoverage = state.Objects, state.Coverage
	return current, nil
}
