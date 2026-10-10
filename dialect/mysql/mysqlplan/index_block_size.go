package mysqlplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/mysql/mysqlast"
	"ptah.run/dialect/mysql/mysqldiff"
	"ptah.run/dialect/mysql/mysqlschema"
)

// IndexBlockSizeService plans a change to the KEY_BLOCK_SIZE hint of an
// index that survives, and accounts for the hints of every other index
// through the common table operations. No statement changes the hint of an
// index that exists, so a change replaces the index: one ALTER TABLE drops it
// and adds it again with its declared definition. On MySQL the statement asks
// for ALGORITHM=COPY, since an in-place rebuild of an otherwise unchanged key
// keeps the old hint; MariaDB stores the new one in place.
//
// When the common plan already writes the same index, which on MySQL and
// MariaDB is the one-statement replacement of a definition that changed for
// another reason, that replacement is written with the declared hint and this
// service contributes nothing: measured on MySQL 8.4.11, an in-place
// replacement that also changes the key, the comment or the uniqueness stores
// the new hint. Its zero value is ready for concurrent use.
type IndexBlockSizeService struct{}

// PlanFeatures returns complete receipts or a completed refusal with no
// usable prefix. Errors describe invalid requests or cancellation.
func (IndexBlockSizeService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != platform.MySQL && request.Target != platform.MariaDB {
		return featureplan.Result{}, fmt.Errorf("%w: MySQL index block size planning on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{mysqlschema.IndexBlockSizeKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: unsupported MySQL index block size parent kinds", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	for i, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		contribution, plan, err := planBlockSize(request, record, i)
		if err != nil {
			return blockSizeRefusal(mysqldiff.IndexBlockSizeKind, err, new(i), nil), nil
		}
		if len(contribution.Steps) > 0 {
			result.Contributions = append(result.Contributions, contribution)
		}
		result.Changes = append(result.Changes, plan)
	}
	for i, table := range request.Tables {
		if table.Action == "" || len(request.ParentKinds) == 0 {
			continue
		}
		strategy, err := blockSizeStrategy(table.Action)
		if err != nil {
			return blockSizeRefusal(mysqlschema.IndexBlockSizeKind, err, nil, new(i)), nil
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: mysqlschema.IndexBlockSizeKind, Action: table.Action, Strategy: strategy})
	}
	return result, ctx.Err()
}

func blockSizeStrategy(action featureplan.ParentAction) (string, error) {
	switch action {
	case featureplan.CreateTable:
		return "CREATE TABLE writes each index's declared block size", nil
	case featureplan.AlterTable:
		return "an index keeps the block size it holds unless a planned change, or the common plan's replacement of the index, writes the declared one", nil
	case featureplan.DropTable:
		return "remove the indexes' block sizes with the table", nil
	case featureplan.RebuildTable:
		return "the rebuilt table's indexes are created with their declared block sizes", nil
	default:
		return "", fmt.Errorf("MySQL index block sizes have no plan for parent action %q", action)
	}
}

func planBlockSize(request featureplan.Request, record schemaext.ChangeRecord, position int) (plangraph.Contribution[featureplan.Operation], featureplan.ChangePlan, error) {
	var contribution plangraph.Contribution[featureplan.Operation]
	plan := featureplan.ChangePlan{Subject: record.Subject, Kind: mysqldiff.IndexBlockSizeKind}
	change, ok := record.Value.(*mysqldiff.IndexBlockSize)
	if !ok || record.Subject.Kind != objectidentity.KindIndex || record.Subject.Name.Empty() || record.Subject.Parent.Empty() {
		return contribution, plan, fmt.Errorf("%w: expected a MySQL block size change on a table's index", schemaext.ErrInvalidValue)
	}
	if err := mysqldiff.ValidateIndexBlockSize(change); err != nil {
		return contribution, plan, err
	}
	table, desired, err := captureIndex(request, record.Subject, change)
	if err != nil {
		return contribution, plan, err
	}
	switch replaced, err := commonWrites(request, record.Subject); {
	case err != nil:
		return contribution, plan, err
	case replaced:
		plan.Strategy = "the common plan replaces the index, and writes it with the declared block size"
		return contribution, plan, nil
	}
	definition, err := mysqlast.IndexFromModel(desired)
	if err != nil {
		return contribution, plan, err
	}
	replace := &mysqlast.ReplaceIndex{Index: definition, TableCopy: request.Target == platform.MySQL}
	if err := replace.Validate(); err != nil {
		return contribution, plan, err
	}
	contribution.Owner = mysqlschema.Owner
	id := plangraph.StepID{Owner: contribution.Owner, Name: fmt.Sprintf("index-block-size/%d", position)}
	contribution.Steps = []plangraph.Step[featureplan.Operation]{{
		ID:      id,
		Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: table.Subject, Payload: replace},
		Effects: []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: record.Subject, Action: plangraph.Alter}},
		// A MySQL-family DDL statement commits whatever transaction it
		// runs in, as the common statements around it do.
		Transaction: plangraph.TransactionAllowed, Impact: replace.Effect(),
	}}
	plan.Strategy = "drop and add the index again in one statement with its declared definition and block size"
	if replace.TableCopy {
		plan.Strategy += ", with ALGORITHM=COPY, which rebuilds the table"
	}
	plan.Steps = []plangraph.StepID{id}
	return contribution, plan, nil
}

// captureIndex finds the index on the captured sides of its table and
// requires the change to agree with them, so a replacement cannot write a
// definition the comparison did not decide. It returns the table and the
// declared index.
//
// The declared side is required: it is the definition the replacement
// writes. The current side is checked where it is captured. A rollback's
// generator leaves it out where it cannot project the table the forward plan
// leaves, beside a key constraint change on MySQL, and the hint it would hold
// there is the one the forward change declared, which is what the reversed
// change states.
func captureIndex(request featureplan.Request, subject objectidentity.ID, change *mysqldiff.IndexBlockSize) (featureplan.Table, schemamodel.Index, error) {
	position := slices.IndexFunc(request.Tables, func(table featureplan.Table) bool { return indexOf(subject, table.Subject) })
	if position < 0 {
		return featureplan.Table{}, schemamodel.Index{}, fmt.Errorf("%w: an index block size change requires captured parent state", schemaext.ErrInvalidValue)
	}
	table := request.Tables[position]
	if table.Action != "" && table.Action != featureplan.AlterTable {
		return featureplan.Table{}, schemamodel.Index{}, fmt.Errorf("%w: an index block size change requires a surviving table outside a rebuild", schemaext.ErrInvalidValue)
	}
	if !table.Desired.HasTable() {
		return featureplan.Table{}, schemamodel.Index{}, fmt.Errorf("%w: an index block size change requires the captured declaration of its table", schemaext.ErrInvalidValue)
	}
	builder := objectidentity.NewBuilder(request.Identifiers)
	desired := slices.IndexFunc(table.Desired.Indexes, func(index schemamodel.Index) bool {
		return builder.IndexParts(table.Subject.Schema.Source, table.Subject.Name.Source, index.Name).Key() == subject.Key()
	})
	if desired < 0 {
		return featureplan.Table{}, schemamodel.Index{}, fmt.Errorf("%w: index %s is not declared in its captured table", schemaext.ErrInvalidValue, subject)
	}
	declared, _, err := mysqlschema.IndexBlockSize(table.Desired.Indexes[desired].Facets)
	if err != nil {
		return featureplan.Table{}, schemamodel.Index{}, err
	}
	if declared != change.After.KeyBlockSize {
		return featureplan.Table{}, schemamodel.Index{}, fmt.Errorf("%w: the block size change of index %s disagrees with its declaration", schemaext.ErrInvalidValue, subject)
	}
	if !table.Current.HasTable() {
		return table, table.Desired.Indexes[desired], nil
	}
	current := slices.IndexFunc(table.Current.Indexes, func(index catalog.Index) bool {
		return builder.IndexParts(index.Schema, index.TableName, index.Name).Key() == subject.Key()
	})
	if current < 0 {
		return featureplan.Table{}, schemamodel.Index{}, fmt.Errorf("%w: index %s is not captured in its current table", schemaext.ErrInvalidValue, subject)
	}
	held, _, err := mysqlschema.IndexBlockSize(table.Current.Indexes[current].Facets)
	if err != nil {
		return featureplan.Table{}, schemamodel.Index{}, err
	}
	if held != change.Before.KeyBlockSize {
		return featureplan.Table{}, schemamodel.Index{}, fmt.Errorf("%w: the block size change of index %s disagrees with the hint it holds", schemaext.ErrInvalidValue, subject)
	}
	return table, table.Desired.Indexes[desired], nil
}

// commonWrites reports whether a common step writes the index. On MySQL and
// MariaDB the host writes an index that survives only as the one-statement
// replacement of a definition that changed, which it records as an alteration
// of the index, and with it the declared hint; a visibility change in place
// writes no index effect. A common step that only drops or only creates the
// index contradicts the comparison that found the index on both sides.
func commonWrites(request featureplan.Request, subject objectidentity.ID) (bool, error) {
	var dropped, created, altered bool
	for _, step := range request.CommonSteps {
		for _, effect := range step.Effects {
			if effect.Subject.Key() != subject.Key() {
				continue
			}
			switch effect.Action {
			case plangraph.Drop:
				dropped = true
			case plangraph.Create:
				created = true
			case plangraph.Alter:
				altered = true
			}
		}
	}
	if dropped != created {
		return false, fmt.Errorf("%w: the common plan removes or adds %s, whose block size changes", schemaext.ErrInvalidValue, subject)
	}
	return altered || dropped, nil
}

// indexOf reports whether index names an index of table.
func indexOf(index, table objectidentity.ID) bool {
	return table.Kind == objectidentity.KindTable && index.Catalog.Normalized == table.Catalog.Normalized &&
		index.Schema.Normalized == table.Schema.Normalized && index.Parent.Normalized == table.Name.Normalized
}

func blockSizeRefusal(kind schemaext.Kind, err error, change, parent *int) featureplan.Result {
	return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{
		Problem: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(kind),
			Feature: "MySQL index block size planning", Message: err.Error()},
		Change: change, Parent: parent,
	}}}
}
