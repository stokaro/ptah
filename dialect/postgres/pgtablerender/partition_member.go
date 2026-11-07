package pgtablerender

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/sqlident"
)

// CreateTablePartitionOf renders the clause that takes the place of a declared
// partition's column list, with its leading space: PARTITION OF, the parent and
// the bound. An ordinary table returns an empty clause. Invalid declarations and
// targets without the statement fail.
func CreateTablePartitionOf(target string, _ capability.Capabilities, _ string, facets schemaext.Facets) (string, error) {
	value, found, err := schemaext.FacetAs[*pgtable.DesiredPartitionMember](facets, pgtable.PartitionMemberKind)
	if err != nil || !found {
		return "", err
	}
	if err := pgtable.ValidateDesiredPartitionMember(value); err != nil {
		return "", err
	}
	if err := pgtable.RefuseForeignPartitionMember(target); err != nil {
		return "", err
	}
	return " PARTITION OF " + sqlident.Qualified(platform.Postgres, value.ParentSchema, value.Parent) + " " + value.Bound, nil
}

// preparePartitionMember refuses a declared partition on a target without the
// statement. Unlike a partition key it has no omission: a table with no column
// list and no parent is nothing a target can create.
func preparePartitionMember(target string, facets schemaext.Facets) error {
	value, found, err := schemaext.FacetAs[*pgtable.DesiredPartitionMember](facets, pgtable.PartitionMemberKind)
	if err != nil || !found {
		return err
	}
	if err := pgtable.ValidateDesiredPartitionMember(value); err != nil {
		return err
	}
	return pgtable.RefuseForeignPartitionMember(target)
}

// PartitionMoveHandlers returns the handler of a partition move's statements,
// which every target that takes PARTITION OF renders.
func PartitionMoveHandlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{renderer.TypedHandler(&pgtable.MovePartition{}, ast.AlterExtension, validatePartitionMove, renderPartitionMove)}
}

func validatePartitionMove(ctx renderer.ExtensionContext, op *pgtable.MovePartition) error {
	if err := pgtable.ValidateMovePartition(op); err != nil {
		return err
	}
	if op.Step == pgtable.PartitionAttach {
		return pgtable.RefuseForeignPartitionMember(ctx.Target)
	}
	return nil
}

// renderPartitionMove writes one statement of a partition move: the detachment
// from the parent the table held, or the attachment to the declared parent with
// the declared bound. Both run inside a transaction; DETACH CONCURRENTLY, which
// cannot, is not written.
func renderPartitionMove(ctx renderer.ExtensionContext, op *pgtable.MovePartition) ([]string, error) {
	table := sqlident.QuotePostgresQualified(ctx.Parent.Name)
	if op.Step == pgtable.PartitionDetach {
		parent := sqlident.Qualified(platform.Postgres, op.Change.Before.ParentSchema, op.Change.Before.Parent)
		return []string{"ALTER TABLE " + parent + " DETACH PARTITION " + table + ";"}, nil
	}
	parent := sqlident.Qualified(platform.Postgres, op.Change.After.ParentSchema, op.Change.After.Parent)
	return []string{"ALTER TABLE " + parent + " ATTACH PARTITION " + table + " " + op.Change.After.Bound + ";"}, nil
}
