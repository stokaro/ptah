package generator

import (
	"slices"

	"ptah.run/core/difftypes"
)

// splitEnumValueAdditionDiff moves enum VALUE ADDITIONS into their own diff and
// leaves everything else behind.
//
// Only the additions move. A removal is not a statement PostgreSQL can execute
// at all -- the planner writes a warning comment for it -- so moving it would
// put a comment in a file of its own and take it away from the change it
// documents.
func splitEnumValueAdditionDiff(diff *difftypes.SchemaDiff) splitSchemaDiffs {
	txDiff := cloneSchemaDiff(diff)
	noTxDiff := &difftypes.SchemaDiff{
		IdentifierSemantics: cloneIdentifierSemantics(diff.IdentifierSemantics),
		TablePreparation:    cloneTablePreparation(diff.TablePreparation),
	}

	txEnums := make([]difftypes.EnumDiff, 0, len(diff.EnumsModified))
	noTxEnums := make([]difftypes.EnumDiff, 0, len(diff.EnumsModified))
	for _, enum := range diff.EnumsModified {
		if len(enum.ValuesAdded) > 0 {
			noTxEnums = append(noTxEnums, difftypes.EnumDiff{
				EnumName:    enum.EnumName,
				ValuesAdded: enum.ValuesAdded,
			})
		}
		if len(enum.ValuesRemoved) > 0 {
			txEnums = append(txEnums, difftypes.EnumDiff{
				EnumName:      enum.EnumName,
				ValuesRemoved: enum.ValuesRemoved,
			})
		}
	}
	txDiff.EnumsModified = txEnums
	noTxDiff.EnumsModified = noTxEnums
	return splitSchemaDiffs{transactional: txDiff, noTransaction: noTxDiff}
}

// splitConstraintValidationDiff separates the validations of constraints the
// database holds NOT VALID from the rest of the diff.
//
// A validation depends on nothing else in the plan: the constraint is already
// there, and VALIDATE CONSTRAINT only scans rows. It scans under a weaker lock
// than the one that added the constraint, which is worth having only when it
// commits apart from the other statements, so it moves to a file of its own.
func splitConstraintValidationDiff(diff *difftypes.SchemaDiff) splitSchemaDiffs {
	txDiff := cloneSchemaDiff(diff)
	txDiff.ConstraintsValidated = nil
	noTxDiff := &difftypes.SchemaDiff{
		IdentifierSemantics:  cloneIdentifierSemantics(diff.IdentifierSemantics),
		TablePreparation:     cloneTablePreparation(diff.TablePreparation),
		ConstraintsValidated: slices.Clone(diff.ConstraintsValidated),
	}
	return splitSchemaDiffs{transactional: txDiff, noTransaction: noTxDiff}
}
