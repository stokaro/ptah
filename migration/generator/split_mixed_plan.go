package generator

import (
	"ptah.run/migration/schemadiff/difftypes"
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
