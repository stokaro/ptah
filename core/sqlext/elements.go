package sqlext

import (
	"fmt"

	"ptah.run/core/schemaext"
)

// namesObjects is shared by the grammar and value checks: every site that
// names an object can match words after its name and declare the object.
func namesObjects(site Site) bool {
	return site == Statement || site == TableElement || site == AlterOperation
}

func checkElementValue(extension Extension, claim Claim, value Value) error {
	var problem string
	switch {
	case claim.Site == TableElement && (value.Index == nil) == (len(value.Objects) == 0):
		problem = "neither an index nor objects, or both"
	case claim.Site == TableElement && value.Operation != nil:
		problem = "an operation at a table element"
	case claim.Site == AlterOperation && value.Operation == nil:
		problem = "no operation"
	case claim.Site == AlterOperation && value.Index != nil && len(value.Objects) > 0:
		problem = "both an index and objects"
	case (claim.Site == TableElement || claim.Site == AlterOperation) && !value.Facets.IsZero():
		problem = "facets outside their index or object"
	}
	if problem != "" {
		return fmt.Errorf("%w: SQL parser of %s answered its %s claim %q with %s", schemaext.ErrInvalidValue, extension.Owner, claim.Site, claim.Pattern, problem)
	}
	for _, object := range value.Objects {
		if object.Ref.Parent.Empty() {
			return fmt.Errorf("%w: SQL parser of %s answered its %s claim %q with object %s without a parent table", schemaext.ErrInvalidValue, extension.Owner, claim.Site, claim.Pattern, object.Ref)
		}
	}
	return nil
}
