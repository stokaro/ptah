package goschematodb

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaprojection"
)

// applyCreationObservations puts what each predicted CREATE leaves that its
// declared values do not convert to onto the converted tables and indexes
// (see [schemaprojection.TableCreation.Observed]). It runs after conversion,
// so an observation replaces the converted value of its kind, or adds one
// where the declaration held none, bound to target.
func applyCreationObservations(out *catalog.Database, tables []schemaprojection.TableCreation, semantics identifier.Semantics, target string) error {
	builder := objectidentity.NewBuilder(semantics)
	owners := make(map[objectidentity.Key]*catalog.Table, len(out.Tables))
	for i := range out.Tables {
		owners[builder.TableParts(out.Tables[i].Schema, out.Tables[i].Name).Key()] = &out.Tables[i]
	}
	indexes := make(map[objectidentity.Key]*catalog.Index, len(out.Indexes))
	for i := range out.Indexes {
		indexes[builder.IndexParts(out.Indexes[i].Schema, out.Indexes[i].TableName, out.Indexes[i].Name).Key()] = &out.Indexes[i]
	}
	for _, table := range tables {
		for _, record := range table.Observed {
			key := record.Subject.Key()
			switch {
			case owners[key] != nil:
				if err := applyCreationValues(&owners[key].Facets, record.Values, target); err != nil {
					return err
				}
			case indexes[key] != nil:
				if err := applyCreationValues(&indexes[key].Facets, record.Values, target); err != nil {
					return err
				}
			default:
				return fmt.Errorf("%w: a predicted observation has no converted owner %s", schemaprojection.ErrInvalid, record.Subject)
			}
		}
	}
	return nil
}
