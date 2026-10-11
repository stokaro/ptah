package goschematodb

import (
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemaprojection"
)

// applyCreationObservations puts what each predicted CREATE leaves that its
// declared values do not convert to onto the converted tables, indexes and
// primary keys (see [schemaprojection.TableCreation.Observed]). It runs after
// conversion, so an observation replaces the converted value of its kind, or
// adds one where the declaration held none, bound to target.
func applyCreationObservations(out *catalog.Database, tables []schemaprojection.TableCreation, semantics identifier.Semantics, target string) error {
	builder := objectidentity.NewBuilder(semantics)
	owners := make(map[objectidentity.Key]*schemaext.Facets, len(out.Tables)+len(out.Indexes))
	for i := range out.Tables {
		owners[builder.TableParts(out.Tables[i].Schema, out.Tables[i].Name).Key()] = &out.Tables[i].Facets
	}
	for i := range out.Indexes {
		owners[builder.IndexParts(out.Indexes[i].Schema, out.Indexes[i].TableName, out.Indexes[i].Name).Key()] = &out.Indexes[i].Facets
	}
	for i := range out.Constraints {
		if constraint := &out.Constraints[i]; strings.EqualFold(constraint.Type, "PRIMARY KEY") {
			owners[builder.PrimaryKeyParts(constraint.Schema, constraint.TableName).Key()] = &constraint.Facets
		}
	}
	for _, table := range tables {
		for _, record := range table.Observed {
			facets := owners[record.Subject.Key()]
			if facets == nil {
				return fmt.Errorf("%w: a predicted observation has no converted owner %s", schemaprojection.ErrInvalid, record.Subject)
			}
			if err := applyCreationValues(facets, record.Values, target); err != nil {
				return err
			}
		}
	}
	return nil
}
