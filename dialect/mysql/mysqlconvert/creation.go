package mysqlconvert

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/mysqlindex"
)

// RowFormatProperty is the table platform property that declares a MySQL or
// MariaDB table's row format, `platform.mysql.row_format`.
const RowFormatProperty = "row_format"

// CreationService predicts what a CREATE TABLE of a declaration leaves on
// MySQL and MariaDB that the declared values do not convert to: whether each
// index and the primary key keep a KEY_BLOCK_SIZE hint. That depends on the
// table's row format, which a conversion of one index's value cannot see.
// MariaDB keeps the hint on every table; MySQL only on a table declared
// ROW_FORMAT=COMPRESSED. An index or a key gets the observation where it says
// more than its absence, as the reader gives one (see
// [mysqlschema.ObservationNeeded]). A key gets one where the declaration
// converts it to a PRIMARY KEY constraint: one it declares as a constraint,
// or in the table's primary key attribute. No declared value is changed. Its
// zero value is ready for concurrent use.
type CreationService struct{}

// ProjectTableCreations returns, for each table in order, the block-size
// observation of each of its indexes and of its primary key. Another target wraps
// ptaherr.ErrUnsupportedDialect; an invalid declared hint wraps
// schemaext.ErrInvalidValue. Errors and cancellation return no partial result.
func (CreationService) ProjectTableCreations(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	if ctx == nil {
		return schemaprojection.TableCreationResult{}, fmt.Errorf("%w: creation projection requires a context", schemaprojection.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return schemaprojection.TableCreationResult{}, err
	}
	if !slices.Contains(mysqlschema.Targets(), request.Target) {
		return schemaprojection.TableCreationResult{}, fmt.Errorf("%w: MySQL creation projection on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	builder := objectidentity.NewBuilder(request.Identifiers)
	result := schemaprojection.TableCreationResult{Complete: true, Tables: make([]schemaprojection.TableCreation, len(request.Tables))}
	for i, table := range request.Tables {
		if err := ctx.Err(); err != nil {
			return schemaprojection.TableCreationResult{}, err
		}
		result.Tables[i].Subject = table.Subject
		retained := mysqlindex.KeepsBlockSize(request.Target, table.Declaration.Table.Overrides[request.Target][RowFormatProperty])
		holders := make([]holder, 0, len(table.Declaration.Indexes)+1)
		for _, index := range table.Declaration.Indexes {
			holders = append(holders, holder{builder.IndexParts(table.Subject.Schema.Source, table.Subject.Name.Source, index.Name),
				index.Facets, "index " + strconv.Quote(index.Name)})
		}
		if key, found := declaredKey(table.Declaration); found {
			holders = append(holders, holder{objectidentity.PrimaryKeyOf(table.Subject), key, "the primary key"})
		}
		for _, holder := range holders {
			record, needed, err := observation(request.Target, holder.facets, retained)
			if err != nil {
				return schemaprojection.TableCreationResult{}, fmt.Errorf("%s: %w", holder.label, err)
			}
			if needed {
				record.Subject = holder.subject
				result.Tables[i].Observed = append(result.Tables[i].Observed, record)
			}
		}
	}
	return result, ctx.Err()
}

// holder is an index or a primary key whose hint is predicted.
type holder struct {
	subject objectidentity.ID
	facets  schemaext.Facets
	label   string
}

// declaredKey returns the settings of the primary key a declaration converts
// to a PRIMARY KEY constraint, where it converts one.
func declaredKey(declaration schemacapture.TableDeclaration) (schemaext.Facets, bool) {
	owned := make([]*schemamodel.Constraint, 0, len(declaration.Constraints))
	for i := range declaration.Constraints {
		owned = append(owned, &declaration.Constraints[i])
	}
	facets := schemacapture.PrimaryKeyFacets(&declaration.Table, owned)
	// The table's own facets are not the ones read where the declaration
	// holds a PRIMARY KEY constraint.
	constraint := facets != &declaration.Table.PrimaryKeyFacets
	return *facets, constraint || len(declaration.Table.PrimaryKey) > 0
}

// observation is what a read reports for an index or a key created with
// facets on a table that retains a hint or not, and whether it says more than
// its absence.
func observation(target string, facets schemaext.Facets, retained bool) (schemaext.FacetRecord, bool, error) {
	size, _, err := mysqlschema.IndexBlockSize(facets)
	if err != nil {
		return schemaext.FacetRecord{}, false, err
	}
	value := mysqlschema.ObservedIndexBlockSize{KeyBlockSize: size, Retained: retained}
	if !mysqlschema.ObservationNeeded(target, value) {
		return schemaext.FacetRecord{}, false, nil
	}
	observed, err := mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{}, value)
	return schemaext.FacetRecord{Values: observed}, err == nil, err
}
