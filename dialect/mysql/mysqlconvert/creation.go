package mysqlconvert

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemaprojection"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/mysqlindex"
)

// RowFormatProperty is the table platform property that declares a MySQL or
// MariaDB table's row format, `platform.mysql.row_format`.
const RowFormatProperty = "row_format"

// CreationService predicts what a CREATE TABLE of a declaration leaves on
// MySQL and MariaDB that the declared values do not convert to: whether each
// index keeps a KEY_BLOCK_SIZE hint. That depends on the table's row format,
// which a conversion of one index's value cannot see. MariaDB keeps the hint
// on every table; MySQL only on a table declared ROW_FORMAT=COMPRESSED. An
// index gets the observation where it says more than its absence, as the
// reader gives one (see [mysqlschema.ObservationNeeded]). No declared value is
// changed. Its zero value is ready for concurrent use.
type CreationService struct{}

// ProjectTableCreations returns, for each table in order, the block-size
// observation of each of its indexes. Another target wraps
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
		for _, index := range table.Declaration.Indexes {
			size, _, err := mysqlschema.IndexBlockSize(index.Facets)
			if err != nil {
				return schemaprojection.TableCreationResult{}, fmt.Errorf("index %q: %w", index.Name, err)
			}
			value := mysqlschema.ObservedIndexBlockSize{KeyBlockSize: size, Retained: retained}
			if !mysqlschema.ObservationNeeded(request.Target, value) {
				continue
			}
			observed, err := mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{}, value)
			if err != nil {
				return schemaprojection.TableCreationResult{}, fmt.Errorf("index %q: %w", index.Name, err)
			}
			subject := builder.IndexParts(table.Subject.Schema.Source, table.Subject.Name.Source, index.Name)
			result.Tables[i].Observed = append(result.Tables[i].Observed, schemaext.FacetRecord{Subject: subject, Values: observed})
		}
	}
	return result, ctx.Err()
}
