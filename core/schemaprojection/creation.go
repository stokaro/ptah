package schemaprojection

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"ptah.run/core/internal/capturefacets"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
)

// TableCreationInput captures a scoped declaration for offline CREATE prediction.
// It carries no current state: creation rules must not pretend to inspect a table.
type TableCreationInput struct {
	Subject     objectidentity.ID
	Declaration schemacapture.TableDeclaration
}

// TableCreationRequest predicts a batch of newly created tables in input order.
// Source properties must already be decoded by their selected owners. Services
// must not read files, inspect a server, or mutate captured declarations.
type TableCreationRequest struct {
	Target       string
	Identifiers  identifier.Semantics
	Capabilities capability.Capabilities
	Tables       []TableCreationInput
}

// Clone returns independent declarations and target facts.
func (r TableCreationRequest) Clone() TableCreationRequest {
	r.Identifiers = r.Identifiers.Clone()
	r.Capabilities = r.Capabilities.Clone()
	r.Tables = slices.Clone(r.Tables)
	for i := range r.Tables {
		r.Tables[i].Declaration = r.Tables[i].Declaration.Clone()
	}
	return r
}

// TableCreation contains computed effects, separate from authored intent. Facets
// identifies the captured table or index owning each computed value set, using
// the Desired representation for subsequent owner conversion to Observed.
// They may supply undeclared defaults, but cannot restore an excluded kind.
// Values carry no target bindings; the host retains existing bindings and binds
// new kinds to the selected target. Missing kinds retain their source values.
// ColumnPrimaryKeys is authoritative only when ColumnPrimaryKeysPrepared is true;
// an empty prepared list means the table has no primary-key columns.
type TableCreation struct {
	Subject                   objectidentity.ID
	Facets                    []schemaext.FacetRecord
	ColumnPrimaryKeys         []string
	ColumnPrimaryKeysPrepared bool
}

// TableCreationResult explicitly completes the whole batch, including an empty
// request. It establishes a prediction, never execution or catalog inspection.
type TableCreationResult struct {
	Complete bool
	Tables   []TableCreation
}

// Clone returns independent common data and immutable feature snapshots.
func (r TableCreationResult) Clone() TableCreationResult {
	r.Tables = slices.Clone(r.Tables)
	for i := range r.Tables {
		r.Tables[i].Facets = slices.Clone(r.Tables[i].Facets)
		r.Tables[i].ColumnPrimaryKeys = slices.Clone(r.Tables[i].ColumnPrimaryKeys)
	}
	return r
}

// TableCreationService supplies target-owned CREATE effects. It is safe for
// concurrent calls. Errors and cancellation must publish no partial result.
type TableCreationService interface {
	ProjectTableCreations(context.Context, TableCreationRequest) (TableCreationResult, error)
}

// IdentityCreations explicitly selects no additional CREATE effects. Missing
// registration does not imply this policy. Its zero value is usable.
type IdentityCreations struct{}

// ProjectTableCreations completes a batch without overriding any source value.
// Nil context wraps ErrInvalid; cancellation returns the context error.
func (IdentityCreations) ProjectTableCreations(ctx context.Context, request TableCreationRequest) (TableCreationResult, error) {
	if ctx == nil {
		return TableCreationResult{}, fmt.Errorf("%w: creation projection requires a context", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return TableCreationResult{}, err
	}
	result := TableCreationResult{Complete: true, Tables: make([]TableCreation, len(request.Tables))}
	for i, table := range request.Tables {
		result.Tables[i].Subject = table.Subject
	}
	if err := ctx.Err(); err != nil {
		return TableCreationResult{}, err
	}
	return result, nil
}

// AcceptTableCreations checks completeness, identity, exclusions, and column
// membership before returning an independent result. Model codec and owner
// validation belong to the selected runtime. Neither input is mutated. Invalid
// replies wrap ErrInvalid and return no partial result.
func AcceptTableCreations(request TableCreationRequest, result TableCreationResult) (TableCreationResult, error) {
	if !result.Complete || len(result.Tables) != len(request.Tables) {
		return TableCreationResult{}, fmt.Errorf("%w: creation projection did not complete the batch", ErrInvalid)
	}
	for i, table := range request.Tables {
		if err := validateTableCreation(table, result.Tables[i], request.Identifiers); err != nil {
			return TableCreationResult{}, err
		}
	}
	return result.Clone(), nil
}

func validateTableCreation(input TableCreationInput, output TableCreation, semantics identifier.Semantics) error {
	if !reflect.DeepEqual(input.Subject, output.Subject) {
		return fmt.Errorf("%w: creation projection changed table identity", ErrInvalid)
	}
	if err := validateCreationFacets(input, output.Facets, semantics); err != nil {
		return err
	}
	if len(output.ColumnPrimaryKeys) != 0 && !output.ColumnPrimaryKeysPrepared {
		return fmt.Errorf("%w: projected column keys have no completion receipt", ErrInvalid)
	}
	columns := make(map[string]bool)
	for _, field := range input.Declaration.Fields {
		columns[field.Name] = true
	}
	seen := make(map[string]bool)
	for _, name := range output.ColumnPrimaryKeys {
		if name == "" || seen[name] || !columns[name] {
			return fmt.Errorf("%w: creation projection names an unknown or duplicate key column", ErrInvalid)
		}
		seen[name] = true
	}
	return nil
}

func validateCreationFacets(input TableCreationInput, records []schemaext.FacetRecord, semantics identifier.Semantics) error {
	declared, err := capturefacets.Declared(input.Declaration, input.Subject, semantics)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	seen := make(map[objectidentity.Key]bool)
	for _, record := range records {
		original, found := declared[record.Subject.Key()]
		if !found || original.Subject != record.Subject || seen[record.Subject.Key()] || record.Values.IsZero() {
			return fmt.Errorf("%w: unknown, duplicate, or empty computed facet owner %s", ErrInvalid, record.Subject)
		}
		seen[record.Subject.Key()] = true
		for _, kind := range record.Values.DeclaredKinds() {
			if !slices.Contains(record.Values.Kinds(), kind) || len(record.Values.TargetScope(kind)) != 0 ||
				(slices.Contains(original.Values.DeclaredKinds(), kind) && !slices.Contains(original.Values.Kinds(), kind)) {
				return fmt.Errorf("%w: creation projection changed a facet exclusion or binding", ErrInvalid)
			}
		}
	}
	return nil
}
