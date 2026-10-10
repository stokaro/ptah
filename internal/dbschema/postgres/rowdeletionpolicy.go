package postgres

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/internal/spannerttl"
)

// rowDeletionPolicyExpr renders the projection carrying a table's row deletion
// policy, and a constant for a target that has none.
//
// It reads a column of information_schema.tables, which this query already
// selects from, so it adds no join. That is not a stylistic preference here:
// PGAdapter refuses a catalog query past twenty joins, and this reader runs
// against it (stokaro/ptah#2236).
func (r *Reader) rowDeletionPolicyExpr() string {
	if !r.readsRowDeletionPolicy() {
		return "'' AS row_deletion_policy"
	}
	return "COALESCE(t.row_deletion_policy_expression, '') AS row_deletion_policy"
}

// readsRowDeletionPolicy reports whether this read describes the Spanner row
// deletion policy. Only then do the tables it returns carry the owned facet
// and its coverage. The capability is Spanner's alone, so the facet and its
// coverage are Spanner's whatever dialect name the caller gave the reader.
func (r *Reader) readsRowDeletionPolicy() bool {
	return r.caps.Has(capability.RowDeletionPolicy)
}

// rowDeletionPolicyFacets decodes the projection above into the observed
// policy, attached as the Spanner owner's facet with the server's spelling of
// the interval. A table without a policy gets no facet; the coverage
// [Reader.rowDeletionPolicyCoverage] records for it is what makes that an
// observed absence.
//
// A policy this cannot read is an error rather than a table reported without
// one. The failure being prevented is silent: a table whose policy stopped
// existing keeps every row it was declared to delete, and that is discovered on
// the storage bill rather than by anything Ptah prints.
func (r *Reader) rowDeletionPolicyFacets(facets schemaext.Facets, expression string) (schemaext.Facets, error) {
	if !r.readsRowDeletionPolicy() {
		return facets, nil
	}
	parsed, found, err := spannerttl.ParseExpression(expression)
	if err != nil || !found {
		return facets, err
	}
	observed := &spannerschema.ObservedRowDeletion{Policy: spannerschema.Policy{Column: parsed.Column, Interval: parsed.Interval}}
	if err := spannerschema.ValidateObserved(observed); err != nil {
		return facets, err
	}
	facets, err = facets.With(observed)
	if err != nil {
		return facets, err
	}
	return facets.WithTargetScope(spannerschema.RowDeletionKind, platform.Spanner)
}

// rowDeletionPolicyCoverage records complete row deletion policy knowledge
// for exactly the tables this read returned. A table the read did not return
// is not known to have no policy.
func (r *Reader) rowDeletionPolicyCoverage(schema *catalog.Database) error {
	if !r.readsRowDeletionPolicy() {
		return nil
	}
	identities := objectidentity.NewBuilder(identifier.ForDialect(platform.Spanner))
	subjects := make([]schemaext.SubjectCoverage, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		subjects = append(subjects, schemaext.SubjectCoverage{
			Kind: spannerschema.RowDeletionKind, Subject: identities.TableParts(table.Schema, table.Name),
			Knowledge: schemaext.Knowledge{State: schemaext.Complete},
		})
	}
	known, err := spannerschema.RowDeletionCoverage(schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables have an inspected Spanner row deletion policy"}, subjects)
	if err != nil {
		return fmt.Errorf("failed to record row deletion policy coverage: %w", err)
	}
	schema.FeatureCoverage, err = schema.FeatureCoverage.Combine(known)
	return err
}
