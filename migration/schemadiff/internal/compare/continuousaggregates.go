package compare

import (
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/objectidentity"
	"ptah.run/migration/schemadiff/difftypes"
)

// ContinuousAggregates compares declared TimescaleDB continuous aggregates
// against the ones the database reports.
//
// The identity is the qualified name, which is also the view name: an aggregate
// occupies the same namespace as a view, and two objects of the two kinds
// cannot share a name.
//
// All three directions are planned, which is what separates this family from
// [Hypertables]. A removal is DROP MATERIALIZED VIEW -- measured on 2.29.2,
// DROP VIEW answers `cannot drop continuous aggregate using DROP VIEW` -- and a
// modification is a drop followed by a create, because there is no CREATE OR
// REPLACE for one: `syntax error at or near "MATERIALIZED"`.
//
// The body is compared only when a resolver put the declaration through the
// server, and bodies carries the answer. TimescaleDB stores a rewritten
// definition, so the declared text and the catalog's text differ for every
// aggregate that has not changed at all; comparing them directly would plan a
// drop and a create on every run, and each one discards the materialized
// history the aggregate exists to keep. Without a resolver the body stays
// uncompared and only the option is (stokaro/ptah#1026).
func ContinuousAggregates(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
	bodies map[string]config.ContinuousAggregateBody,
	semantics identifier.Semantics,
) {
	declared := make(map[objectIdentity]schemamodel.ContinuousAggregate, len(desired.ContinuousAggregates))
	for _, aggregate := range desired.ContinuousAggregates {
		declared[continuousAggregateIdentity(aggregate.Schema, aggregate.Name, semantics)] = aggregate
	}
	live := make(map[objectIdentity]catalog.ContinuousAggregate, len(database.ContinuousAggregates))
	for _, aggregate := range database.ContinuousAggregates {
		live[continuousAggregateIdentity(aggregate.Schema, aggregate.Name, semantics)] = aggregate
	}

	for key, aggregate := range declared {
		reported, exists := live[key]
		if !exists {
			diff.ContinuousAggregatesAdded = append(diff.ContinuousAggregatesAdded, aggregate)
			continue
		}
		if changed := continuousAggregateChange(aggregate, reported, bodies); changed != nil {
			diff.ContinuousAggregatesModified = append(diff.ContinuousAggregatesModified, *changed)
		}
	}

	for key, reported := range live {
		if _, ok := declared[key]; ok {
			continue
		}
		// A description that could not express a continuous aggregate has not
		// asked for one to be dropped. The hypertable underneath it is in the
		// document, so the silence reads as a complete description with one
		// object missing -- and the drop it would plan discards a
		// materialization no rollback rebuilds.
		name := reported.QualifiedName()
		if !cov.PlansRemoval(coverage.ContinuousAggregate, reported.Schema, reported.Name, name) {
			continue
		}
		diff.ContinuousAggregatesRemoved = append(
			diff.ContinuousAggregatesRemoved, continuousAggregateFromCatalog(reported))
	}

	sortContinuousAggregates(diff.ContinuousAggregatesAdded)
	sortContinuousAggregates(diff.ContinuousAggregatesRemoved)
	sort.Slice(diff.ContinuousAggregatesModified, func(i, j int) bool {
		return diff.ContinuousAggregatesModified[i].Name < diff.ContinuousAggregatesModified[j].Name
	})
}

// continuousAggregateChange reports how a declaration differs from the catalog,
// or nil when it does not.
func continuousAggregateChange(
	declared schemamodel.ContinuousAggregate,
	reported catalog.ContinuousAggregate,
	bodies map[string]config.ContinuousAggregateBody,
) *difftypes.ContinuousAggregateDiff {
	sameOption := declaredOptionMatches(declared.MaterializedOnly, reported.MaterializedOnly)
	sameBody, comparable := continuousAggregateBodiesAgree(declared, reported, bodies)
	if sameOption && (!comparable || sameBody) {
		return nil
	}
	return &difftypes.ContinuousAggregateDiff{
		Name:                declared.QualifiedName(),
		OldBody:             reported.Definition,
		NewBody:             declared.Body,
		OldMaterializedOnly: reported.MaterializedOnly,
		NewMaterializedOnly: declaredMaterializedOnly(declared.MaterializedOnly, reported.MaterializedOnly),
		Desired:             declared,
	}
}

// declaredOptionMatches reports whether the declaration asks for what the
// catalog holds.
//
// A declaration that did not write the option always matches. It takes the
// server's own default, and the default is not a constant -- measured on
// 2.29.2, an aggregate created without `timescaledb.materialized_only` is
// reported with it TRUE -- so reading an unwritten option as false would report
// a change on every run for a declaration that asked for whatever the server
// chose. It is the rule an omitted chunk interval takes in [Hypertables].
func declaredOptionMatches(declared *bool, reported bool) bool {
	return declared == nil || *declared == reported
}

// declaredMaterializedOnly is what the declaration asks for, which for one that
// did not choose is what the server already has.
func declaredMaterializedOnly(declared *bool, reported bool) bool {
	if declared == nil {
		return reported
	}
	return *declared
}

// continuousAggregateBodiesAgree reports whether the two bodies say the same
// thing, and whether that question could be answered at all.
//
// It is answerable only for a declaration a server normalized. The second
// return is what keeps an unanswerable question from being answered "changed":
// a caller with no connection compares the option and leaves the body alone.
func continuousAggregateBodiesAgree(
	declared schemamodel.ContinuousAggregate,
	reported catalog.ContinuousAggregate,
	bodies map[string]config.ContinuousAggregateBody,
) (agree, comparable bool) {
	resolved, ok := bodies[declared.QualifiedName()]
	if !ok || !resolved.Resolved {
		return false, false
	}
	return foldContinuousAggregateBody(resolved.Body) ==
		foldContinuousAggregateBody(reported.Definition), true
}

// foldContinuousAggregateBody trims what the catalog adds around a definition
// it returns.
//
// Both sides of the comparison come from the same catalog column, so this folds
// nothing that distinguishes two declarations: the trailing semicolon and the
// surrounding whitespace are the catalog's punctuation, not the author's.
func foldContinuousAggregateBody(body string) string {
	trimmed := strings.TrimSpace(body)
	return strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
}

// continuousAggregateIdentity is what makes two aggregates the same one.
//
// It resolves an empty schema to the connection's default, which is not a
// nicety: a read scoped to one schema reports the aggregate unqualified, and a
// document that names `schema.public` reports it qualified. Comparing the two
// strings reported an addition AND a removal for one unchanged aggregate, and
// the plan created it before dropping it -- leaving the database without the
// object it started with.
func continuousAggregateIdentity(
	schema, name string,
	semantics identifier.Semantics,
) objectIdentity {
	return newObjectIdentity(objectidentity.KindContinuousAggregate, schema, name, semantics)
}

// continuousAggregateFromCatalog carries an aggregate the database reported
// into the shape the diff holds.
//
// MaterializedOnly is the one field that changes kind: the catalog reports a
// bool and the model holds a pointer, where nil means a declaration said
// nothing. A description of what exists always has an answer, so the address of
// the reported value is the faithful carry -- writing nil would say the server
// declined to tell us.
func continuousAggregateFromCatalog(reported catalog.ContinuousAggregate) schemamodel.ContinuousAggregate {
	return schemamodel.ContinuousAggregate{
		Name:             reported.Name,
		Schema:           reported.Schema,
		Body:             reported.Definition,
		MaterializedOnly: &reported.MaterializedOnly,
	}
}

// sortContinuousAggregates orders by the key the name list was sorted on.
func sortContinuousAggregates(aggregates difftypes.ContinuousAggregateChanges) {
	sort.Slice(aggregates, func(i, j int) bool {
		return aggregates[i].QualifiedName() < aggregates[j].QualifiedName()
	})
}
