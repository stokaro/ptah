// Package schemacapture defines captured parent state for contextual schema
// services. Declarations and observations remain distinct. Captures retain
// unknown feature coverage and never establish that a migration executed.
package schemacapture

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// TableDeclaration is everything the declaration says about one table.
//
// Planning and rebuild services consume this capture without consulting a
// mutable source document. It includes common children and feature knowledge;
// an empty child list does not establish that its feature namespace is empty.
// The zero value means no table was captured, not known absence.
type TableDeclaration struct {
	// OwnedObjects captures the complete named child state required by a rebuild.
	OwnedObjects schemaext.Objects
	// FeatureCoverage records whether the captured child state can be reconstructed.
	FeatureCoverage schemaext.Coverage
	// Table is the declared table.
	Table schemamodel.Table
	// Fields are this table's columns, with embedded fields folded in.
	Fields []schemamodel.Field
	// Enums are the declared enum types Fields name.
	Enums []schemamodel.Enum
	// Constraints are the table-level constraints declared on it.
	Constraints []schemamodel.Constraint
	// Indexes are the indexes declared on it.
	Indexes []schemamodel.Index
	// Triggers are the triggers declared on it.
	Triggers []schemamodel.Trigger
}

// HasTable reports whether a declaration was assembled at all.
//
// A modification naming a table the declaration does not hold carries none, and
// a planner that rebuilds has to say so rather than rebuild nothing.
func (d TableDeclaration) HasTable() bool {
	return d.Table.Name != ""
}

// Clone returns an independent declaration, including nested common children.
// OwnedObjects, FeatureCoverage, and facets retain immutable value ownership.
func (d TableDeclaration) Clone() TableDeclaration {
	d.Table = d.Table.Clone()
	d.Fields = cloneValues(d.Fields, schemamodel.Field.Clone)
	d.Enums = cloneValues(d.Enums, schemamodel.Enum.Clone)
	d.Constraints = cloneValues(d.Constraints, schemamodel.Constraint.Clone)
	d.Indexes = cloneValues(d.Indexes, schemamodel.Index.Clone)
	d.Triggers = cloneValues(d.Triggers, schemamodel.Trigger.Clone)
	return d
}

func cloneValues[T any](values []T, clone func(T) T) []T {
	result := slices.Clone(values)
	for i := range result {
		result[i] = clone(result[i])
	}
	return result
}

// TableObservation retains the observed table and its local children. Rebuild
// and reverse services use its feature objects together with their knowledge
// limits; missing children alone never prove a reconstructible empty namespace.
// In a reverse plan, an observation may be the projected result of accepted
// forward changes.
// That planning capture is not evidence that the forward migration executed.
// The zero value means no table was captured, not known absence.
type TableObservation struct {
	Table           catalog.Table
	Indexes         []catalog.Index
	Constraints     []catalog.Constraint
	Triggers        []catalog.Trigger
	OwnedObjects    schemaext.Objects
	FeatureCoverage schemaext.Coverage
}

// HasTable reports whether an observation was captured at comparison time.
func (o TableObservation) HasTable() bool { return o.Table.Name != "" }

// Clone returns an independent capture of the table and its common children.
// OwnedObjects and FeatureCoverage already retain immutable value ownership.
func (o TableObservation) Clone() TableObservation {
	o.Table = o.Table.Clone()
	o.Indexes = slices.Clone(o.Indexes)
	for i := range o.Indexes {
		o.Indexes[i] = o.Indexes[i].Clone()
	}
	o.Constraints = slices.Clone(o.Constraints)
	for i := range o.Constraints {
		o.Constraints[i] = o.Constraints[i].Clone()
	}
	o.Triggers = slices.Clone(o.Triggers)
	return o
}
