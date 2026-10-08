package generator

import (
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/internal/constraintscope"
	"ptah.run/internal/tableref"
	"ptah.run/migration/schemadiff/difftypes"
)

// projectConstraints applies intrinsic constraint transitions. Key creation and
// removal need the target's backing-index and column-state effects as well;
// the selected constraint service completes those effects before publication.
func projectConstraints(diff *difftypes.SchemaDiff, table schemacapture.TableObservation, semantics identifier.Semantics) ([]catalog.Constraint, error) {
	p := constraintProjection{table: table.Table, semantics: semantics}
	for _, constraint := range table.Constraints {
		if !p.owns(constraint.QualifiedTableName()) {
			return nil, fmt.Errorf("cannot project constraint %q captured under another table", constraint.Name)
		}
		if constraint.Name != "" && p.position(constraint.Name) >= 0 {
			return nil, fmt.Errorf("cannot project duplicate captured constraint %q", constraint.Name)
		}
		p.constraints = append(p.constraints, constraint.Clone())
	}
	if err := p.remove(diff.ConstraintsRemoved); err != nil {
		return nil, err
	}
	if err := p.add(diff.ConstraintsAdded); err != nil {
		return nil, err
	}
	if err := p.validate(diff.ConstraintsValidated); err != nil {
		return nil, err
	}
	if err := p.comments(diff.ConstraintCommentsChanged); err != nil {
		return nil, err
	}
	return p.constraints, nil
}

type constraintProjection struct {
	table       catalog.Table
	semantics   identifier.Semantics
	constraints []catalog.Constraint
}

func (p *constraintProjection) owns(table string) bool {
	return p.semantics.QualifiedTableIdentityKey(table) == p.semantics.QualifiedTableIdentityKey(p.table.QualifiedName())
}

func (p *constraintProjection) position(name string) int {
	key := p.semantics.IndexIdentityKey(name)
	return slices.IndexFunc(p.constraints, func(constraint catalog.Constraint) bool {
		return p.semantics.IndexIdentityKey(constraint.Name) == key
	})
}

func (p *constraintProjection) conflicts(name string) bool {
	key := p.semantics.IndexConflictKey(name)
	return slices.ContainsFunc(p.constraints, func(constraint catalog.Constraint) bool {
		return p.semantics.IndexConflictUnresolved(name) || p.semantics.IndexConflictUnresolved(constraint.Name) ||
			p.semantics.IndexConflictKey(constraint.Name) == key
	})
}

func (p *constraintProjection) checkIdentity(table, name string, identity difftypes.ConstraintIdentity) error {
	if identity != (difftypes.ConstraintIdentity{}) && identity != constraintscope.Identity(p.semantics, table, name) {
		return fmt.Errorf("cannot project conflicting identity of constraint %q on %q", name, table)
	}
	return nil
}

func (p *constraintProjection) remove(changes difftypes.ConstraintRemovals) error {
	for _, change := range changes {
		if !p.owns(change.TableName) {
			continue
		}
		if err := p.checkIdentity(change.TableName, change.Name, change.Identity); err != nil {
			return err
		}
		position := p.position(change.Name)
		if position < 0 || p.constraints[position].Type != change.Type {
			return fmt.Errorf("cannot project removal of missing or conflicting constraint %q", change.Name)
		}
		p.constraints = slices.Delete(p.constraints, position, position+1)
	}
	return nil
}

func (p *constraintProjection) add(changes difftypes.ConstraintAdditions) error {
	for _, change := range changes {
		if !p.owns(change.TableName) {
			continue
		}
		if err := p.checkIdentity(change.TableName, change.Name, change.Identity); err != nil {
			return err
		}
		if change.Name == "" || (change.Type == "CHECK" && change.CheckExpression == "") {
			return fmt.Errorf("cannot project incomplete constraint %q", change.Name)
		}
		if p.conflicts(change.Name) {
			return fmt.Errorf("cannot project conflicting addition of constraint %q", change.Name)
		}
		constraint, err := projectedConstraintDefinition(change, p.table)
		if err != nil {
			return err
		}
		p.constraints = append(p.constraints, constraint)
	}
	return nil
}

func projectedConstraintDefinition(change difftypes.ConstraintAdditionInfo, table catalog.Table) (catalog.Constraint, error) {
	constraint := catalog.Constraint{
		Name: change.Name, TableName: table.Name, Schema: table.Schema, Type: change.Type,
		ColumnNames: slices.Clone(change.Columns), IncludeColumns: slices.Clone(change.IncludeColumns),
		NullsDistinct: cloneBoolPtr(change.NullsDistinct), KeyBlockSize: change.KeyBlockSize,
		NotValid: change.NotValid, NotEnforced: change.NotEnforced, Comment: change.Comment,
		Deferrable: change.Deferrable, Initially: change.Initially, Match: change.Match,
		OnDeleteColumns: slices.Clone(change.OnDeleteColumns),
		CheckClause:     optionalConstraintText(change.CheckExpression), UsingMethod: optionalConstraintText(change.UsingMethod),
		ExcludeElements: optionalConstraintText(change.ExcludeElements), WhereCondition: optionalConstraintText(change.WhereCondition),
	}
	if len(change.Columns) > 0 {
		constraint.ColumnName = change.Columns[0]
	}
	if change.ForeignTable != "" {
		ref, valid := tableref.Parse(change.ForeignTable)
		if !valid {
			return catalog.Constraint{}, fmt.Errorf("cannot project invalid foreign table of constraint %q", change.Name)
		}
		constraint.ForeignTable, constraint.ForeignSchema = new(ref.Name), ref.Schema
		constraint.ForeignColumns = slices.Clone(change.ForeignColumns)
		constraint.ForeignColumn = optionalConstraintText(change.ForeignColumn)
		constraint.DeleteRule, constraint.UpdateRule = optionalConstraintText(change.OnDelete), optionalConstraintText(change.OnUpdate)
	}
	return constraint, nil
}

func optionalConstraintText(value string) *string {
	if value == "" {
		return nil
	}
	return new(value)
}

func (p *constraintProjection) validate(changes []difftypes.ConstraintValidation) error {
	for _, change := range changes {
		if !p.owns(change.TableName) {
			continue
		}
		position := p.position(change.Name)
		if position < 0 {
			return fmt.Errorf("cannot project validation of missing constraint %q", change.Name)
		}
		constraint := &p.constraints[position]
		if constraint.NotEnforced || (constraint.Type != "CHECK" && constraint.Type != "FOREIGN KEY") {
			return fmt.Errorf("cannot project validation of ineligible constraint %q", change.Name)
		}
		constraint.NotValid = false
	}
	return nil
}

func (p *constraintProjection) comments(changes []difftypes.ConstraintCommentChange) error {
	for _, change := range changes {
		if !p.owns(change.TableName) {
			continue
		}
		position := p.position(change.Name)
		if position < 0 {
			return fmt.Errorf("cannot project comment of missing constraint %q", change.Name)
		}
		p.constraints[position].Comment = change.Desired
	}
	return nil
}
