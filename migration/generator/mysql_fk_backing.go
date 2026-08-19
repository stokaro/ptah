package generator

import (
	"fmt"
	"slices"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/indexscope"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

type mysqlIndexCandidate struct {
	ref            types.IndexRef
	keyColumns     []string
	positionsKnown bool
	incomplete     bool
	generated      bool
}

type mysqlIndexCoverage uint8

const (
	mysqlIndexDoesNotCover mysqlIndexCoverage = iota
	mysqlIndexCoverageAmbiguous
	mysqlIndexCovers
)

type mysqlForeignKeyState struct {
	ref     types.IndexRef
	columns []string
	added   bool
}

type mysqlForeignKeyIndexSimulation struct {
	dialect           string
	semantics         identifier.Semantics
	indexes           []mysqlIndexCandidate
	foreignKeys       []mysqlForeignKeyState
	reverseRemovals   []types.IndexRef
	reverseRemovalSet map[types.IndexRef]struct{}
	autoRemovalSet    map[types.IndexRef]struct{}
	createdTables     map[string]struct{}
	addedColumns      map[string]map[string]struct{}
}

func addMySQLFamilyForeignKeyBackingIndexRemovals(
	reverseDiff,
	upDiff *types.SchemaDiff,
	current *dbschematypes.DBSchema,
	dialect string,
	forwardNodes []ast.Node,
) error {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
	default:
		return nil
	}

	semantics := upDiff.EffectiveIdentifierSemantics(dialect)
	simulation := mysqlForeignKeyIndexSimulation{
		dialect:           platform.NormalizeDialect(dialect),
		semantics:         semantics,
		indexes:           mysqlCurrentKeyCandidates(current),
		foreignKeys:       mysqlCurrentForeignKeys(current),
		reverseRemovals:   reverseDiff.IndexRemovals(),
		reverseRemovalSet: mysqlIndexIdentitySet(reverseDiff.IndexRemovals(), semantics),
		autoRemovalSet:    make(map[types.IndexRef]struct{}),
		createdTables:     make(map[string]struct{}),
		addedColumns:      make(map[string]map[string]struct{}),
	}
	for _, node := range forwardNodes {
		if err := simulation.applyNode(node); err != nil {
			return err
		}
	}
	reverseDiff.SetIndexRemovals(simulation.reverseRemovals)
	return nil
}

func (s *mysqlForeignKeyIndexSimulation) applyNode(node ast.Node) error {
	switch typed := node.(type) {
	case *ast.CreateTableNode:
		return s.createTable(typed)
	case *ast.IndexNode:
		s.addIndex(mysqlIndexCandidateFromNode(typed))
	case *ast.DropIndexNode:
		return s.removeIndex(types.IndexRef{Name: typed.Name, TableName: typed.Table})
	case *ast.AlterTableNode:
		return s.applyAlterTable(typed)
	case *ast.DropTableNode:
		s.dropTables(typed)
	}
	return nil
}

func (s *mysqlForeignKeyIndexSimulation) createTable(node *ast.CreateTableNode) error {
	s.dropTableState(node.Name)
	s.createdTables[s.tableIdentity(node.Name)] = struct{}{}
	for _, column := range node.Columns {
		s.addColumnKeys(node.Name, column)
	}
	for _, constraint := range node.Constraints {
		if constraint != nil && constraint.Type != ast.ForeignKeyConstraint {
			if err := s.addConstraint(node.Name, constraint); err != nil {
				return err
			}
		}
	}
	for _, constraint := range node.Constraints {
		if constraint != nil && constraint.Type == ast.ForeignKeyConstraint {
			if err := s.addConstraint(node.Name, constraint); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *mysqlForeignKeyIndexSimulation) addColumnKeys(table string, column *ast.ColumnNode) {
	if column == nil {
		return
	}
	if column.Primary {
		s.addIndex(mysqlColumnIndexCandidate(table, "PRIMARY", column.Name))
	}
	if column.Unique {
		s.addIndex(mysqlColumnIndexCandidate(table, column.Name, column.Name))
	}
}

func (s *mysqlForeignKeyIndexSimulation) dropColumn(table, column string) error {
	for position := range s.indexes {
		candidate := &s.indexes[position]
		if s.tableIdentity(candidate.ref.TableName) != s.tableIdentity(table) {
			continue
		}
		candidate.keyColumns = slices.DeleteFunc(candidate.keyColumns, func(keyColumn string) bool {
			return s.sameColumnIdentity(keyColumn, column)
		})
	}
	s.indexes = slices.DeleteFunc(s.indexes, func(candidate mysqlIndexCandidate) bool {
		return s.tableIdentity(candidate.ref.TableName) == s.tableIdentity(table) && len(candidate.keyColumns) == 0
	})
	if columns := s.addedColumns[s.tableIdentity(table)]; columns != nil {
		delete(columns, s.semantics.ColumnIdentityKey(column))
	}
	return s.validateForeignKeyCoverage()
}

func (s *mysqlForeignKeyIndexSimulation) renameColumn(table, oldName, newName string) {
	for position := range s.indexes {
		candidate := &s.indexes[position]
		if s.tableIdentity(candidate.ref.TableName) != s.tableIdentity(table) {
			continue
		}
		for columnPosition, column := range candidate.keyColumns {
			if s.sameColumnIdentity(column, oldName) {
				candidate.keyColumns[columnPosition] = newName
			}
		}
	}
	for position := range s.foreignKeys {
		foreignKey := &s.foreignKeys[position]
		if s.tableIdentity(foreignKey.ref.TableName) != s.tableIdentity(table) {
			continue
		}
		for columnPosition, column := range foreignKey.columns {
			if s.sameColumnIdentity(column, oldName) {
				foreignKey.columns[columnPosition] = newName
			}
		}
	}
	if columns := s.addedColumns[s.tableIdentity(table)]; columns != nil {
		oldIdentity := s.semantics.ColumnIdentityKey(oldName)
		if _, added := columns[oldIdentity]; added {
			delete(columns, oldIdentity)
			columns[s.semantics.ColumnIdentityKey(newName)] = struct{}{}
		}
	}
}

func (s *mysqlForeignKeyIndexSimulation) renameTable(oldName, newName string) {
	oldIdentity := s.tableIdentity(oldName)
	for position := range s.indexes {
		if s.tableIdentity(s.indexes[position].ref.TableName) == oldIdentity {
			s.indexes[position].ref.TableName = newName
		}
	}
	for position := range s.foreignKeys {
		if s.tableIdentity(s.foreignKeys[position].ref.TableName) == oldIdentity {
			s.foreignKeys[position].ref.TableName = newName
		}
	}
	if _, created := s.createdTables[oldIdentity]; created {
		delete(s.createdTables, oldIdentity)
		s.createdTables[s.tableIdentity(newName)] = struct{}{}
	}
	if columns := s.addedColumns[oldIdentity]; columns != nil {
		delete(s.addedColumns, oldIdentity)
		s.addedColumns[s.tableIdentity(newName)] = columns
	}
}

func (s *mysqlForeignKeyIndexSimulation) dropTables(node *ast.DropTableNode) {
	tables := node.Names
	if len(tables) == 0 && node.Name != "" {
		tables = []string{node.Name}
	}
	for _, table := range tables {
		s.dropTableState(table)
		delete(s.createdTables, s.tableIdentity(table))
		delete(s.addedColumns, s.tableIdentity(table))
	}
}

func (s *mysqlForeignKeyIndexSimulation) dropTableState(table string) {
	tableIdentity := s.tableIdentity(table)
	s.indexes = slices.DeleteFunc(s.indexes, func(candidate mysqlIndexCandidate) bool {
		return s.tableIdentity(candidate.ref.TableName) == tableIdentity
	})
	s.foreignKeys = slices.DeleteFunc(s.foreignKeys, func(foreignKey mysqlForeignKeyState) bool {
		return s.tableIdentity(foreignKey.ref.TableName) == tableIdentity
	})
}

func (s *mysqlForeignKeyIndexSimulation) applyAlterTable(node *ast.AlterTableNode) error {
	table := node.Name
	for _, operation := range node.Operations {
		switch typed := operation.(type) {
		case *ast.AddColumnOperation:
			s.markAddedColumn(table, typed.Column)
			s.addColumnKeys(table, typed.Column)
		case *ast.ModifyColumnOperation:
			s.addColumnKeys(table, typed.Column)
		case *ast.DropColumnOperation:
			if err := s.dropColumn(table, typed.ColumnName); err != nil {
				return err
			}
		case *ast.RenameColumnOperation:
			s.renameColumn(table, typed.OldName, typed.NewName)
		case *ast.RenameTableOperation:
			s.renameTable(table, typed.NewName)
			table = typed.NewName
		case *ast.AddConstraintOperation:
			if err := s.addConstraint(table, typed.Constraint); err != nil {
				return err
			}
		case *ast.DropConstraintOperation:
			if err := s.dropConstraint(table, typed); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *mysqlForeignKeyIndexSimulation) addConstraint(
	table string,
	constraint *ast.ConstraintNode,
) error {
	if constraint == nil {
		return nil
	}
	switch constraint.Type {
	case ast.PrimaryKeyConstraint:
		s.addIndex(mysqlConstraintIndexCandidate(table, "PRIMARY", constraint))
	case ast.UniqueConstraint:
		s.addIndex(mysqlConstraintIndexCandidate(table, constraint.Name, constraint))
	case ast.ForeignKeyConstraint:
		return s.addForeignKey(table, constraint)
	}
	return nil
}

func (s *mysqlForeignKeyIndexSimulation) addForeignKey(
	table string,
	constraint *ast.ConstraintNode,
) error {
	foreignKey := mysqlForeignKeyState{
		ref:     types.IndexRef{Name: constraint.Name, TableName: table},
		columns: slices.Clone(constraint.Columns),
		added:   true,
	}
	coverage, ambiguous := mysqlForeignKeyCoverage(s.indexes, foreignKey, s.semantics)
	switch coverage {
	case mysqlIndexCoverageAmbiguous:
		return ambiguousMySQLForeignKeyIndexError(foreignKey, ambiguous, s.dialect)
	case mysqlIndexDoesNotCover:
		if s.hasSameNamedIndex(foreignKey.ref) {
			return fmt.Errorf(
				"cannot add foreign key %q on table %q for dialect %q: "+
					"existing same-named index does not cover the foreign-key columns, "+
					"so the server cannot create the required backing index",
				foreignKey.ref.Name,
				foreignKey.ref.TableName,
				s.dialect,
			)
		}
		s.addGeneratedBackingIndex(foreignKey)
	}
	s.foreignKeys = append(s.foreignKeys, foreignKey)
	return nil
}

func (s *mysqlForeignKeyIndexSimulation) hasSameNamedIndex(ref types.IndexRef) bool {
	return slices.ContainsFunc(s.indexes, func(candidate mysqlIndexCandidate) bool {
		return s.sameIndexIdentity(candidate.ref, ref)
	})
}

func (s *mysqlForeignKeyIndexSimulation) addGeneratedBackingIndex(
	foreignKey mysqlForeignKeyState,
) {
	if foreignKey.added &&
		!s.tableWasCreated(foreignKey.ref.TableName) &&
		!s.allForeignKeyColumnsWereAdded(foreignKey) {
		identity := indexscope.IdentityKeyWithSemantics(s.semantics, foreignKey.ref)
		if _, exists := s.reverseRemovalSet[identity]; !exists {
			s.reverseRemovals = append(s.reverseRemovals, foreignKey.ref)
			s.reverseRemovalSet[identity] = struct{}{}
			s.autoRemovalSet[identity] = struct{}{}
		}
	}
	s.addIndex(mysqlIndexCandidate{
		ref:            foreignKey.ref,
		keyColumns:     slices.Clone(foreignKey.columns),
		positionsKnown: true,
		generated:      true,
	})
}

func (s *mysqlForeignKeyIndexSimulation) dropConstraint(
	table string,
	operation *ast.DropConstraintOperation,
) error {
	switch {
	case operation.ForeignKey:
		s.foreignKeys = slices.DeleteFunc(s.foreignKeys, func(foreignKey mysqlForeignKeyState) bool {
			return s.sameIndexIdentity(
				foreignKey.ref,
				types.IndexRef{Name: operation.ConstraintName, TableName: table},
			)
		})
		return nil
	case operation.PrimaryKey:
		return s.removeIndex(types.IndexRef{Name: "PRIMARY", TableName: table})
	case operation.Unique:
		return s.removeIndex(types.IndexRef{Name: operation.ConstraintName, TableName: table})
	default:
		return nil
	}
}

func (s *mysqlForeignKeyIndexSimulation) addIndex(candidate mysqlIndexCandidate) {
	s.indexes = append(s.indexes, candidate)
	if !candidate.generated {
		s.discardSupersededGeneratedIndexes(candidate)
	}
}

func (s *mysqlForeignKeyIndexSimulation) discardSupersededGeneratedIndexes(candidate mysqlIndexCandidate) {
	for _, foreignKey := range s.foreignKeys {
		if mysqlCandidateForeignKeyCoverage(candidate, foreignKey, s.semantics) != mysqlIndexCovers ||
			s.sameIndexIdentity(candidate.ref, foreignKey.ref) {
			continue
		}
		removed := false
		s.indexes = slices.DeleteFunc(s.indexes, func(index mysqlIndexCandidate) bool {
			matches := index.generated && s.sameIndexIdentity(index.ref, foreignKey.ref)
			removed = removed || matches
			return matches
		})
		if removed {
			s.removeAutoReverseRemoval(foreignKey.ref)
		}
	}
}

func (s *mysqlForeignKeyIndexSimulation) removeAutoReverseRemoval(ref types.IndexRef) {
	identity := indexscope.IdentityKeyWithSemantics(s.semantics, ref)
	if _, auto := s.autoRemovalSet[identity]; !auto {
		return
	}
	delete(s.autoRemovalSet, identity)
	delete(s.reverseRemovalSet, identity)
	s.reverseRemovals = slices.DeleteFunc(s.reverseRemovals, func(removal types.IndexRef) bool {
		return s.sameIndexIdentity(removal, ref)
	})
}

func (s *mysqlForeignKeyIndexSimulation) removeIndex(ref types.IndexRef) error {
	s.indexes = slices.DeleteFunc(s.indexes, func(candidate mysqlIndexCandidate) bool {
		return s.sameIndexIdentity(candidate.ref, ref)
	})
	return s.validateForeignKeyCoverage()
}

func (s *mysqlForeignKeyIndexSimulation) validateForeignKeyCoverage() error {
	for _, foreignKey := range s.foreignKeys {
		coverage, ambiguous := mysqlForeignKeyCoverage(s.indexes, foreignKey, s.semantics)
		switch coverage {
		case mysqlIndexCovers:
			continue
		case mysqlIndexCoverageAmbiguous:
			return ambiguousMySQLForeignKeyIndexError(foreignKey, ambiguous, s.dialect)
		default:
			return fmt.Errorf(
				"cannot add foreign key %q on table %q for dialect %q: every covering index is removed later in the forward plan",
				foreignKey.ref.Name,
				foreignKey.ref.TableName,
				s.dialect,
			)
		}
	}
	return nil
}

func (s *mysqlForeignKeyIndexSimulation) sameIndexIdentity(left, right types.IndexRef) bool {
	return indexscope.IdentityKeyWithSemantics(s.semantics, left) ==
		indexscope.IdentityKeyWithSemantics(s.semantics, right)
}

func (s *mysqlForeignKeyIndexSimulation) tableIdentity(table string) string {
	return s.semantics.QualifiedTableIdentityKey(table)
}

func (s *mysqlForeignKeyIndexSimulation) sameColumnIdentity(left, right string) bool {
	return left != "" && right != "" &&
		s.semantics.ColumnIdentityKey(left) == s.semantics.ColumnIdentityKey(right)
}

func (s *mysqlForeignKeyIndexSimulation) tableWasCreated(table string) bool {
	_, created := s.createdTables[s.tableIdentity(table)]
	return created
}

func (s *mysqlForeignKeyIndexSimulation) markAddedColumn(table string, column *ast.ColumnNode) {
	if column == nil {
		return
	}
	tableIdentity := s.tableIdentity(table)
	columns := s.addedColumns[tableIdentity]
	if columns == nil {
		columns = make(map[string]struct{})
		s.addedColumns[tableIdentity] = columns
	}
	columns[s.semantics.ColumnIdentityKey(column.Name)] = struct{}{}
}

func (s *mysqlForeignKeyIndexSimulation) allForeignKeyColumnsWereAdded(foreignKey mysqlForeignKeyState) bool {
	columns := s.addedColumns[s.tableIdentity(foreignKey.ref.TableName)]
	for _, column := range foreignKey.columns {
		if _, added := columns[s.semantics.ColumnIdentityKey(column)]; !added {
			return false
		}
	}
	return len(foreignKey.columns) > 0
}

func mysqlCurrentKeyCandidates(schema *dbschematypes.DBSchema) []mysqlIndexCandidate {
	if schema == nil {
		return nil
	}
	candidates := make([]mysqlIndexCandidate, 0, len(schema.Indexes)+len(schema.Constraints))
	for _, index := range schema.Indexes {
		columns, positionsKnown := mysqlDatabaseIndexKeyColumns(index)
		candidates = append(candidates, mysqlIndexCandidate{
			ref: types.IndexRef{
				Name:      index.Name,
				TableName: index.QualifiedTableName(),
			},
			keyColumns:     columns,
			positionsKnown: positionsKnown,
			incomplete:     index.KeyPartsIncomplete,
		})
	}
	for _, constraint := range schema.Constraints {
		switch constraint.Type {
		case "PRIMARY KEY":
			candidates = append(candidates, mysqlIndexCandidate{
				ref: types.IndexRef{
					Name:      "PRIMARY",
					TableName: constraint.QualifiedTableName(),
				},
				keyColumns:     slices.Clone(constraint.ColumnNamesOrDefault()),
				positionsKnown: true,
			})
		case "UNIQUE":
			candidates = append(candidates, mysqlIndexCandidate{
				ref: types.IndexRef{
					Name:      constraint.Name,
					TableName: constraint.QualifiedTableName(),
				},
				keyColumns:     slices.Clone(constraint.ColumnNamesOrDefault()),
				positionsKnown: true,
			})
		}
	}
	return candidates
}

func mysqlCurrentForeignKeys(schema *dbschematypes.DBSchema) []mysqlForeignKeyState {
	if schema == nil {
		return nil
	}
	foreignKeys := make([]mysqlForeignKeyState, 0, len(schema.Constraints))
	for _, constraint := range schema.Constraints {
		if constraint.Type != "FOREIGN KEY" {
			continue
		}
		foreignKeys = append(foreignKeys, mysqlForeignKeyState{
			ref: types.IndexRef{
				Name:      constraint.Name,
				TableName: constraint.QualifiedTableName(),
			},
			columns: slices.Clone(constraint.ColumnNamesOrDefault()),
		})
	}
	return foreignKeys
}

func mysqlDatabaseIndexKeyColumns(index dbschematypes.DBIndex) ([]string, bool) {
	if len(index.Parts) == 0 {
		return slices.Clone(index.Columns), !index.KeyPartsIncomplete
	}
	columns := make([]string, len(index.Parts))
	for position, part := range index.Parts {
		if part.Expr == "" {
			columns[position] = part.Name
		}
	}
	return columns, true
}

func mysqlIndexCandidateFromNode(index *ast.IndexNode) mysqlIndexCandidate {
	columns := slices.Clone(index.Columns)
	if len(index.Parts) > 0 {
		columns = make([]string, len(index.Parts))
		for position, part := range index.Parts {
			if part.Expr == "" && part.Prefix == "" {
				columns[position] = part.Name
			}
		}
	}
	return mysqlIndexCandidate{
		ref:            types.IndexRef{Name: index.Name, TableName: index.Table},
		keyColumns:     columns,
		positionsKnown: true,
	}
}

func mysqlConstraintIndexCandidate(
	table,
	name string,
	constraint *ast.ConstraintNode,
) mysqlIndexCandidate {
	columns := slices.Clone(constraint.Columns)
	if len(constraint.ColumnParts) > 0 {
		columns = make([]string, len(constraint.ColumnParts))
		for position, part := range constraint.ColumnParts {
			if part.Prefix == "" {
				columns[position] = part.Name
			}
		}
	}
	return mysqlIndexCandidate{
		ref:            types.IndexRef{Name: name, TableName: table},
		keyColumns:     columns,
		positionsKnown: true,
	}
}

func mysqlColumnIndexCandidate(table, name, column string) mysqlIndexCandidate {
	return mysqlIndexCandidate{
		ref:            types.IndexRef{Name: name, TableName: table},
		keyColumns:     []string{column},
		positionsKnown: true,
	}
}

func mysqlForeignKeyCoverage(
	candidates []mysqlIndexCandidate,
	foreignKey mysqlForeignKeyState,
	semantics identifier.Semantics,
) (mysqlIndexCoverage, types.IndexRef) {
	var ambiguous types.IndexRef
	for _, candidate := range candidates {
		coverage := mysqlCandidateForeignKeyCoverage(candidate, foreignKey, semantics)
		if coverage == mysqlIndexCovers {
			return coverage, types.IndexRef{}
		}
		if coverage == mysqlIndexCoverageAmbiguous && ambiguous.Name == "" {
			ambiguous = candidate.ref
		}
	}
	if ambiguous.Name != "" {
		return mysqlIndexCoverageAmbiguous, ambiguous
	}
	return mysqlIndexDoesNotCover, types.IndexRef{}
}

func mysqlCandidateForeignKeyCoverage(
	candidate mysqlIndexCandidate,
	foreignKey mysqlForeignKeyState,
	semantics identifier.Semantics,
) mysqlIndexCoverage {
	if semantics.QualifiedTableIdentityKey(candidate.ref.TableName) !=
		semantics.QualifiedTableIdentityKey(foreignKey.ref.TableName) ||
		len(foreignKey.columns) == 0 {
		return mysqlIndexDoesNotCover
	}
	if candidate.positionsKnown {
		if mysqlIndexColumnsCoverForeignKey(candidate.keyColumns, foreignKey.columns, semantics) {
			return mysqlIndexCovers
		}
		return mysqlIndexDoesNotCover
	}
	if candidate.incomplete &&
		mysqlIndexColumnsCoverForeignKey(candidate.keyColumns, foreignKey.columns, semantics) {
		return mysqlIndexCoverageAmbiguous
	}
	return mysqlIndexDoesNotCover
}

func mysqlIndexColumnsCoverForeignKey(
	indexColumns,
	foreignKeyColumns []string,
	semantics identifier.Semantics,
) bool {
	if len(foreignKeyColumns) == 0 || len(indexColumns) < len(foreignKeyColumns) {
		return false
	}
	for position, column := range foreignKeyColumns {
		if indexColumns[position] == "" ||
			semantics.ColumnIdentityKey(indexColumns[position]) != semantics.ColumnIdentityKey(column) {
			return false
		}
	}
	return true
}

func mysqlIndexIdentitySet(
	refs []types.IndexRef,
	semantics identifier.Semantics,
) map[types.IndexRef]struct{} {
	set := make(map[types.IndexRef]struct{}, len(refs))
	for _, ref := range refs {
		set[indexscope.IdentityKeyWithSemantics(semantics, ref)] = struct{}{}
	}
	return set
}

func ambiguousMySQLForeignKeyIndexError(
	foreignKey mysqlForeignKeyState,
	index types.IndexRef,
	dialect string,
) error {
	return fmt.Errorf(
		"cannot determine whether incomplete index %q on table %q covers foreign key %q for dialect %q; refusing to plan backing-index cleanup",
		index.Name,
		index.TableName,
		foreignKey.ref.Name,
		platform.NormalizeDialect(dialect),
	)
}
