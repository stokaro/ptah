package sqlschema

import (
	"slices"
	"strings"

	"ptah.run/core/schemamodel"
)

// createdKeyIndexes is what the naming pass of one CREATE TABLE learned about
// the indexes the server builds for the table's foreign keys.
type createdKeyIndexes struct {
	// built are the indexes the server builds, one per key that gets one.
	built []builtKeyIndex
	// dropped are the positions in Database.Indexes of the indexes a MySQL
	// `FOREIGN KEY name (columns)` clause names that the server does not
	// build, because another index covers the key.
	dropped []int
}

// builtKeyIndex is one index the server builds for a foreign key of a CREATE
// TABLE body. Exactly one of field and constraint is set; the other is absent.
type builtKeyIndex struct {
	// field is the position in Database.Fields of the column whose REFERENCES
	// declares the key.
	field int
	// constraint is the position in Database.Constraints of a table-level key.
	constraint int
	name       string
	columns    []string
	// declared is whether the model declares the index: the one a MySQL
	// FOREIGN KEY clause names.
	declared bool
}

// keyCandidate is one foreign key of a CREATE TABLE body, as the index the
// server builds for it where nothing covers the key.
type keyCandidate struct {
	// field and constraint locate the key as builtKeyIndex does.
	field      int
	constraint int
	// clause is the position in Database.Indexes of the index a MySQL
	// FOREIGN KEY clause names for the key, and noPosition without one.
	clause  int
	columns []string
	// order is the key's place among the table's keys, in the order the body
	// writes them.
	order int
}

// createKeys decides which foreign keys of one CREATE TABLE body the server
// builds an index for, and claims each index's name at its key's place.
//
// A key gets an index of its own unless another index of the table begins
// with its columns: one the body declares, the index of a key over more
// columns, or the index of an identical key written later. Measured on MySQL
// 8.4.11 and 26.7.0 and MariaDB 11.8.9 and 12.3.3, with `FK (a)` standing for
// `FOREIGN KEY (a) REFERENCES p(id)`:
//
//	body                                            indexes of c
//	FK (a), FK (a, b)                               a (a, b)
//	FK (a, b), FK (a)                               a (a, b)
//	CONSTRAINT fk FK (a), CONSTRAINT fk2 FK (a)     fk2 (a)
//	KEY k (a), FOREIGN KEY idx (a) ...              k (a), on MySQL
//	FOREIGN KEY idx (a) ..., KEY k (a, b)           k (a, b), on MySQL
//	FOREIGN KEY idx (a) ..., FK (a, b)              a (a, b), on MySQL
//
// So the index a MySQL FOREIGN KEY clause names is the key's own index under
// that name, and it is not built where the key's index would not be. See
// [keyIndex] for what happens to these indexes after the CREATE TABLE.
//
// A column's key is claimed at the column's place in the body, as a
// column-level UNIQUE is; see [bodyItems]. Measured on MariaDB 11.8.9 and MySQL
// 26.7.0, which build a key from the clause, `a INT REFERENCES p(id), KEY a
// (id)` is `ERROR 1061 Duplicate key name 'a'`, and `b INT, KEY a (b), a INT
// REFERENCES p(id)` names the key's index a_2. On MariaDB a key the column
// names, `a INT CONSTRAINT fkx REFERENCES p(id)`, builds its index under that
// name. The column's place decides which of two identical keys is later, too:
// `CONSTRAINT fk FOREIGN KEY (a) ..., a INT REFERENCES p(id)` builds the
// column's key's index, a, where the other order builds fk.
type createKeys struct {
	database   *schemamodel.Database
	naming     engineIndexNaming
	candidates []keyCandidate
	declared   coverage
	created    createdKeyIndexes
}

// newCreateKeys lists the foreign keys of the table whose fields start at
// fieldsStart and whose body elements are order. declared is every access path
// the body declares; see [coversOf].
func newCreateKeys(
	database *schemamodel.Database, fieldsStart int, order []namedElement,
	declared coverage, naming engineIndexNaming,
) *createKeys {
	keys := &createKeys{database: database, naming: naming, declared: declared}
	var clauses []int
	for _, item := range bodyItems(order, len(database.Fields)-fieldsStart) {
		if item.field != noPosition {
			field := database.Fields[fieldsStart+item.field]
			if field.Foreign == "" {
				continue
			}
			keys.candidates = append(keys.candidates, keyCandidate{
				field: fieldsStart + item.field, constraint: noPosition, clause: noPosition,
				columns: []string{field.Name}, order: len(keys.candidates),
			})
			continue
		}
		element := order[item.element]
		if element.keyIndex {
			clauses = append(clauses, element.index)
			continue
		}
		if element.isIndex() || !isForeignKey(database.Constraints[element.constraint]) {
			continue
		}
		constraint := database.Constraints[element.constraint]
		keys.candidates = append(keys.candidates, keyCandidate{
			field: noPosition, constraint: element.constraint,
			clause:  takeClause(database, &clauses, constraint),
			columns: constraint.Columns, order: len(keys.candidates),
		})
	}
	return keys
}

// takeClause answers the index a FOREIGN KEY clause names for constraint, and
// removes it from clauses: the first one over the key's columns, since the
// parser records the clause's index beside its key. A key written with a
// symbol has none; the server ignores the clause's name then.
func takeClause(database *schemamodel.Database, clauses *[]int, constraint schemamodel.Constraint) int {
	if constraint.Name != "" {
		return noPosition
	}
	for i, position := range *clauses {
		if slices.EqualFunc(database.Indexes[position].Fields, constraint.Columns, strings.EqualFold) {
			*clauses = slices.Delete(*clauses, i, i+1)
			return position
		}
	}
	return noPosition
}

// builds reports whether the server builds an index for candidate.
func (k *createKeys) builds(candidate keyCandidate) bool {
	if len(candidate.columns) == 0 || k.declared.covers(candidate.columns, k.naming) {
		return false
	}
	for _, other := range k.candidates {
		if other.order == candidate.order || !(coverage{ascending(other.columns)}).covers(candidate.columns, k.naming) {
			continue
		}
		if len(other.columns) > len(candidate.columns) || other.order > candidate.order {
			return false
		}
	}
	return true
}

// claimColumnKey claims the index of the key the column at field declares, if
// the server builds one.
func (k *createKeys) claimColumnKey(claimed indexNames, field int, table schemamodel.Table) error {
	for _, candidate := range k.candidates {
		if candidate.field != field {
			continue
		}
		return k.claim(claimed, candidate, k.database.Fields[field].ForeignKeyName, table)
	}
	return nil
}

// claimTableKey claims the index of the table-level key at constraint, if the
// server builds one, and drops the index its FOREIGN KEY clause names if the
// server does not.
func (k *createKeys) claimTableKey(claimed indexNames, constraint int, table schemamodel.Table) error {
	for _, candidate := range k.candidates {
		if candidate.constraint != constraint {
			continue
		}
		return k.claim(claimed, candidate, k.database.Constraints[constraint].Name, table)
	}
	return nil
}

// claim claims candidate's index under the key's own name, the name its clause
// gives, or its first column's, and records it.
func (k *createKeys) claim(claimed indexNames, candidate keyCandidate, keyName string, table schemamodel.Table) error {
	if !k.builds(candidate) {
		if candidate.clause != noPosition {
			k.created.dropped = append(k.created.dropped, candidate.clause)
		}
		return nil
	}
	built := builtKeyIndex{field: candidate.field, constraint: candidate.constraint, columns: candidate.columns}
	switch {
	case keyName != "":
		built.name = keyName
	case candidate.clause != noPosition:
		built.name, built.declared = k.database.Indexes[candidate.clause].Name, true
	default:
		// The key's own index takes the column's name, which no model object
		// carries; claiming it is what gives the next unnamed index the
		// server's name.
		name, err := derive(claimed, candidate.columns[0], table, k.naming)
		if err != nil {
			return err
		}
		built.name = name
		k.created.built = append(k.created.built, built)
		return nil
	}
	if err := claimExplicit(claimed, built.name, table); err != nil {
		return err
	}
	k.created.built = append(k.created.built, built)
	return nil
}

// recordCreatedKeyIndexes adds the indexes one CREATE TABLE built to the
// document, owned by the keys' final names, and takes out of the model the
// clause indexes the server did not build. It runs after the keys are named.
func recordCreatedKeyIndexes(
	database *schemamodel.Database, document *Document, table schemamodel.Table, created createdKeyIndexes,
) {
	for _, built := range created.built {
		owner := ""
		if built.field != noPosition {
			owner = database.Fields[built.field].ForeignKeyName
		} else {
			owner = database.Constraints[built.constraint].Name
		}
		document.keys.add(keyIndex{
			table: table.QualifiedName(), name: built.name, columns: built.columns,
			owner: owner, declared: built.declared,
		})
	}
	dropped := slices.Clone(created.dropped)
	slices.Sort(dropped)
	for _, position := range slices.Backward(dropped) {
		database.Indexes = slices.Delete(database.Indexes, position, position+1)
	}
}
