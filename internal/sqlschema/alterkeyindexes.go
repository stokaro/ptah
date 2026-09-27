package sqlschema

import (
	"slices"
	"strings"

	"ptah.run/core/schemamodel"
)

// releaseKeyIndexes takes out of the table the indexes the server built for
// foreign keys that an index over covering begins with, which the server drops
// when it builds that index; see [keyIndex]. It runs before the added index is
// named, because the server frees the name first: measured, `FOREIGN KEY (a)
// ...` followed by `ALTER TABLE c ADD UNIQUE (a)` leaves one index, `a`.
func (t alterTarget) releaseKeyIndexes(covering candidateKey) {
	naming, ok := namingFor(t.sourcePlatform)
	if !ok {
		return
	}
	for _, released := range t.keys.releaseCoveredBy(t.qualified, covering, naming) {
		t.removeModelIndex(released.name)
	}
}

// buildKeyIndex records the index the server builds for a foreign key an ALTER
// TABLE adds, by the rule [keyIndex] states: none where an index the table
// declares covers the key, none where a built index over more columns begins
// with the key's, and otherwise one that replaces every built index the key's
// columns begin with. derived is whether the reader derived the key's name
// rather than reading one the statement wrote.
//
// The index takes the name a MySQL `ADD FOREIGN KEY name (columns)` clause
// gives it, the key's own name when the statement wrote one, and otherwise its
// first column's, `_2` and on when that is taken. Measured on MySQL 8.4.11 and
// 26.7.0 and MariaDB 11.8.9 and 12.3.3:
//
//	the table holds          ALTER TABLE c ...                        indexes of c
//	CONSTRAINT fk FK (a)     ADD CONSTRAINT fk2 FK (a)                fk2 (a)
//	CONSTRAINT fk FK (a, b)  ADD CONSTRAINT fk2 FK (a)                fk (a, b)
//	CONSTRAINT fk FK (a)     ADD CONSTRAINT fk2 FK (a, b)             fk2 (a, b)
//	KEY a (b)                ADD FOREIGN KEY (a) ...                  a (b), a_2 (a)
//	nothing                  ADD FOREIGN KEY idx (a) ..., on MySQL    idx (a)
func (t alterTarget) buildKeyIndex(key string, derived bool, columns []string) {
	naming, ok := namingFor(t.sourcePlatform)
	clause := t.statement.clause
	t.statement.clause = nil
	if !ok || len(columns) == 0 {
		return
	}
	exclude := ""
	if clause != nil {
		exclude = clause.Name
	}
	if t.declaredCoverage(exclude).covers(columns, naming) || t.keys.reusable(t.qualified, columns, naming) {
		if clause != nil {
			t.removeModelIndex(clause.Name)
		}
		return
	}
	for _, released := range t.keys.releaseCoveredBy(t.qualified, ascending(columns), naming) {
		t.removeModelIndex(released.name)
	}
	built := keyIndex{table: t.qualified, name: key, columns: columns, owner: key}
	switch {
	case clause != nil:
		built.name, built.declared = clause.Name, true
	case derived:
		built.name = firstFree(heldIndexNames(t), columns[0], naming)
	}
	t.keys.add(built)
}

// buildColumnKeyIndexes records the index the server builds for each key a
// column added by ALTER TABLE ... ADD COLUMN declares with REFERENCES.
func (t alterTarget) buildColumnKeyIndexes(fields []schemamodel.Field) {
	for _, field := range fields {
		if field.Foreign == "" {
			continue
		}
		t.buildKeyIndex(field.ForeignKeyName, true, []string{field.Name})
	}
}

// declaredCoverage is every access path the table declares, the indexes the
// server built for keys left out: its primary key, its columns' own keys, its
// indexes and its UNIQUEs. exclude names one more index to leave out.
func (t alterTarget) declaredCoverage(exclude string) coverage {
	covered := coverage{ascending(t.table.PrimaryKey)}
	for _, database := range t.databases {
		for _, field := range database.Fields {
			if field.StructName == t.structName && (field.Primary || field.Unique) {
				covered = append(covered, ascending([]string{field.Name}))
			}
		}
		for _, index := range database.Indexes {
			if !t.ownsIndex(index) || strings.EqualFold(index.Name, exclude) ||
				t.keys.isDeclaredKeyIndex(t.qualified, index.Name) {
				continue
			}
			covered = append(covered, indexCandidate(index))
		}
		for _, constraint := range database.Constraints {
			if constraint.Table == t.qualified && strings.EqualFold(constraint.Type, "UNIQUE") {
				covered = append(covered, ascending(constraint.Columns))
			}
		}
	}
	return covered
}

// removeModelIndex takes the table's index called name out of the model.
func (t alterTarget) removeModelIndex(name string) {
	for _, database := range t.databases {
		database.Indexes = slices.DeleteFunc(database.Indexes, func(index schemamodel.Index) bool {
			return t.ownsIndex(index) && strings.EqualFold(index.Name, name)
		})
	}
}

// holdsForeignKey reports whether the table holds a foreign key called name,
// declared on the table or on one of its columns.
func (t alterTarget) holdsForeignKey(name string) bool {
	for _, database := range t.databases {
		for _, constraint := range database.Constraints {
			if isForeignKey(constraint) && constraint.StructName == t.structName && constraint.Name == name {
				return true
			}
		}
		for _, field := range database.Fields {
			if field.StructName == t.structName && field.Foreign != "" && field.ForeignKeyName == name {
				return true
			}
		}
	}
	return false
}

// removeForeignKey takes the table's foreign key called name out of the model
// and reports whether there was one. Only the key: an index of the same name
// stays, as the server keeps it. Measured on MySQL 8.4.11 and 26.7.0 and
// MariaDB 11.8.9 and 12.3.3, `KEY fk (a), CONSTRAINT fk FOREIGN KEY (a) ...`
// followed by `ALTER TABLE c DROP FOREIGN KEY fk` leaves the index `fk (a)`.
func (t alterTarget) removeForeignKey(name string) bool {
	for _, database := range t.databases {
		before := len(database.Constraints)
		database.Constraints = slices.DeleteFunc(database.Constraints, func(constraint schemamodel.Constraint) bool {
			return isForeignKey(constraint) && constraint.StructName == t.structName && constraint.Name == name
		})
		if len(database.Constraints) != before {
			return true
		}
		for i := range database.Fields {
			field := &database.Fields[i]
			if field.StructName == t.structName && field.Foreign != "" && field.ForeignKeyName == name {
				field.Foreign, field.ForeignKeyName, field.OnDelete, field.OnUpdate = "", "", "", ""
				field.Deferrable, field.Initially = false, ""
				return true
			}
		}
	}
	return false
}

// keepKeyIndexes declares in the model the indexes the server built for the
// foreign key called name, which a statement just dropped. The server keeps
// them: measured, `CONSTRAINT fk FOREIGN KEY (a) ...` followed by `DROP
// FOREIGN KEY fk` leaves the index `fk (a)`, and an unnamed key leaves the one
// named after its column. Left out of the model, the comparison plans to drop
// an index the database the file built holds (stokaro/ptah#3763).
func (t alterTarget) keepKeyIndexes(name string) {
	for _, orphan := range t.keys.disown(t.qualified, name) {
		t.databases[0].Indexes = append(t.databases[0].Indexes, schemamodel.Index{
			StructName: t.structName,
			Name:       orphan.name,
			Fields:     orphan.columns,
			TableName:  t.qualified,
		})
	}
}
