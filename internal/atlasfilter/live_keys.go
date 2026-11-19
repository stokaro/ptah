package atlasfilter

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/indexbacking"
)

// LiveKeys are the primary keys a read of the database holds, by table. The
// comparison pairs a declared primary key with the database's key on the same
// table whatever either is named, so the desired side names a declared key by
// the name of that key, and one selector leaves out the same key on both
// sides. A declaration predicts the name of a key it leaves unnamed, and the
// server may hold the key under another name: one an author gave it, or one a
// renamed table kept. A UNIQUE is paired by its name, so the name the
// declaration predicts is the one to name it by, and LiveKeys holds none.
//
// The zero value holds no read, and the desired side then names a key by the
// name it predicts. LiveKeys is comparable, and two values are equal when they
// hold the same read.
type LiveKeys struct {
	read *liveRead
}

// liveRead is the primary key of every table of one read.
type liveRead struct {
	tables []livePrimaryKey
}

// livePrimaryKey is the primary key of one table: its name, its columns, and
// whether the table holds one.
type livePrimaryKey struct {
	schema, table string
	name          string
	columns       []string
	held          bool
}

// KeysOf returns the primary keys db holds: each constraint row, and each key
// the read reports through its index alone, as MariaDB does for an account that
// sees no constraint row. A nil db holds no read.
func KeysOf(db *catalog.Database) LiveKeys {
	if db == nil {
		return LiveKeys{}
	}
	read := &liveRead{tables: make([]livePrimaryKey, 0, len(db.Tables))}
	for _, table := range db.Tables {
		read.tables = append(read.tables, livePrimaryKey{schema: table.Schema, table: table.Name})
	}
	at := func(schema, table string) *livePrimaryKey {
		for i := range read.tables {
			if read.tables[i].schema == schema && read.tables[i].table == table {
				return &read.tables[i]
			}
		}
		return nil
	}
	for _, constraint := range db.Constraints {
		if key := at(constraint.Schema, constraint.TableName); key != nil && indexbacking.KindOf(constraint.Type) == indexbacking.PrimaryKey {
			key.name, key.columns, key.held = constraint.Name, slices.Clone(constraint.ColumnNamesOrDefault()), true
		}
	}
	for _, index := range db.Indexes {
		if key := at(index.Schema, index.TableName); key != nil && index.IsPrimary && !key.held {
			key.name, key.columns, key.held = index.Name, slices.Clone(index.Columns), true
		}
	}
	return LiveKeys{read: read}
}

// primary returns the name of the database's primary key on the declared
// table, in the schema each one resolves to, and whether the database holds
// one there.
func (k LiveKeys) primary(s *exclusionState, table schemamodel.Table) (string, bool) {
	key, held := k.primaryKey(s, table)
	return key.name, held
}

// primaryColumns returns the columns of the database's primary key on the
// declared table, or none where the database holds no key there.
func (k LiveKeys) primaryColumns(s *exclusionState, table schemamodel.Table) []string {
	key, _ := k.primaryKey(s, table)
	return key.columns
}

func (k LiveKeys) primaryKey(s *exclusionState, table schemamodel.Table) (livePrimaryKey, bool) {
	if k.read == nil {
		return livePrimaryKey{}, false
	}
	schema := s.effectiveSchema(table.Schema)
	for _, key := range k.read.tables {
		if key.held && key.table == table.Name && s.effectiveSchema(key.schema) == schema {
			return key, true
		}
	}
	return livePrimaryKey{}, false
}
