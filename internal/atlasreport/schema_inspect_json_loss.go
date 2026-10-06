package atlasreport

import (
	"encoding/json"
	"fmt"
	"io"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaexportloss"
)

// inspectJSONLoss belongs to a projected JSON value, so templates marshaling
// .Realm, .Schema or an individual table report the properties they omit.
// Keeping it off the wire preserves the inspection document's existing shape.
type inspectJSONLoss struct {
	writer  io.Writer
	scope   string
	omitted []string
}

func (l *inspectJSONLoss) write() {
	if l == nil || l.writer == nil {
		return
	}
	for _, omitted := range l.omitted {
		fmt.Fprintf(l.writer, "warning: JSON schema inspection leaves out %s%s\n", omitted, l.scope)
	}
}

func (r atlasSchemaInspectJSONRealm) MarshalJSON() ([]byte, error) {
	type document atlasSchemaInspectJSONRealm
	r.loss.write()
	return json.Marshal(document(r))
}

func (t atlasSchemaInspectJSONTable) MarshalJSON() ([]byte, error) {
	type document atlasSchemaInspectJSONTable
	t.loss.write()
	return json.Marshal(document(t))
}

func (c atlasSchemaInspectJSONColumn) MarshalJSON() ([]byte, error) {
	type document atlasSchemaInspectJSONColumn
	c.loss.write()
	return json.Marshal(document(c))
}

func (i atlasSchemaInspectJSONIndex) MarshalJSON() ([]byte, error) {
	type document atlasSchemaInspectJSONIndex
	i.loss.write()
	return json.Marshal(document(i))
}

func attachYDBJSONLoss(realm *atlasSchemaInspectJSONRealm, db *schemamodel.Database, described *catalog.Database, info catalog.ServerInfo, writer io.Writer) {
	if db == nil || writer == nil || platform.NormalizeDialect(info.Dialect) != platform.YDB {
		return
	}
	realm.loss = &inspectJSONLoss{writer: writer, omitted: schemaexportloss.CommonFamilies(db)}
	sources := indexJSONLossSources(db, described, info)
	for s := range realm.Schemas {
		schema := &realm.Schemas[s]
		for t := range schema.Tables {
			table := &schema.Tables[t]
			if source, ok := sources.tables[inspectJSONIdentity{schema.Name, table.Name}]; ok {
				attachJSONTableLoss(table, source, sources, writer)
			}
		}
	}
}

type inspectJSONIdentity struct {
	parent string
	name   string
}

type inspectJSONTableSource struct {
	table   schemamodel.Table
	columns []catalog.Column
}

type inspectJSONSources struct {
	tables  map[inspectJSONIdentity]inspectJSONTableSource
	indexes map[inspectJSONIdentity]schemamodel.Index
}

func indexJSONLossSources(db *schemamodel.Database, described *catalog.Database, info catalog.ServerInfo) inspectJSONSources {
	sources := inspectJSONSources{
		tables:  make(map[inspectJSONIdentity]inspectJSONTableSource, len(db.Tables)),
		indexes: make(map[inspectJSONIdentity]schemamodel.Index, len(db.Indexes)),
	}
	for _, table := range db.Tables {
		sources.tables[inspectJSONIdentity{atlasSchemaInspectSchemaName(table.Schema, info), table.Name}] = inspectJSONTableSource{table: table}
	}
	for _, table := range described.Tables {
		key := inspectJSONIdentity{atlasSchemaInspectSchemaName(table.Schema, info), table.Name}
		if source, ok := sources.tables[key]; ok {
			source.columns = table.Columns
			sources.tables[key] = source
		}
	}
	for _, index := range db.Indexes {
		sources.indexes[inspectJSONIdentity{index.StructName, index.Name}] = index
	}
	return sources
}

func attachJSONTableLoss(
	table *atlasSchemaInspectJSONTable, source inspectJSONTableSource, sources inspectJSONSources, writer io.Writer,
) {
	counts := make(map[string]int)
	schemaexportloss.CountTableStorage(counts, source.table)
	scope := fmt.Sprintf(" from table %q", source.table.QualifiedName())
	table.loss = &inspectJSONLoss{writer: writer, scope: scope, omitted: schemaexportloss.Descriptions(counts)}
	// Both slices come from the same catalog table, in declaration order.
	for c, described := range source.columns {
		table.Columns[c].loss = &inspectJSONLoss{
			writer: writer, scope: fmt.Sprintf(" from column %q of table %q", described.Name, source.table.QualifiedName()),
			omitted: jsonColumnLoss(described),
		}
	}
	for i := range table.Indexes {
		index := &table.Indexes[i]
		if described, ok := sources.indexes[inspectJSONIdentity{source.table.StructName, index.Name}]; ok {
			index.loss = &inspectJSONLoss{
				writer: writer, scope: fmt.Sprintf(" from index %q of table %q", index.Name, source.table.QualifiedName()),
				omitted: jsonIndexLoss(described),
			}
		}
	}
}

// Read default presence from the catalog, where an empty value and an absent
// default remain distinct.
func jsonColumnLoss(column catalog.Column) []string {
	counts := make(map[string]int)
	schemaexportloss.CountColumnIdentity(counts, schemamodel.Field{
		IdentityGeneration: column.IdentityGeneration,
		IdentityStart:      column.IdentityStart,
		IdentityIncrement:  column.IdentityIncrement,
	})
	if column.IsAutoIncrement {
		counts["automatic column generation"]++
	}
	if column.ColumnDefault != nil {
		counts["column defaults"]++
	}
	if column.Comment != "" {
		counts["column comments"]++
	}
	return schemaexportloss.Descriptions(counts)
}

func jsonIndexLoss(index schemamodel.Index) []string {
	counts := make(map[string]int)
	schemaexportloss.CountIndexStorage(counts, index)
	if index.Type != "" {
		counts["index kinds"]++
	}
	if index.Comment != "" {
		counts["index comments"]++
	}
	return schemaexportloss.Descriptions(counts)
}
