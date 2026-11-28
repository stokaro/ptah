package identifiervalidation

import (
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
	"ptah.run/migration/internal/generatedschema"
)

// Hosted is a constraint or a trigger as a shared namespace reads it
// ([ptah.run/core/platform/descriptor.ObjectNamespace]): its own name, which
// carries no schema, and the table it belongs to, whose schema it is in.
type Hosted struct {
	Name  string
	Table string
}

// QualifiedName is the name with the schema of the table written out, or
// defaultSchema where the table names none: the constraint Report of dbo.t1
// is dbo.Report. A part that holds a dot or a quote is quoted, so the name
// reads back as two parts.
func (h Hosted) QualifiedName(defaultSchema string) string {
	schema := defaultSchema
	if ref, ok := tableref.Parse(h.Table); ok && ref.Qualified {
		schema = ref.Schema
	}
	return tableref.Canonical(schema, h.Name)
}

// Apart reports whether two objects of a shared namespace in a schema, each
// given by the noun of its kind and the table it belongs to, are two objects
// whose names the namespace compares: objects of two kinds, or two
// constraints or two triggers of two tables. Two objects of one kind that no
// table holds, such as two views, are compared by the checks of that kind,
// and so are two constraints or two triggers of one table.
func Apart(noun, host, otherNoun, otherHost string) bool {
	return noun != otherNoun || host != otherHost
}

// NamespaceClause says why dialect reads the names of two objects of a shared
// namespace, each given by the noun of its kind, as names in one namespace:
// `sqlserver keeps both kinds in one namespace in a schema`, or for two of one
// kind `sqlserver keeps the constraints of every table in one namespace in a
// schema`.
func NamespaceClause(noun, otherNoun, dialect string) string {
	if noun == otherNoun {
		return dialect + " keeps the " + noun + "s of every table in one namespace in a schema"
	}
	return dialect + " keeps both kinds in one namespace in a schema"
}

// HostedTrigger is the trigger name on table. A trigger name may carry the
// schema of its table, as `dbo.Audit` does, and the schema is left to the
// table's name.
func HostedTrigger(name, table string) Hosted {
	if ref, ok := tableref.Parse(name); ok && ref.Qualified {
		name = ref.Name
	}
	return Hosted{Name: name, Table: table}
}

// DeclaredConstraints lists the constraints desired names, each with the
// table it belongs to: the ones [TableConstraints] reads off each table, and
// each table-level constraint that carries a name. A nil desired lists none.
func DeclaredConstraints(desired *schemamodel.Database) []Hosted {
	if desired == nil {
		return nil
	}
	var hosted []Hosted
	for _, table := range desired.Tables {
		hosted = append(hosted, TableConstraints(table.QualifiedName(), table, generatedschema.FieldsForTable(desired, table), nil)...)
	}
	for _, constraint := range desired.Constraints {
		if constraint.Name == "" {
			continue
		}
		host := constraint.Table
		for _, table := range desired.Tables {
			if host == "" && table.StructName == constraint.StructName {
				host = table.QualifiedName()
			}
		}
		hosted = append(hosted, Hosted{Name: constraint.Name, Table: host})
	}
	return hosted
}

// TableConstraints lists the constraints the declaration of table names, as
// constraints of host, the name the table goes by: its primary key, the
// foreign key and the check of each of fields, and each of constraints. A
// constraint the declaration leaves unnamed takes a name the server makes up,
// and a foreign key name Ptah derives
// ([schemamodel.Field.ForeignKeyNameDerived]) is left out too: neither is a
// name an author chose, so neither is compared with the names of other
// objects.
func TableConstraints(host string, table schemamodel.Table, fields []schemamodel.Field, constraints []schemamodel.Constraint) []Hosted {
	var hosted []Hosted
	named := func(name string) {
		if name != "" {
			hosted = append(hosted, Hosted{Name: name, Table: host})
		}
	}
	named(table.PrimaryKeyName)
	for _, field := range fields {
		if !field.ForeignKeyNameDerived {
			named(field.ForeignKeyName)
		}
		named(field.CheckName)
	}
	for _, constraint := range constraints {
		named(constraint.Name)
	}
	return hosted
}

// CreatedConstraints lists the constraints diff creates: each one it adds to a
// table the database holds, and each one [TableConstraints] reads off a table
// it creates, which a target writes inside its CREATE TABLE. A nil diff lists
// none.
func CreatedConstraints(diff *difftypes.SchemaDiff) []Hosted {
	if diff == nil {
		return nil
	}
	var hosted []Hosted
	for _, created := range diff.TablesAdded {
		host := created.Name
		if host == "" {
			host = created.Table.QualifiedName()
		}
		hosted = append(hosted, TableConstraints(host, created.Table, created.Fields, created.Constraints)...)
	}
	for _, added := range diff.ConstraintsAdded {
		if added.Name != "" {
			hosted = append(hosted, Hosted{Name: added.Name, Table: added.TableName})
		}
	}
	return hosted
}
