package identifiervalidation

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/sqlservername"
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

// DeclaredConstraints lists the constraints desired names on dialect, each
// with the table it belongs to: the ones [TableConstraints] reads off each
// table, and each table-level constraint that carries a name. Every caller
// passes a desired whose foreign keys and table-level CHECKs carry the names
// the plan writes ([schemaprep.AssignDefaultConstraintNames]). A nil desired
// lists none.
func DeclaredConstraints(desired *schemamodel.Database, dialect string) []Hosted {
	if desired == nil {
		return nil
	}
	var hosted []Hosted
	for _, table := range desired.Tables {
		hosted = append(hosted, TableConstraints(table.QualifiedName(), table, generatedschema.FieldsForTable(desired, table), nil, dialect)...)
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

// TableConstraints lists the constraints the declaration of table names on
// dialect, as constraints of host, the name the table goes by: its primary
// key, the foreign key and the check of each of fields, and each of
// constraints. A foreign key name Ptah derives
// ([schemamodel.Field.ForeignKeyNameDerived]) is listed as a written one is,
// because the plan writes it. On a server that would name an unnamed
// constraint with a hash ([descriptor.ConstraintNamingServerHash]), Ptah
// writes a name for an unnamed primary key ([sqlservername.PrimaryKey]) and an
// unnamed column CHECK ([schemaprep.DefaultCheckName]), and each is listed
// under it. Measured on SQL Server 2025, CREATE VIEW t1_pkey beside a table t1
// whose primary key nobody named fails with error 2714. On another server the
// server names such a constraint, and it is left out.
func TableConstraints(host string, table schemamodel.Table, fields []schemamodel.Field, constraints []schemamodel.Constraint, dialect string) []Hosted {
	described, _ := platform.Lookup(dialect)
	writesNames := described.ConstraintNaming == descriptor.ConstraintNamingServerHash
	var hosted []Hosted
	named := func(name string) {
		if name != "" {
			hosted = append(hosted, Hosted{Name: name, Table: host})
		}
	}
	key := table.PrimaryKeyName
	if key == "" && writesNames && unnamedPrimaryKey(table, fields, constraints) {
		leaf := table.Name
		if ref, ok := tableref.Parse(table.Name); ok {
			leaf = ref.Name
		}
		key = sqlservername.PrimaryKey(leaf)
	}
	named(key)
	for _, field := range fields {
		named(field.ForeignKeyName)
		check := field.CheckName
		if check == "" && field.Check != "" && writesNames {
			check = schemaprep.DefaultCheckName(table.Name, field.Name)
		}
		named(check)
	}
	for _, constraint := range constraints {
		named(constraint.Name)
	}
	return hosted
}

// HeldDerivedConstraintNames lists, on a server that would name an unnamed
// constraint with a hash ([descriptor.ConstraintNamingServerHash]), the names
// [TableConstraints] would give the primary key of each table database holds
// and each CHECK it holds on a column, if the declaration form of that table
// left them unnamed. A rollback validates the database's own schema as its
// target, and the declaration form of a held key under a name the read folds
// away is unnamed, so the names a comparison resolves include these. Another
// server lists none, and so does a nil database.
func HeldDerivedConstraintNames(database *catalog.Database, dialect string) []string {
	described, _ := platform.Lookup(dialect)
	if database == nil || described.ConstraintNaming != descriptor.ConstraintNamingServerHash {
		return nil
	}
	var names []string
	for _, table := range database.Tables {
		if slices.ContainsFunc(table.Columns, func(column catalog.Column) bool { return column.IsPrimaryKey }) {
			names = append(names, sqlservername.PrimaryKey(table.Name))
		}
	}
	for _, constraint := range database.Constraints {
		switch strings.ToUpper(strings.TrimSpace(constraint.Type)) {
		case "PRIMARY KEY":
			names = append(names, sqlservername.PrimaryKey(constraint.TableName))
		case "CHECK":
			for _, column := range constraint.ColumnNames {
				names = append(names, schemaprep.DefaultCheckName(constraint.TableName, column))
			}
		}
	}
	return names
}

// unnamedPrimaryKey reports whether table declares a primary key, on the
// table, on one of fields or as one of constraints, and none of them names it.
func unnamedPrimaryKey(table schemamodel.Table, fields []schemamodel.Field, constraints []schemamodel.Constraint) bool {
	keyed := len(table.PrimaryKey) > 0 || len(table.PrimaryKeyParts) > 0
	for _, field := range fields {
		keyed = keyed || field.Primary
	}
	for _, constraint := range constraints {
		if !strings.EqualFold(strings.TrimSpace(constraint.Type), "PRIMARY KEY") {
			continue
		}
		if constraint.Name != "" {
			return false
		}
		keyed = true
	}
	return keyed
}

// CreatedConstraints lists the constraints diff creates on dialect: each one
// it adds to a table the database holds, and each one [TableConstraints] reads
// off a table it creates, which a target writes inside its CREATE TABLE. A nil
// diff lists none.
func CreatedConstraints(diff *difftypes.SchemaDiff, dialect string) []Hosted {
	if diff == nil {
		return nil
	}
	var hosted []Hosted
	for _, created := range diff.TablesAdded {
		host := created.Name
		if host == "" {
			host = created.Table.QualifiedName()
		}
		hosted = append(hosted, TableConstraints(host, created.Table, created.Fields, created.Constraints, dialect)...)
	}
	for _, added := range diff.ConstraintsAdded {
		if added.Name != "" {
			hosted = append(hosted, Hosted{Name: added.Name, Table: added.TableName})
		}
	}
	return hosted
}
