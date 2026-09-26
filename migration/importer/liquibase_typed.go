package importer

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/internal/liquibaserun"
)

// A Liquibase typed change -- `createTable`, `addColumn` and the rest -- is not
// SQL. Liquibase generates its SQL when it runs, for whatever database it is
// pointed at. An imported directory holds SQL files, so converting one means
// choosing that database now: the change is lowered to Ptah's AST and rendered
// for the dialect the caller named, and the file that results is written for
// that dialect only (stokaro/ptah#3625).
//
// # What converts
//
// The change types in [liquibaseConverters]. Each one reads the attributes and
// nested elements it understands and refuses the others by name, because an
// attribute read by nothing is an attribute dropped: a `defaultValue` nobody
// interpreted is a column that silently lost its default.
//
// The render is asked to report what the target could not carry, and anything
// it reports refuses the conversion. That catches a declaration this file maps
// correctly and a target renders without -- `autoIncrement` on a dialect with
// no spelling for it -- which the attribute check above cannot see.
//
// # Rollback
//
// An explicit rollback in the changeset is converted like any other change and
// wins. Without one, the rollback is derived the way Liquibase derives it: each
// change that can be undone from the changelog alone contributes its inverse,
// in reverse order. A changeset holding any change that cannot -- a drop, whose
// definition the changelog no longer has, or SQL, whose inverse Ptah cannot
// know -- gets no derived rollback, which is also when Liquibase requires one to
// be written.

// errLiquibaseNotConverted marks a change type Ptah does not convert at all.
var errLiquibaseNotConverted = errors.New("liquibase change type not converted")

// errLiquibaseNeedsDialect marks a typed change that would convert given a
// target dialect.
var errLiquibaseNeedsDialect = errors.New("liquibase typed change needs a target dialect")

// liquibaseConverted is one change converted to SQL.
type liquibaseConverted struct {
	up string
	// down undoes up. It is meaningful only when reversible is set.
	down string
	// reversible reports that down could be derived from the change alone.
	reversible bool
}

// liquibaseConverter converts the changes of one changelog file.
type liquibaseConverter struct {
	fsys fs.FS
	// file is the changelog the changes came from, for sqlFile paths and for
	// messages.
	file string
	// dialect is the normalized target, empty when the caller named none.
	dialect string
	// caps is the capability preset the target is rendered against.
	caps capability.Capabilities
	// consumed collects the source files a sqlFile change read, so the import
	// accounts for them as converted rather than reporting them as left behind.
	consumed []string
}

type liquibaseConvertFunc func(*liquibaseConverter, liquibaseChange) (liquibaseConverted, error)

// liquibaseConverters is the one declaration of which Liquibase change types
// convert. The XML walk and the YAML/JSON walk both hand their changes here, so
// a type added to this map converts in all three serializations at once.
//
// `sql` and `sqlFile` carry SQL rather than describe it, so they convert with
// or without a dialect; every other entry is rendered and needs one.
var liquibaseConverters = map[string]liquibaseConvertFunc{
	"sql":                     (*liquibaseConverter).sql,
	"sqlFile":                 (*liquibaseConverter).sqlFile,
	"createTable":             (*liquibaseConverter).createTable,
	"dropTable":               (*liquibaseConverter).dropTable,
	"addColumn":               (*liquibaseConverter).addColumn,
	"dropColumn":              (*liquibaseConverter).dropColumn,
	"createIndex":             (*liquibaseConverter).createIndex,
	"dropIndex":               (*liquibaseConverter).dropIndex,
	"addPrimaryKey":           (*liquibaseConverter).addPrimaryKey,
	"addForeignKeyConstraint": (*liquibaseConverter).addForeignKeyConstraint,
	"renameTable":             (*liquibaseConverter).renameTable,
	"renameColumn":            (*liquibaseConverter).renameColumn,
}

// liquibaseCarriesSQL reports the change types whose SQL is already written.
func liquibaseCarriesSQL(name string) bool {
	return name == "sql" || name == "sqlFile"
}

// convert converts one change, or reports why it did not.
func (c *liquibaseConverter) convert(change liquibaseChange) (liquibaseConverted, error) {
	convertFunc, ok := liquibaseConverters[change.name]
	if !ok {
		return liquibaseConverted{}, errLiquibaseNotConverted
	}
	if c.dialect == "" && !liquibaseCarriesSQL(change.name) {
		return liquibaseConverted{}, errLiquibaseNeedsDialect
	}
	return convertFunc(c, change)
}

// ----------------------------------------------------------------- SQL text

// sql reads the statement a `sql` change holds.
//
// `dbms` never reaches it: [liquibaseChange.withoutRunCondition] takes the
// attribute off first. `splitStatements` and `stripComments` are accepted and
// not needed, for the reason [liquibaseConverter.sqlFile] gives, and a `comment`
// documents the change. Any other attribute or element is refused by name, as
// every other converter refuses one, because an attribute nothing reads is an
// attribute dropped: `endDelimiter` would leave a delimiter Ptah does not know
// in the SQL.
func (c *liquibaseConverter) sql(change liquibaseChange) (liquibaseConverted, error) {
	if err := change.only("sql", "comment", "splitStatements", "stripComments"); err != nil {
		return liquibaseConverted{}, err
	}
	if err := change.childrenOnly("comment"); err != nil {
		return liquibaseConverted{}, err
	}
	text := change.text
	if text == "" {
		text = strings.TrimSpace(change.attrs["sql"])
	}
	return liquibaseConverted{up: text}, nil
}

// sqlFile reads the statements of the file a `sqlFile` change names.
//
// The path resolves inside the source directory and nowhere else: fs.FS paths
// cannot be absolute or climb with "..", so a changelog cannot make the import
// read a file the caller did not hand it. `splitStatements` and `stripComments`
// are accepted and not needed, because a Ptah migration file is split into
// statements by Ptah and its comments do not execute. `endDelimiter` is refused:
// a delimiter Ptah does not know would stay in the SQL.
func (c *liquibaseConverter) sqlFile(change liquibaseChange) (liquibaseConverted, error) {
	if err := change.only("path", "relativeToChangelogFile", "encoding", "splitStatements", "stripComments"); err != nil {
		return liquibaseConverted{}, err
	}
	name, err := change.required("path")
	if err != nil {
		return liquibaseConverted{}, err
	}
	if encoding := change.attrs["encoding"]; encoding != "" && !strings.EqualFold(encoding, "UTF-8") {
		return liquibaseConverted{}, fmt.Errorf("%s reads %q as %s; Ptah reads migration files as UTF-8", change.display, name, encoding)
	}
	relative, err := change.flag("relativeToChangelogFile")
	if err != nil {
		return liquibaseConverted{}, err
	}
	resolved := strings.TrimPrefix(name, "./")
	if relative {
		resolved = path.Join(path.Dir(c.file), resolved)
	}
	if !fs.ValidPath(resolved) || strings.Contains(name, `\`) {
		return liquibaseConverted{}, fmt.Errorf(
			"%s names %q, which is not a path inside the source directory", change.display, name)
	}
	content, err := fs.ReadFile(c.fsys, resolved)
	if err != nil {
		return liquibaseConverted{}, fmt.Errorf("%s names %q: %w", change.display, name, err)
	}
	c.consumed = append(c.consumed, resolved)
	return liquibaseConverted{up: strings.TrimSpace(string(content))}, nil
}

// ------------------------------------------------------------------ tables

var liquibaseColumnAttrs = []string{
	"name", "type", "remarks", "autoIncrement",
	"defaultValue", "defaultValueNumeric", "defaultValueBoolean", "defaultValueDate", "defaultValueComputed",
}

func (c *liquibaseConverter) createTable(change liquibaseChange) (liquibaseConverted, error) {
	if err := change.only("tableName", "schemaName", "remarks"); err != nil {
		return liquibaseConverted{}, err
	}
	if err := change.childrenOnly("column"); err != nil {
		return liquibaseConverted{}, err
	}
	name, err := change.qualifiedTable("schemaName", "tableName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	columns := change.childrenNamed("column")
	if len(columns) == 0 {
		return liquibaseConverted{}, fmt.Errorf("%s %s declares no column", change.display, name)
	}

	table := &ast.CreateTableNode{Name: name, Comment: change.attrs["remarks"]}
	var primary []*ast.ColumnNode
	var primaryName string
	for _, columnChange := range columns {
		column, constraints, err := c.column(columnChange, liquibaseTableConstraints)
		if err != nil {
			return liquibaseConverted{}, err
		}
		table.Columns = append(table.Columns, column)
		if constraints.primaryKey {
			primary = append(primary, column)
			if constraints.primaryKeyName != "" {
				if primaryName != "" && primaryName != constraints.primaryKeyName {
					return liquibaseConverted{}, fmt.Errorf(
						"%s %s names its primary key both %q and %q", change.display, name, primaryName, constraints.primaryKeyName)
				}
				primaryName = constraints.primaryKeyName
			}
		}
		if constraints.uniqueName != "" {
			table.Constraints = append(table.Constraints, &ast.ConstraintNode{
				Type: ast.UniqueConstraint, Name: constraints.uniqueName, Columns: []string{column.Name},
			})
		}
		if constraints.foreignKey != nil {
			table.Constraints = append(table.Constraints, constraints.foreignKey)
		}
	}
	// One unnamed key column is declared on the column, which is how it reads
	// back; a named or composite key is a table constraint, the only place
	// either can be written.
	switch {
	case len(primary) == 1 && primaryName == "":
		primary[0].Primary = true
	case len(primary) > 0:
		key := &ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Name: primaryName}
		for _, column := range primary {
			key.Columns = append(key.Columns, column.Name)
		}
		table.Constraints = append([]*ast.ConstraintNode{key}, table.Constraints...)
	}

	up, err := c.render(table)
	if err != nil {
		return liquibaseConverted{}, err
	}
	down, err := c.render(ast.NewDropTable(name))
	if err != nil {
		return liquibaseConverted{}, err
	}
	return liquibaseConverted{up: up, down: down, reversible: true}, nil
}

func (c *liquibaseConverter) dropTable(change liquibaseChange) (liquibaseConverted, error) {
	if err := change.only("tableName", "schemaName", "cascadeConstraints"); err != nil {
		return liquibaseConverted{}, err
	}
	if err := change.childrenOnly(); err != nil {
		return liquibaseConverted{}, err
	}
	name, err := change.qualifiedTable("schemaName", "tableName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	cascade, err := change.flag("cascadeConstraints")
	if err != nil {
		return liquibaseConverted{}, err
	}
	drop := ast.NewDropTable(name)
	drop.Cascade = cascade
	up, err := c.render(drop)
	return liquibaseConverted{up: up}, err
}

func (c *liquibaseConverter) renameTable(change liquibaseChange) (liquibaseConverted, error) {
	if err := change.only("schemaName", "oldTableName", "newTableName"); err != nil {
		return liquibaseConverted{}, err
	}
	if err := change.childrenOnly(); err != nil {
		return liquibaseConverted{}, err
	}
	oldName, err := change.required("oldTableName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	newName, err := change.required("newTableName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	schema := change.attrs["schemaName"]
	up, err := c.render(&ast.AlterTableNode{
		Name:       liquibaseQualify(schema, oldName),
		Operations: []ast.AlterOperation{&ast.RenameTableOperation{NewName: newName}},
	})
	if err != nil {
		return liquibaseConverted{}, err
	}
	down, err := c.render(&ast.AlterTableNode{
		Name:       liquibaseQualify(schema, newName),
		Operations: []ast.AlterOperation{&ast.RenameTableOperation{NewName: oldName}},
	})
	if err != nil {
		return liquibaseConverted{}, err
	}
	return liquibaseConverted{up: up, down: down, reversible: true}, nil
}

// ----------------------------------------------------------------- columns

func (c *liquibaseConverter) addColumn(change liquibaseChange) (liquibaseConverted, error) {
	if err := change.only("tableName", "schemaName"); err != nil {
		return liquibaseConverted{}, err
	}
	if err := change.childrenOnly("column"); err != nil {
		return liquibaseConverted{}, err
	}
	table, err := change.qualifiedTable("schemaName", "tableName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	columns := change.childrenNamed("column")
	if len(columns) == 0 {
		return liquibaseConverted{}, fmt.Errorf("%s %s declares no column", change.display, table)
	}
	add := &ast.AlterTableNode{Name: table}
	drop := &ast.AlterTableNode{Name: table}
	for _, columnChange := range columns {
		column, _, err := c.column(columnChange, liquibaseAddedColumnConstraints)
		if err != nil {
			return liquibaseConverted{}, err
		}
		add.Operations = append(add.Operations, &ast.AddColumnOperation{Column: column})
		drop.Operations = append([]ast.AlterOperation{&ast.DropColumnOperation{ColumnName: column.Name}}, drop.Operations...)
	}
	up, err := c.render(add)
	if err != nil {
		return liquibaseConverted{}, err
	}
	down, err := c.render(drop)
	if err != nil {
		return liquibaseConverted{}, err
	}
	return liquibaseConverted{up: up, down: down, reversible: true}, nil
}

func (c *liquibaseConverter) dropColumn(change liquibaseChange) (liquibaseConverted, error) {
	if err := change.only("tableName", "schemaName", "columnName"); err != nil {
		return liquibaseConverted{}, err
	}
	if err := change.childrenOnly("column"); err != nil {
		return liquibaseConverted{}, err
	}
	table, err := change.qualifiedTable("schemaName", "tableName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	var names []string
	if name := change.attrs["columnName"]; name != "" {
		names = append(names, name)
	}
	for _, column := range change.childrenNamed("column") {
		if err := column.only("name"); err != nil {
			return liquibaseConverted{}, err
		}
		if err := column.childrenOnly(); err != nil {
			return liquibaseConverted{}, err
		}
		name, err := column.required("name")
		if err != nil {
			return liquibaseConverted{}, err
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return liquibaseConverted{}, fmt.Errorf("%s on %s names no column", change.display, table)
	}
	drop := &ast.AlterTableNode{Name: table}
	for _, name := range names {
		drop.Operations = append(drop.Operations, &ast.DropColumnOperation{ColumnName: name})
	}
	up, err := c.render(drop)
	return liquibaseConverted{up: up}, err
}

func (c *liquibaseConverter) renameColumn(change liquibaseChange) (liquibaseConverted, error) {
	// columnDataType is read by nothing on purpose: Liquibase needs it to spell
	// a rename on MySQL servers older than RENAME COLUMN, and every renderer
	// here writes RENAME COLUMN, which takes no type.
	if err := change.only("schemaName", "tableName", "oldColumnName", "newColumnName", "columnDataType"); err != nil {
		return liquibaseConverted{}, err
	}
	if err := change.childrenOnly(); err != nil {
		return liquibaseConverted{}, err
	}
	table, err := change.qualifiedTable("schemaName", "tableName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	oldName, err := change.required("oldColumnName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	newName, err := change.required("newColumnName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	up, err := c.render(&ast.AlterTableNode{
		Name:       table,
		Operations: []ast.AlterOperation{&ast.RenameColumnOperation{OldName: oldName, NewName: newName}},
	})
	if err != nil {
		return liquibaseConverted{}, err
	}
	down, err := c.render(&ast.AlterTableNode{
		Name:       table,
		Operations: []ast.AlterOperation{&ast.RenameColumnOperation{OldName: newName, NewName: oldName}},
	})
	if err != nil {
		return liquibaseConverted{}, err
	}
	return liquibaseConverted{up: up, down: down, reversible: true}, nil
}

// liquibaseColumnConstraints is what a column's `constraints` element declared.
type liquibaseColumnConstraints struct {
	primaryKey     bool
	primaryKeyName string
	uniqueName     string
	foreignKey     *ast.ConstraintNode
}

// The `constraints` attributes each change accepts. A column added to an
// existing table takes the subset that stays a column property; a key or a
// reference there would be a table constraint added in the same statement,
// which this conversion does not write.
var (
	liquibaseTableConstraints = []string{
		"nullable", "primaryKey", "primaryKeyName", "unique", "uniqueConstraintName",
		"foreignKeyName", "references", "referencedTableName", "referencedTableSchemaName",
		"referencedColumnNames", "deleteCascade",
	}
	liquibaseAddedColumnConstraints = []string{"nullable", "unique"}
)

// column converts one `column` element and what its `constraints` declared.
func (c *liquibaseConverter) column(
	change liquibaseChange,
	allowedConstraints []string,
) (*ast.ColumnNode, liquibaseColumnConstraints, error) {
	var declared liquibaseColumnConstraints
	if err := change.only(liquibaseColumnAttrs...); err != nil {
		return nil, declared, err
	}
	if err := change.childrenOnly("constraints"); err != nil {
		return nil, declared, err
	}
	name, err := change.required("name")
	if err != nil {
		return nil, declared, err
	}
	declaredType, err := change.required("type")
	if err != nil {
		return nil, declared, err
	}
	columnType, err := liquibaseColumnType(declaredType)
	if err != nil {
		return nil, declared, fmt.Errorf("%s %s: %w", change.display, name, err)
	}
	column := &ast.ColumnNode{Name: name, Type: columnType, Nullable: true, Comment: change.attrs["remarks"]}

	autoIncrement, err := change.flag("autoIncrement")
	if err != nil {
		return nil, declared, err
	}
	if autoIncrement {
		column.AutoInc = true
		// Liquibase writes an identity column for autoIncrement on the
		// PostgreSQL family, and it is the only spelling this renderer has
		// that does not change the declared type.
		if platform.IsPostgresFamily(c.dialect) {
			column.IdentityGeneration = "BY_DEFAULT"
		}
	}
	if column.Default, err = liquibaseColumnDefault(change); err != nil {
		return nil, declared, err
	}

	constraintsElements := change.childrenNamed("constraints")
	switch len(constraintsElements) {
	case 0:
		return column, declared, nil
	case 1:
		declared, err = liquibaseApplyConstraints(column, constraintsElements[0], allowedConstraints)
		if err != nil {
			return nil, declared, err
		}
		return column, declared, nil
	default:
		return nil, declared, fmt.Errorf("%s %s declares its constraints more than once", change.display, name)
	}
}

// liquibaseApplyConstraints reads a column's `constraints` element: the column
// properties go onto column, and what becomes a table constraint is returned.
func liquibaseApplyConstraints(
	column *ast.ColumnNode,
	constraints liquibaseChange,
	allowed []string,
) (liquibaseColumnConstraints, error) {
	var declared liquibaseColumnConstraints
	if err := constraints.only(allowed...); err != nil {
		return declared, err
	}
	if err := constraints.childrenOnly(); err != nil {
		return declared, err
	}
	nullable, set, err := constraints.optionalFlag("nullable")
	if err != nil {
		return declared, err
	}
	if set {
		column.Nullable = nullable
	}
	if declared.primaryKey, err = constraints.flag("primaryKey"); err != nil {
		return declared, err
	}
	if declared.primaryKey {
		column.Nullable = false
		declared.primaryKeyName = constraints.attrs["primaryKeyName"]
	}
	unique, err := constraints.flag("unique")
	if err != nil {
		return declared, err
	}
	switch {
	case unique && constraints.attrs["uniqueConstraintName"] != "":
		declared.uniqueName = constraints.attrs["uniqueConstraintName"]
	case unique:
		column.Unique = true
	}
	declared.foreignKey, err = liquibaseColumnForeignKey(constraints, column.Name)
	return declared, err
}

// liquibaseColumnType reduces a Liquibase column type to SQL.
//
// The type is kept as the author wrote it and the dialect renderer
// canonicalizes it, which is the path every declared type in Ptah takes.
// `java.sql.Types.VARCHAR(255)` is Liquibase's JDBC spelling of the same type
// and loses its prefix. A property reference is refused: its value is defined
// outside the changelog, and writing `${id_type}` into a migration is writing a
// statement that cannot run.
func liquibaseColumnType(declared string) (string, error) {
	columnType := strings.TrimSpace(declared)
	if strings.Contains(columnType, "${") {
		return "", fmt.Errorf("type %q is a property reference, which Ptah does not resolve", declared)
	}
	if rest, ok := strings.CutPrefix(columnType, "java.sql.Types."); ok {
		columnType = rest
	}
	if columnType == "" {
		return "", fmt.Errorf("type %q is empty", declared)
	}
	return columnType, nil
}

// liquibaseColumnDefault reads the one default a column may declare.
func liquibaseColumnDefault(change liquibaseChange) (*ast.DefaultValue, error) {
	var set []string
	for _, key := range []string{
		"defaultValue", "defaultValueNumeric", "defaultValueBoolean", "defaultValueDate", "defaultValueComputed",
	} {
		if _, ok := change.attrs[key]; ok {
			set = append(set, key)
		}
	}
	switch {
	case len(set) == 0:
		return nil, nil
	case len(set) > 1:
		return nil, fmt.Errorf("%s %s declares more than one default (%s)",
			change.display, change.attrs["name"], strings.Join(set, ", "))
	}
	value := change.attrs[set[0]]
	if strings.Contains(value, "${") {
		return nil, fmt.Errorf("%s %s: %s %q is a property reference, which Ptah does not resolve",
			change.display, change.attrs["name"], set[0], value)
	}
	switch set[0] {
	case "defaultValueComputed":
		return &ast.DefaultValue{Expression: value}, nil
	case "defaultValueBoolean":
		flag, err := liquibaseBool(change.display, set[0], value)
		if err != nil {
			return nil, err
		}
		return &ast.DefaultValue{Value: fmt.Sprint(flag), ValueSet: true}, nil
	default:
		return &ast.DefaultValue{Value: value, ValueSet: true}, nil
	}
}

// liquibaseColumnForeignKey reads a reference declared on a column.
//
// Liquibase requires foreignKeyName whenever a column references another table,
// so a reference without one is refused rather than given a name Ptah invented.
func liquibaseColumnForeignKey(constraints liquibaseChange, column string) (*ast.ConstraintNode, error) {
	name := constraints.attrs["foreignKeyName"]
	references := constraints.attrs["references"]
	table := constraints.attrs["referencedTableName"]
	if name == "" && references == "" && table == "" {
		return nil, nil
	}
	if name == "" {
		return nil, fmt.Errorf("%s on %s references another table without foreignKeyName", constraints.display, column)
	}
	reference := &ast.ForeignKeyRef{Name: name}
	switch {
	case references != "" && table != "":
		return nil, fmt.Errorf("%s on %s sets both references and referencedTableName", constraints.display, column)
	case references != "":
		open := strings.Index(references, "(")
		if open <= 0 || !strings.HasSuffix(references, ")") {
			return nil, fmt.Errorf("%s on %s: references %q is not table(column)", constraints.display, column, references)
		}
		reference.Table = strings.TrimSpace(references[:open])
		reference.Columns = liquibaseNames(references[open+1 : len(references)-1])
	default:
		reference.Table = liquibaseQualify(constraints.attrs["referencedTableSchemaName"], table)
		reference.Columns = liquibaseNames(constraints.attrs["referencedColumnNames"])
	}
	if len(reference.Columns) == 0 {
		return nil, fmt.Errorf("%s on %s names no referenced column", constraints.display, column)
	}
	cascade, err := constraints.flag("deleteCascade")
	if err != nil {
		return nil, err
	}
	if cascade {
		reference.OnDelete = "CASCADE"
	}
	return &ast.ConstraintNode{
		Type: ast.ForeignKeyConstraint, Name: name, Columns: []string{column}, Reference: reference,
	}, nil
}

// ----------------------------------------------------------------- indexes

func (c *liquibaseConverter) createIndex(change liquibaseChange) (liquibaseConverted, error) {
	if err := change.only("indexName", "tableName", "schemaName", "unique"); err != nil {
		return liquibaseConverted{}, err
	}
	if err := change.childrenOnly("column"); err != nil {
		return liquibaseConverted{}, err
	}
	name, err := change.required("indexName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	table, err := change.qualifiedTable("schemaName", "tableName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	unique, err := change.flag("unique")
	if err != nil {
		return liquibaseConverted{}, err
	}
	index := &ast.IndexNode{Name: name, Table: table, Unique: unique}
	for _, column := range change.childrenNamed("column") {
		if err := column.only("name", "descending"); err != nil {
			return liquibaseConverted{}, err
		}
		if err := column.childrenOnly(); err != nil {
			return liquibaseConverted{}, err
		}
		columnName, err := column.required("name")
		if err != nil {
			return liquibaseConverted{}, err
		}
		descending, err := column.flag("descending")
		if err != nil {
			return liquibaseConverted{}, err
		}
		index.Columns = append(index.Columns, columnName)
		index.Parts = append(index.Parts, ast.IndexPart{Name: columnName, Desc: descending})
	}
	if len(index.Columns) == 0 {
		return liquibaseConverted{}, fmt.Errorf("%s %s names no column", change.display, name)
	}
	up, err := c.render(index)
	if err != nil {
		return liquibaseConverted{}, err
	}
	down, err := c.render(&ast.DropIndexNode{Name: name, Table: table})
	if err != nil {
		return liquibaseConverted{}, err
	}
	return liquibaseConverted{up: up, down: down, reversible: true}, nil
}

func (c *liquibaseConverter) dropIndex(change liquibaseChange) (liquibaseConverted, error) {
	if err := change.only("indexName", "tableName", "schemaName"); err != nil {
		return liquibaseConverted{}, err
	}
	if err := change.childrenOnly(); err != nil {
		return liquibaseConverted{}, err
	}
	name, err := change.required("indexName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	// Liquibase lets tableName go unsaid on engines that name an index by
	// schema. The drop then carries the schema on the index, which is where a
	// statement naming no table keeps it.
	drop := &ast.DropIndexNode{Name: liquibaseQualify(change.attrs["schemaName"], name)}
	if table := change.attrs["tableName"]; table != "" {
		drop = &ast.DropIndexNode{Name: name, Table: liquibaseQualify(change.attrs["schemaName"], table)}
	}
	up, err := c.render(drop)
	return liquibaseConverted{up: up}, err
}

// ------------------------------------------------------------- constraints

func (c *liquibaseConverter) addPrimaryKey(change liquibaseChange) (liquibaseConverted, error) {
	if err := change.only("tableName", "schemaName", "columnNames", "constraintName"); err != nil {
		return liquibaseConverted{}, err
	}
	if err := change.childrenOnly(); err != nil {
		return liquibaseConverted{}, err
	}
	table, err := change.qualifiedTable("schemaName", "tableName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	columns := liquibaseNames(change.attrs["columnNames"])
	if len(columns) == 0 {
		return liquibaseConverted{}, fmt.Errorf("%s on %s names no column", change.display, table)
	}
	name := change.attrs["constraintName"]
	up, err := c.render(&ast.AlterTableNode{
		Name: table,
		Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{
			Type: ast.PrimaryKeyConstraint, Name: name, Columns: columns,
		}}},
	})
	if err != nil {
		return liquibaseConverted{}, err
	}
	// Without a name the key is named by the server, and the name a drop needs
	// is known only to the database; Liquibase looks it up when it rolls back,
	// and an import has no database to ask.
	if name == "" {
		return liquibaseConverted{up: up}, nil
	}
	down, err := c.render(&ast.AlterTableNode{
		Name:       table,
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{ConstraintName: name, PrimaryKey: true}},
	})
	if err != nil {
		return liquibaseConverted{}, err
	}
	return liquibaseConverted{up: up, down: down, reversible: true}, nil
}

// liquibaseReferentialActions are the actions Liquibase accepts for onDelete and
// onUpdate, spelled as SQL writes them.
var liquibaseReferentialActions = []string{"CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT", "NO ACTION"}

func (c *liquibaseConverter) addForeignKeyConstraint(change liquibaseChange) (liquibaseConverted, error) {
	if err := change.only(
		"constraintName", "baseTableName", "baseTableSchemaName", "baseColumnNames",
		"referencedTableName", "referencedTableSchemaName", "referencedColumnNames",
		"onDelete", "onUpdate", "deleteCascade",
	); err != nil {
		return liquibaseConverted{}, err
	}
	if err := change.childrenOnly(); err != nil {
		return liquibaseConverted{}, err
	}
	name, err := change.required("constraintName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	base, err := change.qualifiedTable("baseTableSchemaName", "baseTableName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	referenced, err := change.qualifiedTable("referencedTableSchemaName", "referencedTableName")
	if err != nil {
		return liquibaseConverted{}, err
	}
	baseColumns := liquibaseNames(change.attrs["baseColumnNames"])
	referencedColumns := liquibaseNames(change.attrs["referencedColumnNames"])
	if len(baseColumns) == 0 || len(baseColumns) != len(referencedColumns) {
		return liquibaseConverted{}, fmt.Errorf(
			"%s %s pairs %d base column(s) with %d referenced column(s)",
			change.display, name, len(baseColumns), len(referencedColumns))
	}
	onDelete, err := change.referentialAction("onDelete")
	if err != nil {
		return liquibaseConverted{}, err
	}
	cascade, err := change.flag("deleteCascade")
	if err != nil {
		return liquibaseConverted{}, err
	}
	if cascade {
		if onDelete != "" && onDelete != "CASCADE" {
			return liquibaseConverted{}, fmt.Errorf(
				"%s %s sets deleteCascade and onDelete %s", change.display, name, onDelete)
		}
		onDelete = "CASCADE"
	}
	onUpdate, err := change.referentialAction("onUpdate")
	if err != nil {
		return liquibaseConverted{}, err
	}
	up, err := c.render(&ast.AlterTableNode{
		Name: base,
		Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{
			Type: ast.ForeignKeyConstraint, Name: name, Columns: baseColumns,
			Reference: &ast.ForeignKeyRef{
				Name: name, Table: referenced, Columns: referencedColumns, OnDelete: onDelete, OnUpdate: onUpdate,
			},
		}}},
	})
	if err != nil {
		return liquibaseConverted{}, err
	}
	down, err := c.render(&ast.AlterTableNode{
		Name:       base,
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{ConstraintName: name, ForeignKey: true}},
	})
	if err != nil {
		return liquibaseConverted{}, err
	}
	return liquibaseConverted{up: up, down: down, reversible: true}, nil
}

// ------------------------------------------------------------------- render

// render writes one node for the target, refusing a render that left out
// something the node declared.
func (c *liquibaseConverter) render(node ast.Node) (string, error) {
	sql, omissions, err := renderer.RenderSQLReportingOmissions(c.dialect, c.caps, node)
	if err != nil {
		return "", err
	}
	if len(omissions) > 0 {
		lost := make([]string, 0, len(omissions))
		for _, omission := range omissions {
			entry := fmt.Sprintf("%s %s: %s", omission.Kind, omission.Name, omission.Message())
			if omission.Remedy != "" {
				entry += " (" + omission.Remedy + ")"
			}
			lost = append(lost, entry)
		}
		return "", fmt.Errorf("%s cannot carry the whole change: %s", c.dialect, strings.Join(lost, "; "))
	}
	return strings.TrimSpace(sql), nil
}

// ------------------------------------------------------------------ helpers

// only refuses the attributes a converter does not read, by name.
func (ch liquibaseChange) only(allowed ...string) error {
	var extra []string
	for _, key := range slices.Sorted(maps.Keys(ch.attrs)) {
		if !slices.Contains(allowed, key) {
			extra = append(extra, key)
		}
	}
	if len(extra) > 0 {
		return fmt.Errorf("%s sets %s, which Ptah does not convert", ch.display, strings.Join(extra, ", "))
	}
	return nil
}

// childrenOnly refuses nested elements a converter does not read, by name.
func (ch liquibaseChange) childrenOnly(allowed ...string) error {
	var extra []string
	for _, child := range ch.children {
		if !slices.Contains(allowed, child.name) && !slices.Contains(extra, child.display) {
			extra = append(extra, child.display)
		}
	}
	if len(extra) > 0 {
		return fmt.Errorf("%s contains %s, which Ptah does not convert", ch.display, strings.Join(extra, ", "))
	}
	return nil
}

func (ch liquibaseChange) childrenNamed(name string) []liquibaseChange {
	var named []liquibaseChange
	for _, child := range ch.children {
		if child.name == name {
			named = append(named, child)
		}
	}
	return named
}

func (ch liquibaseChange) required(key string) (string, error) {
	value := strings.TrimSpace(ch.attrs[key])
	if value == "" {
		return "", fmt.Errorf("%s has no %s", ch.display, key)
	}
	if strings.Contains(value, "${") {
		return "", fmt.Errorf("%s %s %q is a property reference, which Ptah does not resolve", ch.display, key, value)
	}
	return value, nil
}

// qualifiedTable reads a table name and the schema attribute beside it.
func (ch liquibaseChange) qualifiedTable(schemaKey, tableKey string) (string, error) {
	table, err := ch.required(tableKey)
	if err != nil {
		return "", err
	}
	schema := strings.TrimSpace(ch.attrs[schemaKey])
	if strings.Contains(schema, "${") {
		return "", fmt.Errorf("%s %s %q is a property reference, which Ptah does not resolve", ch.display, schemaKey, schema)
	}
	return liquibaseQualify(schema, table), nil
}

// flag reads a boolean attribute that defaults to false.
func (ch liquibaseChange) flag(key string) (bool, error) {
	value, _, err := ch.optionalFlag(key)
	return value, err
}

// optionalFlag reads a boolean attribute and whether it was written at all.
func (ch liquibaseChange) optionalFlag(key string) (value, set bool, err error) {
	text, ok := ch.attrs[key]
	if !ok {
		return false, false, nil
	}
	value, err = liquibaseBool(ch.display, key, text)
	return value, true, err
}

func (ch liquibaseChange) referentialAction(key string) (string, error) {
	value := strings.ToUpper(strings.Join(strings.Fields(ch.attrs[key]), " "))
	if value == "" || slices.Contains(liquibaseReferentialActions, value) {
		return value, nil
	}
	return "", fmt.Errorf("%s %s %q is not a referential action", ch.display, key, ch.attrs[key])
}

// liquibaseBool reads Liquibase's boolean spelling. Anything else is refused:
// an attribute that did not parse is an attribute nothing read.
func liquibaseBool(display, key, value string) (bool, error) {
	parsed, ok := liquibaserun.ParseBool(value)
	if !ok {
		return false, fmt.Errorf("%s %s %q is not true or false", display, key, value)
	}
	return parsed, nil
}

// liquibaseNames splits Liquibase's comma-separated column list.
func liquibaseNames(value string) []string {
	var names []string
	for part := range strings.SplitSeq(value, ",") {
		if name := strings.TrimSpace(part); name != "" {
			names = append(names, name)
		}
	}
	return names
}

func liquibaseQualify(schema, name string) string {
	if schema = strings.TrimSpace(schema); schema == "" {
		return name
	}
	return schema + "." + name
}
