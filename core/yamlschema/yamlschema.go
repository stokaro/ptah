// Package yamlschema parses language-agnostic YAML schema files into goschema.
package yamlschema

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/stokaro/ptah/core/goschema"
)

// ParseFile parses a YAML schema file into the same Database IR used by Go annotations.
func ParseFile(path string) (*goschema.Database, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schema file: %w", err)
	}

	return Parse(data)
}

// Parse parses a YAML schema document into the same Database IR used by Go annotations.
func Parse(data []byte) (*goschema.Database, error) {
	var doc document
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil {
		return nil, fmt.Errorf("parse YAML schema: %w", err)
	}
	var extraDoc document
	if err := decoder.Decode(&extraDoc); err == nil {
		return nil, fmt.Errorf("parse YAML schema: multiple YAML documents are not supported")
	} else if err != io.EOF {
		return nil, fmt.Errorf("parse YAML schema: %w", err)
	}

	return doc.toDatabase()
}

type document struct {
	Tables            map[string]tableSpec      `yaml:"tables"`
	Indexes           map[string]indexSpec      `yaml:"indexes"`
	Constraints       map[string]constraintSpec `yaml:"constraints"`
	Enums             map[string]enumSpec       `yaml:"enums"`
	Extensions        map[string]extensionSpec  `yaml:"extensions"`
	Functions         map[string]functionSpec   `yaml:"functions"`
	RLSPolicies       map[string]rlsPolicySpec  `yaml:"rls_policies"`
	RLSEnabledTables  map[string]rlsEnableSpec  `yaml:"rls_enabled_tables"`
	RLSEnabled        map[string]rlsEnableSpec  `yaml:"rls_enabled"`
	Roles             map[string]roleSpec       `yaml:"roles"`
	Grants            map[string]grantSpec      `yaml:"grants"`
	Views             map[string]viewSpec       `yaml:"views"`
	MaterializedViews map[string]matViewSpec    `yaml:"matviews"`
	Triggers          map[string]triggerSpec    `yaml:"triggers"`
}

type tableSpec struct {
	StructName  stringScalar               `yaml:"struct_name"`
	Name        stringScalar               `yaml:"name"`
	Engine      stringScalar               `yaml:"engine"`
	Comment     stringScalar               `yaml:"comment"`
	PrimaryKey  stringList                 `yaml:"primary_key"`
	Checks      stringList                 `yaml:"checks"`
	CustomSQL   stringScalar               `yaml:"custom_sql"`
	Columns     orderedMap[fieldSpec]      `yaml:"columns"`
	Fields      orderedMap[fieldSpec]      `yaml:"fields"`
	Indexes     orderedMap[indexSpec]      `yaml:"indexes"`
	Constraints orderedMap[constraintSpec] `yaml:"constraints"`
	RLSEnabled  bool                       `yaml:"rls_enabled"`
	Platform    platformSpec               `yaml:"platform"`
	Overrides   platformSpec               `yaml:"overrides"`
}

type fieldSpec struct {
	FieldName          stringScalar `yaml:"field_name"`
	Name               stringScalar `yaml:"name"`
	Type               stringScalar `yaml:"type"`
	Nullable           *bool        `yaml:"nullable"`
	NotNull            bool         `yaml:"not_null"`
	Primary            bool         `yaml:"primary"`
	AutoIncrement      bool         `yaml:"auto_increment"`
	AutoInc            bool         `yaml:"auto_inc"`
	IdentityGeneration stringScalar `yaml:"identity_generation"`
	IdentityStart      stringScalar `yaml:"identity_start"`
	IdentityIncrement  stringScalar `yaml:"identity_increment"`
	IdentityOptions    stringScalar `yaml:"identity_options"`
	Unique             bool         `yaml:"unique"`
	UniqueExpr         stringScalar `yaml:"unique_expr"`
	Index              bool         `yaml:"index"`
	Generated          stringScalar `yaml:"generated"`
	GeneratedKind      stringScalar `yaml:"generated_kind"`
	Stored             bool         `yaml:"stored"`
	Default            stringScalar `yaml:"default"`
	DefaultExpr        stringScalar `yaml:"default_expr"`
	Foreign            stringScalar `yaml:"foreign"`
	ForeignKeyName     stringScalar `yaml:"foreign_key_name"`
	OnDelete           stringScalar `yaml:"on_delete"`
	OnUpdate           stringScalar `yaml:"on_update"`
	Enum               stringList   `yaml:"enum"`
	Check              stringScalar `yaml:"check"`
	CheckName          stringScalar `yaml:"check_name"`
	Charset            stringScalar `yaml:"charset"`
	Collate            stringScalar `yaml:"collate"`
	Comment            stringScalar `yaml:"comment"`
	Platform           platformSpec `yaml:"platform"`
	Overrides          platformSpec `yaml:"overrides"`
}

type indexSpec struct {
	Name        stringScalar `yaml:"name"`
	Fields      stringList   `yaml:"fields"`
	Columns     stringList   `yaml:"columns"`
	Unique      bool         `yaml:"unique"`
	Comment     stringScalar `yaml:"comment"`
	Type        stringScalar `yaml:"type"`
	Condition   stringScalar `yaml:"condition"`
	Where       stringScalar `yaml:"where"`
	Operator    stringScalar `yaml:"ops"`
	TableName   stringScalar `yaml:"table"`
	Granularity int          `yaml:"granularity"`
}

type constraintSpec struct {
	Name            stringScalar `yaml:"name"`
	Type            stringScalar `yaml:"type"`
	Table           stringScalar `yaml:"table"`
	UsingMethod     stringScalar `yaml:"using"`
	ExcludeElements stringScalar `yaml:"elements"`
	WhereCondition  stringScalar `yaml:"condition"`
	CheckExpression stringScalar `yaml:"check"`
	Columns         stringList   `yaml:"columns"`
	ForeignTable    stringScalar `yaml:"foreign_table"`
	ForeignColumn   stringScalar `yaml:"foreign_column"`
	OnDelete        stringScalar `yaml:"on_delete"`
	OnUpdate        stringScalar `yaml:"on_update"`
	Comment         stringScalar `yaml:"comment"`
}

type enumSpec struct {
	Values stringList `yaml:"values"`
}

func (s *enumSpec) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.SequenceNode || value.Kind == yaml.ScalarNode {
		var values stringList
		if err := value.Decode(&values); err != nil {
			return err
		}
		s.Values = values
		return nil
	}

	type rawEnumSpec enumSpec
	var raw rawEnumSpec
	if err := decodeKnownFields(value, &raw); err != nil {
		return err
	}
	*s = enumSpec(raw)
	return nil
}

type extensionSpec struct {
	Name        stringScalar `yaml:"name"`
	IfNotExists bool         `yaml:"if_not_exists"`
	Version     stringScalar `yaml:"version"`
	Comment     stringScalar `yaml:"comment"`
}

type functionSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Name       stringScalar `yaml:"name"`
	Parameters stringScalar `yaml:"parameters"`
	Params     stringScalar `yaml:"params"`
	Returns    stringScalar `yaml:"returns"`
	Language   stringScalar `yaml:"language"`
	Security   stringScalar `yaml:"security"`
	Volatility stringScalar `yaml:"volatility"`
	Body       stringScalar `yaml:"body"`
	Comment    stringScalar `yaml:"comment"`
}

type viewSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Name       stringScalar `yaml:"name"`
	Body       stringScalar `yaml:"body"`
	WithCheck  bool         `yaml:"with_check"`
	Comment    stringScalar `yaml:"comment"`
}

type matViewSpec struct {
	StructName      stringScalar `yaml:"struct_name"`
	Name            stringScalar `yaml:"name"`
	Body            stringScalar `yaml:"body"`
	RefreshStrategy stringScalar `yaml:"refresh_strategy"`
	Comment         stringScalar `yaml:"comment"`
}

type triggerSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Name       stringScalar `yaml:"name"`
	Table      stringScalar `yaml:"table"`
	Timing     stringScalar `yaml:"timing"`
	Event      stringScalar `yaml:"event"`
	ForEach    stringScalar `yaml:"for"`
	Body       stringScalar `yaml:"body"`
	Comment    stringScalar `yaml:"comment"`
}

type rlsPolicySpec struct {
	StructName          stringScalar `yaml:"struct_name"`
	Name                stringScalar `yaml:"name"`
	Table               stringScalar `yaml:"table"`
	PolicyFor           stringScalar `yaml:"for"`
	ToRoles             stringScalar `yaml:"to"`
	UsingExpression     stringScalar `yaml:"using"`
	WithCheckExpression stringScalar `yaml:"with_check"`
	Comment             stringScalar `yaml:"comment"`
}

type rlsEnableSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Table      stringScalar `yaml:"table"`
	Comment    stringScalar `yaml:"comment"`
}

type roleSpec struct {
	StructName  stringScalar `yaml:"struct_name"`
	Name        stringScalar `yaml:"name"`
	Login       bool         `yaml:"login"`
	Password    stringScalar `yaml:"password"`
	Superuser   bool         `yaml:"superuser"`
	CreateDB    bool         `yaml:"create_db"`
	CreateRole  bool         `yaml:"create_role"`
	Inherit     *bool        `yaml:"inherit"`
	Replication bool         `yaml:"replication"`
	Comment     stringScalar `yaml:"comment"`
}

type grantSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Role       stringScalar `yaml:"role"`
	Privilege  stringList   `yaml:"privilege"`
	Privileges stringList   `yaml:"privileges"`
	OnTable    stringScalar `yaml:"on_table"`
	OnSchema   stringScalar `yaml:"on_schema"`
	WithOption bool         `yaml:"with_option"`
	Comment    stringScalar `yaml:"comment"`
}

type platformSpec map[string]map[string]stringScalar

func (d document) toDatabase() (*goschema.Database, error) {
	db := &goschema.Database{
		Dependencies:               make(map[string][]string),
		FunctionDependencies:       make(map[string][]string),
		SelfReferencingForeignKeys: make(map[string][]goschema.SelfReferencingFK),
	}

	d.addEnums(db)
	if err := d.addTables(db); err != nil {
		return nil, err
	}
	if err := d.addIndexes(db); err != nil {
		return nil, err
	}
	if err := d.addConstraints(db); err != nil {
		return nil, err
	}
	d.addExtensions(db)
	d.addFunctions(db)
	if err := d.addViews(db); err != nil {
		return nil, err
	}
	if err := d.addMaterializedViews(db); err != nil {
		return nil, err
	}
	if err := d.addTriggers(db); err != nil {
		return nil, err
	}
	d.addRLS(db)
	d.addRoles(db)
	d.addGrants(db)

	goschema.Finalize(db)
	return db, nil
}

func (d document) addEnums(db *goschema.Database) {
	for _, name := range sortedKeys(d.Enums) {
		db.Enums = append(db.Enums, goschema.Enum{
			Name:   name,
			Values: cleanStrings(d.Enums[name].Values),
		})
	}
}

func (d document) addTables(db *goschema.Database) error {
	for _, tableKey := range sortedKeys(d.Tables) {
		table := d.Tables[tableKey]
		structName := valueOrDefault(table.StructName, tableKey)
		tableName := valueOrDefault(table.Name, tableKey)

		db.Tables = append(db.Tables, goschema.Table{
			StructName: structName,
			Name:       tableName,
			Engine:     string(table.Engine),
			Comment:    string(table.Comment),
			PrimaryKey: cleanStrings(table.PrimaryKey),
			Checks:     cleanStrings(table.Checks),
			CustomSQL:  string(table.CustomSQL),
			Overrides:  mergePlatform(table.Platform, table.Overrides),
		})

		if err := addFields(db, structName, table.Columns, table.Fields); err != nil {
			return err
		}
		if err := addTableIndexes(db, structName, table.Indexes); err != nil {
			return err
		}
		if err := addTableConstraints(db, structName, tableName, table.Constraints); err != nil {
			return err
		}
		if table.RLSEnabled {
			db.RLSEnabledTables = append(db.RLSEnabledTables, goschema.RLSEnabledTable{
				StructName: structName,
				Table:      tableName,
			})
		}
	}

	return nil
}

func addFields(db *goschema.Database, structName string, columns, fields orderedMap[fieldSpec]) error {
	seen := make(map[string]bool)
	for _, column := range columns {
		seen[column.Name] = true
		field, err := buildField(structName, column.Name, column.Value, db)
		if err != nil {
			return err
		}
		db.Fields = append(db.Fields, field)
	}

	for _, field := range fields {
		if seen[field.Name] {
			return fmt.Errorf("duplicate column %q in columns and fields", field.Name)
		}
		parsedField, err := buildField(structName, field.Name, field.Value, db)
		if err != nil {
			return err
		}
		db.Fields = append(db.Fields, parsedField)
	}

	return nil
}

func buildField(structName, key string, spec fieldSpec, db *goschema.Database) (goschema.Field, error) {
	fieldName := valueOrDefault(spec.FieldName, key)
	columnName := valueOrDefault(spec.Name, key)
	fieldType := string(spec.Type)
	enumValues := cleanStrings(spec.Enum)
	if len(enumValues) > 0 && (fieldType == "" || fieldType == "ENUM") {
		enumName := "enum_" + strings.ToLower(structName) + "_" + strings.ToLower(fieldName)
		db.Enums = append(db.Enums, goschema.Enum{Name: enumName, Values: enumValues})
		fieldType = enumName
	}

	nullable := true
	if spec.Nullable != nil {
		nullable = *spec.Nullable
	}
	if spec.NotNull {
		nullable = false
	}
	identityGeneration := normalizeIdentityGeneration(string(spec.IdentityGeneration))
	if spec.IdentityGeneration != "" && identityGeneration == "" {
		return goschema.Field{}, fmt.Errorf("column %q has unsupported identity_generation %q", key, spec.IdentityGeneration)
	}
	if identityGeneration == "" && hasIdentitySettings(spec) {
		identityGeneration = "BY_DEFAULT"
	}

	return goschema.Field{
		StructName:          structName,
		FieldName:           fieldName,
		Name:                columnName,
		Type:                fieldType,
		Nullable:            nullable,
		Primary:             spec.Primary,
		AutoInc:             spec.AutoIncrement || spec.AutoInc || identityGeneration != "",
		IdentityGeneration:  identityGeneration,
		IdentityStart:       string(spec.IdentityStart),
		IdentityIncrement:   string(spec.IdentityIncrement),
		IdentityOptions:     string(spec.IdentityOptions),
		Unique:              spec.Unique,
		UniqueExpr:          string(spec.UniqueExpr),
		Default:             string(spec.Default),
		DefaultExpr:         string(spec.DefaultExpr),
		Foreign:             string(spec.Foreign),
		ForeignKeyName:      string(spec.ForeignKeyName),
		OnDelete:            string(spec.OnDelete),
		OnUpdate:            string(spec.OnUpdate),
		Enum:                enumValues,
		Check:               string(spec.Check),
		CheckName:           string(spec.CheckName),
		GeneratedExpression: string(spec.Generated),
		GeneratedKind:       yamlGeneratedColumnKind(spec),
		Charset:             string(spec.Charset),
		Collate:             string(spec.Collate),
		Comment:             string(spec.Comment),
		Overrides:           mergePlatform(spec.Platform, spec.Overrides),
	}, nil
}

func hasIdentitySettings(spec fieldSpec) bool {
	return spec.IdentityStart != "" || spec.IdentityIncrement != "" || spec.IdentityOptions != ""
}

func yamlGeneratedColumnKind(spec fieldSpec) string {
	if strings.TrimSpace(string(spec.Generated)) == "" {
		return ""
	}
	if kind := strings.TrimSpace(string(spec.GeneratedKind)); kind != "" {
		return strings.ToUpper(kind)
	}
	if spec.Stored {
		return "STORED"
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func normalizeIdentityGeneration(value string) string {
	switch strings.ToUpper(strings.ReplaceAll(value, " ", "_")) {
	case "ALWAYS":
		return "ALWAYS"
	case "BY_DEFAULT":
		return "BY_DEFAULT"
	default:
		return ""
	}
}

func addTableIndexes(db *goschema.Database, structName string, indexes orderedMap[indexSpec]) error {
	for _, index := range indexes {
		value, err := buildIndex(index.Name, structName, index.Value)
		if err != nil {
			return err
		}
		db.Indexes = append(db.Indexes, value)
	}
	return nil
}

func (d document) addIndexes(db *goschema.Database) error {
	for _, key := range sortedKeys(d.Indexes) {
		index, err := buildIndex(key, "", d.Indexes[key])
		if err != nil {
			return err
		}
		db.Indexes = append(db.Indexes, index)
	}
	return nil
}

func buildIndex(key, structName string, spec indexSpec) (goschema.Index, error) {
	fields := cleanStrings(spec.Fields)
	if len(fields) == 0 {
		fields = cleanStrings(spec.Columns)
	}
	if len(fields) == 0 {
		return goschema.Index{}, fmt.Errorf("index %q requires fields", key)
	}
	if structName == "" && string(spec.TableName) == "" {
		return goschema.Index{}, fmt.Errorf("top-level index %q requires table", key)
	}

	return goschema.Index{
		StructName:  structName,
		Name:        valueOrDefault(spec.Name, key),
		Fields:      fields,
		Unique:      spec.Unique,
		Comment:     string(spec.Comment),
		Type:        string(spec.Type),
		Condition:   firstNonEmpty(string(spec.Where), string(spec.Condition)),
		Operator:    string(spec.Operator),
		TableName:   string(spec.TableName),
		Granularity: spec.Granularity,
	}, nil
}

func addTableConstraints(db *goschema.Database, structName, tableName string, constraints orderedMap[constraintSpec]) error {
	for _, constraint := range constraints {
		value, err := buildConstraint(constraint.Name, structName, tableName, constraint.Value)
		if err != nil {
			return err
		}
		db.Constraints = append(db.Constraints, value)
	}
	return nil
}

func (d document) addConstraints(db *goschema.Database) error {
	for _, key := range sortedKeys(d.Constraints) {
		constraint, err := buildConstraint(key, "", "", d.Constraints[key])
		if err != nil {
			return err
		}
		db.Constraints = append(db.Constraints, constraint)
	}
	return nil
}

func buildConstraint(key, structName, tableName string, spec constraintSpec) (goschema.Constraint, error) {
	constraintTable := string(spec.Table)
	if constraintTable == "" && structName == "" {
		constraintTable = tableName
	}
	constraintType := strings.ToUpper(string(spec.Type))
	columns := cleanStrings(spec.Columns)
	if err := validateConstraint(key, structName, constraintTable, constraintType, columns, spec); err != nil {
		return goschema.Constraint{}, err
	}

	return goschema.Constraint{
		StructName:      structName,
		Name:            valueOrDefault(spec.Name, key),
		Type:            constraintType,
		Table:           constraintTable,
		UsingMethod:     string(spec.UsingMethod),
		ExcludeElements: string(spec.ExcludeElements),
		WhereCondition:  string(spec.WhereCondition),
		CheckExpression: string(spec.CheckExpression),
		Columns:         columns,
		ForeignTable:    string(spec.ForeignTable),
		ForeignColumn:   string(spec.ForeignColumn),
		OnDelete:        string(spec.OnDelete),
		OnUpdate:        string(spec.OnUpdate),
		Comment:         string(spec.Comment),
	}, nil
}

func validateConstraint(key, structName, tableName, constraintType string, columns []string, spec constraintSpec) error {
	if structName == "" && tableName == "" {
		return fmt.Errorf("top-level constraint %q requires table", key)
	}

	switch constraintType {
	case "PRIMARY KEY", "UNIQUE":
		if len(columns) == 0 {
			return fmt.Errorf("constraint %q requires columns", key)
		}
	case "FOREIGN KEY":
		if len(columns) == 0 {
			return fmt.Errorf("constraint %q requires columns", key)
		}
		if spec.ForeignTable == "" || spec.ForeignColumn == "" {
			return fmt.Errorf("constraint %q requires foreign_table and foreign_column", key)
		}
	case "CHECK":
		if spec.CheckExpression == "" {
			return fmt.Errorf("constraint %q requires check", key)
		}
	case "EXCLUDE":
		if spec.UsingMethod == "" || spec.ExcludeElements == "" {
			return fmt.Errorf("constraint %q requires using and elements", key)
		}
	case "":
		return fmt.Errorf("constraint %q requires type", key)
	default:
		return fmt.Errorf("constraint %q has unsupported type %q", key, constraintType)
	}
	return nil
}

func (d document) addExtensions(db *goschema.Database) {
	for _, key := range sortedKeys(d.Extensions) {
		spec := d.Extensions[key]
		db.Extensions = append(db.Extensions, goschema.Extension{
			Name:        valueOrDefault(spec.Name, key),
			IfNotExists: spec.IfNotExists,
			Version:     string(spec.Version),
			Comment:     string(spec.Comment),
		})
	}
}

func (d document) addFunctions(db *goschema.Database) {
	for _, key := range sortedKeys(d.Functions) {
		spec := d.Functions[key]
		parameters := string(spec.Parameters)
		if parameters == "" {
			parameters = string(spec.Params)
		}

		fn := goschema.Function{
			StructName: string(spec.StructName),
			Name:       valueOrDefault(spec.Name, key),
			Parameters: parameters,
			Returns:    string(spec.Returns),
			Language:   string(spec.Language),
			Security:   string(spec.Security),
			Volatility: string(spec.Volatility),
			Body:       string(spec.Body),
			Comment:    string(spec.Comment),
		}
		fn.Canonicalize()
		db.Functions = append(db.Functions, fn)
	}
}

func (d document) addViews(db *goschema.Database) error {
	for _, key := range sortedKeys(d.Views) {
		spec := d.Views[key]
		if string(spec.Body) == "" {
			return fmt.Errorf("view %q requires body", key)
		}
		db.Views = append(db.Views, goschema.View{
			StructName: string(spec.StructName),
			Name:       valueOrDefault(spec.Name, key),
			Body:       string(spec.Body),
			WithCheck:  spec.WithCheck,
			Comment:    string(spec.Comment),
		})
	}
	return nil
}

func (d document) addMaterializedViews(db *goschema.Database) error {
	for _, key := range sortedKeys(d.MaterializedViews) {
		spec := d.MaterializedViews[key]
		if string(spec.Body) == "" {
			return fmt.Errorf("materialized view %q requires body", key)
		}
		view := goschema.MaterializedView{
			StructName:      string(spec.StructName),
			Name:            valueOrDefault(spec.Name, key),
			Body:            string(spec.Body),
			RefreshStrategy: string(spec.RefreshStrategy),
			Comment:         string(spec.Comment),
		}
		view.Canonicalize()
		db.MaterializedViews = append(db.MaterializedViews, view)
	}
	return nil
}

func (d document) addTriggers(db *goschema.Database) error {
	for _, key := range sortedKeys(d.Triggers) {
		spec := d.Triggers[key]
		if string(spec.Table) == "" {
			return fmt.Errorf("trigger %q requires table", key)
		}
		if string(spec.Timing) == "" {
			return fmt.Errorf("trigger %q requires timing", key)
		}
		if string(spec.Event) == "" {
			return fmt.Errorf("trigger %q requires event", key)
		}
		if string(spec.Body) == "" {
			return fmt.Errorf("trigger %q requires body", key)
		}
		trigger := goschema.Trigger{
			StructName: string(spec.StructName),
			Name:       valueOrDefault(spec.Name, key),
			Table:      string(spec.Table),
			Timing:     string(spec.Timing),
			Event:      string(spec.Event),
			ForEach:    string(spec.ForEach),
			Body:       string(spec.Body),
			Comment:    string(spec.Comment),
		}
		trigger.Canonicalize()
		db.Triggers = append(db.Triggers, trigger)
	}
	return nil
}

func (d document) addRLS(db *goschema.Database) {
	for _, key := range sortedKeys(d.RLSEnabledTables) {
		db.RLSEnabledTables = append(db.RLSEnabledTables, buildRLSEnabledTable(key, d.RLSEnabledTables[key]))
	}
	for _, key := range sortedKeys(d.RLSEnabled) {
		db.RLSEnabledTables = append(db.RLSEnabledTables, buildRLSEnabledTable(key, d.RLSEnabled[key]))
	}

	for _, key := range sortedKeys(d.RLSPolicies) {
		spec := d.RLSPolicies[key]
		db.RLSPolicies = append(db.RLSPolicies, goschema.RLSPolicy{
			StructName:          string(spec.StructName),
			Name:                valueOrDefault(spec.Name, key),
			Table:               string(spec.Table),
			PolicyFor:           string(spec.PolicyFor),
			ToRoles:             string(spec.ToRoles),
			UsingExpression:     string(spec.UsingExpression),
			WithCheckExpression: string(spec.WithCheckExpression),
			Comment:             string(spec.Comment),
		})
	}
}

func buildRLSEnabledTable(key string, spec rlsEnableSpec) goschema.RLSEnabledTable {
	return goschema.RLSEnabledTable{
		StructName: string(spec.StructName),
		Table:      valueOrDefault(spec.Table, key),
		Comment:    string(spec.Comment),
	}
}

func (d document) addRoles(db *goschema.Database) {
	for _, key := range sortedKeys(d.Roles) {
		spec := d.Roles[key]
		inherit := true
		if spec.Inherit != nil {
			inherit = *spec.Inherit
		}
		db.Roles = append(db.Roles, goschema.Role{
			StructName:  string(spec.StructName),
			Name:        valueOrDefault(spec.Name, key),
			Login:       spec.Login,
			Password:    string(spec.Password),
			Superuser:   spec.Superuser,
			CreateDB:    spec.CreateDB,
			CreateRole:  spec.CreateRole,
			Inherit:     inherit,
			Replication: spec.Replication,
			Comment:     string(spec.Comment),
		})
	}
}

func (d document) addGrants(db *goschema.Database) {
	for _, key := range sortedKeys(d.Grants) {
		spec := d.Grants[key]
		privileges := cleanStrings(spec.Privilege)
		if len(privileges) == 0 {
			privileges = cleanStrings(spec.Privileges)
		}
		grant := goschema.Grant{
			StructName: string(spec.StructName),
			Role:       string(spec.Role),
			Privileges: privileges,
			OnTable:    string(spec.OnTable),
			OnSchema:   string(spec.OnSchema),
			WithOption: spec.WithOption,
			Comment:    string(spec.Comment),
		}
		grant.Canonicalize()
		db.Grants = append(db.Grants, grant)
	}
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func valueOrDefault(value stringScalar, fallback string) string {
	if string(value) != "" {
		return string(value)
	}
	return fallback
}

func cleanStrings(values stringList) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func mergePlatform(primary, secondary platformSpec) map[string]map[string]string {
	if len(primary) == 0 && len(secondary) == 0 {
		return nil
	}

	result := make(map[string]map[string]string)
	copyPlatform(result, primary)
	copyPlatform(result, secondary)
	return result
}

func copyPlatform(target map[string]map[string]string, source platformSpec) {
	for platform, values := range source {
		if target[platform] == nil {
			target[platform] = make(map[string]string)
		}
		for key, value := range values {
			target[platform][key] = string(value)
		}
	}
}

type stringScalar string

func (s *stringScalar) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("expected scalar, got %s", value.ShortTag())
	}
	if value.Tag == "!!null" {
		*s = ""
		return nil
	}
	*s = stringScalar(value.Value)
	return nil
}

type stringList []string

func (s *stringList) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.SequenceNode:
		values := make([]string, 0, len(value.Content))
		for _, item := range value.Content {
			var scalar stringScalar
			if err := item.Decode(&scalar); err != nil {
				return err
			}
			values = append(values, string(scalar))
		}
		*s = values
	case yaml.ScalarNode:
		if value.Tag == "!!null" || value.Value == "" {
			*s = nil
			return nil
		}
		*s = strings.Split(value.Value, ",")
	default:
		return fmt.Errorf("expected scalar or sequence, got %s", value.ShortTag())
	}
	return nil
}

type orderedMap[V any] []orderedEntry[V]

type orderedEntry[V any] struct {
	Name  string
	Value V
}

func (m *orderedMap[V]) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode && value.Tag == "!!null" {
		*m = nil
		return nil
	}
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("expected mapping, got %s", value.ShortTag())
	}

	entries := make([]orderedEntry[V], 0, len(value.Content)/2)
	seen := make(map[string]bool, len(value.Content)/2)
	for i := 0; i < len(value.Content); i += 2 {
		keyNode := value.Content[i]
		valueNode := value.Content[i+1]

		if seen[keyNode.Value] {
			return fmt.Errorf("duplicate key %q", keyNode.Value)
		}
		seen[keyNode.Value] = true

		var entryValue V
		if err := decodeKnownFields(valueNode, &entryValue); err != nil {
			return err
		}
		entries = append(entries, orderedEntry[V]{
			Name:  keyNode.Value,
			Value: entryValue,
		})
	}

	*m = entries
	return nil
}

func decodeKnownFields[V any](node *yaml.Node, target *V) error {
	var buffer bytes.Buffer
	encoder := yaml.NewEncoder(&buffer)
	if err := encoder.Encode(node); err != nil {
		return err
	}
	if err := encoder.Close(); err != nil {
		return err
	}

	decoder := yaml.NewDecoder(&buffer)
	decoder.KnownFields(true)
	return decoder.Decode(target)
}
