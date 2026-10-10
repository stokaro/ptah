// Package yamlschema reads a desired schema written in Ptah's YAML format.
//
// # One of the authoring formats
//
// A desired schema reaches Ptah as a *schemamodel.Database, and YAML is one of the
// formats that produce it. Go annotations, YAML, HCL, SQL, and DBML each have a
// reader of their own, each returns the same model, and everything downstream —
// rendering, diffing, planning, linting, migration generation — sees only that
// model and cannot tell which reader filled it. A schema authored in YAML is
// therefore as complete an input as one authored in Go: it is a peer of the
// other four, not a convenience wrapper over one of them.
//
// This package is the YAML reader. Parse takes the document as bytes, ParseFile
// reads it from a path; both return the model:
//
//	db, err := yamlschema.ParseFile(runtime.YAML(), "schema.yaml")
//	if err != nil {
//		return err
//	}
//	statements, err := renderer.GetOrderedCreateStatements(db, "postgres")
//
// ptah/core/schemasource covers the other direction, running an external
// program that writes YAML, HCL, or SQL to its standard output. It parses that
// output through this package. Reach for it when the schema is produced by
// another tool; reach for this package when the YAML is already in hand.
//
// # What the document holds
//
// The top level is a set of object collections, each keyed by name: tables,
// indexes, constraints, enums, extensions, functions, rls_policies,
// rls_enabled_tables (also accepted as rls_enabled), roles, grants, revokes,
// default_privileges, views, matviews and triggers. A selected feature owner
// adds keys of its own, such as YDB's topics and secrets and ClickHouse's
// row_policies, and keys to a table or an index entry, such as a YDB table's
// changefeeds; a key no selected owner reads is unknown. A table carries
// its columns in declaration order, along with its primary key, checks, engine,
// comment, and per-platform overrides. A column carries the type, its
// nullability, key and uniqueness flags, defaults, generated and identity
// expressions, a foreign key with its referential actions, character set and
// collation, and its own per-platform overrides. Tables and columns can also
// declare shared and per-target API names; columns additionally carry API-only
// type and exposure metadata.
//
// A grant or revoke names its target with on_table, on_schema, on_sequence,
// on_database, or on_function and on_procedure, whose value carries the
// argument types: purge(uuid). A revokes entry says the role must not hold the privileges,
// and a privilege both granted and revoked to one role on one object is
// refused.
//
// A default_privileges key is a label rather than a default name, because a
// default privilege has no name. The role whose new objects it covers, the
// schema, the object type and the grantee identify it. An entry without a
// schema is the global default, ALTER DEFAULT PRIVILEGES without IN SCHEMA.
//
// # Strictness
//
// Parsing is strict in two ways that a permissive YAML reader is not. An
// unknown key is an error rather than a silent drop, so a misspelled attribute
// cannot pass as an intentional setting. A second YAML document in the same
// stream is refused rather than ignored, so a schema split across a `---`
// separator cannot half-apply.
//
// Errors are returned, never printed, and a decoding failure carries the parse
// position the YAML decoder reported.
package yamlschema

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlext"
	"ptah.run/internal/dialectscope"
	"ptah.run/internal/matviewrefresh"
	"ptah.run/internal/routineargs"
	"ptah.run/internal/routinesetting"
	"ptah.run/internal/yamlvalue"
)

// ParseFile reads a YAML schema file and parses it with Parse, returning the
// same *schemamodel.Database that Go annotations, HCL, SQL, and DBML produce.
// A read failure is wrapped but keeps the underlying filesystem error, so
// errors.Is(err, fs.ErrNotExist) still answers for a missing file and
// errors.As reaches the *fs.PathError naming the path.
//
// owners selects the feature owners whose models the document can declare, as
// it does for Parse.
func ParseFile(owners yamlext.Set, path string) (*schemamodel.Database, error) {
	if !owners.Selected() {
		return nil, fmt.Errorf("parse YAML schema: %w", yamlext.ErrUnselected)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schema file: %w", err)
	}

	return Parse(owners, data)
}

// Parse parses a YAML schema document into the same *schemamodel.Database
// that Go annotations, HCL, SQL, and DBML produce.
//
// The input is exactly one YAML document. An unknown key is an error rather
// than a silent drop, and a second document after a `---` separator is
// refused. A document that decodes but declares an incomplete object — an
// index without fields, a top-level constraint without a table, a duplicate
// column — is refused as well. Every refusal is returned as an error naming
// the document position or the object key it concerns, so the author can find
// it; none of the message text is contract.
//
// The model does not depend on YAML map order: the same bytes always produce
// the same model, whatever order the decoder walked the maps in. Within a
// table, columns keep the order they were declared in, because column order
// is part of the schema the author wrote. Tables and functions come back
// ordered by their dependencies.
//
// owners selects the feature owners whose models the document can declare,
// usually the YAML set of the runtime the caller renders and compares with.
// The document claims knowledge of an owner's models only when the parse
// selects the owner; otherwise they stay unknown rather than absent. The zero
// set is refused with [yamlext.ErrUnselected]; [yamlext.None] selects no
// owner on purpose.
func Parse(owners yamlext.Set, data []byte) (*schemamodel.Database, error) {
	if !owners.Selected() {
		return nil, fmt.Errorf("parse YAML schema: %w", yamlext.ErrUnselected)
	}
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
	if err := checkOwnedKeys(owners, data, doc.Owned); err != nil {
		return nil, err
	}

	return doc.toDatabase(owners, data)
}

type document struct {
	Tables            map[string]tableSpec            `yaml:"tables"`
	Indexes           map[string]indexSpec            `yaml:"indexes"`
	Constraints       map[string]constraintSpec       `yaml:"constraints"`
	Enums             map[string]enumSpec             `yaml:"enums"`
	Extensions        map[string]extensionSpec        `yaml:"extensions"`
	Functions         map[string]functionSpec         `yaml:"functions"`
	RLSPolicies       map[string]rlsPolicySpec        `yaml:"rls_policies"`
	RLSEnabledTables  map[string]rlsEnableSpec        `yaml:"rls_enabled_tables"`
	RLSEnabled        map[string]rlsEnableSpec        `yaml:"rls_enabled"`
	Roles             map[string]roleSpec             `yaml:"roles"`
	Grants            map[string]grantSpec            `yaml:"grants"`
	Revokes           map[string]revokeSpec           `yaml:"revokes"`
	DefaultPrivileges map[string]defaultPrivilegeSpec `yaml:"default_privileges"`
	Views             map[string]viewSpec             `yaml:"views"`
	MaterializedViews map[string]matViewSpec          `yaml:"matviews"`
	Triggers          map[string]triggerSpec          `yaml:"triggers"`

	// Owned holds the top-level keys the frontend does not read itself. A
	// selected owner's section is handed to the owner; any other key is
	// refused as unknown.
	Owned map[string]yaml.Node `yaml:",inline"`
}

type tableSpec struct {
	Schema      stringScalar               `yaml:"schema"`
	StructName  stringScalar               `yaml:"struct_name"`
	Name        stringScalar               `yaml:"name"`
	APIName     stringScalar               `yaml:"api_name"`
	OpenAPIName stringScalar               `yaml:"openapi_name"`
	GraphQLName stringScalar               `yaml:"graphql_name"`
	ProtoName   stringScalar               `yaml:"proto_name"`
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

	Platform  platformSpec `yaml:"platform"`
	Overrides platformSpec `yaml:"overrides"`

	// Owned holds the keys the frontend does not read itself: the ones a
	// selected owner adds to a table, such as a YDB table's changefeeds, and
	// anything else, which is refused as unknown.
	Owned map[string]yaml.Node `yaml:",inline"`
}

type fieldSpec struct {
	FieldName          stringScalar `yaml:"field_name"`
	Name               stringScalar `yaml:"name"`
	APIName            stringScalar `yaml:"api_name"`
	OpenAPIName        stringScalar `yaml:"openapi_name"`
	GraphQLName        stringScalar `yaml:"graphql_name"`
	ProtoName          stringScalar `yaml:"proto_name"`
	APIType            stringScalar `yaml:"api_type"`
	APIExpose          stringScalar `yaml:"api_expose"`
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
	Collate            stringScalar `yaml:"collate"`
	Comment            stringScalar `yaml:"comment"`
	Platform           platformSpec `yaml:"platform"`
	Overrides          platformSpec `yaml:"overrides"`
}

type indexSpec struct {
	Name      stringScalar `yaml:"name"`
	Fields    stringList   `yaml:"fields"`
	Columns   stringList   `yaml:"columns"`
	Include   stringList   `yaml:"include"`
	Unique    bool         `yaml:"unique"`
	Comment   stringScalar `yaml:"comment"`
	Type      stringScalar `yaml:"type"`
	Condition stringScalar `yaml:"condition"`
	Where     stringScalar `yaml:"where"`
	Operator  stringScalar `yaml:"ops"`
	TableName stringScalar `yaml:"table"`
	Platform  platformSpec `yaml:"platform"`

	// Owned holds the keys the frontend does not read itself: the ones a
	// selected owner adds to an index, such as a YDB index's partitioning,
	// and anything else, which is refused as unknown.
	Owned map[string]yaml.Node `yaml:",inline"`
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
	Values  stringList   `yaml:"values"`
	Comment stringScalar `yaml:"comment"`
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
	Schema      stringScalar `yaml:"schema"`
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
	// Leakproof, Parallel and Strict are the planner attributes; see
	// [schemamodel.Function].
	Leakproof bool         `yaml:"leakproof"`
	Parallel  stringScalar `yaml:"parallel"`
	Strict    bool         `yaml:"strict"`
	// Settings are the routine's own configuration settings, each `name=value`.
	Settings []stringScalar `yaml:"settings"`
	Body     stringScalar   `yaml:"body"`
	Comment  stringScalar   `yaml:"comment"`
}

type viewSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Name       stringScalar `yaml:"name"`
	Body       stringScalar `yaml:"body"`
	WithCheck  bool         `yaml:"with_check"`
	Comment    stringScalar `yaml:"comment"`
}

type matViewSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Name       stringScalar `yaml:"name"`
	Body       stringScalar `yaml:"body"`
	// RefreshStrategy is retired and kept only so a document that declares it
	// is refused by name. Deleting the field would make the strict decoder
	// answer `field refresh_strategy not found`, which reads as a typo and says
	// nothing about why a correctly spelled key stopped working
	// (stokaro/ptah#1625). It is a yaml.Node so that `refresh_strategy:` with
	// no value, and an empty string, are refused as well as a real value.
	RefreshStrategy yaml.Node    `yaml:"refresh_strategy"`
	Comment         stringScalar `yaml:"comment"`
}

type triggerSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Name       stringScalar `yaml:"name"`
	Table      stringScalar `yaml:"table"`
	Timing     stringScalar `yaml:"timing"`
	Event      stringScalar `yaml:"event"`
	ForEach    stringScalar `yaml:"for"`
	When       stringScalar `yaml:"when"`
	OldTable   stringScalar `yaml:"old_table"`
	NewTable   stringScalar `yaml:"new_table"`
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
	// As is PERMISSIVE or RESTRICTIVE, as the annotation's `as` is.
	As      stringScalar `yaml:"as"`
	Comment stringScalar `yaml:"comment"`
	// Dialects scopes the policy, as it scopes a default privilege. A scope
	// naming ClickHouse is refused, and one naming only targets no selected
	// owner reads keeps it a shared declaration.
	Dialects yaml.Node `yaml:"dialects"`
}

type rlsEnableSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Table      stringScalar `yaml:"table"`
	Comment    stringScalar `yaml:"comment"`
	Dialects   yaml.Node    `yaml:"dialects"`
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
	// Group declares a YDB group, and MemberOf the groups the role is a
	// member of; see [schemamodel.Role].
	Group    bool       `yaml:"group"`
	MemberOf stringList `yaml:"member_of"`
}

type grantSpec struct {
	StructName stringScalar `yaml:"struct_name"`
	Role       stringScalar `yaml:"role"`
	Privilege  stringList   `yaml:"privilege"`
	Privileges stringList   `yaml:"privileges"`
	OnTable    stringScalar `yaml:"on_table"`
	OnSchema   stringScalar `yaml:"on_schema"`
	OnSequence stringScalar `yaml:"on_sequence"`
	// OnFunction and OnProcedure name a routine with its argument types,
	// `purge(uuid)`, as the Go annotations do: PostgreSQL overloads a name by
	// them.
	OnFunction  stringScalar `yaml:"on_function"`
	OnProcedure stringScalar `yaml:"on_procedure"`
	OnDatabase  bool         `yaml:"on_database"`
	WithOption  bool         `yaml:"with_option"`
	Comment     stringScalar `yaml:"comment"`
}

// revokeSpec is one entry under `revokes`: privileges a role is declared not
// to hold on an object, the YAML spelling of a schema file's REVOKE. It names
// its target as a grant does and has no grant option.
type revokeSpec struct {
	StructName  stringScalar `yaml:"struct_name"`
	Role        stringScalar `yaml:"role"`
	Privilege   stringList   `yaml:"privilege"`
	Privileges  stringList   `yaml:"privileges"`
	OnTable     stringScalar `yaml:"on_table"`
	OnSchema    stringScalar `yaml:"on_schema"`
	OnSequence  stringScalar `yaml:"on_sequence"`
	OnFunction  stringScalar `yaml:"on_function"`
	OnProcedure stringScalar `yaml:"on_procedure"`
	OnDatabase  bool         `yaml:"on_database"`
	Comment     stringScalar `yaml:"comment"`
}

// defaultPrivilegeSpec is one entry under `default_privileges`: the privileges
// a grantee receives on objects a named role creates, in a named schema or,
// without `schema`, in every schema of the database.
//
// The keys are the attribute names //ptah:schema:defaultprivilege accepts, so
// one declaration reads the same in both authoring formats.
//
// `for_role` belongs to the object's identity rather than to its payload.
// PostgreSQL refuses ALTER DEFAULT PRIVILEGES from a non-member of that role,
// and two entries differing only in it are two objects, so an entry without it
// cannot name what it declares.
//
// `grantable` names the subset of `privileges` that carries WITH GRANT OPTION.
// The `with_option` bool the grant sibling carries cannot say what the catalog
// records: pg_default_acl explodes to one row per privilege, each with its own
// is_grantable, so one identity granted SELECT plainly and INSERT WITH GRANT
// OPTION reads back as two rows that disagree.
//
// There is no `struct_name` key. Nothing resolves a default privilege against a
// host struct -- the four names below are its whole identity -- so the key
// would take a value and decide nothing with it.
type defaultPrivilegeSpec struct {
	ForRole    stringScalar `yaml:"for_role"`
	Schema     stringScalar `yaml:"schema"`
	ObjectType stringScalar `yaml:"object_type"`
	Grantee    stringScalar `yaml:"grantee"`
	Privileges stringList   `yaml:"privileges"`
	Grantable  stringList   `yaml:"grantable"`
	// Revoked are privileges the grantee must not hold by default: what a SQL
	// schema file writes as ALTER DEFAULT PRIVILEGES ... REVOKE.
	Revoked stringList   `yaml:"revoked"`
	Comment stringScalar `yaml:"comment"`
	// Dialects is a yaml.Node so that a key nobody wrote and a key written with
	// nothing in it stay two answers. A stringList folds them into one: the
	// decoder reports an empty list for both, and the empty scope that
	// [dialectscope.Parse] refuses would be read as every dialect instead --
	// silently widening the declaration whose author wrote the key to narrow it.
	Dialects yaml.Node `yaml:"dialects"`
}

type platformSpec map[string]map[string]stringScalar

func (d document) toDatabase(owners yamlext.Set, data []byte) (*schemamodel.Database, error) {
	db := &schemamodel.Database{
		Dependencies:               make(map[string][]string),
		FunctionDependencies:       make(map[string][]string),
		SelfReferencingForeignKeys: make(map[string][]schemamodel.SelfReferencingFK),
	}

	d.addEnums(db)
	if err := d.addTables(db, owners, data); err != nil {
		return nil, err
	}
	if err := d.addIndexes(db, owners); err != nil {
		return nil, err
	}
	if err := d.addConstraints(db); err != nil {
		return nil, err
	}
	d.addExtensions(db)
	if err := d.addFunctions(db); err != nil {
		return nil, err
	}
	if err := d.addViews(db); err != nil {
		return nil, err
	}
	if err := d.addMaterializedViews(db); err != nil {
		return nil, err
	}
	if err := d.addTriggers(db); err != nil {
		return nil, err
	}
	if err := d.addRLS(db, owners); err != nil {
		return nil, err
	}
	if err := d.addOwnerSections(db, owners, data); err != nil {
		return nil, err
	}
	// Once every owner has declared what it reads, each narrows the claim
	// by what the document declares of its models.
	coverage, err := owners.Cover(db.FeatureCoverage, db.FeatureObjects)
	if err != nil {
		return nil, err
	}
	db.FeatureCoverage = coverage
	d.addRoles(db)
	if err := d.addGrants(db); err != nil {
		return nil, err
	}
	if err := d.addRevokes(db); err != nil {
		return nil, err
	}
	if err := d.addDefaultPrivileges(db); err != nil {
		return nil, err
	}

	schemamodel.Finalize(db)
	// After Finalize, which merges the entries of one identity and resolves the
	// table a grant or revoke names: a document has no statement order, so a
	// privilege one entry grants and another revokes cannot be resolved.
	if err := schemamodel.ValidateRevokedGrants(db); err != nil {
		return nil, fmt.Errorf("parse YAML schema: %w", err)
	}
	return db, nil
}

func (d document) addEnums(db *schemamodel.Database) {
	for _, name := range sortedKeys(d.Enums) {
		db.Enums = append(db.Enums, schemamodel.Enum{
			Name:    name,
			Values:  cleanStrings(d.Enums[name].Values),
			Comment: strings.TrimSpace(string(d.Enums[name].Comment)),
		})
	}
}

func (d document) addTables(db *schemamodel.Database, owners yamlext.Set, data []byte) error {
	// The selected owners claim what a YAML document can declare of their
	// models, such as CockroachDB row-level TTL and the Spanner row deletion
	// policy in a table's cockroachdb and spanner platform groups, so a table
	// without one requests none.
	claims, err := owners.Coverage()
	if err != nil {
		return err
	}
	db.FeatureCoverage = claims

	for _, tableKey := range sortedKeys(d.Tables) {
		table := d.Tables[tableKey]
		structName := valueOrDefault(table.StructName, tableKey)
		tableName := valueOrDefault(table.Name, tableKey)

		scalars, sections, err := ownedKeys(owners, yamlext.EntryTable, fmt.Sprintf("table %q", tableKey), table.Owned)
		if err != nil {
			return err
		}
		facets, err := owners.DecodeEntryAttributes(yamlext.EntryTable, scalars)
		if err != nil {
			return fmt.Errorf("table %q: %w", tableKey, err)
		}
		db.Tables = append(db.Tables, schemamodel.Table{
			StructName: structName,
			Name:       tableName,
			Schema:     string(table.Schema),
			APIName:    string(table.APIName),
			APINames: schemamodel.TargetNames{
				OpenAPI:  string(table.OpenAPIName),
				GraphQL:  string(table.GraphQLName),
				Protobuf: string(table.ProtoName),
			},
			Engine:     string(table.Engine),
			Comment:    string(table.Comment),
			PrimaryKey: cleanStrings(table.PrimaryKey),
			Checks:     cleanStrings(table.Checks),
			CustomSQL:  string(table.CustomSQL),
			Overrides:  mergePlatform(table.Platform, table.Overrides),

			Facets: facets,
		})
		owned := yamlext.Table{Key: tableKey, Schema: string(table.Schema), Name: tableName, Struct: structName}
		if err := addTableSections(db, owners, data, owned, sections); err != nil {
			return err
		}

		if err := addFields(db, structName, table.Columns, table.Fields); err != nil {
			return err
		}
		if err := addTableIndexes(db, owners, structName, table.Indexes); err != nil {
			return err
		}
		if err := addTableConstraints(db, structName, tableName, table.Constraints); err != nil {
			return err
		}
	}

	return nil
}

func addFields(db *schemamodel.Database, structName string, columns, fields orderedMap[fieldSpec]) error {
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

func buildField(structName, key string, spec fieldSpec, db *schemamodel.Database) (schemamodel.Field, error) {
	fieldName := valueOrDefault(spec.FieldName, key)
	columnName := valueOrDefault(spec.Name, key)
	fieldType := string(spec.Type)
	enumValues := cleanStrings(spec.Enum)
	if len(enumValues) > 0 && (fieldType == "" || fieldType == "ENUM") {
		enumName := "enum_" + strings.ToLower(structName) + "_" + strings.ToLower(fieldName)
		db.Enums = append(db.Enums, schemamodel.Enum{Name: enumName, Values: enumValues})
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
		return schemamodel.Field{}, fmt.Errorf("column %q has unsupported identity_generation %q", key, spec.IdentityGeneration)
	}
	if identityGeneration == "" && hasIdentitySettings(spec) {
		identityGeneration = "BY_DEFAULT"
	}

	return schemamodel.Field{
		StructName: structName,
		FieldName:  fieldName,
		Name:       columnName,
		APIName:    string(spec.APIName),
		APINames: schemamodel.TargetNames{
			OpenAPI:  string(spec.OpenAPIName),
			GraphQL:  string(spec.GraphQLName),
			Protobuf: string(spec.ProtoName),
		},
		APIType:             string(spec.APIType),
		APIExpose:           string(spec.APIExpose),
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

func addTableIndexes(db *schemamodel.Database, owners yamlext.Set, structName string, indexes orderedMap[indexSpec]) error {
	for _, index := range indexes {
		value, err := buildIndex(owners, index.Name, structName, index.Value)
		if err != nil {
			return err
		}
		db.Indexes = append(db.Indexes, value)
	}
	return nil
}

func (d document) addIndexes(db *schemamodel.Database, owners yamlext.Set) error {
	for _, key := range sortedKeys(d.Indexes) {
		index, err := buildIndex(owners, key, "", d.Indexes[key])
		if err != nil {
			return err
		}
		db.Indexes = append(db.Indexes, index)
	}
	return nil
}

func buildIndex(owners yamlext.Set, key, structName string, spec indexSpec) (schemamodel.Index, error) {
	fields := cleanStrings(spec.Fields)
	if len(fields) == 0 {
		fields = cleanStrings(spec.Columns)
	}
	if len(fields) == 0 {
		return schemamodel.Index{}, fmt.Errorf("index %q requires fields", key)
	}
	if structName == "" && string(spec.TableName) == "" {
		return schemamodel.Index{}, fmt.Errorf("top-level index %q requires table", key)
	}
	var include []string
	if len(spec.Include) > 0 {
		include = cleanStrings(spec.Include)
		if len(include) != len(spec.Include) {
			return schemamodel.Index{}, fmt.Errorf("index %q names an empty include column", key)
		}
	}
	// The keys owners add to an index, such as YDB's partitioning and
	// full-text options, are read by their owners into settings and options
	// of the index; an owner reads the index's type and operator class too.
	scalars, _, err := ownedKeys(owners, yamlext.EntryIndex, fmt.Sprintf("index %q", key), spec.Owned)
	if err != nil {
		return schemamodel.Index{}, err
	}
	scalars["type"] = string(spec.Type)
	if spec.Operator != "" {
		scalars["ops"] = string(spec.Operator)
	}
	facets, err := owners.DecodeEntryAttributes(yamlext.EntryIndex, scalars)
	if err != nil {
		return schemamodel.Index{}, fmt.Errorf("index %q: %w", key, err)
	}
	options, err := owners.DecodeEntryParameters(yamlext.EntryIndex, scalars)
	if err != nil {
		return schemamodel.Index{}, fmt.Errorf("index %q: %w", key, err)
	}

	return schemamodel.Index{
		Facets:         facets,
		StructName:     structName,
		Name:           valueOrDefault(spec.Name, key),
		Fields:         fields,
		IncludeColumns: include,
		Unique:         spec.Unique,
		Comment:        string(spec.Comment),
		Type:           string(spec.Type),
		Condition:      firstNonEmpty(string(spec.Where), string(spec.Condition)),
		Operator:       string(spec.Operator),
		TableName:      string(spec.TableName),
		Overrides:      mergePlatform(spec.Platform, nil),
		StorageParams:  options,
	}, nil
}

func addTableConstraints(db *schemamodel.Database, structName, tableName string, constraints orderedMap[constraintSpec]) error {
	for _, constraint := range constraints {
		value, err := buildConstraint(constraint.Name, structName, tableName, constraint.Value)
		if err != nil {
			return err
		}
		db.Constraints = append(db.Constraints, value)
	}
	return nil
}

func (d document) addConstraints(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.Constraints) {
		constraint, err := buildConstraint(key, "", "", d.Constraints[key])
		if err != nil {
			return err
		}
		db.Constraints = append(db.Constraints, constraint)
	}
	return nil
}

func buildConstraint(key, structName, tableName string, spec constraintSpec) (schemamodel.Constraint, error) {
	constraintTable := string(spec.Table)
	if constraintTable == "" && structName == "" {
		constraintTable = tableName
	}
	constraintType := strings.ToUpper(string(spec.Type))
	columns := cleanStrings(spec.Columns)
	if err := validateConstraint(key, structName, constraintTable, constraintType, columns, spec); err != nil {
		return schemamodel.Constraint{}, err
	}

	return schemamodel.Constraint{
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

func (d document) addExtensions(db *schemamodel.Database) {
	for _, key := range sortedKeys(d.Extensions) {
		spec := d.Extensions[key]
		db.Extensions = append(db.Extensions, schemamodel.Extension{
			Name:        valueOrDefault(spec.Name, key),
			Schema:      string(spec.Schema),
			IfNotExists: spec.IfNotExists,
			Version:     string(spec.Version),
			Comment:     string(spec.Comment),
		})
	}
}

func (d document) addFunctions(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.Functions) {
		spec := d.Functions[key]
		parameters := string(spec.Parameters)
		if parameters == "" {
			parameters = string(spec.Params)
		}
		// An unrecognized level is refused rather than read as the default,
		// for the reason the HCL reader gives: UNSAFE is the most
		// restrictive, so a misspelled SAFE would forbid what was asked for.
		parallel := strings.ToUpper(strings.TrimSpace(string(spec.Parallel)))
		if parallel != "" && parallel != "SAFE" && parallel != "RESTRICTED" && parallel != "UNSAFE" {
			return fmt.Errorf("function %q: parallel must be SAFE, RESTRICTED or UNSAFE, not %q", key, string(spec.Parallel))
		}
		settings := make([]string, 0, len(spec.Settings))
		for _, setting := range spec.Settings {
			settings = append(settings, string(setting))
		}

		fn := schemamodel.Function{
			StructName: string(spec.StructName),
			Name:       valueOrDefault(spec.Name, key),
			Parameters: parameters,
			Returns:    string(spec.Returns),
			Language:   string(spec.Language),
			Security:   string(spec.Security),
			Volatility: string(spec.Volatility),
			// The settings key was accepted and never read, so a YAML
			// document's search_path reached nothing (stokaro/ptah#3630).
			Settings:  routinesetting.NormalizeAll(settings),
			Leakproof: spec.Leakproof,
			Parallel:  parallel,
			Strict:    spec.Strict,
			Body:      string(spec.Body),
			Comment:   string(spec.Comment),
		}
		fn.Canonicalize()
		db.Functions = append(db.Functions, fn)
	}
	return nil
}

func (d document) addViews(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.Views) {
		spec := d.Views[key]
		if string(spec.Body) == "" {
			return fmt.Errorf("view %q requires body", key)
		}
		db.Views = append(db.Views, schemamodel.View{
			StructName: string(spec.StructName),
			Name:       valueOrDefault(spec.Name, key),
			Body:       string(spec.Body),
			WithCheck:  spec.WithCheck,
			Comment:    string(spec.Comment),
		})
	}
	return nil
}

// addCoordinationNodes reads the YDB coordination nodes, in key order.
func (d document) addMaterializedViews(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.MaterializedViews) {
		spec := d.MaterializedViews[key]
		if string(spec.Body) == "" {
			return fmt.Errorf("materialized view %q requires body", key)
		}
		if spec.RefreshStrategy.Kind != 0 {
			return matviewrefresh.Refuse(valueOrDefault(spec.Name, key))
		}
		db.MaterializedViews = append(db.MaterializedViews, schemamodel.MaterializedView{
			StructName: string(spec.StructName),
			Name:       valueOrDefault(spec.Name, key),
			Body:       string(spec.Body),
			Comment:    string(spec.Comment),
		})
	}
	return nil
}

func (d document) addTriggers(db *schemamodel.Database) error {
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
		trigger := schemamodel.Trigger{
			StructName: string(spec.StructName),
			Name:       valueOrDefault(spec.Name, key),
			Table:      string(spec.Table),
			Timing:     string(spec.Timing),
			Event:      string(spec.Event),
			ForEach:    string(spec.ForEach),
			When:       string(spec.When),
			OldTable:   string(spec.OldTable),
			NewTable:   string(spec.NewTable),
			Body:       string(spec.Body),
			Comment:    string(spec.Comment),
		}
		trigger.Canonicalize()
		db.Triggers = append(db.Triggers, trigger)
	}
	return nil
}

// addRLS hands the document's row-level security to its owners. An entry a
// selected owner reads by its target scope goes to that owner: without
// `dialects`, or scoped to the PostgreSQL family, it is the row-security
// owner's, a policy becoming an object and an enablement the switches facet
// of its table, which the document must declare. An entry scoped to
// ClickHouse is refused: a ClickHouse row policy is its owner's, declared
// under the owner's row_policies key, and ClickHouse has no switch. An entry
// scoped to other targets stays shared. [yamlext.Set.TargetOwner] is the one
// place that routes an entry by its scope.
func (d document) addRLS(db *schemamodel.Database, owners yamlext.Set) error {
	var entries []yamlext.Entry
	for _, key := range sortedKeys(d.Tables) {
		if d.Tables[key].RLSEnabled {
			entries = append(entries, yamlext.Entry{Key: rlsEnabledTablesKey, Origin: fmt.Sprintf("tables.%s.rls_enabled", key),
				Attributes: map[string]string{"struct_name": valueOrDefault(d.Tables[key].StructName, key)}})
		}
	}
	for _, group := range []struct {
		name  string
		specs map[string]rlsEnableSpec
	}{{"rls_enabled_tables", d.RLSEnabledTables}, {"rls_enabled", d.RLSEnabled}} {
		for _, key := range sortedKeys(group.specs) {
			spec := group.specs[key]
			origin := fmt.Sprintf("%s.%s", group.name, key)
			attributes := presentAttributes(map[string]string{"struct_name": string(spec.StructName),
				"table": valueOrDefault(spec.Table, key), "comment": string(spec.Comment)})
			entry, owned, err := routeRowSecurity(owners, rlsEnabledTablesKey, origin, spec.Dialects, attributes)
			if err != nil {
				return err
			}
			if owned {
				entries = append(entries, entry)
				continue
			}
			if scopesClickHouse(entry.Targets) {
				return fmt.Errorf("%s: %w: ClickHouse has no row-level security switch: a row policy filters rows once it exists; "+
					"leave clickhouse out of the enablement's dialects", origin, ptaherr.ErrInvalidAttributeValue)
			}
			db.RLSEnabledTables = append(db.RLSEnabledTables, schemamodel.RLSEnabledTable{StructName: string(spec.StructName),
				Table: valueOrDefault(spec.Table, key), Comment: string(spec.Comment), Dialects: entry.Targets})
		}
	}
	for _, key := range sortedKeys(d.RLSPolicies) {
		spec := d.RLSPolicies[key]
		origin := "rls_policies." + key
		restrictive, err := policyRestrictive(origin, string(spec.As))
		if err != nil {
			return err
		}
		attributes := presentAttributes(map[string]string{"struct_name": string(spec.StructName), "name": valueOrDefault(spec.Name, key),
			"table": string(spec.Table), "for": string(spec.PolicyFor), "to": string(spec.ToRoles), "as": string(spec.As),
			"using": string(spec.UsingExpression), "with_check": string(spec.WithCheckExpression), "comment": string(spec.Comment)})
		entry, owned, err := routeRowSecurity(owners, rlsPoliciesKey, origin, spec.Dialects, attributes)
		if err != nil {
			return err
		}
		if owned {
			entries = append(entries, entry)
			continue
		}
		if scopesClickHouse(entry.Targets) {
			return fmt.Errorf("%s: %w: a ClickHouse row policy is not a row-level security policy; declare it under "+
				"row_policies instead", origin, ptaherr.ErrInvalidAttributeValue)
		}
		db.RLSPolicies = append(db.RLSPolicies, schemamodel.RLSPolicy{
			StructName: string(spec.StructName), Name: valueOrDefault(spec.Name, key), Table: string(spec.Table),
			PolicyFor: string(spec.PolicyFor), ToRoles: string(spec.ToRoles), UsingExpression: string(spec.UsingExpression),
			WithCheckExpression: string(spec.WithCheckExpression), Comment: string(spec.Comment), Restrictive: restrictive,
			Dialects: entry.Targets,
		})
	}
	contributions, err := owners.ReadEntries(entries, d.documentTables(db))
	if err != nil {
		return err
	}
	for _, contribution := range contributions {
		if err := contribute(db, "row-level security", contribution); err != nil {
			return err
		}
	}
	return nil
}

// policyRestrictive reads a policy's `as`, which decides how the policy
// combines with the table's others. An unrecognized value is refused rather
// than read as the default: PERMISSIVE is the weaker of the two, so folding a
// misspelled RESTRICTIVE into it would grant the access the policy was
// written to withhold, as the annotation refuses it (stokaro/ptah#3121).
func policyRestrictive(origin, written string) (bool, error) {
	switch strings.ToUpper(strings.TrimSpace(written)) {
	case "", "PERMISSIVE":
		return false, nil
	case "RESTRICTIVE":
		return true, nil
	default:
		return false, fmt.Errorf("%s: %w: as must be PERMISSIVE or RESTRICTIVE, got %q", origin, ptaherr.ErrInvalidAttributeValue, written)
	}
}

// The frontend keys whose row-level security entries an owner may read by
// their target scope. rls_enabled and a table's rls_enabled route as
// rls_enabled_tables.
const (
	rlsPoliciesKey      = "rls_policies"
	rlsEnabledTablesKey = "rls_enabled_tables"
)

// routeRowSecurity reads a row-level security entry's `dialects` and asks
// the owners whether one reads the entry. It returns the entry as an owner
// reads it, with the scope it was written with either way.
func routeRowSecurity(owners yamlext.Set, key, origin string, dialects yaml.Node, attributes map[string]string) (yamlext.Entry, bool, error) {
	scope, err := rowSecurityScope(origin, dialects)
	if err != nil {
		return yamlext.Entry{}, false, err
	}
	entry := yamlext.Entry{Key: key, Origin: origin, Attributes: attributes, Targets: scope}
	_, owned, err := owners.TargetOwner(key, scope)
	if err != nil {
		return yamlext.Entry{}, false, fmt.Errorf("%s: %w", origin, err)
	}
	return entry, owned, nil
}

// presentAttributes drops the attributes an entry leaves empty, so an owner
// reads a field the entry leaves out as absent, as it does from a Go
// annotation.
func presentAttributes(attributes map[string]string) map[string]string {
	for name, value := range attributes {
		if value == "" {
			delete(attributes, name)
		}
	}
	return attributes
}

// scopesClickHouse reports whether a row-level security entry's scope names
// ClickHouse, whose row policy is its owner's and not row-level security.
func scopesClickHouse(scope []string) bool {
	return slices.ContainsFunc(scope, func(target string) bool { return platform.NormalizeDialect(target) == platform.ClickHouse })
}

// rowSecurityScope reads a row-level security entry's `dialects`. An entry
// without the key names no target.
func rowSecurityScope(origin string, node yaml.Node) ([]string, error) {
	if node.Kind == 0 {
		return nil, nil
	}
	var written stringList
	if err := node.Decode(&written); err != nil {
		return nil, fmt.Errorf("%s has an invalid dialects value: %w", origin, err)
	}
	scope, err := dialectscope.Parse(strings.Join(cleanStrings(written), ","))
	if err != nil {
		return nil, fmt.Errorf("%s dialects: %w", origin, err)
	}
	return scope, nil
}

func (d document) addRoles(db *schemamodel.Database) {
	for _, key := range sortedKeys(d.Roles) {
		spec := d.Roles[key]
		inherit := true
		if spec.Inherit != nil {
			inherit = *spec.Inherit
		}
		// Nil for none, as the annotation parser leaves it.
		var memberOf []string
		if groups := cleanStrings(spec.MemberOf); len(groups) > 0 {
			memberOf = groups
		}
		db.Roles = append(db.Roles, schemamodel.Role{
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
			Group:       spec.Group,
			MemberOf:    memberOf,
		})
	}
}

func (d document) addGrants(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.Grants) {
		spec := d.Grants[key]
		grant, err := buildGrant("grant", key, revokeSpec{
			StructName: spec.StructName, Role: spec.Role, Privilege: spec.Privilege, Privileges: spec.Privileges,
			OnTable: spec.OnTable, OnSchema: spec.OnSchema, OnSequence: spec.OnSequence,
			OnFunction: spec.OnFunction, OnProcedure: spec.OnProcedure, OnDatabase: spec.OnDatabase,
			Comment: spec.Comment,
		})
		if err != nil {
			return err
		}
		grant.WithOption = spec.WithOption
		db.Grants = append(db.Grants, grant)
	}
	return nil
}

// addRevokes reads the `revokes` entries into the model's revoked grants.
func (d document) addRevokes(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.Revokes) {
		revoked, err := buildGrant("revoke", key, d.Revokes[key])
		if err != nil {
			return err
		}
		if revoked.Role == "" || len(revoked.Privileges) == 0 {
			return fmt.Errorf("revoke %q requires role and privileges", key)
		}
		db.RevokedGrants = append(db.RevokedGrants, revoked)
	}
	return nil
}

// buildGrant reads the part a grant and a revoke share: the role, the
// privileges and the target.
func buildGrant(kind, key string, spec revokeSpec) (schemamodel.Grant, error) {
	privileges := cleanStrings(spec.Privilege)
	if len(privileges) == 0 {
		privileges = cleanStrings(spec.Privileges)
	}
	grant := schemamodel.Grant{
		StructName: string(spec.StructName),
		Role:       string(spec.Role),
		Privileges: privileges,
		OnTable:    string(spec.OnTable),
		OnSchema:   string(spec.OnSchema),
		OnSequence: string(spec.OnSequence),
		OnDatabase: spec.OnDatabase,
		Comment:    string(spec.Comment),
	}
	for _, routine := range []struct {
		attribute, kind, value string
	}{
		{"on_function", "FUNCTION", string(spec.OnFunction)},
		{"on_procedure", "PROCEDURE", string(spec.OnProcedure)},
	} {
		if strings.TrimSpace(routine.value) == "" {
			continue
		}
		name, arguments, ok := routineargs.SplitTarget(routine.value)
		if !ok {
			return schemamodel.Grant{}, fmt.Errorf(
				"%s %q %s %q needs the argument types in parentheses, as in purge(uuid): "+
					"PostgreSQL tells overloaded routines apart by them", kind, key, routine.attribute, routine.value)
		}
		grant.OnRoutine, grant.RoutineArguments, grant.RoutineKind = name, arguments, routine.kind
	}
	grant.Canonicalize()
	return grant, nil
}

// addDefaultPrivileges reads the `default_privileges` entries into the model.
//
// The keys are walked through [sortedKeys] rather than by ranging over the map,
// for the reason every other top-level collection is: a range takes Go's
// randomized map order, so the same document would produce models whose
// default privileges are in a different order on each run. That contradicts
// what [Parse] promises, and it surfaces as a comparison that flakes rather
// than as a test that fails.
func (d document) addDefaultPrivileges(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.DefaultPrivileges) {
		privilege, err := buildDefaultPrivilege(key, d.DefaultPrivileges[key])
		if err != nil {
			return err
		}
		db.DefaultPrivileges = append(db.DefaultPrivileges, privilege)
	}
	return nil
}

// defaultPrivilegeObjectTypes is the set of object classes an entry may name.
// SCHEMAS and LARGE OBJECTS need an entry without a schema, which
// [schemamodel.ValidateRevokedGrants] checks for every source.
var defaultPrivilegeObjectTypes = []string{"TABLES", "SEQUENCES", "FUNCTIONS", "TYPES", "SCHEMAS", "LARGE OBJECTS"}

// buildDefaultPrivilege turns one entry into a model object, refusing an entry
// that does not say what it declares.
//
// key names the entry in every refusal. It is a label rather than a default
// name, because the four identity components have no name to fall back to.
//
// The object type is checked here and not left to the renderer. A misspelled
// keyword otherwise reaches the server as a syntax error in a statement the
// author did not write, and the comparison never converges either: the reader
// reports one of the four keywords back, so nothing the server refused can
// match what the document declared.
func buildDefaultPrivilege(key string, spec defaultPrivilegeSpec) (schemamodel.DefaultPrivilege, error) {
	required := []struct {
		attribute string
		value     string
	}{
		{attribute: "for_role", value: string(spec.ForRole)},
		{attribute: "object_type", value: string(spec.ObjectType)},
		{attribute: "grantee", value: string(spec.Grantee)},
	}
	for _, attribute := range required {
		if strings.TrimSpace(attribute.value) == "" {
			return schemamodel.DefaultPrivilege{}, fmt.Errorf("default privilege %q requires %s", key, attribute.attribute)
		}
	}

	objectType := strings.ToUpper(strings.TrimSpace(string(spec.ObjectType)))
	if !slices.Contains(defaultPrivilegeObjectTypes, objectType) {
		return schemamodel.DefaultPrivilege{}, fmt.Errorf(
			"default privilege %q has unsupported object_type %q, expected one of %s",
			key, string(spec.ObjectType), strings.Join(defaultPrivilegeObjectTypes, ", "),
		)
	}

	privileges, err := defaultPrivilegeGrants(key, spec)
	if err != nil {
		return schemamodel.DefaultPrivilege{}, err
	}
	scope, err := defaultPrivilegeScope(key, spec.Dialects)
	if err != nil {
		return schemamodel.DefaultPrivilege{}, err
	}

	privilege := schemamodel.DefaultPrivilege{
		Grantor:    string(spec.ForRole),
		Schema:     string(spec.Schema),
		ObjectType: objectType,
		Grantee:    string(spec.Grantee),
		Privileges: privileges,
		Revoked:    cleanStrings(spec.Revoked),
		Comment:    string(spec.Comment),
		Dialects:   scope,
	}
	privilege.Canonicalize()
	return privilege, nil
}

// defaultPrivilegeGrants folds `privileges` and `grantable` into the one list
// of pairs the model holds, keeping the order the entry wrote.
//
// A `grantable` name outside `privileges` is refused rather than granted on its
// own. Granting it would render a privilege the author did not ask for; keeping
// it as a flag on nothing would put a contradiction in the model, and a
// contradiction renders no statement and compares as a difference no plan can
// resolve.
func defaultPrivilegeGrants(key string, spec defaultPrivilegeSpec) ([]schemamodel.PrivilegeGrant, error) {
	privileges := cleanStrings(spec.Privileges)
	if len(privileges) == 0 && len(cleanStrings(spec.Revoked)) == 0 {
		return nil, fmt.Errorf("default privilege %q requires privileges or revoked", key)
	}

	granted := make(map[string]bool, len(privileges))
	for _, name := range privileges {
		granted[strings.ToUpper(name)] = true
	}
	grantable := make(map[string]bool, len(spec.Grantable))
	for _, name := range cleanStrings(spec.Grantable) {
		normalized := strings.ToUpper(name)
		if !granted[normalized] {
			return nil, fmt.Errorf("default privilege %q marks %q grantable, which is not in privileges", key, name)
		}
		grantable[normalized] = true
	}

	grants := make([]schemamodel.PrivilegeGrant, 0, len(privileges))
	for _, name := range privileges {
		grants = append(grants, schemamodel.PrivilegeGrant{
			Privilege:  name,
			WithOption: grantable[strings.ToUpper(name)],
		})
	}
	return grants, nil
}

// defaultPrivilegeScope resolves the `dialects` key of one entry.
//
// A zero Kind is the key nobody wrote, and a declaration carrying no scope
// reaches every dialect. Everything written goes to [dialectscope.Parse],
// which resolves each alias to the canonical dialect name and refuses a value
// naming none.
func defaultPrivilegeScope(key string, node yaml.Node) ([]string, error) {
	if node.Kind == 0 {
		return nil, nil
	}

	var written stringList
	if err := node.Decode(&written); err != nil {
		return nil, fmt.Errorf("default privilege %q has an invalid dialects value: %w", key, err)
	}
	scope, err := dialectscope.Parse(strings.Join(cleanStrings(written), ","))
	if err != nil {
		return nil, fmt.Errorf("default privilege %q dialects: %w", key, err)
	}
	return scope, nil
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
	for group, values := range source {
		if target[group] == nil {
			target[group] = make(map[string]string)
		}
		for key, value := range values {
			target[group][key] = string(value)
		}
	}
}

// The value types the frontend reads its keys with, shared with the owners
// that read keys of their own.
type (
	stringScalar      = yamlvalue.Scalar
	stringList        = yamlvalue.List
	orderedMap[V any] = yamlvalue.OrderedMap[V]
)

func decodeKnownFields[V any](node *yaml.Node, target *V) error {
	return yamlvalue.DecodeKnownFields(node, target)
}
